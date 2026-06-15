{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Join where

import Control.Applicative ((<|>))
import Control.Exception (throw)
import Control.Monad (when)
import Control.Monad.ST (ST, runST)
import Data.Bits ((.&.))
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.STRef (newSTRef, readSTRef, writeSTRef)
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..))
import qualified Data.Vector as VB
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors (
    DataFrameException (ColumnsNotFoundException),
 )
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D
import DataFrame.Internal.ParRadixSort (parSortByHash)
import DataFrame.Operations.Aggregation as D
import DataFrame.Operations.Core as D
import DataFrame.Operations.JoinPar (
    parInnerProbe,
    parLeftProbe,
    shouldParallelizeJoin,
    shouldParallelizeSmallBuildProbe,
 )
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection

-- | Equivalent to SQL join types.
data JoinType
    = INNER
    | LEFT
    | RIGHT
    | FULL_OUTER
    deriving (Show)

-- | Join two dataframes using SQL join semantics.
join ::
    JoinType ->
    [T.Text] ->
    DataFrame -> -- Right hand side
    DataFrame -> -- Left hand side
    DataFrame
join INNER xs right = innerJoin xs right
join LEFT xs right = leftJoin xs right
join RIGHT xs right = rightJoin xs right
join FULL_OUTER xs right = fullOuterJoin xs right

{- | Row-count threshold for the build side.
When the build side exceeds this, sort-merge join is used instead of the
single-threaded hash join. The parallel chunked-probe hash join
('parInnerKernel' \/ 'parLeftKernel') is preferred above this size when more
than one capability is available (see 'shouldParallelizeJoin'): partitioning
the probe over cores beats sort-merge for the large build sides measured here
(e.g. the 2M-row build in the join pipeline). Sort-merge remains the
single-threaded fallback.
-}
joinStrategyThreshold :: Int
joinStrategyThreshold = 500_000

{- | A compact index mapping hash values to contiguous slices of
original row indices. All indices live in a single unboxed vector
(@ciSortedIndices@, sorted by hash). The lookup table is an open-addressing
linear-probe hash table held in three parallel unboxed vectors keyed by hash:
@ciKeys@ holds the hash at each slot, @ciStarts@ the run offset into
@ciSortedIndices@ (@-1@ marks an empty slot, since real offsets are @>= 0@),
and @ciLens@ the run length. @ciMask@ is @tableSize - 1@ (table size is a
power of two), used to map a hash to its home slot.
-}
data CompactIndex = CompactIndex
    { ciSortedIndices :: {-# UNPACK #-} !(VU.Vector Int)
    , ciKeys :: {-# UNPACK #-} !(VU.Vector Int)
    , ciStarts :: {-# UNPACK #-} !(VU.Vector Int)
    , ciLens :: {-# UNPACK #-} !(VU.Vector Int)
    , ciMask :: {-# UNPACK #-} !Int
    }

{- | Look up a hash in the open-addressing table.
Returns @(start, len)@ of the matching run, or @(-1, 0)@ on a miss.
-}
ciLookup :: CompactIndex -> Int -> (Int, Int)
ciLookup ci !h = go (h .&. mask)
  where
    !mask = ciMask ci
    !keys = ciKeys ci
    !starts = ciStarts ci
    !lens = ciLens ci
    go !slot =
        let !s = starts `VU.unsafeIndex` slot
         in if s < 0
                then (-1, 0)
                else
                    if keys `VU.unsafeIndex` slot == h
                        then (s, lens `VU.unsafeIndex` slot)
                        else go ((slot + 1) .&. mask)
{-# INLINE ciLookup #-}

{- | Smallest power of two strictly greater than @n@, at least 2.
Used to size the open-addressing table so the load factor stays below ~0.5.
-}
nextPow2Above :: Int -> Int
nextPow2Above n = go 2
  where
    go !p
        | p > n = p
        | otherwise = go (p * 2)

{- | Build a compact index from a vector of row hashes.
Sorts @(hash, originalIndex)@ pairs by hash, scans for contiguous runs, then
inserts each run into an open-addressing linear-probe table sized to keep the
load factor under ~0.5.
-}
buildCompactIndex :: VU.Vector Int -> CompactIndex
buildCompactIndex hashes =
    let n = VU.length hashes
        (sortedHashes, sortedIndices) = parSortByHash n hashes
        -- Worst case every row is a distinct group; 2*n+1 keeps the table
        -- sparse even then. Capacity is independent of the actual group count
        -- so building stays a single pass with no resize.
        !cap = nextPow2Above (2 * n)
        !mask = cap - 1
        (keys, starts, lens) = runST $ do
            mKeys <- VUM.unsafeNew cap
            mStarts <- VUM.replicate cap (-1)
            mLens <- VUM.unsafeNew cap
            let insert !i
                    | i >= n = return ()
                    | otherwise = do
                        let !h = sortedHashes `VU.unsafeIndex` i
                            !end = findGroupEnd sortedHashes h (i + 1) n
                        probe h (h .&. mask) i (end - i)
                        insert end
                probe !h !slot !start !len = do
                    s <- VUM.unsafeRead mStarts slot
                    if s < 0
                        then do
                            VUM.unsafeWrite mKeys slot h
                            VUM.unsafeWrite mStarts slot start
                            VUM.unsafeWrite mLens slot len
                        else probe h ((slot + 1) .&. mask) start len
            insert 0
            (,,)
                <$> VU.unsafeFreeze mKeys
                <*> VU.unsafeFreeze mStarts
                <*> VU.unsafeFreeze mLens
     in CompactIndex sortedIndices keys starts lens mask

-- | Find the end of a contiguous run of equal values starting at @j@.
findGroupEnd :: VU.Vector Int -> Int -> Int -> Int -> Int
findGroupEnd !v !h !j !n
    | j >= n = j
    | v `VU.unsafeIndex` j == h = findGroupEnd v h (j + 1) n
    | otherwise = j
{-# INLINE findGroupEnd #-}

{- | Sort a hash vector, returning sorted hashes and corresponding original indices.
Sorts an index array using hash values as the comparison key, avoiding the
intermediate pair vector used by the naive zip-then-sort approach.
-}
sortWithIndices :: VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortWithIndices hashes = runST $ do
    let n = VU.length hashes
    mv <- VU.thaw (VU.enumFromN 0 n)
    VA.sortBy
        (\i j -> compare (hashes `VU.unsafeIndex` i) (hashes `VU.unsafeIndex` j))
        mv
    sortedIdxs <- VU.unsafeFreeze mv
    return (VU.unsafeBackpermute hashes sortedIdxs, sortedIdxs)

-- | Write the cross product of two index ranges into mutable vectors.
fillCrossProduct ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    Int ->
    VUM.MVector s Int ->
    VUM.MVector s Int ->
    Int ->
    ST s ()
fillCrossProduct !leftSI !rightSI !lStart !lEnd !rStart !rEnd !lv !rv !pos = goL lStart pos
  where
    !rLen = rEnd - rStart
    goL !li !p
        | li >= lEnd = return ()
        | otherwise = do
            let !lOrigIdx = leftSI `VU.unsafeIndex` li
            goR lOrigIdx rStart p
            goL (li + 1) (p + rLen)
    goR !lOrigIdx !ri !q
        | ri >= rEnd = return ()
        | otherwise = do
            VUM.unsafeWrite lv q lOrigIdx
            VUM.unsafeWrite rv q (rightSI `VU.unsafeIndex` ri)
            goR lOrigIdx (ri + 1) (q + 1)
{-# INLINE fillCrossProduct #-}

-- | Compute key-column indices from the column index map.
keyColIndices :: S.Set T.Text -> DataFrame -> [Int]
keyColIndices csSet df = M.elems $ M.restrictKeys (D.columnIndices df) csSet

-- | Validate that all requested join keys exist, then return their indices.
validatedKeyColIndices :: T.Text -> S.Set T.Text -> DataFrame -> [Int]
validatedKeyColIndices callPoint csSet df =
    let columnIdxs = D.columnIndices df
        missingKeys = S.toAscList (csSet `S.difference` M.keysSet columnIdxs)
     in case missingKeys of
            [] -> M.elems $ M.restrictKeys columnIdxs csSet
            _ -> throw (ColumnsNotFoundException missingKeys callPoint (M.keys columnIdxs))

-- ============================================================
-- Inner Join
-- ============================================================

{- | Performs an inner join on two dataframes using the specified key columns.
Returns only rows where the key values exist in both dataframes.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.innerJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1
 K2   | A2  | B2

@
-}
innerJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoin cs left right
    | D.null right || D.null left = D.empty
    | otherwise = innerJoinNonEmpty cs left right

innerJoinNonEmpty :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoinNonEmpty cs left right =
    let
        csSet = S.fromList cs
        leftRows = fst (D.dimensions left)
        rightRows = fst (D.dimensions right)

        leftKeyIdxs = validatedKeyColIndices "innerJoin" csSet left
        rightKeyIdxs = validatedKeyColIndices "innerJoin" csSet right
        leftHashes = D.computeRowHashes leftKeyIdxs left
        rightHashes = D.computeRowHashes rightKeyIdxs right

        buildRows = min leftRows rightRows
        -- Probe with the larger side, build on the smaller. The probe side
        -- drives parallelism (it is partitioned across cores), so probing the
        -- larger side maximises the win.
        probeRows = max leftRows rightRows
        -- Parallelize the probe either when the build is large enough to spill
        -- cache (the original sort-merge regime) or when the build is small
        -- (cache-resident, read-only) but the probe is huge: partitioning a
        -- ~1e7-row probe over cores wins even against a tiny hot build (the
        -- medium-factor lever). Both routes use the same bit-identical
        -- 'parInnerKernel'.
        useParallel =
            shouldParallelizeJoin probeRows buildRows
                || shouldParallelizeSmallBuildProbe probeRows
        (leftIxs, rightIxs)
            | buildRows > joinStrategyThreshold && not useParallel =
                sortMergeInnerKernel leftHashes rightHashes
            | rightRows <= leftRows =
                -- Build on right (smaller or equal), probe with left
                innerKernel useParallel leftHashes rightHashes
            | otherwise =
                -- Build on left (smaller), probe with right, swap result
                let (!rIxs, !lIxs) = innerKernel useParallel rightHashes leftHashes
                 in (lIxs, rIxs)
     in
        assembleInner csSet left right leftIxs rightIxs

{- | Select the inner-join kernel: parallel chunked-probe hash join when the
caller decided the probe side is large enough and multi-core, otherwise the
sequential single-threaded hash kernel.
-}
innerKernel ::
    Bool -> VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
innerKernel True = parInnerKernel
innerKernel False = hashInnerKernel
{-# INLINE innerKernel #-}

{- | Parallel inner-join kernel: build the 'CompactIndex' on @buildHashes@ once,
then probe @probeHashes@ in parallel. Bit-for-bit identical output to
'hashInnerKernel' (probe-row order preserved). Runs the IO probe via
'unsafePerformIO'; the computation is pure (no observable effects, fixed result
for fixed inputs).
-}
parInnerKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
parInnerKernel probeHashes buildHashes =
    let !ci = buildCompactIndex buildHashes
        (pf, bf) =
            unsafePerformIO (parInnerProbe (ciSortedIndices ci) (ciLookup ci) probeHashes)
     in (pf, bf)
{-# NOINLINE parInnerKernel #-}

-- | Compute hashes for the given key column names in a DataFrame.
buildHashColumn :: [T.Text] -> DataFrame -> VU.Vector Int
buildHashColumn keys df =
    let csSet = S.fromList keys
        keyIdxs = validatedKeyColIndices "buildHashColumn" csSet df
     in D.computeRowHashes keyIdxs df

{- | Probe one batch of rows against a pre-built 'CompactIndex'.
Returns @(probeExpandedIxs, buildExpandedIxs)@.
Unlike 'hashInnerKernel', does not build the index (it is pre-built once)
and has no cross-product row guard — the caller controls probe batch size.
-}
hashProbeKernel ::
    -- | Built once from the full right\/build side.
    CompactIndex ->
    -- | Probe hashes (one batch).
    VU.Vector Int ->
    (VU.Vector Int, VU.Vector Int)
hashProbeKernel ci probeHashes =
    let ciIxs = ciSortedIndices ci
        (pFrozen, bFrozen) = runST $ do
            let !probeN = VU.length probeHashes
                initCap = max 1 (min probeN 1_000_000)

            initPv <- VUM.unsafeNew initCap
            initBv <- VUM.unsafeNew initCap
            pvRef <- newSTRef initPv
            bvRef <- newSTRef initBv
            capRef <- newSTRef initCap
            posRef <- newSTRef (0 :: Int)

            let ensureCapacity needed = do
                    cap <- readSTRef capRef
                    when (needed > cap) $ do
                        let newCap = max needed (cap * 2)
                            delta = newCap - cap
                        pv <- readSTRef pvRef
                        bv <- readSTRef bvRef
                        newPv <- VUM.unsafeGrow pv delta
                        newBv <- VUM.unsafeGrow bv delta
                        writeSTRef pvRef newPv
                        writeSTRef bvRef newBv
                        writeSTRef capRef newCap

                go !i
                    | i >= probeN = return ()
                    | otherwise = do
                        let !h = probeHashes `VU.unsafeIndex` i
                            (!start, !len) = ciLookup ci h
                        if start < 0
                            then go (i + 1)
                            else do
                                !p <- readSTRef posRef
                                ensureCapacity (p + len)
                                pv <- readSTRef pvRef
                                bv <- readSTRef bvRef
                                fillBuild i start len p 0 pv bv
                                writeSTRef posRef (p + len)
                                go (i + 1)
                fillBuild !probeIdx !start !len !p !j !pv !bv
                    | j >= len = return ()
                    | otherwise = do
                        VUM.unsafeWrite pv (p + j) probeIdx
                        VUM.unsafeWrite bv (p + j) (ciIxs `VU.unsafeIndex` (start + j))
                        fillBuild probeIdx start len p (j + 1) pv bv
            go 0

            !total <- readSTRef posRef
            pv <- readSTRef pvRef
            bv <- readSTRef bvRef
            (,)
                <$> VU.unsafeFreeze (VUM.slice 0 total pv)
                <*> VU.unsafeFreeze (VUM.slice 0 total bv)
     in (VU.force pFrozen, VU.force bFrozen)

{- | Hash-based inner join kernel.
Builds compact index on @buildHashes@ (second arg), probes with
@probeHashes@ (first arg).
Returns @(probeExpandedIndices, buildExpandedIndices)@.
Uses a dynamically growing output buffer to avoid pre-allocating the full
cross-product size (which can be astronomically large for low-cardinality keys).
-}

{- | Maximum number of output rows allowed from a join kernel.
Exceeding this limit indicates a cross-product explosion (e.g. low-cardinality keys).
-}
maxJoinOutputRows :: Int
maxJoinOutputRows = 500_000_000

hashInnerKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashInnerKernel probeHashes buildHashes =
    let (pFrozen, bFrozen) = runST $ do
            let ci = buildCompactIndex buildHashes
                ciIxs = ciSortedIndices ci
                !probeN = VU.length probeHashes
                !buildN = VU.length buildHashes
                initCap = max 1 (min (probeN + buildN) 1_000_000)

            initPv <- VUM.unsafeNew initCap
            initBv <- VUM.unsafeNew initCap
            pvRef <- newSTRef initPv
            bvRef <- newSTRef initBv
            capRef <- newSTRef initCap
            posRef <- newSTRef (0 :: Int)

            let ensureCapacity needed = do
                    cap <- readSTRef capRef
                    when (needed > cap) $ do
                        let newCap = max needed (cap * 2)
                            delta = newCap - cap
                        pv <- readSTRef pvRef
                        bv <- readSTRef bvRef
                        newPv <- VUM.unsafeGrow pv delta
                        newBv <- VUM.unsafeGrow bv delta
                        writeSTRef pvRef newPv
                        writeSTRef bvRef newBv
                        writeSTRef capRef newCap

                go !i
                    | i >= probeN = return ()
                    | otherwise = do
                        let !h = probeHashes `VU.unsafeIndex` i
                            (!start, !len) = ciLookup ci h
                        if start < 0
                            then go (i + 1)
                            else do
                                !p <- readSTRef posRef
                                when (p + len > maxJoinOutputRows) $
                                    error $
                                        "Join output would exceed "
                                            ++ show maxJoinOutputRows
                                            ++ " rows (cross-product explosion). "
                                            ++ "Consider filtering or using higher-cardinality join keys or using the lazy API."
                                ensureCapacity (p + len)
                                pv <- readSTRef pvRef
                                bv <- readSTRef bvRef
                                fillBuild i start len p 0 pv bv
                                writeSTRef posRef (p + len)
                                go (i + 1)
                fillBuild !probeIdx !start !len !p !j !pv !bv
                    | j >= len = return ()
                    | otherwise = do
                        VUM.unsafeWrite pv (p + j) probeIdx
                        VUM.unsafeWrite bv (p + j) (ciIxs `VU.unsafeIndex` (start + j))
                        fillBuild probeIdx start len p (j + 1) pv bv
            go 0

            !total <- readSTRef posRef
            pv <- readSTRef pvRef
            bv <- readSTRef bvRef
            (,)
                <$> VU.unsafeFreeze (VUM.slice 0 total pv)
                <*> VU.unsafeFreeze (VUM.slice 0 total bv)
     in -- VU.force copies the slice into a compact array, releasing the oversized
        -- backing buffer allocated by the doubling strategy.
        (VU.force pFrozen, VU.force bFrozen)

{- | Sort-merge inner join kernel.
Sorts both sides by hash, walks in lockstep.
Returns @(leftExpandedIndices, rightExpandedIndices)@.
Uses a dynamically growing output buffer instead of a two-pass count-then-allocate
strategy, which OOMs when low-cardinality keys produce large cross products.
-}
sortMergeInnerKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeInnerKernel leftHashes rightHashes =
    let (lFrozen, rFrozen) = runST $ do
            let (leftSH, leftSI) = sortWithIndices leftHashes
                (rightSH, rightSI) = sortWithIndices rightHashes
                !leftN = VU.length leftHashes
                !rightN = VU.length rightHashes
                initCap = max 1 (min (leftN + rightN) 1_000_000)

            initLv <- VUM.unsafeNew initCap
            initRv <- VUM.unsafeNew initCap
            lvRef <- newSTRef initLv
            rvRef <- newSTRef initRv
            capRef <- newSTRef initCap
            posRef <- newSTRef (0 :: Int)

            let ensureCapacity needed = do
                    cap <- readSTRef capRef
                    when (needed > cap) $ do
                        let newCap = max needed (cap * 2)
                            delta = newCap - cap
                        lv <- readSTRef lvRef
                        rv <- readSTRef rvRef
                        newLv <- VUM.unsafeGrow lv delta
                        newRv <- VUM.unsafeGrow rv delta
                        writeSTRef lvRef newLv
                        writeSTRef rvRef newRv
                        writeSTRef capRef newCap

                fillGroup !li !lEnd !ri !rEnd = do
                    let !lLen = lEnd - li
                        !rLen = rEnd - ri
                        !groupSize = lLen * rLen
                    !p <- readSTRef posRef
                    when (p + groupSize > maxJoinOutputRows) $
                        error $
                            "Join output would exceed "
                                ++ show maxJoinOutputRows
                                ++ " rows (cross-product explosion with group sizes "
                                ++ show lLen
                                ++ " × "
                                ++ show rLen
                                ++ "). Consider filtering or using higher-cardinality join keys."
                    ensureCapacity (p + groupSize)
                    lv <- readSTRef lvRef
                    rv <- readSTRef rvRef
                    let goL !lIdx !pos
                            | lIdx >= lEnd = return ()
                            | otherwise = do
                                let !lOrig = leftSI `VU.unsafeIndex` lIdx
                                goR lOrig ri pos
                                goL (lIdx + 1) (pos + rLen)
                        goR !lOrig !rIdx !pos
                            | rIdx >= rEnd = return ()
                            | otherwise = do
                                VUM.unsafeWrite lv pos lOrig
                                VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` rIdx)
                                goR lOrig (rIdx + 1) (pos + 1)
                    goL li p
                    writeSTRef posRef (p + groupSize)

                fill !li !ri
                    | li >= leftN || ri >= rightN = return ()
                    | lh < rh = fill (li + 1) ri
                    | lh > rh = fill li (ri + 1)
                    | otherwise = do
                        let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                            !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                        fillGroup li lEnd ri rEnd
                        fill lEnd rEnd
                  where
                    !lh = leftSH `VU.unsafeIndex` li
                    !rh = rightSH `VU.unsafeIndex` ri

            fill 0 0

            !total <- readSTRef posRef
            lv <- readSTRef lvRef
            rv <- readSTRef rvRef
            (,)
                <$> VU.unsafeFreeze (VUM.slice 0 total lv)
                <*> VU.unsafeFreeze (VUM.slice 0 total rv)
     in -- VU.force copies the slice into a compact array, releasing the oversized
        -- backing buffer allocated by the doubling strategy.
        (VU.force lFrozen, VU.force rFrozen)

-- | Assemble the result DataFrame for an inner join from expanded index vectors.
assembleInner ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleInner csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        -- Pre-expand every column once
        expandedLeftCols = VB.map (D.atIndicesStable leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.atIndicesStable rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        -- Base DataFrame: all left columns, expanded
        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then df -- Key column already present from left side
                    else
                        if S.member name leftColSet
                            then -- Overlapping non-key column: merge with These
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else -- Right-only column
                                insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf

-- ============================================================
-- Left Join
-- ============================================================

{- | Performs a left join on two dataframes using the specified key columns.
Returns all rows from the left dataframe, with matching rows from the right dataframe.
Non-matching rows will have Nothing/null values for columns from the right dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.leftJoin ["key"] df other

------------------------
 key  |  A  |     B
------|-----|----------
 Text | Text| Maybe Text
------|-----|----------
 K0   | A0  | Just "B0"
 K1   | A1  | Just "B1"
 K2   | A2  | Just "B2"
 K3   | A3  | Nothing

@
-}
leftJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoin = leftJoinWithCallPoint "leftJoin"

leftJoinWithCallPoint ::
    T.Text -> [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoinWithCallPoint callPoint cs left right
    | D.null right || D.nRows right == 0 = left
    | D.null left || D.nRows left == 0 = D.empty
    | otherwise = leftJoinNonEmpty callPoint cs left right

leftJoinNonEmpty :: T.Text -> [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoinNonEmpty callPoint cs left right =
    let
        csSet = S.fromList cs
        rightRows = fst (D.dimensions right)

        leftKeyIdxs = validatedKeyColIndices callPoint csSet left
        rightKeyIdxs = validatedKeyColIndices callPoint csSet right
        leftHashes = D.computeRowHashes leftKeyIdxs left
        rightHashes = D.computeRowHashes rightKeyIdxs right

        leftRows = fst (D.dimensions left)
        -- Right is always the build side for left join; left is the probe side
        -- (and drives parallelism). Parallelize either in the large-build regime
        -- or when the build is small but the probe (left) is huge: the
        -- read-only shared index is probed across cores with no synchronization.
        useParallel =
            shouldParallelizeJoin leftRows rightRows
                || shouldParallelizeSmallBuildProbe leftRows
        (leftIxs, rightIxs)
            | rightRows > joinStrategyThreshold && not useParallel =
                sortMergeLeftKernel leftHashes rightHashes
            | useParallel =
                parLeftKernel leftHashes rightHashes
            | otherwise =
                hashLeftKernel leftHashes rightHashes
     in
        -- rightIxs uses -1 as sentinel for "no match"
        assembleLeft csSet left right leftIxs rightIxs

{- | Parallel left-join kernel: build the 'CompactIndex' on @rightHashes@ once,
then probe @leftHashes@ in parallel. Bit-for-bit identical output to
'hashLeftKernel' (left-row order preserved, @-1@ sentinel for misses).
-}
parLeftKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
parLeftKernel leftHashes rightHashes =
    let !ci = buildCompactIndex rightHashes
     in unsafePerformIO (parLeftProbe (ciSortedIndices ci) (ciLookup ci) leftHashes)
{-# NOINLINE parLeftKernel #-}

{- | Hash-based left join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ where
right indices use @-1@ as sentinel for unmatched rows.
Uses a dynamically growing output buffer to avoid pre-allocating the full
cross-product size (which can be astronomically large for low-cardinality keys).
-}
hashLeftKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashLeftKernel leftHashes rightHashes = runST $ do
    let ci = buildCompactIndex rightHashes
        ciIxs = ciSortedIndices ci
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes
        initCap = max 1 (min (leftN + rightN) 1_000_000)

    initLv <- VUM.unsafeNew initCap
    initRv <- VUM.unsafeNew initCap
    lvRef <- newSTRef initLv
    rvRef <- newSTRef initRv
    capRef <- newSTRef initCap
    posRef <- newSTRef (0 :: Int)

    let ensureCapacity needed = do
            cap <- readSTRef capRef
            when (needed > cap) $ do
                let newCap = max needed (cap * 2)
                    delta = newCap - cap
                lv <- readSTRef lvRef
                rv <- readSTRef rvRef
                newLv <- VUM.unsafeGrow lv delta
                newRv <- VUM.unsafeGrow rv delta
                writeSTRef lvRef newLv
                writeSTRef rvRef newRv
                writeSTRef capRef newCap

        go !i
            | i >= leftN = return ()
            | otherwise = do
                let !h = leftHashes `VU.unsafeIndex` i
                    (!start, !len) = ciLookup ci h
                !p <- readSTRef posRef
                if start < 0
                    then do
                        ensureCapacity (p + 1)
                        lv <- readSTRef lvRef
                        rv <- readSTRef rvRef
                        VUM.unsafeWrite lv p i
                        VUM.unsafeWrite rv p (-1)
                        writeSTRef posRef (p + 1)
                    else do
                        ensureCapacity (p + len)
                        lv <- readSTRef lvRef
                        rv <- readSTRef rvRef
                        fillBuild i start len p 0 lv rv
                        writeSTRef posRef (p + len)
                go (i + 1)
        fillBuild !leftIdx !start !len !p !j !lv !rv
            | j >= len = return ()
            | otherwise = do
                VUM.unsafeWrite lv (p + j) leftIdx
                VUM.unsafeWrite rv (p + j) (ciIxs `VU.unsafeIndex` (start + j))
                fillBuild leftIdx start len p (j + 1) lv rv
    go 0

    !total <- readSTRef posRef
    lv <- readSTRef lvRef
    rv <- readSTRef rvRef
    (,)
        <$> VU.unsafeFreeze (VUM.slice 0 total lv)
        <*> VU.unsafeFreeze (VUM.slice 0 total rv)

{- | Sort-merge left join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinel.
Uses a dynamically growing output buffer instead of a two-pass count-then-allocate
strategy, which OOMs when low-cardinality keys produce large cross products.
-}
sortMergeLeftKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeLeftKernel leftHashes rightHashes = runST $ do
    let (leftSH, leftSI) = sortWithIndices leftHashes
        (rightSH, rightSI) = sortWithIndices rightHashes
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes
        initCap = max 1 (min (leftN + rightN) 1_000_000)

    initLv <- VUM.unsafeNew initCap
    initRv <- VUM.unsafeNew initCap
    lvRef <- newSTRef initLv
    rvRef <- newSTRef initRv
    capRef <- newSTRef initCap
    posRef <- newSTRef (0 :: Int)

    let ensureCapacity needed = do
            cap <- readSTRef capRef
            when (needed > cap) $ do
                let newCap = max needed (cap * 2)
                    delta = newCap - cap
                lv <- readSTRef lvRef
                rv <- readSTRef rvRef
                newLv <- VUM.unsafeGrow lv delta
                newRv <- VUM.unsafeGrow rv delta
                writeSTRef lvRef newLv
                writeSTRef rvRef newRv
                writeSTRef capRef newCap

        fillGroup !li !lEnd !ri !rEnd = do
            let !lLen = lEnd - li
                !rLen = rEnd - ri
                !groupSize = lLen * rLen
            !p <- readSTRef posRef
            ensureCapacity (p + groupSize)
            lv <- readSTRef lvRef
            rv <- readSTRef rvRef
            let goL !lIdx !pos
                    | lIdx >= lEnd = return ()
                    | otherwise = do
                        let !lOrig = leftSI `VU.unsafeIndex` lIdx
                        goR lOrig ri pos
                        goL (lIdx + 1) (pos + rLen)
                goR !lOrig !rIdx !pos
                    | rIdx >= rEnd = return ()
                    | otherwise = do
                        VUM.unsafeWrite lv pos lOrig
                        VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` rIdx)
                        goR lOrig (rIdx + 1) (pos + 1)
            goL li p
            writeSTRef posRef (p + groupSize)

        fill !li !ri
            | li >= leftN = return ()
            | ri >= rightN = fillRemainingLeft li
            | lh < rh = do
                !p <- readSTRef posRef
                ensureCapacity (p + 1)
                lv <- readSTRef lvRef
                rv <- readSTRef rvRef
                VUM.unsafeWrite lv p (leftSI `VU.unsafeIndex` li)
                VUM.unsafeWrite rv p (-1)
                writeSTRef posRef (p + 1)
                fill (li + 1) ri
            | lh > rh = fill li (ri + 1)
            | otherwise = do
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                fillGroup li lEnd ri rEnd
                fill lEnd rEnd
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri

        fillRemainingLeft !i = do
            let !remaining = leftN - i
            when (remaining > 0) $ do
                !p <- readSTRef posRef
                ensureCapacity (p + remaining)
                lv <- readSTRef lvRef
                rv <- readSTRef rvRef
                let go !j
                        | j >= remaining = return ()
                        | otherwise = do
                            VUM.unsafeWrite lv (p + j) (leftSI `VU.unsafeIndex` (i + j))
                            VUM.unsafeWrite rv (p + j) (-1)
                            go (j + 1)
                go 0
                writeSTRef posRef (p + remaining)

    fill 0 0

    !total <- readSTRef posRef
    lv <- readSTRef lvRef
    rv <- readSTRef rvRef
    (,)
        <$> VU.unsafeFreeze (VUM.slice 0 total lv)
        <*> VU.unsafeFreeze (VUM.slice 0 total rv)

{- | Assemble the result DataFrame for a left join.
Right index vectors use @-1@ sentinel, gathered via 'gatherWithSentinel'.
-}
assembleLeft ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleLeft csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        expandedLeftCols = VB.map (D.atIndicesStable leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.gatherWithSentinel rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then df
                    else
                        if S.member name leftColSet
                            then
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf

{- | Performs a right join on two dataframes using the specified key columns.
Returns all rows from the right dataframe, with matching rows from the left dataframe.
Non-matching rows will have Nothing/null values for columns from the left dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1"]), ("B", D.fromList ["B0", "B1"])]
ghci> D.rightJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1

@
-}
rightJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
rightJoin cs left right = leftJoinWithCallPoint "rightJoin" cs right left

fullOuterJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
fullOuterJoin cs left right
    | D.null right || D.nRows right == 0 = left
    | D.null left || D.nRows left == 0 = right
    | otherwise = fullOuterJoinNonEmpty cs left right

fullOuterJoinNonEmpty :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
fullOuterJoinNonEmpty cs left right =
    let
        csSet = S.fromList cs
        leftRows = fst (D.dimensions left)
        rightRows = fst (D.dimensions right)

        leftKeyIdxs = validatedKeyColIndices "fullOuterJoin" csSet left
        rightKeyIdxs = validatedKeyColIndices "fullOuterJoin" csSet right
        leftHashes = D.computeRowHashes leftKeyIdxs left
        rightHashes = D.computeRowHashes rightKeyIdxs right

        -- Both sides can have nulls in full outer
        (leftIxs, rightIxs)
            | max leftRows rightRows > joinStrategyThreshold =
                sortMergeFullOuterKernel leftHashes rightHashes
            | otherwise =
                hashFullOuterKernel leftHashes rightHashes
     in
        -- Both index vectors use -1 as sentinel
        assembleFullOuter csSet left right leftIxs rightIxs

{- | Hash-based full outer join kernel.
Builds compact indices on both sides.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinels.
-}
hashFullOuterKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
hashFullOuterKernel leftHashes rightHashes = runST $ do
    let leftCI = buildCompactIndex leftHashes
        rightCI = buildCompactIndex rightHashes
        leftSI = ciSortedIndices leftCI
        rightSI = ciSortedIndices rightCI
        leftKeys = ciKeys leftCI
        leftStarts = ciStarts leftCI
        leftLens = ciLens leftCI
        rightKeys = ciKeys rightCI
        rightStarts = ciStarts rightCI
        rightLens = ciLens rightCI
        !leftCap = VU.length leftStarts
        !rightCap = VU.length rightStarts

    -- Count: matched + left-only + right-only. Iterate over occupied slots
    -- (ciStarts /= -1) in each table, cross-referencing the other via ciLookup.
    let countLeft !slot !acc
            | slot >= leftCap = acc
            | otherwise =
                let !lStart = leftStarts `VU.unsafeIndex` slot
                 in if lStart < 0
                        then countLeft (slot + 1) acc
                        else
                            let !h = leftKeys `VU.unsafeIndex` slot
                                !ll = leftLens `VU.unsafeIndex` slot
                                (!rs, !rl) = ciLookup rightCI h
                             in countLeft (slot + 1) (acc + if rs < 0 then ll else ll * rl)
        countRightOnly !slot !acc
            | slot >= rightCap = acc
            | otherwise =
                let !rStart = rightStarts `VU.unsafeIndex` slot
                 in if rStart < 0
                        then countRightOnly (slot + 1) acc
                        else
                            let !h = rightKeys `VU.unsafeIndex` slot
                                !rl = rightLens `VU.unsafeIndex` slot
                                (!ls, _) = ciLookup leftCI h
                             in countRightOnly (slot + 1) (acc + if ls < 0 then rl else 0)
        !leftPlusMatched = countLeft 0 0
        !rightOnlyCount = countRightOnly 0 0
        !totalCount = leftPlusMatched + rightOnlyCount

    lv <- VUM.unsafeNew totalCount
    rv <- VUM.unsafeNew totalCount
    posRef <- newSTRef (0 :: Int)

    -- Fill matched + left-only (iterate left slots)
    let fillLeft !slot
            | slot >= leftCap = return ()
            | otherwise = do
                let !lStart = leftStarts `VU.unsafeIndex` slot
                if lStart < 0
                    then fillLeft (slot + 1)
                    else do
                        let !h = leftKeys `VU.unsafeIndex` slot
                            !lLen = leftLens `VU.unsafeIndex` slot
                            (!rStart, !rLen) = ciLookup rightCI h
                        !p <- readSTRef posRef
                        if rStart < 0
                            then do
                                let goL !j !q
                                        | j >= lLen = return ()
                                        | otherwise = do
                                            VUM.unsafeWrite lv q (leftSI `VU.unsafeIndex` (lStart + j))
                                            VUM.unsafeWrite rv q (-1)
                                            goL (j + 1) (q + 1)
                                goL 0 p
                                writeSTRef posRef (p + lLen)
                            else do
                                fillCrossProduct
                                    leftSI
                                    rightSI
                                    lStart
                                    (lStart + lLen)
                                    rStart
                                    (rStart + rLen)
                                    lv
                                    rv
                                    p
                                writeSTRef posRef (p + lLen * rLen)
                        fillLeft (slot + 1)
    fillLeft 0

    -- Fill right-only (iterate right slots not in left)
    let fillRightOnly !slot
            | slot >= rightCap = return ()
            | otherwise = do
                let !rStart = rightStarts `VU.unsafeIndex` slot
                if rStart < 0
                    then fillRightOnly (slot + 1)
                    else do
                        let !h = rightKeys `VU.unsafeIndex` slot
                            !rLen = rightLens `VU.unsafeIndex` slot
                            (!ls, _) = ciLookup leftCI h
                        if ls >= 0
                            then fillRightOnly (slot + 1)
                            else do
                                !p <- readSTRef posRef
                                let goR !j !q
                                        | j >= rLen = return ()
                                        | otherwise = do
                                            VUM.unsafeWrite lv q (-1)
                                            VUM.unsafeWrite rv q (rightSI `VU.unsafeIndex` (rStart + j))
                                            goR (j + 1) (q + 1)
                                goR 0 p
                                writeSTRef posRef (p + rLen)
                                fillRightOnly (slot + 1)
    fillRightOnly 0

    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Sort-merge full outer join kernel.
Returns @(leftExpandedIndices, rightExpandedIndices)@ with @-1@ sentinels.
-}
sortMergeFullOuterKernel ::
    VU.Vector Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
sortMergeFullOuterKernel leftHashes rightHashes = runST $ do
    let (leftSH, leftSI) = sortWithIndices leftHashes
        (rightSH, rightSI) = sortWithIndices rightHashes
        !leftN = VU.length leftHashes
        !rightN = VU.length rightHashes

    -- Pass 1: count
    let countLoop !li !ri !c
            | li >= leftN && ri >= rightN = c
            | li >= leftN = c + (rightN - ri)
            | ri >= rightN = c + (leftN - li)
            | lh < rh = countLoop (li + 1) ri (c + 1)
            | lh > rh = countLoop li (ri + 1) (c + 1)
            | otherwise =
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                 in countLoop lEnd rEnd (c + (lEnd - li) * (rEnd - ri))
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri
        !totalRows = countLoop 0 0 0

    -- Pass 2: fill
    lv <- VUM.unsafeNew totalRows
    rv <- VUM.unsafeNew totalRows

    let fill !li !ri !pos
            | li >= leftN && ri >= rightN = return ()
            | li >= leftN = fillRemainingRight ri pos
            | ri >= rightN = fillRemainingLeft li pos
            | lh < rh = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` li)
                VUM.unsafeWrite rv pos (-1)
                fill (li + 1) ri (pos + 1)
            | lh > rh = do
                VUM.unsafeWrite lv pos (-1)
                VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` ri)
                fill li (ri + 1) (pos + 1)
            | otherwise = do
                let !lEnd = findGroupEnd leftSH lh (li + 1) leftN
                    !rEnd = findGroupEnd rightSH rh (ri + 1) rightN
                    !groupSize = (lEnd - li) * (rEnd - ri)
                fillCrossProduct leftSI rightSI li lEnd ri rEnd lv rv pos
                fill lEnd rEnd (pos + groupSize)
          where
            !lh = leftSH `VU.unsafeIndex` li
            !rh = rightSH `VU.unsafeIndex` ri

        fillRemainingLeft !i !pos
            | i >= leftN = return ()
            | otherwise = do
                VUM.unsafeWrite lv pos (leftSI `VU.unsafeIndex` i)
                VUM.unsafeWrite rv pos (-1)
                fillRemainingLeft (i + 1) (pos + 1)

        fillRemainingRight !i !pos
            | i >= rightN = return ()
            | otherwise = do
                VUM.unsafeWrite lv pos (-1)
                VUM.unsafeWrite rv pos (rightSI `VU.unsafeIndex` i)
                fillRemainingRight (i + 1) (pos + 1)

    fill 0 0 0
    (,) <$> VU.unsafeFreeze lv <*> VU.unsafeFreeze rv

{- | Assemble the result DataFrame for a full outer join.
Both index vectors use @-1@ sentinel; all columns gathered via
'gatherWithSentinel'.  Key columns are coalesced (first non-null wins).
-}
assembleFullOuter ::
    S.Set T.Text ->
    DataFrame ->
    DataFrame ->
    VU.Vector Int ->
    VU.Vector Int ->
    DataFrame
assembleFullOuter csSet left right leftIxs rightIxs =
    let !resultLen = VU.length leftIxs
        leftColSet = S.fromList (D.columnNames left)
        rightColNames = D.columnNames right

        expandedLeftCols = VB.map (D.gatherWithSentinel leftIxs) (D.columns left)
        expandedRightCols = VB.map (D.gatherWithSentinel rightIxs) (D.columns right)

        getExpandedLeft name = do
            idx <- M.lookup name (D.columnIndices left)
            return (expandedLeftCols `VB.unsafeIndex` idx)

        getExpandedRight name = do
            idx <- M.lookup name (D.columnIndices right)
            return (expandedRightCols `VB.unsafeIndex` idx)

        baseDf =
            left
                { columns = expandedLeftCols
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                , derivingExpressions = M.empty
                }

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df

        -- Coalesce two nullable columns: take first non-Nothing per row,
        -- producing a non-optional column.
        coalesceKeyColumn :: Column -> Column -> Column
        coalesceKeyColumn l r
            | D.isPackedText l || D.isPackedText r =
                coalesceKeyColumn (D.materializePacked l) (D.materializePacked r)
        coalesceKeyColumn
            (BoxedColumn lBm (lCol :: VB.Vector a))
            (BoxedColumn rBm (rCol :: VB.Vector b)) =
                case testEquality (typeRep @a) (typeRep @b) of
                    Just Refl ->
                        let asMaybe bm =
                                VB.imap
                                    ( \i v -> case bm of
                                        Just bm' -> if bitmapTestBit bm' i then Just v else Nothing
                                        Nothing -> Just v
                                    )
                            lMaybe = asMaybe lBm lCol
                            rMaybe = asMaybe rBm rCol
                         in D.fromVector $
                                VB.zipWith
                                    ( \l r ->
                                        fromMaybe (error "fullOuterJoin: null on both sides of key column") (l <|> r)
                                    )
                                    lMaybe
                                    rMaybe
                    Nothing -> error "Cannot join columns of different types"
        coalesceKeyColumn
            (UnboxedColumn lBm (lCol :: VU.Vector a))
            (UnboxedColumn rBm (rCol :: VU.Vector b)) =
                case testEquality (typeRep @a) (typeRep @b) of
                    Just Refl ->
                        let validAt bm i = case bm of
                                Just bm' -> bitmapTestBit bm' i
                                Nothing -> True
                            dat = VU.generate resultLen $ \i ->
                                if validAt lBm i
                                    then VU.unsafeIndex lCol i
                                    else VU.unsafeIndex rCol i
                         in D.fromUnboxedVector dat
                    Nothing -> error "Cannot join columns of different types"
        coalesceKeyColumn lc rc =
            error $
                "fullOuterJoin: key columns have mismatched storage representations "
                    <> "(one boxed, one unboxed): "
                    <> columnTypeString lc
                    <> " vs "
                    <> columnTypeString rc
     in D.fold
            ( \name df ->
                if S.member name csSet
                    then -- Key column: coalesce left and right
                        case (getExpandedLeft name, getExpandedRight name) of
                            (Just lc, Just rc) -> D.insertColumn name (coalesceKeyColumn lc rc) df
                            _ -> df
                    else
                        if S.member name leftColSet
                            then
                                insertIfPresent
                                    name
                                    (D.mergeColumns <$> getExpandedLeft name <*> getExpandedRight name)
                                    df
                            else insertIfPresent name (getExpandedRight name) df
            )
            rightColNames
            baseDf
