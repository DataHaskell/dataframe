{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Grouping (
    groupBy,
    groupBySeq,
    groupByPar,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (ST, runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.DictEncode (dictEncodeColumnUpTo)
import DataFrame.Internal.GroupingDirect (
    DirectGrouping (..),
    tryDirectGroupColumn,
 )
import DataFrame.Internal.GroupingPar (parallelAssignGroups, shouldParallelize)
import DataFrame.Internal.Hash
import DataFrame.Internal.HashTable (htInsert, newHashTable)
import DataFrame.Internal.PackedText (
    PackedTextData,
    packedLength,
    packedSlice,
    sliceEqBytes,
 )
import DataFrame.Internal.RadixRank (rankByHash)
import DataFrame.Internal.Types
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

{- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
into vector that should be reduced later.

Rows are bucketed with an unboxed open-addressing hash table
('DataFrame.Internal.HashTable') that maps each row's key-hash to a dense group
id, re-verifying the real key columns on every hash hit. This both cuts the
per-row boxed allocation of the previous 'Data.IntMap' bucketing (less GC) and
fixes a latent collision bug where two distinct keys sharing a hash were merged.
Groups are numbered in first-appearance order; 'valueIndices' / 'offsets' are
then derived by a stable counting sort on the group id.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | nRows df == 0 =
        Grouped
            df
            names
            VU.empty
            (VU.fromList [0])
            VU.empty
    | Just dg <- tryDirectGroup names df = dg
    | shouldParallelize n = groupByPar names df
    | otherwise = groupBySeq names df
  where
    !n = nRows df

{- | The low-cardinality direct-indexed grouping fast path
('DataFrame.Internal.GroupingDirect'). Fires only for a SINGLE clean small-range
@Int@ key column; one shared function feeds both -N1 and -N8 so the result is
identical at any capability count (parallel==sequential by construction). Returns
'Nothing' on any other key shape, falling through to the hash group-by.
-}
tryDirectGroup :: [T.Text] -> DataFrame -> Maybe GroupedDataFrame
tryDirectGroup [name] df = do
    col <- M.lookup name (columnIndices df) >>= \i -> columns df V.!? i
    case tryDirectGroupColumn col of
        Just dg ->
            Just (Grouped df [name] (dgValueIndices dg) (dgOffsets dg) (dgRowToGroup dg))
        Nothing -> tryDictGroup (nRows df) df [name] col
tryDirectGroup _ _ = Nothing

{- | Dictionary-encode a single text key to dense int codes (the codes ARE the
first-appearance group ids), then derive @valueIndices@/@offsets@ from those
codes with one counting sort.

PROFILED AS A LOSS, so this currently always falls back ('Nothing'). The
dict-build is its own hash pass over every row, and the hash group-by already
hashes and groups in one fused pass: at -N1 the dict path measured ~0.44s vs
~0.33s for the hash path on Q1 (id1, 100 groups) at 1e7 rows, and at -N8 the
parallel partitioned grouping is far faster than any sequential dict-build. The
high-card single keys (Q3/Q7 id3 ~1e5) and the multi-key composites (Q2 id1:id2,
Q10 six keys) were each tried and also lost — substituting int codes does not
remove the dominant grouping passes, it adds the encode passes on top. The
'DataFrame.Internal.DictEncode.dictEncodeColumnUpTo' step is kept and unit-tested
as a correct building block; the routing here is deliberately disabled. The
@_n@/@_df@/@_names@/@_col@ wiring is retained so re-enabling is a one-line change
should a parallel dict-encode ever change the verdict.
-}
tryDictGroup ::
    Int -> DataFrame -> [T.Text] -> Column -> Maybe GroupedDataFrame
tryDictGroup n df names col
    | dictGroupEnabled && not (shouldParallelize n) = do
        (codes, card) <- dictEncodeColumnUpTo dictSingleThreshold col
        let (vis, os) = indicesFromGroups codes card
        Just (Grouped df names vis os codes)
    | otherwise = Nothing

{- | Master switch for the single-key dict-encode grouping path. 'False' because
it profiled slower than the hash group-by on every db-benchmark group-by question
(see 'tryDictGroup'); the path is kept compiled and tested but not taken.
-}
dictGroupEnabled :: Bool
dictGroupEnabled = False

{- | Cardinality ceiling the single-key dict-encode probe would use: it bails to
'Nothing' once the distinct count passes this, so a high-card key never pays for
a full encode pass. Only consulted when 'dictGroupEnabled' is 'True'.
-}
dictSingleThreshold :: Int
dictSingleThreshold = 4096

{- | The sequential grouping path: a single open-addressing table over all rows,
canonically remapped. Always available regardless of capabilities; the parallel
path is verified equal to it by a property test.
-}
groupBySeq :: [T.Text] -> DataFrame -> GroupedDataFrame
groupBySeq names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        (rtg0, repHash, repRow) = assignGroups df indicesToGroup n
        !nGroups = VU.length repHash
        !remap = canonicalRemap repHash repRow
        !rtg = VU.map (VU.unsafeIndex remap) rtg0
        (vis, os) = indicesFromGroups rtg nGroups
     in Grouped df names vis os rtg

{- | The parallel partitioned grouping path (see
'DataFrame.Internal.GroupingPar'). Forks one task per capability; produces an
output bit-for-bit identical to 'groupBySeq'. Pure via 'unsafePerformIO' (the IO
is deterministic thread fan-out only).
-}
groupByPar :: [T.Text] -> DataFrame -> GroupedDataFrame
groupByPar names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        !hashes = runST (computeHashes df indicesToGroup n)
        !eqRow = eqKeyRow df indicesToGroup
        (rtg, vis, os) = unsafePerformIO (parallelAssignGroups n hashes eqRow)
     in Grouped df names vis os rtg
{-# NOINLINE groupByPar #-}

-- | Column indices of the requested key columns, in column order.
keyColIndices :: [T.Text] -> DataFrame -> [Int]
keyColIndices names df =
    M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)

{- | Assign every row to a dense group id via the open-addressing table, in
first-appearance order. Returns @(rowToGroup, repHash, repRow)@ where @repHash@ /
@repRow@ are the hash and representative row index of each group (indexed by the
first-appearance id). The table re-verifies the real key with 'eqKeyRow' on each
hash hit so colliding keys are kept apart.
-}
assignGroups ::
    DataFrame -> [Int] -> Int -> (VU.Vector Int, VU.Vector Int, VU.Vector Int)
assignGroups df indicesToGroup n = runST $ do
    hashes <- computeHashes df indicesToGroup n
    let !eqRow = eqKeyRow df indicesToGroup
    ht <- newHashTable n
    rtg <- VUM.new n
    -- At most n groups; trimmed to the actual count on freeze.
    repHashM <- VUM.new n
    repRowM <- VUM.new n
    let go !i !next
            | i >= n = pure next
            | otherwise = do
                let !h = VU.unsafeIndex hashes i
                (gid, isNew) <- htInsert ht eqRow next i h
                VUM.unsafeWrite rtg i gid
                when isNew $ do
                    VUM.unsafeWrite repHashM next h
                    VUM.unsafeWrite repRowM next i
                go (i + 1) (if isNew then next + 1 else next)
    !nGroups <- go 0 0
    frozen <- VU.unsafeFreeze rtg
    repHash <- VU.unsafeFreeze (VUM.slice 0 nGroups repHashM)
    repRow <- VU.unsafeFreeze (VUM.slice 0 nGroups repRowM)
    pure (frozen, repHash, repRow)

{- | Map each first-appearance group id to its canonical id: groups are ordered
by ascending representative hash, tie-broken by representative row index. This
makes the emitted group order a deterministic function of the key set (not of
input row order), so set operations like @union a b@ and @union b a@ agree, and
reproduces the ascending-hash order of the previous 'Data.IntMap' grouping.
Returns @remap@ with @remap[firstAppearanceId] = canonicalId@.

The ordering is the stable hash-rank of 'DataFrame.Internal.RadixRank': @repRow@
is strictly ascending in first-appearance id order (a new group's representative
is the first row that reaches it, scanned in increasing row index), so the stable
sort's equal-hash tie-break reproduces the old @(hash, repRow)@ comparison. O(g)
with no boxed-tuple comparison sort — this keeps the @1e7@-distinct-group case
(Q10) off an @n log n@ list sort.
-}
canonicalRemap :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
canonicalRemap repHash _repRow =
    runST (rankByHash (pure . VU.unsafeIndex repHash) (VU.length repHash))

{- | Compute the FNV row-hash of the key columns into a fresh unboxed vector,
mixing 'nullSalt' for null slots so a missing value never collides with a
present one of the same bits.
-}
computeHashes :: DataFrame -> [Int] -> Int -> ST s (VU.Vector Int)
computeHashes df indicesToGroup n = do
    mh <- VUM.replicate n fnvOffset
    let selectedCols = map (columns df V.!) indicesToGroup
    forM_ selectedCols $ \case
        UnboxedColumn ubm (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> hashUnboxed mh ubm mixInt v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl -> hashUnboxed mh ubm mixDouble v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    hashUnboxed mh ubm (\h d -> mixInt h (fromIntegral @a @Int d)) v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            hashUnboxed mh ubm (\h d -> mixDouble h (realToFrac d :: Double)) v
                                        SFalse ->
                                            hashUnboxed mh ubm mixShow v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i t -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixText h t
                            VUM.unsafeWrite mh i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixShow h d
                            VUM.unsafeWrite mh i h'
                        )
                        v
        PackedText bm p -> hashPacked mh bm p
    VU.unsafeFreeze mh

{- | Build the row-key equality predicate over the selected key columns.
@eqKeyRow df idxs a b@ is 'True' iff rows @a@ and @b@ are equal across all key
columns, comparing validity bits first (a null equals only another null) then
the underlying value. Used by the hash table to reject hash collisions.
-}
eqKeyRow :: DataFrame -> [Int] -> Int -> Int -> Bool
eqKeyRow df indicesToGroup =
    let !preds = map (colEqRow . (columns df V.!)) indicesToGroup
        go [] _ _ = True
        go (p : ps) a b = p a b && go ps a b
     in go preds

{- | Per-column row equality respecting nulls. Two rows are equal at a column
when both are null, or both are valid and their values compare equal.
-}
colEqRow :: Column -> (Int -> Int -> Bool)
colEqRow (UnboxedColumn bm v) =
    let eqV a b = VU.unsafeIndex v a == VU.unsafeIndex v b
     in withNulls bm eqV
colEqRow (BoxedColumn bm v) =
    let eqV a b = V.unsafeIndex v a == V.unsafeIndex v b
     in withNulls bm eqV
colEqRow (PackedText bm p) =
    let eqV a b =
            let (arrA, oA, lA) = packedSlice p a
                (arrB, oB, lB) = packedSlice p b
             in sliceEqBytes arrA oA lA arrB oB lB
     in withNulls bm eqV
{-# INLINE colEqRow #-}

{- | Wrap a value-equality with null handling: equal iff both valid and the
values agree, or both null.
-}
withNulls :: Maybe Bitmap -> (Int -> Int -> Bool) -> (Int -> Int -> Bool)
withNulls Nothing eqV = eqV
withNulls (Just bm) eqV = \a b ->
    case (bitmapTestBit bm a, bitmapTestBit bm b) of
        (True, True) -> eqV a b
        (False, False) -> True
        _ -> False
{-# INLINE withNulls #-}

{- | Derive @(valueIndices, offsets)@ from @rowToGroup@ via a stable counting
sort on the group id: a per-group count, a prefix-sum into group offsets, then a
single placement pass keeps rows in original order within each group.
-}
indicesFromGroups :: VU.Vector Int -> Int -> (VU.Vector Int, VU.Vector Int)
indicesFromGroups rtg nGroups = runST $ do
    let !n = VU.length rtg
    -- counts[g] = size of group g (g in [0, nGroups)). Slot nGroups stays 0 so
    -- the exclusive scan below lands n in offsets[nGroups].
    counts <- VUM.replicate (nGroups + 1) 0
    let countLoop !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                c <- VUM.unsafeRead counts g
                VUM.unsafeWrite counts g (c + 1)
                countLoop (i + 1)
    countLoop 0
    -- Exclusive prefix scan of counts into offsets: offsets[k] is the start of
    -- group k and offsets[nGroups] == n.
    offsM <- VUM.new (nGroups + 1)
    let scan !k !acc
            | k > nGroups = pure ()
            | otherwise = do
                VUM.unsafeWrite offsM k acc
                c <- VUM.unsafeRead counts k
                scan (k + 1) (acc + c)
    scan 0 0
    -- 'counts' is repurposed as a per-group write cursor seeded at each group's
    -- start offset, giving a stable placement (rows keep original order).
    let seed !k
            | k > nGroups = pure ()
            | otherwise = do
                s <- VUM.unsafeRead offsM k
                VUM.unsafeWrite counts k s
                seed (k + 1)
    seed 0
    vis <- VUM.new n
    let place !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                pos <- VUM.unsafeRead counts g
                VUM.unsafeWrite vis pos i
                VUM.unsafeWrite counts g (pos + 1)
                place (i + 1)
    place 0
    offs <- VU.unsafeFreeze offsM
    frozenVis <- VU.unsafeFreeze vis
    pure (frozenVis, offs)

{- | Fold a value-mix over an unboxed column into the running hash vector,
respecting the null bitmap: a null slot mixes a fixed 'nullSalt' sentinel.
-}
hashUnboxed ::
    (VU.Unbox a) =>
    VUM.MVector s Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    VU.Vector a ->
    ST s ()
hashUnboxed mh ubm mix v = case ubm of
    Nothing ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite mh i (mix h x)
            )
            v
    Just bm ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite
                    mh
                    i
                    (if bitmapTestBit bm i then mix h x else mixInt h nullSalt)
            )
            v
{-# INLINE hashUnboxed #-}

{- | Hash a packed-text column over its raw UTF-8 byte slices (no per-row
'Data.Text.Text'), mixing 'nullSalt' for null rows. Shares 'mixBytes' with
'mixText' so packed and boxed Text columns hash identically.
-}
hashPacked ::
    VUM.MVector s Int -> Maybe Bitmap -> PackedTextData -> ST s ()
hashPacked mh bm p = go 0
  where
    !n = packedLength p
    go !i
        | i >= n = pure ()
        | otherwise = do
            !h <- VUM.unsafeRead mh i
            let h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> let (arr, o, l) = packedSlice p i in mixBytes h arr o l
            VUM.unsafeWrite mh i h'
            go (i + 1)
{-# INLINE hashPacked #-}

-- Inline accessors to avoid depending on Operations.Core

columnNames :: DataFrame -> [T.Text]
columnNames = M.keys . columnIndices

nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

{- | Build the rowToGroup lookup vector from valueIndices and offsets.
rowToGroup[i] = k means row i belongs to group k.
-}
buildRowToGroup :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
buildRowToGroup n vis os = runST $ do
    rtg <- VUM.new n
    let nGroups = VU.length os - 1
    forM_ [0 .. nGroups - 1] $ \k ->
        let s = VU.unsafeIndex os k
            e = VU.unsafeIndex os (k + 1)
         in forM_ [s .. e - 1] $ \i ->
                VUM.unsafeWrite rtg (VU.unsafeIndex vis i) k
    VU.unsafeFreeze rtg
{-# NOINLINE buildRowToGroup #-}

changingPoints :: VU.Vector (Int, Int) -> VU.Vector Int
changingPoints vs =
    VU.reverse
        (VU.fromList (VU.length vs : fst (VU.ifoldl' findChangePoints initialState vs)))
  where
    initialState = ([0], snd (VU.head vs))
    findChangePoints (!offs, !currentVal) index (_, !newVal)
        | currentVal == newVal = (offs, currentVal)
        | otherwise = (index : offs, newVal)
