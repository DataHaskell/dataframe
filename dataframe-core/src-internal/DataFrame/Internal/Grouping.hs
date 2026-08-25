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
import DataFrame.Internal.Algorithms.Hash
import DataFrame.Internal.Algorithms.Rank.Radix (rankByHash)
import DataFrame.Internal.Column (
    Column (..),
    materializeMerged,
 )
import DataFrame.Internal.Column.Bitmap (
    Bitmap,
    bitmapTestBit,
 )
import DataFrame.Internal.Column.Encode (dictEncodeColumnUpTo)
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Data.HashTable (htInsert, newHashTable)
import DataFrame.Internal.Data.PackedText (
    PackedSel,
    PackedTextData (..),
    offAt,
    offCount,
    packedLength,
    packedSlice,
    selAt,
    selLength,
    sliceEqBytes,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Grouping.Direct (
    DirectGrouping (..),
    directGroupThreshold,
    tryDirectGroupColumn,
 )
import DataFrame.Internal.Grouping.Parallel (
    parallelAssignGroups,
    shouldParallelize,
 )
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

{- | O(k * n) group the dataframe by the given key columns, bucketing rows with an
open-addressing hash table that re-verifies keys on each hash hit. Groups are
numbered in first-appearance order; 'valueIndices'/'offsets' follow by counting sort.
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

{- | Low-cardinality direct-indexed grouping fast path
('DataFrame.Internal.Grouping.Direct'): fires only for a single clean small-range
@Int@ key. Returns 'Nothing' on any other key shape, falling back to the hash path.
-}
tryDirectGroup :: [T.Text] -> DataFrame -> Maybe GroupedDataFrame
tryDirectGroup [name] df = do
    col <- M.lookup name (columnIndices df) >>= \i -> columns df V.!? i
    case tryDirectGroupColumn col of
        Just dg ->
            Just (Grouped df [name] (dgValueIndices dg) (dgOffsets dg) (dgRowToGroup dg))
        Nothing -> case col of
            PackedText Nothing p -> dictCodesGroup df [name] p
            _ -> tryDictGroup (nRows df) df [name] col
tryDirectGroup _ _ = Nothing

dictCodesGroup ::
    DataFrame -> [T.Text] -> PackedTextData -> Maybe GroupedDataFrame
dictCodesGroup df names p = do
    sel <- ptSel p
    guard (ptCanonicalSel p)
    let offs = ptOffsets p
        card = offCount offs - 1
        n = selLength sel
    guard (card > 0 && card <= directGroupThreshold && n > 0)
    counts <- codeHistogram card sel
    let occupied = VU.filter (\g -> VU.unsafeIndex counts g > 0) (VU.enumFromN 0 card)
        nGroups = VU.length occupied
        entryHash g =
            let o = offAt offs g
             in mixBytes fnvOffset (ptBytes p) o (offAt offs (g + 1) - o)
        rank =
            runST (rankByHash (pure . entryHash . VU.unsafeIndex occupied) nGroups)
        remap = runST $ do
            m <- VUM.new card
            VU.imapM_ (\j g -> VUM.unsafeWrite m g (VU.unsafeIndex rank j)) occupied
            VU.unsafeFreeze m
        rtg = VU.generate n (VU.unsafeIndex remap . selAt sel)
        (vis, os) = indicesFromGroups rtg nGroups
    pure (Grouped df names vis os rtg)

-- | Per-code occupancy counts; 'Nothing' as soon as any code is negative.
codeHistogram :: Int -> PackedSel -> Maybe (VU.Vector Int)
codeHistogram card sel = runST $ do
    counts <- VUM.replicate card 0
    let n = selLength sel
        go i
            | i >= n = Just <$> VU.unsafeFreeze counts
            | otherwise = do
                let c = selAt sel i
                if c < 0 || c >= card
                    then pure Nothing
                    else do
                        VUM.unsafeModify counts (+ 1) c
                        go (i + 1)
    go 0

{- | Dictionary-encode a single text key to dense int codes, then derive
@valueIndices@/@offsets@ by counting sort. Profiled slower than the fused hash
group-by on every db-benchmark question, so it always falls back ('dictGroupEnabled').
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

{- | Cardinality ceiling for the single-key dict-encode probe: it bails to 'Nothing'
once the distinct count passes this. Only consulted when 'dictGroupEnabled' is 'True'.
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

{- | The parallel partitioned grouping path (see 'DataFrame.Internal.Grouping.Parallel'):
forks one task per capability, producing output bit-for-bit identical to
'groupBySeq'. Pure via 'unsafePerformIO' (deterministic thread fan-out only).
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

{- | Assign every row to a dense group id in first-appearance order. Returns
@(rowToGroup, repHash, repRow)@ — the hash and representative row of each group.
'eqKeyRow' re-verifies the real key on each hash hit so colliding keys stay apart.
-}
assignGroups ::
    DataFrame -> [Int] -> Int -> (VU.Vector Int, VU.Vector Int, VU.Vector Int)
assignGroups df indicesToGroup n = runST $ do
    hashes <- computeHashes df indicesToGroup n
    let !eqRow = eqKeyRow df indicesToGroup
    ht <- newHashTable n
    rtg <- VUM.new n
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

{- | Map each first-appearance group id to its canonical id: groups ordered by
ascending representative hash (tie-broken by representative row), making group
order a deterministic function of the key set so set ops commute. O(g), no sort.
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
    -- Merged key columns are exotic; hash their eager form.
    let selectedCols = map (materializeMerged . (columns df V.!)) indicesToGroup
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
        MergedColumn _ _ ->
            error "computeHashes: MergedColumn is normalized before hashing"
    VU.unsafeFreeze mh

{- | Build the row-key equality predicate over the selected key columns.
@eqKeyRow df idxs a b@ is 'True' iff rows @a@ and @b@ agree on all key columns
(validity first, a null equals only a null). Used to reject hash collisions.
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
colEqRow c@(MergedColumn _ _) = colEqRow (materializeMerged c)
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
    counts <- VUM.replicate (nGroups + 1) 0
    let countLoop !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                c <- VUM.unsafeRead counts g
                VUM.unsafeWrite counts g (c + 1)
                countLoop (i + 1)
    countLoop 0
    offsM <- VUM.new (nGroups + 1)
    let scan !k !acc
            | k > nGroups = pure ()
            | otherwise = do
                VUM.unsafeWrite offsM k acc
                c <- VUM.unsafeRead counts k
                scan (k + 1) (acc + c)
    scan 0 0
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
