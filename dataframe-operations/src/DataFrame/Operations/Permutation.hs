{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Permutation (
    -- * Sorting
    SortOrder (..),
    sortOrderColumns,
    sortBy,
    topBy,

    -- * Shuffling
    shuffle,
    shuffledIndices,
) where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Algorithms.Radix as Radix
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad (forM_, when)
import Control.Monad.ST (runST)
import Data.Bits (
    complement,
    countLeadingZeros,
    testBit,
    unsafeShiftR,
    xor,
    (.&.),
    (.|.),
 )
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Data.Typeable (Typeable)
import Data.Vector.Internal.Check (HasCallStack)
import Data.Word (Word16, Word32, Word64, Word8)
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    atIndicesStable,
    bitmapHasNulls,
    sliceColumn,
    toVector,
 )
import DataFrame.Internal.Data.PackedText (
    packedLength,
    packedSlice,
    sliceCmpBytes,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnNames,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (Expr (Col), getColumns)
import DataFrame.Operations.Core (dimensions)
import DataFrame.Operations.Transformations (derive)
import GHC.Float (castDoubleToWord64)
import System.Random (Random (randomR), RandomGen)
import Type.Reflection (typeRep)

-- | Sort order taken as a parameter by the 'sortBy' function.
data SortOrder where
    Asc :: (Columnable a, Ord a) => Expr a -> SortOrder
    Desc :: (Columnable a, Ord a) => Expr a -> SortOrder

instance Eq SortOrder where
    (==) :: SortOrder -> SortOrder -> Bool
    (==) (Asc _) (Asc _) = True
    (==) (Desc _) (Desc _) = True
    (==) _ _ = False

instance Show SortOrder where
    showsPrec d (Asc e) = showParen (d > 10) (showString "Asc " . showsPrec 11 e)
    showsPrec d (Desc e) =
        showParen (d > 10) (showString "Desc " . showsPrec 11 e)

-- | Columns a sort order reads.
sortOrderColumns :: SortOrder -> [T.Text]
sortOrderColumns (Asc e) = getColumns e
sortOrderColumns (Desc e) = getColumns e

mustFlipCompare :: SortOrder -> Bool
mustFlipCompare (Asc _) = True
mustFlipCompare (Desc _) = False

-- | Materialize compound sort expressions into synthetic columns.
prepareSortColumns :: [SortOrder] -> DataFrame -> ([SortOrder], DataFrame)
prepareSortColumns = go 0
  where
    go _ [] acc = ([], acc)
    go i (ord : rest) acc =
        let (ord', acc') = materializeSortOrder i ord acc
            (rest', acc'') = go (i + 1) rest acc'
         in (ord' : rest', acc'')

materializeSortOrder :: Int -> SortOrder -> DataFrame -> (SortOrder, DataFrame)
materializeSortOrder _ ord@(Asc (Col _)) df = (ord, df)
materializeSortOrder _ ord@(Desc (Col _)) df = (ord, df)
materializeSortOrder i (Asc (e :: Expr a)) df =
    let name = syntheticName i
     in (Asc (Col name :: Expr a), derive name e df)
materializeSortOrder i (Desc (e :: Expr a)) df =
    let name = syntheticName i
     in (Desc (Col name :: Expr a), derive name e df)

syntheticName :: Int -> T.Text
syntheticName i = "__sortBy_synthetic_" <> T.pack (show i) <> "__"

-- | O(k log n) Sort the dataframe by the given sort orders.
sortBy ::
    [SortOrder] ->
    DataFrame ->
    DataFrame
sortBy sortOrds df
    | not (null missing) =
        throw $
            ColumnsNotFoundException
                missing
                "sortBy"
                (columnNames df)
    | otherwise =
        let
            (sortOrds', df') = prepareSortColumns sortOrds df
            comparators = map (`sortOrderComparator` df') sortOrds'
            compositeCompare i j = mconcat [c i j | c <- comparators]
            nRows = fst (dataframeDimensions df')
            indexes = case sortOrds' of
                [ord] | Just ixs <- radixSortIndices ord df' -> ixs
                _ -> sortIndices compositeCompare nRows
         in
            df{columns = V.map (atIndicesStable indexes) (columns df)}
  where
    referenced = L.nub (concatMap sortOrderColumns sortOrds)
    missing = referenced L.\\ columnNames df

-- | The rows a 'sortBy' would place at positions @[offset, offset + count)@.
topBy :: Int -> Int -> [SortOrder] -> DataFrame -> DataFrame
topBy offset count sortOrds df
    | not (null missing) =
        throw $
            ColumnsNotFoundException
                missing
                "topBy"
                (columnNames df)
    | k >= nRows || k <= 0 =
        let sorted = sortBy sortOrds df
         in sorted
                { columns = V.map (sliceColumn offset' (k - offset')) (columns sorted)
                , dataframeDimensions = (k - offset', snd (dataframeDimensions df))
                }
    | otherwise =
        let
            (sortOrds', df') = prepareSortColumns sortOrds df
            comparators = map (`sortOrderComparator` df') sortOrds'
            -- Ties keep their original order, as in the stable full sort.
            cmp i j = mconcat [c i j | c <- comparators] <> compare i j
            leading = smallestIndices cmp k nRows
            window = VU.slice offset' (k - offset') (sortIndicesOf cmp leading)
         in
            df
                { columns = V.map (atIndicesStable window) (columns df)
                , dataframeDimensions = (VU.length window, snd (dataframeDimensions df))
                }
  where
    nRows = fst (dataframeDimensions df)
    k = min nRows (max 0 offset + max 0 count)
    offset' = min k (max 0 offset)
    referenced = L.nub (concatMap sortOrderColumns sortOrds)
    missing = referenced L.\\ columnNames df

-- | The @k@ smallest of @[0, n)@ under @cmp@, unordered (a bounded max-heap).
smallestIndices :: (Int -> Int -> Ordering) -> Int -> Int -> VU.Vector Int
smallestIndices cmp k n = runST $ do
    heap <- VUM.generate k id
    let siftDown !i = do
            let l = 2 * i + 1
                r = l + 1
            if l >= k
                then pure ()
                else do
                    vi <- VUM.unsafeRead heap i
                    vl <- VUM.unsafeRead heap l
                    (c, vc) <-
                        if r < k
                            then do
                                vr <- VUM.unsafeRead heap r
                                pure (if cmp vr vl == GT then (r, vr) else (l, vl))
                            else pure (l, vl)
                    when (cmp vc vi == GT) $ do
                        VUM.unsafeWrite heap i vc
                        VUM.unsafeWrite heap c vi
                        siftDown c
    forM_ [k `div` 2 - 1, k `div` 2 - 2 .. 0] siftDown
    forM_ [k .. n - 1] $ \i -> do
        top <- VUM.unsafeRead heap 0
        when (cmp i top == LT) $ do
            VUM.unsafeWrite heap 0 i
            siftDown 0
    VU.unsafeFreeze heap

-- | 'sortIndices' over an explicit index set.
sortIndicesOf :: (Int -> Int -> Ordering) -> VU.Vector Int -> VU.Vector Int
sortIndicesOf cmp ixs = runST $ do
    mv <- VG.thaw (VU.convert ixs :: V.Vector Int)
    VA.sortBy cmp mv
    VU.convert <$> VG.unsafeFreeze mv

-- | Build a row-index comparator from a SortOrder and a DataFrame.
sortOrderComparator :: SortOrder -> DataFrame -> Int -> Int -> Ordering
sortOrderComparator (Asc (Col name :: Expr a)) df =
    columnComparator @a name (unsafeGetColumn name df)
sortOrderComparator (Desc (Col name :: Expr a)) df =
    flip (columnComparator @a name (unsafeGetColumn name df))
sortOrderComparator _ _ = error "Sorting on compound column"

columnComparator ::
    forall a. (Columnable a, Ord a) => T.Text -> Column -> Int -> Int -> Ordering
columnComparator name col = case col of
    UnboxedColumn bm (v :: VU.Vector b)
        | noNulls (VU.length v) bm ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl ->
                    \i j -> compare (v `VU.unsafeIndex` i) (v `VU.unsafeIndex` j)
                Nothing -> compareConverted
        | otherwise -> compareConverted
    BoxedColumn bm (v :: V.Vector b)
        | noNulls (V.length v) bm ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl ->
                    \i j -> compare (v `V.unsafeIndex` i) (v `V.unsafeIndex` j)
                Nothing -> compareConverted
        | otherwise -> compareConverted
    PackedText bm p
        | noNulls (packedLength p) bm ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> \i j ->
                    let (ai, oi, li) = packedSlice p i
                        (aj, oj, lj) = packedSlice p j
                     in sliceCmpBytes ai oi li aj oj lj
                Nothing -> compareConverted
        | otherwise -> compareConverted
    MergedColumn _ _ -> compareConverted
  where
    compareConverted = case toVector @a col of
        Right (v :: V.Vector a) ->
            \i j -> compare (v `V.unsafeIndex` i) (v `V.unsafeIndex` j)
        Left (TypeMismatchException ctx) ->
            throw $
                TypeMismatchException
                    ctx
                        { errorColumnName = Just (T.unpack name)
                        , callingFunctionName = Just "sortBy"
                        }
        Left err -> throw err
    noNulls n = maybe True (not . bitmapHasNulls n)

-- | Sort row indices using a comparator function.

-- | Stable LSD radix sort by one clean numeric column, else 'Nothing'.
radixSortIndices :: SortOrder -> DataFrame -> Maybe (VU.Vector Int)
radixSortIndices ord df = case ord of
    Asc (Col name :: Expr a) -> keysOf @a name False
    Desc (Col name :: Expr a) -> keysOf @a name True
    _ -> Nothing
  where
    keysOf :: forall a. (Typeable a) => T.Text -> Bool -> Maybe (VU.Vector Int)
    keysOf name desc = case unsafeGetColumn name df of
        UnboxedColumn Nothing (v :: VU.Vector b) -> do
            Refl <- testEquality (typeRep @a) (typeRep @b)
            key <- orderKey @b
            -- NaN has no 'compare' order; leave it to the comparison sort.
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl | VU.any isNaN v -> Nothing
                _ -> Just ()
            let flipIf k = if desc then complement k else k
            Just (radixByKey (VU.map (flipIf . key) v))
        _ -> Nothing

-- | An unsigned key whose order matches the type's 'compare' order.
orderKey :: forall b. (Typeable b) => Maybe (b -> Word64)
orderKey
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int) = Just signed
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int64) = Just signed
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int32) = Just signed
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int16) = Just signed
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int8) = Just signed
    | Just Refl <- testEquality (typeRep @b) (typeRep @Word64) = Just fromIntegral
    | Just Refl <- testEquality (typeRep @b) (typeRep @Word32) = Just fromIntegral
    | Just Refl <- testEquality (typeRep @b) (typeRep @Word16) = Just fromIntegral
    | Just Refl <- testEquality (typeRep @b) (typeRep @Word8) = Just fromIntegral
    | Just Refl <- testEquality (typeRep @b) (typeRep @Double) = Just double
    | otherwise = Nothing
  where
    signed :: (Integral a) => a -> Word64
    signed x = fromIntegral (fromIntegral x :: Int64) `xor` 0x8000000000000000
    -- -0.0 maps to 0.0's key, as 'compare' treats them equal.
    double d =
        let w = castDoubleToWord64 (if d == 0 then 0 else d)
         in if testBit w 63 then complement w else w `xor` 0x8000000000000000

-- | Row indices ordered by key, ties in row order; skips constant bytes.
radixByKey :: VU.Vector Word64 -> VU.Vector Int
radixByKey keys
    | VU.null keys = VU.empty
    | otherwise = runST $ do
        let !k0 = VU.unsafeHead keys
            !varying = VU.foldl' (\acc k -> acc .|. (k `xor` k0)) 0 keys
            !passes = (64 - countLeadingZeros varying + 7) `div` 8
        pairs <- VU.thaw (VU.imap (\i k -> (k, i)) keys)
        Radix.sortBy
            passes
            256
            (\p (k, _) -> fromIntegral ((k `unsafeShiftR` (8 * p)) .&. 255))
            pairs
        VU.map snd <$> VU.unsafeFreeze pairs

sortIndices :: (Int -> Int -> Ordering) -> Int -> VU.Vector Int
sortIndices cmp nRows = runST $ do
    withIndexes <- VG.thaw (V.generate nRows id :: V.Vector Int)
    VA.sortBy cmp withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return (VU.convert sorted)

shuffle ::
    (RandomGen g) =>
    g ->
    DataFrame ->
    DataFrame
shuffle pureGen df =
    let
        indexes = shuffledIndices pureGen (fst (dimensions df))
     in
        df{columns = V.map (atIndicesStable indexes) (columns df)}

shuffledIndices :: (HasCallStack, RandomGen g) => g -> Int -> VU.Vector Int
shuffledIndices pureGen k
    | k < 0 = error $ "Vector index may not be a neative number: " <> show k
    | k == 0 = VU.empty
    | otherwise = shuffleVec pureGen
  where
    shuffleVec :: (RandomGen g) => g -> VU.Vector Int
    shuffleVec g = runST $ do
        vm <- VUM.generate k id
        go vm (k - 1) g
        VU.unsafeFreeze vm

    go _v i _ | i <= 0 = pure ()
    go v i gen =
        let
            (j, nextGen) = randomR (0, i) gen
         in
            VUM.swap v i j *> go v (i - 1) nextGen
