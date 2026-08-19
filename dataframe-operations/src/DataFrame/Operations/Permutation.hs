{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Permutation (
    -- * Sorting
    SortOrder (..),
    sortBy,

    -- * Shuffling
    shuffle,
    shuffledIndices,
) where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (runST)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Data.Vector.Internal.Check (HasCallStack)
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    atIndicesStable,
    materializeMerged,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnNames,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (Expr (Col), getColumns)
import DataFrame.Internal.PackedText (packedSlice, sliceCmpBytes)
import DataFrame.Operations.Core (dimensions)
import DataFrame.Operations.Transformations (derive)
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

sortOrderColumns :: SortOrder -> [T.Text]
sortOrderColumns (Asc e) = getColumns e
sortOrderColumns (Desc e) = getColumns e

mustFlipCompare :: SortOrder -> Bool
mustFlipCompare (Asc _) = True
mustFlipCompare (Desc _) = False

{- | Materialize any compound sort expressions into synthetic columns on
a working dataframe, returning rewritten 'SortOrder's that reference
those columns by name.
-}
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

{- | O(k log n) Sorts the dataframe by a given row.

> sortBy Ascending ["Age"] df
-}
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
            indexes = sortIndices compositeCompare nRows
         in
            df{columns = V.map (atIndicesStable indexes) (columns df)}
  where
    referenced = L.nub (concatMap sortOrderColumns sortOrds)
    missing = referenced L.\\ columnNames df

{- | Build a row-index comparator from a SortOrder and a DataFrame.
The Ord dictionary is recovered from the SortOrder GADT.
-}
sortOrderComparator :: SortOrder -> DataFrame -> Int -> Int -> Ordering
sortOrderComparator (Asc (Col name :: Expr a)) df =
    case unsafeGetColumn name df of
        BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `V.unsafeIndex` i) (v `V.unsafeIndex` j)
            Nothing -> \_ _ -> EQ
        UnboxedColumn _ (v :: VU.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `VU.unsafeIndex` i) (v `VU.unsafeIndex` j)
            Nothing -> \_ _ -> EQ
        PackedText _ p -> case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> \i j ->
                let (ai, oi, li) = packedSlice p i
                    (aj, oj, lj) = packedSlice p j
                 in sliceCmpBytes ai oi li aj oj lj
            Nothing -> \_ _ -> EQ
        c@(MergedColumn _ _) -> case materializeMerged c of
            BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> \i j -> compare (v `V.unsafeIndex` i) (v `V.unsafeIndex` j)
                Nothing -> \_ _ -> EQ
            _ -> \_ _ -> EQ
sortOrderComparator (Desc (Col name :: Expr a)) df =
    case unsafeGetColumn name df of
        BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `V.unsafeIndex` j) (v `V.unsafeIndex` i)
            Nothing -> \_ _ -> EQ
        UnboxedColumn _ (v :: VU.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `VU.unsafeIndex` j) (v `VU.unsafeIndex` i)
            Nothing -> \_ _ -> EQ
        PackedText _ p -> case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> \i j ->
                let (ai, oi, li) = packedSlice p i
                    (aj, oj, lj) = packedSlice p j
                 in sliceCmpBytes aj oj lj ai oi li
            Nothing -> \_ _ -> EQ
        c@(MergedColumn _ _) -> case materializeMerged c of
            BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> \i j -> compare (v `V.unsafeIndex` j) (v `V.unsafeIndex` i)
                Nothing -> \_ _ -> EQ
            _ -> \_ _ -> EQ
sortOrderComparator _ _ = error "Sorting on compound column"

-- | Sort row indices using a comparator function.
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
        let (n, nGen) = randomR (1, k - 1) g
        go vm n nGen
        VU.unsafeFreeze vm

    go _v (-1) _ = pure ()
    go _v 0 _ = pure ()
    go v maxInd gen =
        let
            (n, nextGen) = randomR (1, maxInd) gen
         in
            VUM.swap v 0 n *> go (VUM.tail v) (maxInd - 1) nextGen
