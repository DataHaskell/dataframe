{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Permutation where

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
import DataFrame.Internal.Column (Column (..), Columnable, atIndicesStable)
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (Col))
import DataFrame.Operations.Core (columnNames, dimensions)
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

getSortColumnName :: SortOrder -> T.Text
getSortColumnName (Asc (Col n)) = n
getSortColumnName (Desc (Col n)) = n
getSortColumnName _ = error "Sorting on compound column"

mustFlipCompare :: SortOrder -> Bool
mustFlipCompare (Asc _) = True
mustFlipCompare (Desc _) = False

{- | O(k log n) Sorts the dataframe by a given row.

> sortBy Ascending ["Age"] df
-}
sortBy ::
    [SortOrder] ->
    DataFrame ->
    DataFrame
sortBy sortOrds df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "sortBy"
                (columnNames df)
    | otherwise =
        let
            comparators = map (`sortOrderComparator` df) sortOrds
            compositeCompare i j = mconcat [c i j | c <- comparators]
            nRows = fst (dataframeDimensions df)
            indexes = sortIndices compositeCompare nRows
         in
            df{columns = V.map (atIndicesStable indexes) (columns df)}
  where
    names = map getSortColumnName sortOrds

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
sortOrderComparator (Desc (Col name :: Expr a)) df =
    case unsafeGetColumn name df of
        BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `V.unsafeIndex` j) (v `V.unsafeIndex` i)
            Nothing -> \_ _ -> EQ
        UnboxedColumn _ (v :: VU.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> \i j -> compare (v `VU.unsafeIndex` j) (v `VU.unsafeIndex` i)
            Nothing -> \_ _ -> EQ
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
