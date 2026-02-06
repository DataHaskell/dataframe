{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Operations.Permutation where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Expression
import DataFrame.Internal.Row
import DataFrame.Operations.Core
import System.Random

-- | Sort order taken as a parameter by the 'sortBy' function.
data SortOrder where
    Asc :: (Columnable a) => Expr a -> SortOrder
    Desc :: (Columnable a) => Expr a -> SortOrder

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
            ColumnNotFoundException
                (T.pack $ show $ names L.\\ columnNames df)
                "sortBy"
                (columnNames df)
    | otherwise =
        let
            indexes = sortedIndexes' mustFlips (toRowVector names df)
         in
            df{columns = V.map (atIndicesStable indexes) (columns df)}
  where
    names = map getSortColumnName sortOrds
    mustFlips = map mustFlipCompare sortOrds

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

shuffledIndices :: (RandomGen g) => g -> Int -> VU.Vector Int
shuffledIndices pureGen k = VU.fromList (fst (uniformShuffleList [0 .. (k - 1)] pureGen))
