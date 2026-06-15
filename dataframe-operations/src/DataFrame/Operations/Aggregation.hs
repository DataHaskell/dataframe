{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation (
    module DataFrame.Operations.Aggregation,
    groupBy,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import DataFrame.Errors
import DataFrame.Internal.AggPlan (MomentPlan, planAgg, planMoments)
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
    insertColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Grouping (buildRowToGroup, changingPoints, groupBy)
import DataFrame.Internal.Interpreter
import DataFrame.Internal.RowHash (computeRowHashesIO)
import DataFrame.Operations.AggregateScatter (runMomentPlan, runPlan)
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import System.IO.Unsafe (unsafePerformIO)

{- | Per-row key hash over the selected key columns. Delegates to the shared
'computeRowHashesIO' kernel, which forks over contiguous row ranges for large
frames (the hashing of a wide 1e7-row text/factor join key dominates that join)
and is bit-for-bit identical to a single sequential pass at any capability count.
-}
computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df =
    let n = fst (dimensions df)
        selectedCols = map (columns df V.!) indices
     in unsafePerformIO (computeRowHashesIO n selectedCols)
{-# NOINLINE computeRowHashes #-}

{- | Aggregate a grouped dataframe using the expressions given.
All ungrouped columns will be dropped.
-}
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf@(Grouped df groupingColumns valIndices offs rowToGroupV) =
    let
        df' =
            selectIndices
                (VU.map (valIndices VU.!) (VU.init offs))
                (select groupingColumns df)

        !nGroups = VU.length offs - 1

        -- Fast path: a recognised reduction scatters in one unboxed pass.
        -- Anything 'planAgg' rejects keeps the existing interpreter, so the
        -- general typed + DSL aggregate API stays correct for arbitrary
        -- expressions.
        f ne@(name, uexpr) d =
            let value = case planAgg gdf uexpr of
                    Just plan -> runPlan gdf rowToGroupV nGroups plan
                    Nothing -> interpretNamed gdf ne
             in insertColumn name value d

        -- Fused fast path: the Q9 regression family (count + five moment sums
        -- of two base columns) becomes one scatter over the base columns,
        -- dropping the derived product columns and the six separate folds.
        -- 'planMoments' returns 'Nothing' on any other set, falling back below.
        fusedMoments = do
            mp <- planMoments gdf aggs :: Maybe MomentPlan
            runMomentPlan gdf nGroups mp
     in
        case fusedMoments of
            Just cols -> fold (uncurry insertColumn) cols df'
            Nothing -> fold f aggs df'

-- | The fall-back path: evaluate one named aggregation via the interpreter.
interpretNamed :: GroupedDataFrame -> NamedExpr -> Column
interpretNamed gdf (_, UExpr (expr :: Expr a)) =
    case interpretAggregation @a gdf expr of
        Left e -> throw e
        Right (UnAggregated _) -> throw $ UnaggregatedException (T.pack $ show expr)
        Right (Aggregated (TColumn col)) -> col

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = V.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VU.length xs, V.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VU.map (indices VU.!) (VU.init os)) df
  where
    (Grouped _ _ indices os _rtg) = groupBy (columnNames df) df
