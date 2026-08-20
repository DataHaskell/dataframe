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

import qualified Data.List as L
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import DataFrame.Errors
import DataFrame.Internal.AggKernelPar (
    mkFusedAgg,
    mkGatherAgg,
    runFusedAggs,
    runGatherAggs,
    streamGroupCap,
 )
import DataFrame.Internal.AggKernel (Reduction (..))
import DataFrame.Internal.AggPlan (AggPlan (..), MomentPlan, planAgg, planMoments)
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
    atIndicesStableMulti,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
    getColumn,
    insertColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Grouping (buildRowToGroup, changingPoints, groupBy)
import DataFrame.Internal.Interpreter
import DataFrame.Internal.RowHash (computeRowHashesIO)
import DataFrame.Operations.AggregateScatter (
    runMedianVarFused,
    runMomentPlan,
    runPlan,
 )
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

NOTE: this function deliberately never pattern-matches or strictly binds the
'Grouped' per-row fields (this module is compiled with @-XStrict@, whose strict
patterns and bindings would force them): on direct-grouped frames BOTH
'valueIndices' (the placement permutation) and 'rowToGroup' are deferred
thunks, and each aggregation path needs at most one of them — always passed as
un-forced argument expressions. Key columns materialize through 'groupRepRows'
(one representative row per group) instead of gathering
@valueIndices[offsets[g]]@.
-}
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf =
    let
        df = fullDataframe gdf
        offs = offsets gdf

        {- Key columns materialize through ONE fused parallel gather over the
        representative rows ('atIndicesStableMulti'): the 1e8-group Q10 result
        was six sequential latency-bound random-gather passes as per-column
        'selectIndices'. Each result column is still identical to (and as
        deferred as) the per-column gather; the row-count field uses the eager
        @offsets@ so nothing here forces 'groupRepRows' early. -}
        df' =
            let sub = select (groupedColumns gdf) df
             in sub
                    { columns =
                        V.fromList
                            ( atIndicesStableMulti
                                (groupRepRows gdf)
                                (V.toList (columns sub))
                            )
                    , dataframeDimensions = (nGroups, snd (dataframeDimensions sub))
                    }

        !nGroups = VU.length offs - 1
        !nRows' = fst (dataframeDimensions df)

        {- Fused multi-reduction fast path (Q3/Q4/Q5-shaped aggregates): every
        recognised simple scatter reduction (sum/mean/count/min/max over a clean
        unboxed Int/Double column) in this aggregate runs in ONE pass instead of
        one full pass per expression. At or below 'streamGroupCap' groups that
        pass streams over (rowToGroup, columns) with per-worker accumulators —
        never touching the (possibly lazy) valueIndices; above it (necessarily a
        hash-path grouping, whose valueIndices is already eager) the accumulator
        arrays would thrash, so the pass gathers by disjoint group range instead
        (register accumulators, bit-identical to the unfused gather kernels,
        one traversal instead of one per expression). Only taken when at
        least two reductions fuse; non-fusable expressions (median, var/std,
        max-min, arbitrary DSL) keep their per-expression path below. -}
        {- This binding is strict (-XStrict), so it must stay empty-and-cheap
        whenever the moment path below already covers the aggregate — otherwise
        the pass would run redundantly before the moment result is consulted. -}
        fusedScatterCols :: MS.Map T.Text Column
        fusedScatterCols = case fusedMoments of
            Just _ -> MS.empty
            Nothing
                | nGroups <= streamGroupCap ->
                    let cands =
                            [ (name, fa)
                            | (name, ue) <- aggs
                            , Just (PlanScatter red cname) <- [planAgg gdf ue]
                            , Just c <- [getColumn cname df]
                            , Just fa <- [mkFusedAgg nGroups (rowToGroup gdf) red c]
                            ]
                     in if length cands >= 2
                            then
                                MS.fromList
                                    (zip (map fst cands) (runFusedAggs nRows' nGroups (map snd cands)))
                            else MS.empty
                | otherwise ->
                    let cands =
                            [ (name, ga)
                            | (name, ue) <- aggs
                            , Just (PlanScatter red cname) <- [planAgg gdf ue]
                            , Just c <- [getColumn cname df]
                            , Just ga <-
                                [mkGatherAgg nGroups (valueIndices gdf) offs red c]
                            ]
                     in {- Unlike the stream branch, a SINGLE candidate also
                        takes this path: the per-expression fallback is a
                        scatter over rowToGroup, which on a hash-path grouping
                        is now a deferred thunk — the gather kernel (documented
                        bit-identical to the unfused kernels) works off the
                        already-eager valueIndices instead and skips that whole
                        random-write pass. -}
                        if not (null cands)
                            then
                                MS.fromList
                                    ( zip
                                        (map fst cands)
                                        ( runGatherAggs
                                            (valueIndices gdf)
                                            offs
                                            nGroups
                                            (map snd cands)
                                        )
                                    )
                            else MS.empty

        {- Fused median + std/var over one column (the Q6 shape): both are
        holistic gathers over the same values, so one shared gather serves the
        Welford fold and the median selection ('runMedianVarFused',
        bit-identical to the separate kernels). Only built when a median and a
        std/var on the same column appear together; empty-and-cheap otherwise
        (same strictness caveat as 'fusedScatterCols'). -}
        medianVarCols :: MS.Map T.Text Column
        medianVarCols = case fusedMoments of
            Just _ -> MS.empty
            Nothing ->
                let plans = [(name, plan) | (name, ue) <- aggs, Just plan <- [planAgg gdf ue]]
                    medCols = L.nub [c | (_, PlanMedian c) <- plans]
                 in MS.fromList
                        [ kv
                        | cname <- medCols
                        , let stds = [nm | (nm, PlanScatter RStd c) <- plans, c == cname]
                        , let vars = [nm | (nm, PlanScatter RVar c) <- plans, c == cname]
                        , not (null stds && null vars)
                        , Just c <- [getColumn cname df]
                        , Just (medC, varC, stdC) <- [runMedianVarFused gdf nGroups c]
                        , kv <-
                            [(nm, medC) | (nm, PlanMedian c') <- plans, c' == cname]
                                ++ map (\nm -> (nm, stdC)) stds
                                ++ map (\nm -> (nm, varC)) vars
                        ]

        -- Fast path: a recognised reduction scatters in one unboxed pass.
        -- Anything 'planAgg' rejects keeps the existing interpreter, so the
        -- general typed + DSL aggregate API stays correct for arbitrary
        -- expressions.
        f ne@(name, uexpr) d =
            let value = case MS.lookup name medianVarCols of
                    Just c -> c
                    Nothing -> case MS.lookup name fusedScatterCols of
                        Just c -> c
                        Nothing -> case planAgg gdf uexpr of
                            Just plan -> runPlan gdf (rowToGroup gdf) nGroups plan
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
    -- The trailing field stays a wildcard: under -XStrict a named pattern
    -- variable would force the (possibly deferred) rowToGroup thunk.
    (Grouped _ _ indices os _) = groupBy (columnNames df) df
