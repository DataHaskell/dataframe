{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Execute a recognised aggregation plan ('AggPlan'), producing one result
column (length @nGroups@, canonical group order).

This is the dispatch layer: it owns the policy of WHICH kernel runs — the dense
direct-indexed one ('DataFrame.Internal.Aggregation.Kernel.Dense') when the
group domain is small enough, otherwise the group-range scatter
('DataFrame.Internal.Aggregation.Kernel.Scatter') — and handles the compound
@max - min@ combine and the holistic grouped median itself. The kernels carry no
policy of their own. A plan only reaches here once 'planAgg' verified the value
columns are clean unboxed Int/Double, so the @error@ branches are unreachable.

Every reduction takes the Round-5 grouping layout @(valueIndices, offsets)@ so
the parallel kernel can split the group-id range across capabilities with no
cross-worker merge. Each group's rows stay in original-row order within one
worker's range, so results are byte-identical to the sequential path at any @-N@.
(Two exceptions, both deterministic at a fixed @-N@: the direct streaming
Double sum/mean above the small-group cutoff, whose chunked partials change the
float summation order, and the direct var/std, which finalize from
(count, sum, sumsq) partials rather than the gather kernel's row-order Welford
recurrence; see 'DataFrame.Internal.AggKernelDirect'.)
-}
module DataFrame.Operations.Aggregation.Run (
    runPlan,
    runMomentPlan,
    runMedianVarFused,
) where

import qualified Data.Text as T
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Concurrent (getNumCapabilities)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.Aggregation.Kernel.Dense (
    denseMaxMinusMin,
    denseReduce,
 )
import DataFrame.Internal.Aggregation.Kernel.Moments (
    Moments (..),
    momentScatterPar,
    momentStreamPar,
 )
import DataFrame.Internal.Aggregation.Kernel.Scatter (
    maxMinusMinScatterPar,
    scatterReducePar,
    streamGroupCap,
 )
import DataFrame.Internal.Aggregation.Plan (
    AggPlan (..),
    MomentPlan (..),
 )
import DataFrame.Internal.Aggregation.Reduction (
    Reduction (..),
    cleanDoubleVector,
 )
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (parallelBounds_)
import DataFrame.Internal.DataFrame (GroupedDataFrame (..), getColumn)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

{- | Group-domain size at or below which the dense direct-indexed kernel is
chosen; wider domains go to the group-range scatter kernel. A @2^18@-slot
accumulator is replicated per worker there, which is what bounds this. Dispatch
policy, so it lives with the dispatcher rather than inside the kernel.
-}
denseThreshold :: Int
denseThreshold = 262144

runPlan :: GroupedDataFrame -> VU.Vector Int -> Int -> AggPlan -> Column
runPlan gdf rtg nGroups plan = case plan of
    PlanScatter red name -> scatterColumn red name
    PlanMaxMinusMin a b ->
        {- min/max are order-independent, so both fused single-pass kernels
        (the direct streaming one up to 'streamGroupCap', the group-range
        gather one above it) are exactly the two gather extrema they replace;
        anything they reject (mixed/unclean columns, small inputs) keeps the
        two-pass gather path. The streaming cap extends past 'denseThreshold'
        for the same reason as the fused multi-reduction pass: on a
        direct-grouped frame it works off the eager @rowToGroup@ and skips the
        deferred @valueIndices@ placement entirely (measured at 1e6 groups /
        1e8 rows on -N16: stream 1.1s against placement 0.7s + gather 0.7s). -}
        let ca = col a
            cb = col b
            direct
                | nGroups <= streamGroupCap = denseMaxMinusMin rtg nGroups ca cb
                | otherwise = maxMinusMinScatterPar vis offs nGroups ca cb
         in case direct of
                Just out -> out
                Nothing -> maxMinusMin vis offs nGroups ca cb
    PlanMedian name -> groupedMedian vis offs nGroups (col name)
  where
    vis = valueIndices gdf
    offs = offsets gdf
    {- The low-cardinality DENSE fast path: for a small dense domain the grouping
    layer's @rowToGroup@ already maps row -> group, so we scatter straight off it
    (no @valueIndices@ gather). 'denseReduce' admits the order-independent
    reductions (exact partial merge, byte-identical to -N1) plus the streaming
    Double sum/mean/var/std variants (byte-identical sequential row order at
    small group counts; deterministic chunked partials for the large-domain
    sum/mean — see "DataFrame.Internal.Aggregation.Kernel.Dense"); anything it
    rejects keeps the order-preserving group-range kernel. -}
    scatterColumn red name =
        let c = col name
            dense
                | nGroups <= denseThreshold = denseReduce red rtg nGroups c
                {- Top-2 selection merges exactly (a multiset selection, no
                float adds until finalize), so like the fused passes it streams
                off @rowToGroup@ up to 'streamGroupCap': on a direct-grouped
                frame that skips the deferred @valueIndices@ placement, which
                costs more than the accumulator cache misses it saves
                (measured at 1e6 groups / 1e8 rows on -N16: stream 1.0s
                against placement 0.7s + gather 0.4s). RTop2Snd shares the
                same accumulator machinery and merge-exactness. -}
                | red == RTop2Sum || red == RTop2Snd
                , nGroups <= streamGroupCap =
                    denseReduce red rtg nGroups c
                | otherwise = Nothing
         in case dense of
                Just out -> out
                Nothing -> case scatterReducePar red vis offs nGroups c of
                    Just out -> out
                    Nothing -> error "runPlan: scatterReducePar rejected a planned column"
    col name = case getColumn name (fullDataframe gdf) of
        Just c -> c
        Nothing -> error ("runPlan: planned column missing: " ++ T.unpack name)

{- | Run a recognised moment (Q9 regression) plan as one fused scatter over the
two base columns, returning each output name bound to its moment field. The six
sufficient statistics (count, Sx, Sy, Sxx, Syy, Sxy) come out of a single pass,
replacing the three derive passes and six independent scatters of the
per-expression path. The streaming kernel's count is exact; its five Double
sums accumulate per worker chunk in original row order and merge in fixed
worker order — deterministic at a fixed @-N@, float summation order chunk-major
rather than per-group (see 'momentStreamPar'). The gather fallback remains
byte-identical to the sequential kernel at any @-N@.
-}
runMomentPlan ::
    GroupedDataFrame -> Int -> MomentPlan -> Maybe [(T.Text, Column)]
runMomentPlan gdf nGroups mp = do
    let cx = col (mpColX mp)
        cy = col (mpColY mp)
        {- Preferred: the streaming kernel — one fused pass over rowToGroup and
        the TYPED base columns (no sequential Int->Double materialization, no
        valueIndices gather, so a direct-grouped frame never runs its placement
        pass). Falls back to the gather kernel above 'streamGroupCap' or on
        unclean columns. -}
        streamed = momentStreamPar (rowToGroup gdf) nGroups cx cy
    ms <- case streamed of
        Just m -> Just m
        Nothing -> momentScatterPar vis offs nGroups cx cy
    pure
        [ (mpNName mp, mN ms)
        , (mpSxName mp, mSx ms)
        , (mpSyName mp, mSy ms)
        , (mpSxxName mp, mSxx ms)
        , (mpSyyName mp, mSyy ms)
        , (mpSxyName mp, mSxy ms)
        ]
  where
    vis = valueIndices gdf
    offs = offsets gdf
    col name = case getColumn name (fullDataframe gdf) of
        Just c -> c
        Nothing -> error ("runMomentPlan: planned column missing: " ++ T.unpack name)

{- | @max a - min b@ on the small @nGroups@ arrays. Preserves the Int element
type of the source columns (matching the interpreter), falling back to a Double
combine otherwise.
-}
maxMinusMin ::
    VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column -> Column
maxMinusMin vis offs nGroups ca cb =
    case (ca, cb) of
        ( UnboxedColumn Nothing (_ :: VU.Vector x)
            , UnboxedColumn Nothing (_ :: VU.Vector y)
            )
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    let mx = scatterExtremaInt RMax vis offs nGroups ca
                        mn = scatterExtremaInt RMin vis offs nGroups cb
                     in fromUnboxedVector (VU.zipWith (-) mx mn)
        _ ->
            let mx = scatterExtremaDbl RMax vis offs nGroups ca
                mn = scatterExtremaDbl RMin vis offs nGroups cb
             in fromUnboxedVector (VU.zipWith (-) mx mn)

scatterExtremaInt ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> VU.Vector Int
scatterExtremaInt red vis offs nGroups c = case scatterReducePar red vis offs nGroups c of
    Just (UnboxedColumn _ (v :: VU.Vector a))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int) -> v
    _ -> error "scatterExtremaInt"

scatterExtremaDbl ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> VU.Vector Double
scatterExtremaDbl red vis offs nGroups c =
    case scatterReducePar red vis offs nGroups c of
        Just (UnboxedColumn _ (v :: VU.Vector a))
            | Just Refl <- testEquality (typeRep @a) (typeRep @Double) -> v
            | Just Refl <- testEquality (typeRep @a) (typeRep @Int) -> VU.map fromIntegral v
        _ -> error "scatterExtremaDbl"

-------------------------------------------------------------------------------
-- Fused holistic median + var/std over one shared gather
-------------------------------------------------------------------------------

{- | Fused grouped median and variance family over the SAME column: one gather
into the shared scratch buffer serves both. Returns
@(median, variance, stddev)@ columns, or 'Nothing' on a non-numeric column
(the caller keeps the separate per-expression kernels).

The Welford fold runs over each gathered slice in ascending original-row order
— exactly the recurrence, order and finalize of the var/std kernels
('DataFrame.Internal.AggKernelPar.varPar' and the sequential @varScatter@,
which agree bit-for-bit) — BEFORE the in-place median selection permutes the
slice, and the selection then proceeds exactly as 'groupedMedian'. Both
outputs are therefore bit-identical to the unfused paths; the second full
gather pass is what the fusion saves (measured ~35% off the median+sd pair at
1e4 groups / 1e8 rows on -N16).
-}
runMedianVarFused ::
    GroupedDataFrame -> Int -> Column -> Maybe (Column, Column, Column)
runMedianVarFused gdf nGroups c = do
    vals <- cleanDoubleVector c
    let (med, var) =
            medianVarByGroup (valueIndices gdf) (offsets gdf) nGroups vals
    pure
        ( fromUnboxedVector med
        , fromUnboxedVector var
        , fromUnboxedVector (VU.map sqrt var)
        )

medianVarByGroup ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    (VU.Vector Double, VU.Vector Double)
medianVarByGroup vis offs nGroups vals = unsafePerformIO $ do
    let !n = VU.length vis
    buf <- VUM.new (max 1 n)
    medOut <- VUM.new (max 1 nGroups)
    varOut <- VUM.new (max 1 nGroups)
    caps <- getNumCapabilities
    let !bounds = groupRangeBounds offs nGroups caps
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !s = VU.unsafeIndex offs g
                        !e = VU.unsafeIndex offs (g + 1)
                        !len = e - s
                        fill !pos
                            | pos >= e = pure ()
                            | otherwise = do
                                VUM.unsafeWrite buf pos (VU.unsafeIndex vals (VU.unsafeIndex vis pos))
                                fill (pos + 1)
                    fill s
                    -- Welford over the gathered slice (still in row order).
                    let welford !pos !c !mu !mm
                            | pos >= e =
                                pure (if c < 2 then 0 else mm / fromIntegral (c - 1))
                            | otherwise = do
                                x <- VUM.unsafeRead buf pos
                                let !c' = c + 1
                                    !delta = x - mu
                                    !mu' = mu + delta / fromIntegral c'
                                    !mm' = mm + delta * (x - mu')
                                welford (pos + 1) c' mu' mm'
                    var <- welford s (0 :: Int) 0 0
                    VUM.unsafeWrite varOut g var
                    -- Median selection, as in 'medianByGroup' (permutes the slice).
                    let slice = VUM.unsafeSlice s len buf
                        !mid = len `div` 2
                    VA.select slice (mid + 1)
                    let scan !i !hi !lo
                            | i > mid = pure (hi, lo)
                            | otherwise = do
                                x <- VUM.unsafeRead slice i
                                if x > hi
                                    then scan (i + 1) x hi
                                    else scan (i + 1) hi (max lo x)
                    (hi, lo) <- scan 0 (negate (1 / 0)) (negate (1 / 0))
                    let med = if odd len then hi else (hi + lo) / 2
                    VUM.unsafeWrite medOut g med
                    grp (g + 1)
         in grp gs
    med <- VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups medOut)
    var <- VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups varOut)
    pure (med, var)
{-# NOINLINE medianVarByGroup #-}

-------------------------------------------------------------------------------
-- Parallel holistic median
-------------------------------------------------------------------------------

{- | Holistic per-group median over a single unboxed Int/Double column. The
@valueIndices@/@offsets@ layout already places each group's rows in a contiguous
run, so we copy each group's values into a scratch buffer at its own offset and
select the median-rank order statistics in that slice in place (O(len) per
group rather than the O(len log len) full sort) — each group's slice is
independent, so the per-group selections split across capabilities by group
range with no merge. Order statistics are value-determined, so the result is
identical to the sorting variant. Empty groups never occur, so the result is
total.
-}
groupedMedian :: VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column
groupedMedian vis offs nGroups c = case cleanDoubleVector c of
    Nothing -> error "groupedMedian: non-numeric planned column"
    Just vals -> fromUnboxedVector (medianByGroup vis offs nGroups vals)

medianByGroup ::
    VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
medianByGroup vis offs nGroups vals = unsafePerformIO $ do
    let !n = VU.length vis
    buf <- VUM.new (max 1 n)
    out <- VUM.new (max 1 nGroups)
    caps <- getNumCapabilities
    let !bounds = groupRangeBounds offs nGroups caps
    -- Each worker fills+sorts the buffer slices of its own group range, then
    -- writes that range's medians. Disjoint ranges => safe to parallelise.
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !s = VU.unsafeIndex offs g
                        !e = VU.unsafeIndex offs (g + 1)
                        !len = e - s
                        fill !pos
                            | pos >= e = pure ()
                            | otherwise = do
                                VUM.unsafeWrite buf pos (VU.unsafeIndex vals (VU.unsafeIndex vis pos))
                                fill (pos + 1)
                    fill s
                    let slice = VUM.unsafeSlice s len buf
                        !mid = len `div` 2
                    {- Move the least mid+1 values to the front (in no
                    particular order); the two largest of those are the order
                    statistics at sorted positions mid and mid-1. -}
                    VA.select slice (mid + 1)
                    let scan !i !hi !lo
                            | i > mid = pure (hi, lo)
                            | otherwise = do
                                x <- VUM.unsafeRead slice i
                                if x > hi
                                    then scan (i + 1) x hi
                                    else scan (i + 1) hi (max lo x)
                    (hi, lo) <- scan 0 (negate (1 / 0)) (negate (1 / 0))
                    let med = if odd len then hi else (hi + lo) / 2
                    VUM.unsafeWrite out g med
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups out)
{-# NOINLINE medianByGroup #-}

-------------------------------------------------------------------------------
-- Group-range partitioning (shared with the median path)
-------------------------------------------------------------------------------

{- | Split @[0, nGroups)@ into @caps@ contiguous group ranges balanced by row
count. Identical policy to 'DataFrame.Internal.Aggregation.Kernel.Scatter.groupRangeBounds'.
-}
groupRangeBounds :: VU.Vector Int -> Int -> Int -> VU.Vector Int
groupRangeBounds offs nGroups caps = VU.create $ do
    b <- VUM.new (caps + 1)
    let !nRows = VU.unsafeIndex offs nGroups
        !per = max 1 ((nRows + caps - 1) `div` caps)
        adv !target !gg
            | gg >= nGroups = nGroups
            | VU.unsafeIndex offs gg >= target = gg
            | otherwise = adv target (gg + 1)
        go !w !prev
            | w >= caps = VUM.unsafeWrite b caps nGroups
            | otherwise = do
                let !target = min nRows (w * per)
                    !g = adv target prev
                VUM.unsafeWrite b w g
                go (w + 1) g
    VUM.unsafeWrite b 0 0
    go 1 0
    pure b
