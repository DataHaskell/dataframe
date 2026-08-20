{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Execute a recognised aggregation plan ('AggPlan') through the vectorized
scatter kernel, producing one result column (length @nGroups@, canonical group
order). The scatter reductions live in 'DataFrame.Internal.AggKernel' (sequential)
and 'DataFrame.Internal.AggKernelPar' (parallel by disjoint group range); this
module handles the compound @max - min@ combine and the holistic grouped median.
A plan only reaches here once 'planAgg' verified the value columns are clean
unboxed Int/Double, so the @error@ branches are unreachable.

Every reduction takes the Round-5 grouping layout @(valueIndices, offsets)@ so
the parallel kernel can split the group-id range across capabilities with no
cross-worker merge. Each group's rows stay in original-row order within one
worker's range, so results are byte-identical to the sequential path at any @-N@.
(The one exception is the direct streaming Double sum/mean above the small-group
cutoff, whose deterministic chunked partials change the float summation order;
see 'DataFrame.Internal.AggKernelDirect'.)
-}
module DataFrame.Operations.AggregateScatter (runPlan, runMomentPlan) where

import qualified Data.Text as T
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.AggKernel (Reduction (..), scatterColumnToDouble)
import DataFrame.Internal.AggKernelDirect (
    directMaxMinusMin,
    directReduce,
    directThreshold,
 )
import DataFrame.Internal.AggKernelPar (momentScatterPar, scatterReducePar)
import DataFrame.Internal.AggPlan (AggPlan (..), MomentPlan (..), Moments (..))
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.DataFrame (GroupedDataFrame (..), getColumn)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

runPlan :: GroupedDataFrame -> VU.Vector Int -> Int -> AggPlan -> Column
runPlan gdf rtg nGroups plan = case plan of
    PlanScatter red name -> scatterColumn red name
    PlanMaxMinusMin a b ->
        {- min/max are order-independent, so the fused single-pass streaming
        kernel is exactly the two gather extrema it replaces; anything it
        rejects (mixed/unclean columns, wide domain) keeps the gather path. -}
        let ca = col a
            cb = col b
            direct
                | nGroups <= directThreshold = directMaxMinusMin rtg nGroups ca cb
                | otherwise = Nothing
         in case direct of
                Just out -> out
                Nothing -> maxMinusMin vis offs nGroups ca cb
    PlanMedian name -> groupedMedian vis offs nGroups (col name)
  where
    vis = valueIndices gdf
    offs = offsets gdf
    {- The low-cardinality DIRECT-INDEXED fast path: for a small dense domain the
    grouping layer's @rowToGroup@ already maps row -> group, so we scatter
    straight off it (no @valueIndices@ gather). 'directReduce' admits the
    order-independent reductions (exact partial merge, byte-identical to -N1)
    plus the streaming Double sum/mean/var/std variants (byte-identical
    sequential row order at small group counts; deterministic chunked partials
    for the large-domain sum/mean — see 'DataFrame.Internal.AggKernelDirect');
    anything it rejects keeps the order-preserving group-range kernel. -}
    scatterColumn red name =
        let c = col name
            direct
                | nGroups <= directThreshold = directReduce red rtg nGroups c
                | otherwise = Nothing
         in case direct of
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
per-expression path. Byte-identical to the sequential kernel at any @-N@.
-}
runMomentPlan ::
    GroupedDataFrame -> Int -> MomentPlan -> Maybe [(T.Text, Column)]
runMomentPlan gdf nGroups mp = do
    ms <- momentScatterPar vis offs nGroups (col (mpColX mp)) (col (mpColY mp))
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
-- Parallel holistic median
-------------------------------------------------------------------------------

{- | Holistic per-group median over a single unboxed Int/Double column. The
@valueIndices@/@offsets@ layout already places each group's rows in a contiguous
run, so we copy each group's values into a scratch buffer at its own offset and
sort that slice in place — each group's slice is independent, so the per-group
sorts split across capabilities by group range with no merge. Empty groups never
occur, so the result is total.
-}
groupedMedian :: VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column
groupedMedian vis offs nGroups c = case scatterColumnToDouble c of
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
    forEachRange bounds caps $ \gs ge ->
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
                    VA.sort slice
                    let mid = s + len `div` 2
                    med <-
                        if odd len
                            then VUM.unsafeRead buf mid
                            else do
                                hi <- VUM.unsafeRead buf mid
                                lo <- VUM.unsafeRead buf (mid - 1)
                                pure ((hi + lo) / 2)
                    VUM.unsafeWrite out g med
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups out)
{-# NOINLINE medianByGroup #-}

-------------------------------------------------------------------------------
-- Group-range partitioning (shared with the median path)
-------------------------------------------------------------------------------

{- | Split @[0, nGroups)@ into @caps@ contiguous group ranges balanced by row
count. Identical policy to 'DataFrame.Internal.AggKernelPar.groupRangeBounds'.
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

forEachRange :: VU.Vector Int -> Int -> (Int -> Int -> IO ()) -> IO ()
forEachRange bounds caps act
    | caps <= 1 = act (VU.unsafeIndex bounds 0) (VU.unsafeIndex bounds caps)
    | otherwise = do
        vars <- mapM spawn [0 .. caps - 1]
        results <- mapM takeMVar vars
        mapM_ (either (throwIO :: SomeException -> IO ()) pure) results
  where
    spawn w = do
        var <- newEmptyMVar
        let !s = VU.unsafeIndex bounds w
            !e = VU.unsafeIndex bounds (w + 1)
        _ <- forkIO (try (act s e) >>= putMVar var)
        pure var
