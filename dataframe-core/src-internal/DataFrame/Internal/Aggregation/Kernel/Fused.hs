{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Fused multi-reduction aggregation kernels: several reductions over the
SAME grouping, evaluated in one pass over the rows.

Two shapes, picked by group-domain width:

* 'mkFusedAgg' \/ 'runFusedAggs' — the STREAMING pass, driven by @rowToGroup@.
  No @valueIndices@ and no placement pass; each worker keeps a private
  accumulator per reduction and the partials merge over group slices. Capped at
  'streamGroupCap', above which the per-worker arrays overflow cache.

* 'mkGatherAgg' \/ 'runGatherAggs' — the GATHER pass, driven by the grouped
  @(valueIndices, offsets)@ layout, for group domains too wide to stream.

Both amortize the memory traffic of the grouping across every reduction in the
batch instead of re-reading it once per reduction.
-}
module DataFrame.Internal.Aggregation.Kernel.Fused (
    FusedAgg,
    mkFusedAgg,
    runFusedAggs,
    GatherAgg,
    mkGatherAgg,
    runGatherAggs,
) where

import Control.Exception (evaluate)
import Control.Monad (when)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Kernel.Scatter (
    groupRangeBounds,
    groupSlices,
    overGroupsAcc,
    streamGroupCap,
 )
import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

-------------------------------------------------------------------------------
-- Fused multi-reduction streaming pass
-------------------------------------------------------------------------------

{- | One fused reduction of a multi-reduction streaming pass: how to allocate a
per-worker accumulator, fold a row range into it, merge another worker's
accumulator into it over a group range (callers merge in worker order), and
finalize the fully merged accumulator into the output column.
-}
data FusedAgg
    = forall s.
      FusedAgg
        (IO s)
        -- \^ allocate one worker's accumulator
        (s -> Int -> Int -> IO ())
        -- \^ accumulate rows [lo, hi) in ascending order
        (s -> s -> Int -> Int -> IO ())
        -- \^ merge the second accumulator into the first over groups [lo, hi)
        (s -> IO Column)
        -- \^ finalize the merged accumulator

{- | Below this many groups a Double sum/mean does NOT join the fused streaming
pass: the per-expression kernel it would replace
('DataFrame.Internal.AggKernelDirect.sumDblDirect' under its @seqFloatGroups@
policy, value mirrored here) runs those as ONE sequential row-order pass that
is byte-identical to the gather kernels and the interpreter, and the vectorized
parity gate asserts exactly that. Above it the per-expression kernel already
merges chunk partials in worker order, so fusing changes nothing semantically
new. Int reductions and count/min/max are exact under any merge and always
fuse.
-}
fusedSeqFloatGroups :: Int
fusedSeqFloatGroups = 65536

{- | Build the fused-pass reduction for one @(reduction, column)@ pair, or
'Nothing' when the pair cannot stream (nullable/boxed columns; the
order-sensitive var/std/top2 reductions, which keep their per-expression
kernels; or a Double sum/mean below 'fusedSeqFloatGroups', which keeps its
byte-identical sequential pass). Sum/min/max/count/mean over Int are exact
under any chunk merge; the admitted Double sum and mean merge their per-worker
partials in worker order (deterministic at a fixed @-N@, chunk-major float
summation order).
-}
mkFusedAgg :: Int -> VU.Vector Int -> Reduction -> Column -> Maybe FusedAgg
mkFusedAgg nGroups rtg red col
    | nGroups <= 0 || nGroups > streamGroupCap = Nothing
    | otherwise = case col of
        UnboxedColumn Nothing (v :: VU.Vector a)
            | Just Refl <- testEquality (typeRep @a) (typeRep @Int) ->
                case red of
                    RCount -> Just (countFusedAgg nGroups rtg)
                    RSum -> Just (sumIntFusedAgg nGroups rtg v)
                    RMean -> Just (meanIntFusedAgg nGroups rtg v)
                    RMin -> Just (extremaIntFusedAgg True nGroups rtg v)
                    RMax -> Just (extremaIntFusedAgg False nGroups rtg v)
                    _ -> Nothing
            | Just Refl <- testEquality (typeRep @a) (typeRep @Double) ->
                case red of
                    RCount -> Just (countFusedAgg nGroups rtg)
                    RSum
                        | nGroups > fusedSeqFloatGroups ->
                            Just (sumDblFusedAgg nGroups rtg v)
                    RMean
                        | nGroups > fusedSeqFloatGroups ->
                            Just (meanDblFusedAgg nGroups rtg v)
                    RMin -> Just (extremaDblFusedAgg True nGroups rtg v)
                    RMax -> Just (extremaDblFusedAgg False nGroups rtg v)
                    -- Top-2 selection is an exact multiset selection (no float
                    -- arithmetic before finalize), so its per-worker merge is
                    -- byte-identical to the per-expression kernels at any -N.
                    RTop2Snd -> Just (top2SndDblFusedAgg nGroups rtg v)
                    _ -> Nothing
        _ -> Nothing

{- | Execute all fused reductions in ONE pass over @rowToGroup@ and the value
columns: the rows are split into one contiguous chunk per capability, each
worker walks its chunk in 4096-row blocks running every reduction's
accumulate-step on the block (the block's @rowToGroup@ slice stays in L1
across the k steps), and each reduction then merges its per-worker partials in
fixed worker order (parallel over group slices) and finalizes. Per-group update
order within each worker is ascending original row order, and chunk boundaries
are a fixed function of the row and capability counts, so the result is
deterministic at a fixed @-N@; on a single capability it is bit-identical to
running each unfused sequential scatter kernel separately.

Pure w.r.t. its immutable inputs (deterministic fan-out and merge order), so
the 'unsafePerformIO' is safe.
-}
runFusedAggs :: Int -> Int -> [FusedAgg] -> [Column]
runFusedAggs n nGroups aggs = unsafePerformIO $ do
    let !caps' = if shouldPar n then capabilities else 1
        !per = (max 1 n + caps' - 1) `div` caps'
    opened <- mapM (openFusedAgg caps' nGroups) aggs
    let stepsFor w = map (\(steps, _) -> steps !! w) opened
    _ <-
        forkJoin
            [ blockRun (stepsFor w) lo hi
            | w <- [0 .. caps' - 1]
            , let lo = min n (w * per)
            , let hi = min n (lo + per)
            ]
    mapM snd opened
{-# NOINLINE runFusedAggs #-}

{- | Open one fused reduction for @caps'@ workers: its per-worker step
functions (worker order) and the merge+finalize action.
-}
openFusedAgg :: Int -> Int -> FusedAgg -> IO ([Int -> Int -> IO ()], IO Column)
openFusedAgg caps' nGroups (FusedAgg new step mergeR fin) = do
    ss <- sequence (replicate caps' new)
    let finish = case ss of
            [] -> error "runFusedAggs: no workers"
            (s0 : rest) -> do
                _ <-
                    forkJoin
                        [ mapM_ (\s -> mergeR s0 s lo hi) rest
                        | (lo, hi) <- groupSlices nGroups
                        ]
                fin s0
    pure (map step ss, finish)

-- | Rows per fused block: the block's rowToGroup slice (32KB) stays in L1.
fusedBlock :: Int
fusedBlock = 4096

blockRun :: [Int -> Int -> IO ()] -> Int -> Int -> IO ()
blockRun steps lo0 hi = go lo0
  where
    go !lo
        | lo >= hi = pure ()
        | otherwise = do
            let !e = min hi (lo + fusedBlock)
            mapM_ (\s -> s lo e) steps
            go e

-- Individual fused reductions. Each step loop is monomorphic.

countFusedAgg :: Int -> VU.Vector Int -> FusedAgg
countFusedAgg nGroups rtg =
    FusedAgg
        (VUM.replicate nGroups (0 :: Int))
        (countStepK rtg)
        addIntRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

countStepK :: VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
countStepK rtg acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + 1)
            go (i + 1)

sumIntFusedAgg :: Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
sumIntFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (0 :: Int))
        (sumStepInt rtg v)
        addIntRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

sumStepInt ::
    VU.Vector Int -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
sumStepInt rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
            go (i + 1)

sumDblFusedAgg :: Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
sumDblFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (0 :: Double))
        (sumStepDbl rtg v)
        addDblRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

sumStepDbl ::
    VU.Vector Int -> VU.Vector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
sumStepDbl rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
            go (i + 1)

{- | The mean aggs hold their (sum, count) pair INTERLEAVED in one array —
slots @2g@/@2g+1@ share a cache line (pairs are 16-byte aligned, so they never
straddle one), halving the accumulator misses of the random per-row update
against two separate arrays (measured ~13% off a fused sum+mean pass at 1e6
groups / 1e8 rows on -N16). The count is exact in both layouts (an integer, or
integer-valued Double additions well below 2^53), so sums, merges and the
finalize divide are bit-identical to the two-array layout.
-}
meanIntFusedAgg :: Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
meanIntFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate (2 * nGroups) (0 :: Int))
        (meanStepInt rtg v)
        (\a b lo hi -> addIntRange a b (2 * lo) (2 * hi))
        ( \s -> do
            sv <- VU.unsafeFreeze s
            pure
                ( fromUnboxedVector
                    ( VU.generate
                        nGroups
                        ( \g ->
                            let !sx = VU.unsafeIndex sv (2 * g)
                                !cx = VU.unsafeIndex sv (2 * g + 1)
                             in if cx == 0
                                    then 0 / 0
                                    else fromIntegral sx / fromIntegral cx :: Double
                        )
                    )
                )
        )

meanStepInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
meanStepInt rtg v s lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k2 = 2 * VU.unsafeIndex rtg i
            sv <- VUM.unsafeRead s k2
            VUM.unsafeWrite s k2 (sv + VU.unsafeIndex v i)
            cv <- VUM.unsafeRead s (k2 + 1)
            VUM.unsafeWrite s (k2 + 1) (cv + 1)
            go (i + 1)

-- | See 'meanIntFusedAgg' for the interleaved accumulator layout.
meanDblFusedAgg :: Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
meanDblFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate (2 * nGroups) (0 :: Double))
        (meanStepDbl rtg v)
        (\a b lo hi -> addDblRange a b (2 * lo) (2 * hi))
        ( \s -> do
            sv <- VU.unsafeFreeze s
            pure
                ( fromUnboxedVector
                    ( VU.generate
                        nGroups
                        ( \g ->
                            let !sx = VU.unsafeIndex sv (2 * g)
                                !cx = VU.unsafeIndex sv (2 * g + 1)
                             in if cx == 0 then 0 / 0 else sx / cx
                        )
                    )
                )
        )

meanStepDbl ::
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
meanStepDbl rtg v s lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k2 = 2 * VU.unsafeIndex rtg i
            sv <- VUM.unsafeRead s k2
            VUM.unsafeWrite s k2 (sv + VU.unsafeIndex v i)
            cv <- VUM.unsafeRead s (k2 + 1)
            VUM.unsafeWrite s (k2 + 1) (cv + 1)
            go (i + 1)

extremaIntFusedAgg :: Bool -> Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
extremaIntFusedAgg isMin nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (if isMin then maxBound else minBound :: Int))
        (extremaStepInt isMin rtg v)
        (combineIntRange isMin)
        (fmap fromUnboxedVector . VU.unsafeFreeze)

extremaStepInt ::
    Bool -> VU.Vector Int -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
extremaStepInt isMin rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
                !x = VU.unsafeIndex v i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (if isMin then min c x else max c x)
            go (i + 1)

extremaDblFusedAgg :: Bool -> Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
extremaDblFusedAgg isMin nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (if isMin then 1 / 0 else negate (1 / 0) :: Double))
        (extremaStepDbl isMin rtg v)
        (combineDblRange isMin)
        (fmap fromUnboxedVector . VU.unsafeFreeze)

extremaStepDbl ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
extremaStepDbl isMin rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
                !x = VU.unsafeIndex v i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (if isMin then min c x else max c x)
            go (i + 1)

{- | Fused second-largest over a Double column. The per-group
(largest, second-largest) pair is INTERLEAVED at slots @2g@/@2g+1@ (one cache
line per group, as 'meanDblFusedAgg'); the update is the same top-2 selection
as every other top2 kernel, the merge keeps the top two of the four candidates
per group (exact — no float arithmetic), and the finalize returns the second
max, NaN for a group of size < 2 (the @-inf@ seed; see
'DataFrame.Internal.AggKernel.top2SndScatter').
-}
top2SndDblFusedAgg :: Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
top2SndDblFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate (2 * nGroups) (negate (1 / 0) :: Double))
        (top2SndStepDbl rtg v)
        mergeTop2Range
        ( \s -> do
            sv <- VU.unsafeFreeze s
            pure
                ( fromUnboxedVector
                    ( VU.generate
                        nGroups
                        ( \g ->
                            let !a2 = VU.unsafeIndex sv (2 * g + 1)
                             in if isInfinite a2 then 0 / 0 else a2
                        )
                    )
                )
        )

top2SndStepDbl ::
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
top2SndStepDbl rtg v s lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k2 = 2 * VU.unsafeIndex rtg i
                !x = VU.unsafeIndex v i
            a1 <- VUM.unsafeRead s k2
            if x > a1
                then do
                    VUM.unsafeWrite s k2 x
                    VUM.unsafeWrite s (k2 + 1) a1
                else do
                    a2 <- VUM.unsafeRead s (k2 + 1)
                    if x > a2
                        then VUM.unsafeWrite s (k2 + 1) x
                        else pure ()
            go (i + 1)

-- | Top two of the four candidates per group (pairs already ordered m1 >= m2).
mergeTop2Range ::
    VUM.IOVector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
mergeTop2Range a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            let !g2 = 2 * g
            a1 <- VUM.unsafeRead a g2
            a2 <- VUM.unsafeRead a (g2 + 1)
            b1 <- VUM.unsafeRead b g2
            b2 <- VUM.unsafeRead b (g2 + 1)
            if b1 > a1
                then do
                    VUM.unsafeWrite a g2 b1
                    VUM.unsafeWrite a (g2 + 1) (max a1 b2)
                else VUM.unsafeWrite a (g2 + 1) (max a2 b1)
            go (g + 1)

addIntRange :: VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
addIntRange a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (x + y)
            go (g + 1)

addDblRange :: VUM.IOVector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
addDblRange a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (x + y)
            go (g + 1)

combineIntRange :: Bool -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
combineIntRange isMin a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (if isMin then min x y else max x y)
            go (g + 1)

combineDblRange ::
    Bool -> VUM.IOVector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
combineDblRange isMin a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (if isMin then min x y else max x y)
            go (g + 1)

-------------------------------------------------------------------------------
-- Fused multi-reduction gather pass (nGroups above 'streamGroupCap')
-------------------------------------------------------------------------------

{- | One fused reduction of a multi-reduction GATHER pass: allocate the output
array, fold a contiguous group range (each group's rows via the shared
@valueIndices@ slice, accumulator in registers, one write per group), finalize.
Group ranges are disjoint across workers, so there is no merge and every
reduction reproduces the exact per-group fold order and formula of its unfused
gather kernel ('reduceParTyped') — results are bit-identical to running the
kernels separately, at any @-N@.
-}
data GatherAgg
    = forall s.
      GatherAgg
        (IO s)
        (s -> Int -> Int -> IO ())
        (s -> IO Column)

{- | Build the fused gather reduction for one @(reduction, column)@ pair, or
'Nothing' when the pair cannot fuse (nullable/boxed columns, or var/std/top2,
which keep their per-expression gather kernels). @vis@/@offs@ are captured
lazily: nothing is forced until the pass actually runs.
-}
mkGatherAgg ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Reduction ->
    Column ->
    Maybe GatherAgg
mkGatherAgg nGroups vis offs red col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int) ->
            case red of
                RCount -> Just countGather
                RSum -> Just (outGather (gatherSumInt vis offs v))
                RMean -> Just (outGatherD (gatherMeanInt vis offs v))
                RMin -> Just (outGather (gatherExtremaInt True vis offs v))
                RMax -> Just (outGather (gatherExtremaInt False vis offs v))
                _ -> Nothing
        | Just Refl <- testEquality (typeRep @a) (typeRep @Double) ->
            case red of
                RCount -> Just countGather
                RSum -> Just (outGatherD (gatherSumDbl vis offs v))
                RMean -> Just (outGatherD (gatherMeanDbl vis offs v))
                RMin -> Just (outGatherD (gatherExtremaDbl True vis offs v))
                RMax -> Just (outGatherD (gatherExtremaDbl False vis offs v))
                _ -> Nothing
    _ -> Nothing
  where
    countGather =
        GatherAgg
            (VUM.new nGroups)
            (gatherCount offs)
            (fmap fromUnboxedVector . VU.unsafeFreeze)
    outGather step =
        GatherAgg
            (VUM.new nGroups :: IO (VUM.IOVector Int))
            step
            (fmap fromUnboxedVector . VU.unsafeFreeze)
    outGatherD step =
        GatherAgg
            (VUM.new nGroups :: IO (VUM.IOVector Double))
            step
            (fmap fromUnboxedVector . VU.unsafeFreeze)

{- | Number of groups each fused-gather block hands to every reduction before
moving on: the block's @valueIndices@ slice stays hot in cache across the k
per-reduction loops.
-}
gatherBlock :: Int
gatherBlock = 32

{- | Execute all fused gather reductions in one traversal: workers own disjoint
contiguous group ranges (row-balanced, same policy as every gather kernel), and
walk them in 'gatherBlock'-group blocks running each reduction's fold on the
block. Deterministic and bit-identical to the unfused kernels (see
'GatherAgg'). Forces @valueIndices@ once, before the fan-out.

Pure w.r.t. its immutable inputs, so the 'unsafePerformIO' is safe.
-}
runGatherAggs ::
    VU.Vector Int -> VU.Vector Int -> Int -> [GatherAgg] -> [Column]
runGatherAggs vis offs nGroups aggs = unsafePerformIO $ do
    _ <- evaluate (VU.length vis)
    let !caps = capabilities
        !bounds = groupRangeBounds offs nGroups caps
    opened <-
        mapM (\(GatherAgg new step fin) -> do s <- new; pure (step s, fin s)) aggs
    parallelBounds_ caps bounds $ \gs ge ->
        let go !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = min ge (g + gatherBlock)
                    mapM_ (\(st, _) -> st g e) opened
                    go e
         in go gs
    mapM snd opened
{-# NOINLINE runGatherAggs #-}

-- Monomorphic per-reduction gather folds; each replicates the exact per-group
-- recurrence of its unfused kernel above ('countPar'/'sumPar'/'extremaPar'/
-- 'meanPar'), so fused results are bit-identical.

gatherCount :: VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
gatherCount offs out gs ge = go gs
  where
    go !g
        | g >= ge = pure ()
        | otherwise = do
            let !c = VU.unsafeIndex offs (g + 1) - VU.unsafeIndex offs g
            VUM.unsafeWrite out g c
            go (g + 1)

gatherSumInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
gatherSumInt vis offs v out gs ge =
    overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
        VUM.unsafeWrite out

gatherSumDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherSumDbl vis offs v out gs ge =
    overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
        VUM.unsafeWrite out

gatherExtremaInt ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
gatherExtremaInt isMin vis offs v out gs ge =
    overGroupsAcc
        vis
        offs
        gs
        ge
        (if isMin then maxBound else minBound)
        ( \acc row ->
            let !x = VU.unsafeIndex v row
             in if isMin then min acc x else max acc x
        )
        (VUM.unsafeWrite out)

gatherExtremaDbl ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherExtremaDbl isMin vis offs v out gs ge =
    overGroupsAcc
        vis
        offs
        gs
        ge
        (if isMin then 1 / 0 else negate (1 / 0))
        ( \acc row ->
            let !x = VU.unsafeIndex v row
             in if isMin then min acc x else max acc x
        )
        (VUM.unsafeWrite out)

-- | Exact replica of 'meanPar''s per-group loop (Int element type).
gatherMeanInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherMeanInt vis offs v out gs ge = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = acc
                    | otherwise =
                        inner
                            (pos + 1)
                            (acc + fromIntegral (VU.unsafeIndex v (VU.unsafeIndex vis pos)))
                !s0 = VU.unsafeIndex offs g
                !total = inner s0 0
                !c = e - s0
            VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
            grp (g + 1)

-- | Exact replica of 'meanPar''s per-group loop (Double element type).
gatherMeanDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherMeanDbl vis offs v out gs ge = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = acc
                    | otherwise =
                        inner
                            (pos + 1)
                            (acc + VU.unsafeIndex v (VU.unsafeIndex vis pos))
                !s0 = VU.unsafeIndex offs g
                !total = inner s0 0
                !c = e - s0
            VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
            grp (g + 1)
