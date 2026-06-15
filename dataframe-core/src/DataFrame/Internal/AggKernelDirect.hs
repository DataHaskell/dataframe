{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Low-cardinality DIRECT-INDEXED accumulator fast path (research #4).

When the group-by key resolves to a small dense domain (@nGroups@ below
'directThreshold'), the dense @rowToGroup@ vector handed down by the grouping
layer already IS the group id per row, so we can bypass the @valueIndices@
gather entirely: scan @rowToGroup@ and the value column /in lockstep, in
original row order/, scattering each value straight into a per-group accumulator
array indexed by the dense id. Both arrays are read sequentially (no random
gather through @valueIndices@), and for a small domain the accumulator stays
cache-resident.

For parallelism we use the two-phase morsel pattern (#2): split the ROW range
into one contiguous chunk per capability, give each worker a private tiny
accumulator array, scan its chunk sequentially, then merge the @caps@ partials in
a cheap O(caps * nGroups) pass. This makes few-group questions scale (the work
per worker is a tight sequential pass) instead of being dominated by parallel
fan-out, which is exactly the regression the group-range gather suffered on Q4.

CRITICAL — byte-identical at any @-N@: the two-phase merge only changes the
fold/combine order, so it is admitted ONLY for reductions whose result is
independent of that order: integer sum (exact, associative), count, min, max,
and integer mean (an exact integer sum and count divided once at finalize). The
order-sensitive float reductions (Double sum/mean, variance, sd, top2) are NOT
routed here — the caller keeps the order-preserving group-range kernel for them,
so the db-benchmark checksums stay byte-identical between -N1 and -N8.
-}
module DataFrame.Internal.AggKernelDirect (
    directThreshold,
    directReduce,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.AggKernel (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    fromUnboxedVector,
    materializePacked,
 )

{- | Group-domain size at or below which the direct-indexed accumulator path is
taken. The db-benchmark low-cardinality questions (id1=100, id4=100, id6=1e5,
id2:id4=1e4) sit at or below it; wider domains keep the group-range kernel. The
admitted reductions are order-independent, so the per-worker accumulator merge is
exact regardless of size.
-}
directThreshold :: Int
directThreshold = 262144

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

{- | Below this many rows the parallel fan-out is not worth it; a single
sequential direct pass runs instead (tiny accumulator, one tight loop). Matches
the grouping/scatter parallel threshold.
-}
parThreshold :: Int
parThreshold = 200000

{- | Run a recognised reduction through the direct-indexed path. Returns
'Nothing' (so the caller falls back to the order-preserving kernel) unless BOTH:
(a) the reduction's result is order-independent at this element type — see the
module note — and (b) the value column is a clean unboxed Int/Double column.

@g@ is the dense @rowToGroup@ vector (group id per row, original row order);
@nGroups@ is the dense domain size, already verified @<= directThreshold@ by the
caller.
-}
directReduce :: Reduction -> VU.Vector Int -> Int -> Column -> Maybe Column
directReduce red g nGroups col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> directInt red g nGroups v
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> directDouble red g nGroups v
                Nothing -> Nothing
    p@(PackedText _ _) -> directReduce red g nGroups (materializePacked p)
    _ -> Nothing
{-# INLINEABLE directReduce #-}

-- | The order-independent reductions over an Int column.
directInt :: Reduction -> VU.Vector Int -> Int -> VU.Vector Int -> Maybe Column
directInt red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDirect g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumIntDirect g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaIntDirect True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaIntDirect False g nGroups v))
    RMean -> Just (fromUnboxedVector (meanIntDirect g nGroups v))
    _ -> Nothing

{- | Over a Double column only @count@ is order-independent; the float
sum/mean/variance reductions must keep the order-preserving kernel.
-}
directDouble ::
    Reduction -> VU.Vector Int -> Int -> VU.Vector Double -> Maybe Column
directDouble red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDirect g nGroups (VU.length v)))
    _ -> Nothing

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar n = n >= parThreshold && capabilities > 1

{- | Fork @caps@ workers over disjoint contiguous ROW ranges @[lo, hi)@ of
@[0, n)@, balanced evenly by row count; each worker @w@ runs @fill lo hi@
producing its OWN private accumulator (thread-local, no shared array, no sync).
Returns the partials in worker order for the caller's merge. Rethrows the first
worker failure.
-}
runPartialsOver ::
    Int -> Int -> (Int -> Int -> IO (VUM.IOVector Int)) -> IO [VUM.IOVector Int]
runPartialsOver n caps fill = do
    let !per = (n + caps - 1) `div` caps
        spawn w = do
            var <- newEmptyMVar
            let !lo = min n (w * per)
                !hi = min n (lo + per)
            _ <- forkIO (try (fill lo hi) >>= putMVar var)
            pure var
    vars <- mapM spawn [0 .. caps - 1]
    results <- mapM takeMVar vars
    mapM (either (throwIO @SomeException) pure) results

{- | As 'runPartialsOver' but each worker produces a PAIR of accumulators (e.g.
sum and count for the fused integer mean).
-}
runPartialsPairOver ::
    Int ->
    Int ->
    (Int -> Int -> IO (VUM.IOVector Int, VUM.IOVector Int)) ->
    IO [(VUM.IOVector Int, VUM.IOVector Int)]
runPartialsPairOver n caps fill = do
    let !per = (n + caps - 1) `div` caps
        spawn w = do
            var <- newEmptyMVar
            let !lo = min n (w * per)
                !hi = min n (lo + per)
            _ <- forkIO (try (fill lo hi) >>= putMVar var)
            pure var
    vars <- mapM spawn [0 .. caps - 1]
    results <- mapM takeMVar vars
    mapM (either (throwIO @SomeException) pure) results

-------------------------------------------------------------------------------
-- Count (order-independent: per-group row count)
-------------------------------------------------------------------------------

countDirect :: VU.Vector Int -> Int -> Int -> VU.Vector Int
countDirect g nGroups n
    | not (shouldPar n) =
        unsafePerformIO (countChunk g nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (countChunk g nGroups)
        mergeIntSum nGroups parts
{-# NOINLINE countDirect #-}

countChunk :: VU.Vector Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
countChunk g nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + 1)
                go (i + 1)
    go lo
    pure acc

-------------------------------------------------------------------------------
-- Integer sum (exact: merge order irrelevant)
-------------------------------------------------------------------------------

sumIntDirect :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
sumIntDirect g nGroups v
    | not (shouldPar n) =
        unsafePerformIO (sumIntChunk g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (sumIntChunk g v nGroups)
        mergeIntSum nGroups parts
  where
    !n = VU.length v
{-# NOINLINE sumIntDirect #-}

sumIntChunk ::
    VU.Vector Int -> VU.Vector Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
sumIntChunk g v nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
                go (i + 1)
    go lo
    pure acc

-------------------------------------------------------------------------------
-- Integer min / max (order-independent)
-------------------------------------------------------------------------------

extremaIntDirect ::
    Bool -> VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
extremaIntDirect isMin g nGroups v
    | not (shouldPar n) =
        unsafePerformIO (extremaIntChunk isMin g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (extremaIntChunk isMin g v nGroups)
        mergeExtremaInt isMin nGroups parts
  where
    !n = VU.length v
{-# NOINLINE extremaIntDirect #-}

extremaIntChunk ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int)
extremaIntChunk isMin g v nGroups lo hi = do
    let !seed = if isMin then maxBound else minBound
        combine a b = if isMin then min a b else max a b
    acc <- VUM.replicate nGroups seed
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (combine c (VU.unsafeIndex v i))
                go (i + 1)
    go lo
    pure acc

-------------------------------------------------------------------------------
-- Integer mean (exact integer sum + count, divided once -> order-independent)
-------------------------------------------------------------------------------

{- | Integer mean in ONE fused pass: a running integer sum and count per group,
divided once at finalize. The integer sum is exact, so the parallel partial
merge is byte-identical to the sequential single pass at any @-N@.
-}
meanIntDirect :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
meanIntDirect g nGroups v
    | not (shouldPar n) = unsafePerformIO $ do
        (s, c) <- meanIntChunk g v nGroups 0 n
        finalizeMeanInt nGroups s c
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsPairOver n capabilities (meanIntChunk g v nGroups)
        (s, c) <- mergePair nGroups parts
        finalizeMeanInt nGroups s c
  where
    !n = VU.length v
{-# NOINLINE meanIntDirect #-}

meanIntChunk ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int)
meanIntChunk g v nGroups lo hi = do
    s <- VUM.replicate nGroups (0 :: Int)
    c <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                sv <- VUM.unsafeRead s k
                VUM.unsafeWrite s k (sv + VU.unsafeIndex v i)
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite c k (cv + 1)
                go (i + 1)
    go lo
    pure (s, c)

finalizeMeanInt ::
    Int -> VUM.IOVector Int -> VUM.IOVector Int -> IO (VU.Vector Double)
finalizeMeanInt nGroups s c = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite
                    out
                    k
                    (if cv == 0 then 0 / 0 else fromIntegral sv / fromIntegral cv)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

-------------------------------------------------------------------------------
-- Partial accumulation + merge
-------------------------------------------------------------------------------

mergeIntSum :: Int -> [VUM.IOVector Int] -> IO (VU.Vector Int)
mergeIntSum nGroups parts = case parts of
    [] -> VU.unsafeFreeze =<< VUM.replicate nGroups 0
    (p0 : rest) -> do
        let add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (a + b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0

{- | Merge per-worker (sum, count) partials into the first worker's pair by
exact integer addition; returns the accumulated pair for finalize.
-}
mergePair ::
    Int ->
    [(VUM.IOVector Int, VUM.IOVector Int)] ->
    IO (VUM.IOVector Int, VUM.IOVector Int)
mergePair nGroups parts = case parts of
    [] -> (,) <$> VUM.replicate nGroups 0 <*> VUM.replicate nGroups 0
    ((s0, c0) : rest) -> do
        let add (s, c) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            sa <- VUM.unsafeRead s0 k
                            sb <- VUM.unsafeRead s k
                            VUM.unsafeWrite s0 k (sa + sb)
                            ca <- VUM.unsafeRead c0 k
                            cb <- VUM.unsafeRead c k
                            VUM.unsafeWrite c0 k (ca + cb)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (s0, c0)

mergeExtremaInt :: Bool -> Int -> [VUM.IOVector Int] -> IO (VU.Vector Int)
mergeExtremaInt isMin nGroups parts = case parts of
    [] ->
        VU.unsafeFreeze =<< VUM.replicate nGroups (if isMin then maxBound else minBound)
    (p0 : rest) -> do
        let combine a b = if isMin then min a b else max a b
            add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (combine a b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0
