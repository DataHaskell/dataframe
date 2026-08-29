{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | The low-cardinality DENSE reduction kernel: when the group domain is small,
the grouping layer's @rowToGroup@ already maps row -> group, so the reduction
scatters straight off it with no @valueIndices@ gather.

Parallel by ROW range with a private per-worker accumulator of @nGroups@ slots,
merged afterwards — which is why it needs a small domain, and why it admits only
order-independent reductions: the merge must be exact for the result to stay
byte-identical to @-N1@. Anything it rejects falls back to
"DataFrame.Internal.Aggregation.Kernel.Scatter".

The caller decides whether the domain is small enough; see @denseThreshold@ in
the operations layer.
-}
module DataFrame.Internal.Aggregation.Kernel.Dense (
    denseReduce,
) where

import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    fromUnboxedVector,
    materializePacked,
 )
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    parThreshold,
    parallelChunks,
 )

{- | Run a recognised reduction through the direct-indexed path. 'Nothing' (so
the caller falls back to the order-preserving kernel) unless the reduction is
order-independent at this element type AND the column is a clean unboxed Int/Double.
-}
denseReduce :: Reduction -> VU.Vector Int -> Int -> Column -> Maybe Column
denseReduce red g nGroups col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> denseInt red g nGroups v
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> denseDouble red g nGroups v
                Nothing -> Nothing
    p@(PackedText _ _) -> denseReduce red g nGroups (materializePacked p)
    _ -> Nothing
{-# INLINEABLE denseReduce #-}

-- | The order-independent reductions over an Int column.
denseInt :: Reduction -> VU.Vector Int -> Int -> VU.Vector Int -> Maybe Column
denseInt red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumIntDense g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaIntDense True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaIntDense False g nGroups v))
    RMean -> Just (fromUnboxedVector (meanIntDense g nGroups v))
    _ -> Nothing

{- | Over a Double column only @count@ is order-independent; the float
sum/mean/variance reductions must keep the order-preserving kernel.
-}
denseDouble ::
    Reduction -> VU.Vector Int -> Int -> VU.Vector Double -> Maybe Column
denseDouble red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    _ -> Nothing

{- | Fork @caps@ workers over disjoint contiguous row ranges of @[0, n)@, each
producing its own private accumulator (no shared array, no sync). Returns the
partials in worker order for the caller's merge; rethrows the first failure.
-}
runPartialsOver ::
    Int -> Int -> (Int -> Int -> IO (VUM.IOVector Int)) -> IO [VUM.IOVector Int]
runPartialsOver n _caps = parallelChunks parThreshold n

{- | As 'runPartialsOver' but each worker produces a PAIR of accumulators (e.g.
sum and count for the fused integer mean).
-}
runPartialsPairOver ::
    Int ->
    Int ->
    (Int -> Int -> IO (VUM.IOVector Int, VUM.IOVector Int)) ->
    IO [(VUM.IOVector Int, VUM.IOVector Int)]
runPartialsPairOver n _caps = parallelChunks parThreshold n

-------------------------------------------------------------------------------
-- Count (order-independent: per-group row count)
-------------------------------------------------------------------------------

countDense :: VU.Vector Int -> Int -> Int -> VU.Vector Int
countDense g nGroups n = unsafePerformIO $ do
    parts <- runPartialsOver n capabilities (countChunk g nGroups)
    mergeIntSum nGroups parts
{-# NOINLINE countDense #-}

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

sumIntDense :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
sumIntDense g nGroups v = unsafePerformIO $ do
    parts <- runPartialsOver (VU.length v) capabilities (sumIntChunk g v nGroups)
    mergeIntSum nGroups parts
{-# NOINLINE sumIntDense #-}

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

extremaIntDense ::
    Bool -> VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
extremaIntDense isMin g nGroups v = unsafePerformIO $ do
    parts <-
        runPartialsOver (VU.length v) capabilities (extremaIntChunk isMin g v nGroups)
    mergeExtremaInt isMin nGroups parts
{-# NOINLINE extremaIntDense #-}

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
meanIntDense :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
meanIntDense g nGroups v = unsafePerformIO $ do
    parts <-
        runPartialsPairOver (VU.length v) capabilities (meanIntChunk g v nGroups)
    (s, c) <- mergePair nGroups parts
    finalizeMeanInt nGroups s c
{-# NOINLINE meanIntDense #-}

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
