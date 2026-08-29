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
    denseMaxMinusMin,
) where

import Control.Monad (when)
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
    shouldParallelize,
 )

{- | Group count at or below which the float (Double sum/mean) direct
reductions run as ONE sequential row-order pass: the accumulators fit in L2, the
pass is memory-bandwidth bound, and adding each value to its group in ascending
row order is exactly the order the group-range gather kernel uses (the grouping
layer's @valueIndices@ is a stable counting sort), so the result is
byte-identical to it. Above this the parallel chunked variant runs instead (see
'sumDblDense'). Var/std are never taken directly: any chunked merge of
variance state changes the float recurrence, and the group-range gather kernel
already runs them in parallel while replaying the interpreter's per-group
update order bit-for-bit.
-}
seqFloatGroups :: Int
seqFloatGroups = 65536

{- | Run a recognised reduction through the direct-indexed path. 'Nothing' (so
the caller falls back to the order-preserving kernel) unless the reduction is
admitted at this element type AND the column is a clean unboxed Int/Double.
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

{- | The reductions admitted over an Int column. Sum/min/max/mean/count are
exact in the Int domain (any merge order gives the same bits); top2sum selects
the two largest values (order-independent as a multiset selection) and only
adds them once at finalize. Var/std stay with the group-range gather kernel:
its per-group Welford recurrence replays the interpreter's update order
bit-for-bit, which no chunk-merged direct pass can.
-}
denseInt :: Reduction -> VU.Vector Int -> Int -> VU.Vector Int -> Maybe Column
denseInt red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumIntDense g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaIntDense True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaIntDense False g nGroups v))
    RMean -> Just (fromUnboxedVector (meanIntDense g nGroups v))
    RTop2Sum -> Just (fromUnboxedVector (top2Dense g nGroups v))
    RTop2Snd -> Just (fromUnboxedVector (top2SndDense g nGroups v))
    _ -> Nothing

{- | The reductions admitted over a Double column. Count/min/max/top2sum are
order-independent (exact per-worker merge, byte-identical at any @-N@). The
float sum/mean run sequentially in row order below 'seqFloatGroups' (matching
the gather kernel's per-group addition order exactly) and as deterministic
chunked partials above it. Var/std keep the gather kernel (see 'denseInt').
-}
denseDouble ::
    Reduction -> VU.Vector Int -> Int -> VU.Vector Double -> Maybe Column
denseDouble red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumDblDense g nGroups v))
    RMean -> Just (fromUnboxedVector (meanDblDense g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaDblDense True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaDblDense False g nGroups v))
    RTop2Sum -> Just (fromUnboxedVector (top2Dense g nGroups v))
    RTop2Snd -> Just (fromUnboxedVector (top2SndDense g nGroups v))
    _ -> Nothing

{- | The fused @max a - min b@ direct pass: BOTH extrema accumulate in one
streaming loop over the rows (min/max are order-independent, so the per-worker
merge is exact and the result byte-identical to the two gather passes it
replaces). 'Nothing' unless both columns are clean unboxed and same-typed
(Int/Int keeps the Int result of the interpreter; Double/Double the Double one);
mixed pairs keep the gather fallback.
-}
denseMaxMinusMin :: VU.Vector Int -> Int -> Column -> Column -> Maybe Column
denseMaxMinusMin g nGroups ca cb = case (ca, cb) of
    ( UnboxedColumn Nothing (va :: VU.Vector x)
        , UnboxedColumn Nothing (vb :: VU.Vector y)
        )
            | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
            , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                Just (fromUnboxedVector (maxMinusMinDenseInt g nGroups va vb))
            | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
            , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                Just (fromUnboxedVector (maxMinusMinDenseDbl g nGroups va vb))
    _ -> Nothing
{-# INLINEABLE denseMaxMinusMin #-}

{- | Monomorphic entry points: the 'testEquality' dispatch above only yields an
unsafe coercion, so a direct call to the polymorphic 'maxMinusMinDense' there
would stay at the abstract element type and never meet its SPECIALIZE rules
(measured ~3x on the whole pass); calling through these fixed-type wrappers
(the coercion lands on the argument) does.
-}
maxMinusMinDenseInt ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
maxMinusMinDenseInt = maxMinusMinDense minBound maxBound
{-# NOINLINE maxMinusMinDenseInt #-}

maxMinusMinDenseDbl ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double -> VU.Vector Double
maxMinusMinDenseDbl = maxMinusMinDense (negate (1 / 0)) (1 / 0)
{-# NOINLINE maxMinusMinDenseDbl #-}

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

{- | Fork @caps@ workers over disjoint contiguous row ranges of @[0, n)@, each
producing its own private accumulator (no shared array, no sync). Returns the
partials in worker order for the caller's merge; rethrows the first failure.
The chunking is a fixed function of @n@ and @caps@, so any merge over the
partials is deterministic at a given @-N@.
-}
runPartialsOver ::
    Int -> Int -> (Int -> Int -> IO acc) -> IO [acc]
runPartialsOver n _caps = parallelChunks parThreshold n

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
-- Double sum / mean (streaming scatter; chunked partials above seqFloatGroups)
-------------------------------------------------------------------------------

{- | Double group sums. At or below 'seqFloatGroups' a single sequential pass in
ascending row order — each group's additions happen in exactly the order the
group-range gather kernel performs them, so the result is byte-identical to it.
Above that, per-worker chunk partials merged in worker order: still
deterministic at a fixed @-N@, but the float summation order differs from the
sequential pass.
-}
sumDblDense :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
sumDblDense g nGroups v
    | nGroups <= seqFloatGroups || not (shouldPar n) =
        unsafePerformIO (sumDblChunk g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (sumDblChunk g v nGroups)
        mergeDblSum nGroups parts
  where
    !n = VU.length v
{-# NOINLINE sumDblDense #-}

sumDblChunk ::
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double)
sumDblChunk g v nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Double)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
                go (i + 1)
    go lo
    pure acc

{- | Double mean: fused (Double sum, count) per group, divided once at finalize.
Same order policy as 'sumDblDense' (sequential row order is byte-identical to
the gather kernel; the chunked variant changes the float summation order).
-}
meanDblDense :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
meanDblDense g nGroups v
    | nGroups <= seqFloatGroups || not (shouldPar n) = unsafePerformIO $ do
        (s, c) <- meanDblChunk g v nGroups 0 n
        finalizeMeanDbl nGroups s c
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (meanDblChunk g v nGroups)
        (s, c) <- mergeMeanDbl nGroups parts
        finalizeMeanDbl nGroups s c
  where
    !n = VU.length v
{-# NOINLINE meanDblDense #-}

meanDblChunk ::
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double, VUM.IOVector Int)
meanDblChunk g v nGroups lo hi = do
    s <- VUM.replicate nGroups (0 :: Double)
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

finalizeMeanDbl ::
    Int -> VUM.IOVector Double -> VUM.IOVector Int -> IO (VU.Vector Double)
finalizeMeanDbl nGroups s c = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite
                    out
                    k
                    (if cv == 0 then 0 / 0 else sv / fromIntegral cv)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

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
-- Double min / max (order-independent: exact per-worker merge)
-------------------------------------------------------------------------------

extremaDblDense ::
    Bool -> VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
extremaDblDense isMin g nGroups v
    | not (shouldPar n) =
        unsafePerformIO (extremaDblChunk isMin g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (extremaDblChunk isMin g v nGroups)
        mergeExtremaDbl isMin nGroups parts
  where
    !n = VU.length v
{-# NOINLINE extremaDblDense #-}

extremaDblChunk ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double)
extremaDblChunk isMin g v nGroups lo hi = do
    let !seed = if isMin then 1 / 0 else negate (1 / 0)
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
-- Fused max(a) - min(b) (order-independent: exact per-worker merge)
-------------------------------------------------------------------------------

{- | One streaming pass accumulating @max a@ and @min b@ together. @maxSeed@ is
the identity of @max@ (the type's least value), @minSeed@ of @min@ (its
greatest).
-}
maxMinusMinDense ::
    (VU.Unbox a, Num a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector a ->
    VU.Vector a
{-# SPECIALIZE maxMinusMinDense ::
    Int ->
    Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int
    #-}
{-# SPECIALIZE maxMinusMinDense ::
    Double ->
    Double ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double
    #-}
maxMinusMinDense maxSeed minSeed g nGroups va vb
    | not (shouldPar n) = unsafePerformIO $ do
        (mx, mn) <- maxMinusMinChunk maxSeed minSeed g va vb nGroups 0 n
        finalizeMaxMinusMin nGroups mx mn
    | otherwise = unsafePerformIO $ do
        parts <-
            runPartialsOver
                n
                capabilities
                (maxMinusMinChunk maxSeed minSeed g va vb nGroups)
        (mx, mn) <- mergeMaxMin nGroups parts
        finalizeMaxMinusMin nGroups mx mn
  where
    !n = VU.length va
{- INLINEABLE (not NOINLINE) so the SPECIALIZE pragmas above take effect; the
kernel is a pure function of its arguments, so the usual unsafePerformIO
sharing concern does not apply. -}
{-# INLINEABLE maxMinusMinDense #-}

maxMinusMinChunk ::
    (VU.Unbox a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    VU.Vector a ->
    VU.Vector a ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector a, VUM.IOVector a)
maxMinusMinChunk maxSeed minSeed g va vb nGroups lo hi = do
    mx <- VUM.replicate nGroups maxSeed
    mn <- VUM.replicate nGroups minSeed
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cx <- VUM.unsafeRead mx k
                VUM.unsafeWrite mx k (max cx (VU.unsafeIndex va i))
                cn <- VUM.unsafeRead mn k
                VUM.unsafeWrite mn k (min cn (VU.unsafeIndex vb i))
                go (i + 1)
    go lo
    pure (mx, mn)

mergeMaxMin ::
    (VU.Unbox a, Ord a) =>
    Int ->
    [(VUM.IOVector a, VUM.IOVector a)] ->
    IO (VUM.IOVector a, VUM.IOVector a)
mergeMaxMin nGroups parts = case parts of
    [] -> error "mergeMaxMin: no partials"
    ((mx0, mn0) : rest) -> do
        let add (mx, mn) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            xa <- VUM.unsafeRead mx0 k
                            xb <- VUM.unsafeRead mx k
                            VUM.unsafeWrite mx0 k (max xa xb)
                            na <- VUM.unsafeRead mn0 k
                            nb <- VUM.unsafeRead mn k
                            VUM.unsafeWrite mn0 k (min na nb)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (mx0, mn0)

finalizeMaxMinusMin ::
    (VU.Unbox a, Num a) =>
    Int ->
    VUM.IOVector a ->
    VUM.IOVector a ->
    IO (VU.Vector a)
finalizeMaxMinusMin nGroups mx mn = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                a <- VUM.unsafeRead mx k
                b <- VUM.unsafeRead mn k
                VUM.unsafeWrite out k (a - b)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

-------------------------------------------------------------------------------
-- Top-2 sum (order-independent multiset selection; one float add at finalize)
-------------------------------------------------------------------------------

{- | Sum of the two largest values per group. Each accumulator holds the
(largest, second-largest) pair seen so far; merging two pairs keeps the top two
of the four candidates. No float ADDITION happens until the single @m1 + m2@ at
finalize, so the result is byte-identical to the gather kernel regardless of
chunking. Mirrors the gather kernel exactly, including the @realToFrac@ per
element and the @-inf -> 0@ guards at finalize.
-}
top2Dense ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
{-# SPECIALIZE top2Dense ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
    #-}
{-# SPECIALIZE top2Dense ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
    #-}
top2Dense g nGroups v
    | not (shouldPar n) = unsafePerformIO $ do
        (m1, m2) <- top2Chunk g v nGroups 0 n
        finalizeTop2 nGroups m1 m2
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (top2Chunk g v nGroups)
        (m1, m2) <- mergeTop2 nGroups parts
        finalizeTop2 nGroups m1 m2
  where
    !n = VU.length v
{- INLINEABLE (not NOINLINE) so the SPECIALIZE pragmas above take effect; pure
function of its arguments, so unsafePerformIO sharing is not a concern. -}
{-# INLINEABLE top2Dense #-}

{- | Second-largest value per group: the exact same per-worker
(largest, second-largest) accumulator and merge as 'top2Dense'
('top2Chunk'/'mergeTop2'), finalized to the second max alone. A group of
size < 2 finalizes its @-inf@ seed to NaN — documented behaviour (the
db-benchmark Q8 data has no size-1 @id6@ groups). Order-independent multiset
selection, so byte-identical at any @-N@.
-}
top2SndDense ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
{-# SPECIALIZE top2SndDense ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
    #-}
{-# SPECIALIZE top2SndDense ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
    #-}
top2SndDense g nGroups v
    | not (shouldPar n) = unsafePerformIO $ do
        (m1, m2) <- top2Chunk g v nGroups 0 n
        finalizeTop2Snd nGroups m1 m2
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (top2Chunk g v nGroups)
        (m1, m2) <- mergeTop2 nGroups parts
        finalizeTop2Snd nGroups m1 m2
  where
    !n = VU.length v
{- INLINEABLE (not NOINLINE) so the SPECIALIZE pragmas above take effect; pure
function of its arguments, so unsafePerformIO sharing is not a concern. -}
{-# INLINEABLE top2SndDense #-}

top2Chunk ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector a ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double, VUM.IOVector Double)
top2Chunk g v nGroups lo hi = do
    let ninf = negate (1 / 0) :: Double
    m1 <- VUM.replicate nGroups ninf
    m2 <- VUM.replicate nGroups ninf
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                    !x = realToFrac (VU.unsafeIndex v i)
                a1 <- VUM.unsafeRead m1 k
                if x > a1
                    then do
                        VUM.unsafeWrite m1 k x
                        VUM.unsafeWrite m2 k a1
                    else do
                        a2 <- VUM.unsafeRead m2 k
                        when (x > a2) (VUM.unsafeWrite m2 k x)
                go (i + 1)
    go lo
    pure (m1, m2)

{- | Top two of the four candidates @{a1, a2, b1, b2}@ per group (each pair
already ordered @m1 >= m2@, @-inf@ seeds included).
-}
mergeTop2 ::
    Int ->
    [(VUM.IOVector Double, VUM.IOVector Double)] ->
    IO (VUM.IOVector Double, VUM.IOVector Double)
mergeTop2 nGroups parts = case parts of
    [] -> error "mergeTop2: no partials"
    ((m10, m20) : rest) -> do
        let add (m1, m2) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a1 <- VUM.unsafeRead m10 k
                            a2 <- VUM.unsafeRead m20 k
                            b1 <- VUM.unsafeRead m1 k
                            b2 <- VUM.unsafeRead m2 k
                            if b1 > a1
                                then do
                                    VUM.unsafeWrite m10 k b1
                                    VUM.unsafeWrite m20 k (max a1 b2)
                                else VUM.unsafeWrite m20 k (max a2 b1)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (m10, m20)

finalizeTop2 ::
    Int -> VUM.IOVector Double -> VUM.IOVector Double -> IO (VU.Vector Double)
finalizeTop2 nGroups m1 m2 = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                a1 <- VUM.unsafeRead m1 k
                a2 <- VUM.unsafeRead m2 k
                let s = (if isInfinite a1 then 0 else a1) + (if isInfinite a2 then 0 else a2)
                VUM.unsafeWrite out k s
                go (k + 1)
    go 0
    VU.unsafeFreeze out

finalizeTop2Snd ::
    Int -> VUM.IOVector Double -> VUM.IOVector Double -> IO (VU.Vector Double)
finalizeTop2Snd nGroups _m1 m2 = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                a2 <- VUM.unsafeRead m2 k
                VUM.unsafeWrite out k (if isInfinite a2 then 0 / 0 else a2)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

-------------------------------------------------------------------------------
-- Integer mean (exact integer sum + count, divided once -> order-independent)
-------------------------------------------------------------------------------

{- | Integer mean in ONE fused pass: a running integer sum and count per group,
divided once at finalize. The integer sum is exact, so the parallel partial
merge is byte-identical to the sequential single pass at any @-N@.
-}
meanIntDense :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
meanIntDense g nGroups v
    | not (shouldPar n) = unsafePerformIO $ do
        (s, c) <- meanIntChunk g v nGroups 0 n
        finalizeMeanInt nGroups s c
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (meanIntChunk g v nGroups)
        (s, c) <- mergePair nGroups parts
        finalizeMeanInt nGroups s c
  where
    !n = VU.length v
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

{- | Sum the per-worker Double partials into the first worker's accumulator IN
WORKER ORDER: deterministic at a fixed @-N@, but the float summation order is
chunk-major rather than the sequential row order.
-}
mergeDblSum :: Int -> [VUM.IOVector Double] -> IO (VU.Vector Double)
mergeDblSum nGroups parts = case parts of
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

{- | As 'mergePair' but for the Double (sum, count) partials of the Double mean;
worker-order float sums (see 'mergeDblSum').
-}
mergeMeanDbl ::
    Int ->
    [(VUM.IOVector Double, VUM.IOVector Int)] ->
    IO (VUM.IOVector Double, VUM.IOVector Int)
mergeMeanDbl nGroups parts = case parts of
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

mergeExtremaDbl :: Bool -> Int -> [VUM.IOVector Double] -> IO (VU.Vector Double)
mergeExtremaDbl isMin nGroups parts = case parts of
    [] ->
        VU.unsafeFreeze
            =<< VUM.replicate nGroups (if isMin then 1 / 0 else negate (1 / 0))
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
