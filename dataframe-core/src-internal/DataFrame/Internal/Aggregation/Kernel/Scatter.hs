{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | The group-range scatter-accumulate reduction kernel: reduces a value column
over the grouped layout @(valueIndices, offsets)@.

Sequential and parallel are the same algorithm at different row counts, so they
live together. 'scatterReducePar' cuts the GROUP axis into @caps@ ranges of
roughly equal row count and lets workers write disjoint slots of one shared
output — no per-worker accumulator, no merge — which keeps each group's
accumulation order identical to 'scatterReduce' and the results byte-identical
at any @-N@. Below 'parThreshold' it delegates to 'scatterReduce' directly.

Contrast "DataFrame.Internal.Aggregation.Kernel.Dense", which scatters off
@rowToGroup@ with no gather but needs a small dense group domain.
-}
module DataFrame.Internal.Aggregation.Kernel.Scatter (
    scatterReduce,
    scatterReducePar,
    maxMinusMinScatterPar,
    top2SndScatter,

    -- * Group-range helpers
    -- $shared
    groupRangeBounds,
    rtgFromVis,
    overGroupsAcc,
    groupSlices,
    streamGroupCap,
) where

import Control.Monad (when)
import Control.Monad.ST (ST, runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    fromUnboxedVector,
    materializePacked,
 )
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    chunksFor,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

{- $shared
Also used by "DataFrame.Internal.Aggregation.Kernel.Moments", which partitions
the group axis the same way.
-}

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

scatterReduce ::
    Reduction -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReduce red g nGroups col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> Just (reduceTyped red g nGroups v intIdent)
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> Just (reduceTyped red g nGroups v dblIdent)
                Nothing -> Nothing
    p@(PackedText _ _) -> scatterReduce red g nGroups (materializePacked p)
    _ -> Nothing
{-# INLINEABLE scatterReduce #-}

-- | Per-type seed identities for the order-preserving reductions.
data Idents a = Idents {minSeed :: !a, maxSeed :: !a}

intIdent :: Idents Int
intIdent = Idents maxBound minBound

dblIdent :: Idents Double
dblIdent = Idents (1 / 0) (negate (1 / 0))

reduceTyped ::
    forall a.
    (Columnable a, VU.Unbox a, Num a, Ord a, Real a) =>
    Reduction -> VU.Vector Int -> Int -> VU.Vector a -> Idents a -> Column
reduceTyped red g nGroups v idents = case red of
    RCount -> fromUnboxedVector (countScatter g nGroups)
    RSum -> fromUnboxedVector (sumScatter g nGroups v)
    RMin -> fromUnboxedVector (extremaScatter min (minSeed idents) g nGroups v)
    RMax -> fromUnboxedVector (extremaScatter max (maxSeed idents) g nGroups v)
    RMean -> fromUnboxedVector (meanScatter g nGroups v)
    RVar -> fromUnboxedVector (varScatter False g nGroups v)
    RStd -> fromUnboxedVector (varScatter True g nGroups v)
    RTop2Sum -> fromUnboxedVector (top2Scatter g nGroups v)
    RTop2Snd -> fromUnboxedVector (top2SndScatter g nGroups v)
{-# INLINE reduceTyped #-}

countScatter :: VU.Vector Int -> Int -> VU.Vector Int
countScatter g nGroups = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    let n = VU.length g
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite cnt k (c + 1)
                go (i + 1)
    go 0
    VU.unsafeFreeze cnt

sumScatter ::
    (VU.Unbox a, Num a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector a
sumScatter g nGroups v = runST $ do
    s <- VUM.replicate nGroups 0
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cur <- VUM.unsafeRead s k
                VUM.unsafeWrite s k (cur + VU.unsafeIndex v i)
                go (i + 1)
    go 0
    VU.unsafeFreeze s
{-# INLINE sumScatter #-}

extremaScatter ::
    (VU.Unbox a) =>
    (a -> a -> a) -> a -> VU.Vector Int -> Int -> VU.Vector a -> VU.Vector a
extremaScatter combine seed g nGroups v = runST $ do
    m <- VUM.replicate nGroups seed
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cur <- VUM.unsafeRead m k
                VUM.unsafeWrite m k (combine cur (VU.unsafeIndex v i))
                go (i + 1)
    go 0
    VU.unsafeFreeze m
{-# INLINE extremaScatter #-}

meanScatter ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
meanScatter g nGroups v = runST $ do
    s <- VUM.replicate nGroups (0 :: Double)
    cnt <- VUM.replicate nGroups (0 :: Int)
    scatterSumCount g v s cnt
    finalizeMean nGroups s cnt
{-# INLINE meanScatter #-}

scatterSumCount ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector a ->
    VUM.MVector s Double ->
    VUM.MVector s Int ->
    ST s ()
scatterSumCount g v s cnt = go 0
  where
    n = VU.length v
    go !i
        | i >= n = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex g i
                !x = realToFrac (VU.unsafeIndex v i)
            curS <- VUM.unsafeRead s k
            VUM.unsafeWrite s k (curS + x)
            curC <- VUM.unsafeRead cnt k
            VUM.unsafeWrite cnt k (curC + 1)
            go (i + 1)
{-# INLINE scatterSumCount #-}

finalizeMean ::
    Int -> VUM.MVector s Double -> VUM.MVector s Int -> ST s (VU.Vector Double)
finalizeMean nGroups s cnt = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite out k (if c == 0 then 0 / 0 else sv / fromIntegral c)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

varScatter ::
    (VU.Unbox a, Real a) =>
    Bool -> VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
varScatter takeSqrt g nGroups v = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    meanV <- VUM.replicate nGroups (0 :: Double)
    m2 <- VUM.replicate nGroups (0 :: Double)
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                    !x = realToFrac (VU.unsafeIndex v i)
                c <- VUM.unsafeRead cnt k
                mu <- VUM.unsafeRead meanV k
                mm <- VUM.unsafeRead m2 k
                let !c' = c + 1
                    !delta = x - mu
                    !mu' = mu + delta / fromIntegral c'
                    !mm' = mm + delta * (x - mu')
                VUM.unsafeWrite cnt k c'
                VUM.unsafeWrite meanV k mu'
                VUM.unsafeWrite m2 k mm'
                go (i + 1)
    go 0
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                c <- VUM.unsafeRead cnt k
                mm <- VUM.unsafeRead m2 k
                let var = if c < 2 then 0 else mm / fromIntegral (c - 1)
                VUM.unsafeWrite out k (if takeSqrt then sqrt var else var)
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE varScatter #-}

top2Scatter ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
top2Scatter g nGroups v = runST $ do
    let ninf = negate (1 / 0) :: Double
    m1 <- VUM.replicate nGroups ninf
    m2 <- VUM.replicate nGroups ninf
    let n = VU.length v
        go !i
            | i >= n = pure ()
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
    go 0
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                a1 <- VUM.unsafeRead m1 k
                a2 <- VUM.unsafeRead m2 k
                let s = (if isInfinite a1 then 0 else a1) + (if isInfinite a2 then 0 else a2)
                VUM.unsafeWrite out k s
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE top2Scatter #-}

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

scatterReducePar ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReducePar red vis offs nGroups col
    | not (shouldParallelize parThreshold (VU.length vis)) || nGroups <= 1 =
        scatterReduce red (rtgFromVis vis offs nGroups) nGroups col
    | otherwise = case col of
        UnboxedColumn Nothing (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> Just (reduceParInt red vis offs nGroups v)
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> Just (reduceParDouble red vis offs nGroups v)
                    Nothing -> Nothing
        p@(PackedText _ _) -> scatterReducePar red vis offs nGroups (materializePacked p)
        _ -> Nothing
{-# NOINLINE scatterReducePar #-}

{- | Monomorphic entry points: the 'testEquality' dispatch above only yields an
unsafe coercion, so a direct call to the polymorphic 'reduceParTyped' there
would stay at the abstract element type and never meet its SPECIALIZE rules;
calling through these fixed-type wrappers (the coercion lands on the argument)
does.
-}
reduceParInt ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int -> Column
reduceParInt red vis offs nGroups v = reduceParTyped red vis offs nGroups v intIdent

reduceParDouble ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Double -> Column
reduceParDouble red vis offs nGroups v = reduceParTyped red vis offs nGroups v dblIdent

rtgFromVis :: VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int
rtgFromVis vis offs nGroups = VU.create $ do
    let n = VU.length vis
    rtg <- VUM.new (max 1 n)
    let go !g
            | g >= nGroups = pure ()
            | otherwise = do
                let !e = VU.unsafeIndex offs (g + 1)
                    inner !pos
                        | pos >= e = pure ()
                        | otherwise = do
                            VUM.unsafeWrite rtg (VU.unsafeIndex vis pos) g
                            inner (pos + 1)
                inner (VU.unsafeIndex offs g)
                go (g + 1)
    go 0
    pure rtg

reduceParTyped ::
    forall a.
    (Columnable a, VU.Unbox a, Num a, Ord a, Real a) =>
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Idents a ->
    Column
{- The SPECIALIZE pragmas matter: without them the @realToFrac@ in the
mean/var/top2 kernels survives to runtime as a dictionary call through
'Rational' (the Double->Double/Int->Double rewrite rules only fire once the
type is concrete), costing ~4x on the whole pass. -}
{-# SPECIALIZE reduceParTyped ::
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    Idents Int ->
    Column
    #-}
{-# SPECIALIZE reduceParTyped ::
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    Idents Double ->
    Column
    #-}
reduceParTyped red vis offs nGroups v idents =
    let !caps = capabilities
        !bounds = groupRangeBounds offs nGroups caps
     in case red of
            RCount -> fromUnboxedVector (unsafePerformIO (countPar vis offs nGroups caps bounds))
            RSum -> fromUnboxedVector (unsafePerformIO (sumPar vis offs nGroups v caps bounds))
            RMin ->
                fromUnboxedVector
                    (unsafePerformIO (extremaPar min (minSeed idents) vis offs nGroups v caps bounds))
            RMax ->
                fromUnboxedVector
                    (unsafePerformIO (extremaPar max (maxSeed idents) vis offs nGroups v caps bounds))
            RMean -> fromUnboxedVector (unsafePerformIO (meanPar vis offs nGroups v caps bounds))
            RVar ->
                fromUnboxedVector
                    (unsafePerformIO (varPar False vis offs nGroups v caps bounds))
            RStd ->
                fromUnboxedVector (unsafePerformIO (varPar True vis offs nGroups v caps bounds))
            RTop2Sum -> fromUnboxedVector (unsafePerformIO (top2Par vis offs nGroups v caps bounds))
            RTop2Snd ->
                fromUnboxedVector (unsafePerformIO (top2SndPar vis offs nGroups v caps bounds))
{-# INLINEABLE reduceParTyped #-}

{- | For each group in @[gs, ge)@, fold the group's rows (in @valueIndices@
order, i.e. ascending original-row order) into an accumulator held in
registers, then hand the final accumulator to @done@ exactly once. Keeping the
running state out of memory leaves one write per group instead of a
read-modify-write per row; the per-group fold order is unchanged, so results
stay byte-identical to the row-wise variant.
-}
overGroupsAcc ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    acc ->
    (acc -> Int -> acc) ->
    (Int -> acc -> IO ()) ->
    IO ()
overGroupsAcc vis offs gs ge seed step done = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = pure acc
                    | otherwise = inner (pos + 1) (step acc (VU.unsafeIndex vis pos))
            acc <- inner (VU.unsafeIndex offs g) seed
            done g acc
            grp (g + 1)
{-# INLINE overGroupsAcc #-}

countPar ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Int)
countPar _vis offs nGroups caps bounds = do
    out <- VUM.replicate nGroups (0 :: Int)
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !c = VU.unsafeIndex offs (g + 1) - VU.unsafeIndex offs g
                    VUM.unsafeWrite out g c
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out

sumPar ::
    (VU.Unbox a, Num a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
sumPar vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups 0
    parallelBounds_ caps bounds $ \gs ge ->
        overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
            VUM.unsafeWrite out
    VU.unsafeFreeze out
{-# INLINE sumPar #-}

extremaPar ::
    (VU.Unbox a) =>
    (a -> a -> a) ->
    a ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
extremaPar combine seed vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups seed
    parallelBounds_ caps bounds $ \gs ge ->
        overGroupsAcc
            vis
            offs
            gs
            ge
            seed
            (\acc row -> combine acc (VU.unsafeIndex v row)) $
            VUM.unsafeWrite out
    VU.unsafeFreeze out
{-# INLINE extremaPar #-}

meanPar ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
meanPar vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !acc
                            | pos >= e = acc
                            | otherwise =
                                inner
                                    (pos + 1)
                                    (acc + realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos)))
                        !s0 = VU.unsafeIndex offs g
                        !total = inner s0 0
                        !c = e - s0
                    VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE meanPar #-}

varPar ::
    (VU.Unbox a, Real a) =>
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
varPar takeSqrt vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        -- Per-group Welford state (count, mean, M2) carried in registers; the
        -- update order per group is the same ascending row order as before.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !c !mu !mm
                            | pos >= e =
                                let var = if c < 2 then 0 else mm / fromIntegral (c - 1)
                                 in if takeSqrt then sqrt var else var
                            | otherwise =
                                let !x = realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos))
                                    !c' = c + 1
                                    !delta = x - mu
                                    !mu' = mu + delta / fromIntegral c'
                                    !mm' = mm + delta * (x - mu')
                                 in inner (pos + 1) c' mu' mm'
                        !res = inner (VU.unsafeIndex offs g) (0 :: Int) 0 0
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE varPar #-}

top2Par ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
top2Par vis offs nGroups v caps bounds = do
    let ninf = negate (1 / 0) :: Double
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        -- The (largest, second-largest) pair carried in registers per group.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !a1 !a2
                            | pos >= e =
                                (if isInfinite a1 then 0 else a1)
                                    + (if isInfinite a2 then 0 else a2)
                            | otherwise =
                                let !x = realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos))
                                 in if x > a1
                                        then inner (pos + 1) x a1
                                        else inner (pos + 1) a1 (max a2 x)
                        !res = inner (VU.unsafeIndex offs g) ninf ninf
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE top2Par #-}

{- | Second-largest value per group: the same (largest, second-largest)
register pair as 'top2Par', finalized to the second max alone. Size-1 groups
finalize the @-inf@ seed to NaN (documented; see
'DataFrame.Internal.AggKernel.top2SndScatter').
-}
top2SndPar ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
top2SndPar vis offs nGroups v caps bounds = do
    let ninf = negate (1 / 0) :: Double
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        -- The (largest, second-largest) pair carried in registers per group.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !a1 !a2
                            | pos >= e = if isInfinite a2 then 0 / 0 else a2
                            | otherwise =
                                let !x = realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos))
                                 in if x > a1
                                        then inner (pos + 1) x a1
                                        else inner (pos + 1) a1 (max a2 x)
                        !res = inner (VU.unsafeIndex offs g) ninf ninf
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE top2SndPar #-}

-------------------------------------------------------------------------------
-- Parallel fused max(a) - min(b) (Q7 at wide group domains)
-------------------------------------------------------------------------------

{- | Fused @max a - min b@ over the group-range layout: ONE traversal of
@valueIndices@ accumulating both extrema, parallel by disjoint group range with
no cross-worker merge. min/max are order-independent, so the result is
byte-identical to running the two gather extrema passes separately; the fusion
halves the index traffic. 'Nothing' below the parallel threshold or unless both
columns are clean unboxed and same-typed (Int/Int keeps the Int result of the
interpreter; Double/Double the Double one) — the caller then keeps its two-pass
fallback.
-}
maxMinusMinScatterPar ::
    VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column -> Maybe Column
maxMinusMinScatterPar vis offs nGroups ca cb
    | not (shouldPar (VU.length vis)) || nGroups <= 1 = Nothing
    | otherwise = case (ca, cb) of
        ( UnboxedColumn Nothing (va :: VU.Vector x)
            , UnboxedColumn Nothing (vb :: VU.Vector y)
            )
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    Just (maxMinusMinParInt vis offs nGroups va vb caps bounds)
                | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                    Just (maxMinusMinParDbl vis offs nGroups va vb caps bounds)
        _ -> Nothing
  where
    !caps = capabilities
    !bounds = groupRangeBounds offs nGroups caps
{-# NOINLINE maxMinusMinScatterPar #-}

{- | Monomorphic entry points (see 'reduceParInt' for why the 'testEquality'
dispatch needs them).
-}
maxMinusMinParInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    Column
maxMinusMinParInt vis offs nGroups va vb caps bounds =
    fromUnboxedVector
        ( unsafePerformIO
            (maxMinusMinPar minBound maxBound vis offs nGroups va vb caps bounds)
        )
{-# NOINLINE maxMinusMinParInt #-}

maxMinusMinParDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    Int ->
    VU.Vector Int ->
    Column
maxMinusMinParDbl vis offs nGroups va vb caps bounds =
    fromUnboxedVector
        ( unsafePerformIO
            (maxMinusMinPar (negate (1 / 0)) (1 / 0) vis offs nGroups va vb caps bounds)
        )
{-# NOINLINE maxMinusMinParDbl #-}

maxMinusMinPar ::
    (VU.Unbox a, Num a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
{-# SPECIALIZE maxMinusMinPar ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Int)
    #-}
{-# SPECIALIZE maxMinusMinPar ::
    Double ->
    Double ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
    #-}
maxMinusMinPar maxSeed minSeed vis offs nGroups va vb caps bounds = do
    out <- VUM.new nGroups
    parallelBounds_ caps bounds $ \gs ge ->
        -- Both extrema carried in registers per group; one traversal of the
        -- shared index slice reads both value columns.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !mx !mn
                            | pos >= e = mx - mn
                            | otherwise =
                                let !row = VU.unsafeIndex vis pos
                                 in inner
                                        (pos + 1)
                                        (max mx (VU.unsafeIndex va row))
                                        (min mn (VU.unsafeIndex vb row))
                        !res = inner (VU.unsafeIndex offs g) maxSeed minSeed
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out

-------------------------------------------------------------------------------
-- Streaming (rowToGroup-scatter) kernels: no valueIndices, no placement pass
-------------------------------------------------------------------------------

{- | Group-count cap for the FUSED streaming rtg-scatter kernels
('DataFrame.Internal.Aggregation.Kernel.Moments.momentStreamPar',
'DataFrame.Internal.Aggregation.Kernel.Fused.runFusedAggs'). Above
'DataFrame.Internal.Grouping.Direct.directThreshold' the per-worker accumulator
arrays overflow cache, so a SINGLE streaming reduction loses to a gather pass —
the per-expression dispatch keeps that threshold. A fused multi-reduction pass
amortizes those misses across all its reductions AND avoids the deferred
@valueIndices@ placement entirely, which flips the comparison (measured at 1e6
groups / 1e8 rows on -N16: rowToGroup 0.4s + fused 3-sum stream 1.4s, against
placement 1.1s + fused gather 1.05s), so the fused cap extends to
'directGroupThreshold' — every direct-grouped frame can stream. Wider
groupings are necessarily hash-path (eager @valueIndices@) and use the fused
GATHER kernel ('DataFrame.Internal.Aggregation.Kernel.Fused.runGatherAggs')
instead. Memory: @capabilities * nGroups@ words per accumulator array, at most
~128MB transient at -N16.
-}
streamGroupCap :: Int
streamGroupCap = 1048576

{- | Near-equal contiguous slices of the group domain for parallel merges.
Below 4096 groups (or single-capability) the merge stays on one thread.
-}
groupSlices :: Int -> [(Int, Int)]
groupSlices = chunksFor 4096

{- | Second-largest value per group: the same (largest, second-largest)
accumulator pair as 'top2Scatter', but the finalize returns the second max
alone. A group of size 1 (or 0) leaves the @-inf@ seed in the second slot, so
its output is NaN — documented behaviour (the db-benchmark Q8 data has no
size-1 @id6@ groups). Like 'top2Scatter''s @-inf -> 0@ guard, an actual
infinite data value in the second slot is indistinguishable from the seed.
-}
top2SndScatter ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
top2SndScatter g nGroups v = runST $ do
    let ninf = negate (1 / 0) :: Double
    m1 <- VUM.replicate nGroups ninf
    m2 <- VUM.replicate nGroups ninf
    let n = VU.length v
        go !i
            | i >= n = pure ()
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
    go 0
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                a2 <- VUM.unsafeRead m2 k
                VUM.unsafeWrite out k (if isInfinite a2 then 0 / 0 else a2)
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE top2SndScatter #-}
