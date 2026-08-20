{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Parallel scatter-accumulate aggregation kernel.
module DataFrame.Internal.AggKernelPar (
    scatterReducePar,
    maxMinusMinScatterPar,
    momentScatterPar,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.AggKernel (
    Reduction (..),
    scatterColumnToDouble,
    scatterReduce,
 )
import DataFrame.Internal.AggPlan (Moments (..), momentScatter)
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    fromUnboxedVector,
    materializePacked,
 )

parThreshold :: Int
parThreshold = 200000

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

-- | Whether to take the parallel path at this row count.
shouldPar :: Int -> Bool
shouldPar n = n >= parThreshold && capabilities > 1

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

scatterReducePar ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReducePar red vis offs nGroups col
    | not (shouldPar (VU.length vis)) || nGroups <= 1 =
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

data Idents a = Idents {minSeed :: !a, maxSeed :: !a}

intIdent :: Idents Int
intIdent = Idents maxBound minBound

dblIdent :: Idents Double
dblIdent = Idents (1 / 0) (negate (1 / 0))

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
    forEachRange bounds caps $ \gs ge ->
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
    forEachRange bounds caps $ \gs ge ->
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
    forEachRange bounds caps $ \gs ge ->
        overGroupsAcc vis offs gs ge seed (\acc row -> combine acc (VU.unsafeIndex v row)) $
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
    forEachRange bounds caps $ \gs ge ->
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
    forEachRange bounds caps $ \gs ge ->
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
    forEachRange bounds caps $ \gs ge ->
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
        (unsafePerformIO (maxMinusMinPar minBound maxBound vis offs nGroups va vb caps bounds))
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
    forEachRange bounds caps $ \gs ge ->
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
-- Parallel fused two-column moments (Q9)
-------------------------------------------------------------------------------

{- | Parallel counterpart of 'momentScatter': one fused pass over both columns,
each group's six sums accumulated within one worker's range. Byte-identical to
'momentScatter'. 'Nothing' unless both columns are non-null unboxed Int/Double.
-}
momentScatterPar ::
    VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column -> Maybe Moments
momentScatterPar vis offs nGroups colX colY
    | not (shouldPar (VU.length vis)) || nGroups <= 1 =
        momentScatter (rtgFromVis vis offs nGroups) nGroups colX colY
    | otherwise = do
        xs <- scatterColumnToDouble colX
        ys <- scatterColumnToDouble colY
        let !caps = capabilities
            !bounds = groupRangeBounds offs nGroups caps
        pure (unsafePerformIO (momentPar vis offs nGroups xs ys caps bounds))
{-# NOINLINE momentScatterPar #-}

momentPar ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    Int ->
    VU.Vector Int ->
    IO Moments
momentPar vis offs nGroups xs ys caps bounds = do
    cnt <- VUM.replicate nGroups (0 :: Int)
    sx <- VUM.replicate nGroups (0 :: Double)
    sy <- VUM.replicate nGroups (0 :: Double)
    sxx <- VUM.replicate nGroups (0 :: Double)
    syy <- VUM.replicate nGroups (0 :: Double)
    sxy <- VUM.replicate nGroups (0 :: Double)
    forEachRange bounds caps $ \gs ge ->
        -- The six running sums carried in registers per group, written once.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !ax !ay !axx !ayy !axy
                            | pos >= e = do
                                VUM.unsafeWrite sx g ax
                                VUM.unsafeWrite sy g ay
                                VUM.unsafeWrite sxx g axx
                                VUM.unsafeWrite syy g ayy
                                VUM.unsafeWrite sxy g axy
                            | otherwise =
                                let !row = VU.unsafeIndex vis pos
                                    !x = VU.unsafeIndex xs row
                                    !y = VU.unsafeIndex ys row
                                 in inner
                                        (pos + 1)
                                        (ax + x)
                                        (ay + y)
                                        (axx + x * x)
                                        (ayy + y * y)
                                        (axy + x * y)
                        !s0 = VU.unsafeIndex offs g
                    VUM.unsafeWrite cnt g (e - s0)
                    inner s0 0 0 0 0 0
                    grp (g + 1)
         in grp gs
    Moments . fromUnboxedVector
        <$> VU.unsafeFreeze cnt
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze syy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxy)
