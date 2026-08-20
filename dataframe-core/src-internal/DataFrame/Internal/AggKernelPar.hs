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
    momentStreamPar,
    streamGroupCap,
    FusedAgg,
    mkFusedAgg,
    runFusedAggs,
    GatherAgg,
    mkGatherAgg,
    runGatherAggs,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, evaluate, throwIO, try)
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

-------------------------------------------------------------------------------
-- Streaming (rowToGroup-scatter) kernels: no valueIndices, no placement pass
-------------------------------------------------------------------------------

{- | Group-count cap for the FUSED streaming rtg-scatter kernels
('momentStreamPar', 'runFusedAggs'). Above
'DataFrame.Internal.AggKernelDirect.directThreshold' the per-worker accumulator
arrays overflow cache, so a SINGLE streaming reduction loses to a gather pass —
the per-expression dispatch keeps that threshold. A fused multi-reduction pass
amortizes those misses across all its reductions AND avoids the deferred
@valueIndices@ placement entirely, which flips the comparison (measured at 1e6
groups / 1e8 rows on -N16: rowToGroup 0.4s + fused 3-sum stream 1.4s, against
placement 1.1s + fused gather 1.05s), so the fused cap extends to
'directGroupThreshold' — every direct-grouped frame can stream. Wider
groupings are necessarily hash-path (eager @valueIndices@) and use the fused
GATHER kernel ('runGatherAggs') instead. Memory: @capabilities * nGroups@
words per accumulator array, at most ~128MB transient at -N16.
-}
streamGroupCap :: Int
streamGroupCap = 1048576

{- | Run each action on its own thread and collect the results in order;
rethrow the first failure. A single action runs on the calling thread.
-}
forkJoinActs :: [IO a] -> IO [a]
forkJoinActs [act] = fmap (: []) act
forkJoinActs actions = do
    vars <- mapM spawn actions
    results <- mapM takeMVar vars
    mapM (either (throwIO :: SomeException -> IO a) pure) results
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkIO (try act >>= putMVar var)
        pure var

-- | Near-equal contiguous slices of the group domain for parallel merges.
groupSlices :: Int -> [(Int, Int)]
groupSlices nGroups
    | nGroups < 4096 || capabilities <= 1 = [(0, nGroups)]
    | otherwise =
        [ (lo, hi)
        | w <- [0 .. capabilities - 1]
        , let lo = min nGroups (w * per)
        , let hi = min nGroups (lo + per)
        , lo < hi
        ]
  where
    !per = (nGroups + capabilities - 1) `div` capabilities

-------------------------------------------------------------------------------
-- Streaming fused two-column moments (Q9)
-------------------------------------------------------------------------------

{- | Streaming counterpart of 'momentScatterPar': one fused pass over
@rowToGroup@ and the two TYPED value columns (Int values convert to Double
in-register — bit-identical to the @VU.map fromIntegral@ materialization it
replaces, with no 800MB intermediate column and no sequential conversion pass).
Each worker accumulates the six per-group sums over its contiguous row chunk in
original row order; partials merge in fixed worker order (counts exactly, the
five Double sums in chunk-major float order — deterministic at a fixed @-N@,
but a different summation order than the per-group gather kernel).
'Nothing' above 'streamGroupCap' or unless both columns are clean unboxed
Int/Double; the caller then keeps the gather path.
-}
momentStreamPar :: VU.Vector Int -> Int -> Column -> Column -> Maybe Moments
momentStreamPar rtg nGroups colX colY
    | nGroups <= 0 || nGroups > streamGroupCap = Nothing
    | otherwise = case (colX, colY) of
        ( UnboxedColumn Nothing (vx :: VU.Vector x)
            , UnboxedColumn Nothing (vy :: VU.Vector y)
            )
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    Just (momentStreamII rtg nGroups vx vy)
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                    Just (momentStreamID rtg nGroups vx vy)
                | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    Just (momentStreamDI rtg nGroups vx vy)
                | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                    Just (momentStreamDD rtg nGroups vx vy)
        _ -> Nothing
{-# NOINLINE momentStreamPar #-}

{- | Monomorphic entry points (see 'reduceParInt' for why the 'testEquality'
dispatch needs them).
-}
momentStreamII :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int -> Moments
momentStreamII = momentStreamTyped
{-# NOINLINE momentStreamII #-}

momentStreamID :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double -> Moments
momentStreamID = momentStreamTyped
{-# NOINLINE momentStreamID #-}

momentStreamDI :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Int -> Moments
momentStreamDI = momentStreamTyped
{-# NOINLINE momentStreamDI #-}

momentStreamDD :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double -> Moments
momentStreamDD = momentStreamTyped
{-# NOINLINE momentStreamDD #-}

-- | The six per-group running sums of one worker chunk.
data MomentAcc = MomentAcc
    { maCnt :: !(VUM.IOVector Int)
    , maSx :: !(VUM.IOVector Double)
    , maSy :: !(VUM.IOVector Double)
    , maSxx :: !(VUM.IOVector Double)
    , maSyy :: !(VUM.IOVector Double)
    , maSxy :: !(VUM.IOVector Double)
    }

newMomentAcc :: Int -> IO MomentAcc
newMomentAcc nGroups =
    MomentAcc
        <$> VUM.replicate nGroups 0
        <*> VUM.replicate nGroups 0
        <*> VUM.replicate nGroups 0
        <*> VUM.replicate nGroups 0
        <*> VUM.replicate nGroups 0
        <*> VUM.replicate nGroups 0

momentStreamTyped ::
    forall a b.
    (VU.Unbox a, VU.Unbox b, Real a, Real b) =>
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector b ->
    Moments
{- The SPECIALIZE pragmas matter for the same reason as 'reduceParTyped': the
per-element @realToFrac@ must rewrite to @int2Double@/@id@ at a concrete type
or it goes through 'Rational' at runtime. -}
{-# SPECIALIZE momentStreamTyped ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int -> Moments
    #-}
{-# SPECIALIZE momentStreamTyped ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double -> Moments
    #-}
{-# SPECIALIZE momentStreamTyped ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Int -> Moments
    #-}
{-# SPECIALIZE momentStreamTyped ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double -> Moments
    #-}
momentStreamTyped rtg nGroups vx vy = unsafePerformIO $ do
    let !n = VU.length rtg
        !caps' = if shouldPar n then capabilities else 1
        !per = (max 1 n + caps' - 1) `div` caps'
    parts <-
        forkJoinActs
            [ momentStreamChunk rtg nGroups vx vy lo hi
            | w <- [0 .. caps' - 1]
            , let lo = min n (w * per)
            , let hi = min n (lo + per)
            ]
    case parts of
        [] -> error "momentStreamTyped: no partials"
        (p0 : rest) -> do
            _ <-
                forkJoinActs
                    [ mapM_ (\p -> mergeMomentRange p0 p lo hi) rest
                    | (lo, hi) <- groupSlices nGroups
                    ]
            freezeMoments p0
{-# INLINEABLE momentStreamTyped #-}

momentStreamChunk ::
    (VU.Unbox a, VU.Unbox b, Real a, Real b) =>
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector b ->
    Int ->
    Int ->
    IO MomentAcc
momentStreamChunk rtg nGroups vx vy lo hi = do
    acc@(MomentAcc cnt sx sy sxx syy sxy) <- newMomentAcc nGroups
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex rtg i
                    !x = realToFrac (VU.unsafeIndex vx i) :: Double
                    !y = realToFrac (VU.unsafeIndex vy i) :: Double
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite cnt k (c + 1)
                ax <- VUM.unsafeRead sx k
                VUM.unsafeWrite sx k (ax + x)
                ay <- VUM.unsafeRead sy k
                VUM.unsafeWrite sy k (ay + y)
                axx <- VUM.unsafeRead sxx k
                VUM.unsafeWrite sxx k (axx + x * x)
                ayy <- VUM.unsafeRead syy k
                VUM.unsafeWrite syy k (ayy + y * y)
                axy <- VUM.unsafeRead sxy k
                VUM.unsafeWrite sxy k (axy + x * y)
                go (i + 1)
    go lo
    pure acc
{-# INLINE momentStreamChunk #-}

mergeMomentRange :: MomentAcc -> MomentAcc -> Int -> Int -> IO ()
mergeMomentRange a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            addI (maCnt a) (maCnt b) g
            addD (maSx a) (maSx b) g
            addD (maSy a) (maSy b) g
            addD (maSxx a) (maSxx b) g
            addD (maSyy a) (maSyy b) g
            addD (maSxy a) (maSxy b) g
            go (g + 1)
    addI p q g = do
        x <- VUM.unsafeRead p g
        y <- VUM.unsafeRead q g
        VUM.unsafeWrite p g (x + y)
    addD p q g = do
        x <- VUM.unsafeRead p g
        y <- VUM.unsafeRead q g
        VUM.unsafeWrite p g (x + y)

freezeMoments :: MomentAcc -> IO Moments
freezeMoments (MomentAcc cnt sx sy sxx syy sxy) =
    Moments . fromUnboxedVector
        <$> VU.unsafeFreeze cnt
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze syy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxy)

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
        forkJoinActs
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
                    forkJoinActs
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
    forEachRange bounds caps $ \gs ge ->
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
