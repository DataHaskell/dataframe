{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Parallel scatter-accumulate aggregation kernel.

This builds on the sequential kernel ('DataFrame.Internal.AggKernel') and the
Round-5 grouping layout: 'groupBy' hands us @valueIndices@ (rows ordered by
group) and @offsets@ (per-group boundaries), so each group's rows are a
contiguous run @valueIndices[offsets[g] .. offsets[g+1])@ in their original row
order.

The parallel driver splits the dense group-id range @[0, nGroups)@ into one
contiguous chunk per capability, balanced by row count. Because group ranges are
disjoint and their @valueIndices@ runs are disjoint, every worker reads and
writes its own slice of the shared output array(s) — there is NO cross-worker
overlap and NO merge. And because each group's rows are visited in the same
original-row order as the sequential @rowToGroup@ scan, the per-group fold order
is unchanged, so the result is /byte-identical/ to the sequential kernel at any
@-N@ (no fold-order drift, even for the float sums).

Forks plain 'forkIO' workers (no sparks), one per capability; falls back to the
sequential 'DataFrame.Internal.AggKernel' path when @caps == 1@ or the row count
is below 'parThreshold'.
-}
module DataFrame.Internal.AggKernelPar (
    scatterReducePar,
    momentScatterPar,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (when)
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

-------------------------------------------------------------------------------
-- Parallelisation policy
-------------------------------------------------------------------------------

{- | Below this many rows the fork overhead is not worth it; the sequential
kernel runs instead. Mirrors 'DataFrame.Internal.GroupingPar.parThreshold'.
-}
parThreshold :: Int
parThreshold = 200000

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

-- | Whether to take the parallel path at this row count.
shouldPar :: Int -> Bool
shouldPar n = n >= parThreshold && capabilities > 1

-------------------------------------------------------------------------------
-- Group-range partitioning by row count
-------------------------------------------------------------------------------

{- | Split @[0, nGroups)@ into @caps@ contiguous group ranges, each holding a
near-equal share of rows. Returns @caps + 1@ boundaries @b@ with @b[0] == 0@ and
@b[caps] == nGroups@; worker @w@ owns groups @[b[w], b[w+1])@. A row-balanced
split keeps skew low even when group sizes vary wildly.
-}
groupRangeBounds :: VU.Vector Int -> Int -> Int -> VU.Vector Int
groupRangeBounds offs nGroups caps = VU.create $ do
    b <- VUM.new (caps + 1)
    let !nRows = VU.unsafeIndex offs nGroups
        !per = max 1 ((nRows + caps - 1) `div` caps)
        -- First group whose start offset reaches @target@ (>= prev), or nGroups.
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

{- | Fork @caps@ workers, worker @w@ running @act (b[w]) (b[w+1])@ over its
disjoint group range; join and rethrow the first failure. Sequential when
@caps == 1@.
-}
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

-------------------------------------------------------------------------------
-- Parallel single-column reductions
-------------------------------------------------------------------------------

{- | Parallel counterpart of 'scatterReduce'. Returns 'Nothing' on the same
columns the sequential kernel rejects (boxed/nullable/non-Int-Double); on the
sequential path or tiny inputs it delegates to 'scatterReduce'. The result is
byte-identical to 'scatterReduce' (same per-group fold order).
-}
scatterReducePar ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReducePar red vis offs nGroups col
    | not (shouldPar (VU.length vis)) || nGroups <= 1 =
        scatterReduce red (rtgFromVis vis offs nGroups) nGroups col
    | otherwise = case col of
        UnboxedColumn Nothing (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> Just (reduceParTyped red vis offs nGroups v intIdent)
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> Just (reduceParTyped red vis offs nGroups v dblIdent)
                    Nothing -> Nothing
        p@(PackedText _ _) -> scatterReducePar red vis offs nGroups (materializePacked p)
        _ -> Nothing
{-# NOINLINE scatterReducePar #-}

{- | Reconstruct @rowToGroup@ from the group layout for the sequential delegate.
Only used on the small/-N1 path, so the extra pass is negligible.
-}
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

{- | The monomorphic parallel reduction body. Each scatter allocates its full
@nGroups@ output array(s) once, then workers fill disjoint group ranges in
parallel. The result type follows the sequential kernel exactly.
-}
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
{-# INLINE reduceParTyped #-}

-- | Iterate the rows of groups @[gs, ge)@ in @valueIndices@/group order.
overGroups ::
    VU.Vector Int -> VU.Vector Int -> Int -> Int -> (Int -> Int -> IO ()) -> IO ()
overGroups vis offs gs ge step = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos
                    | pos >= e = pure ()
                    | otherwise = step g (VU.unsafeIndex vis pos) >> inner (pos + 1)
            inner (VU.unsafeIndex offs g)
            grp (g + 1)
{-# INLINE overGroups #-}

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
        overGroups vis offs gs ge $ \g row -> do
            cur <- VUM.unsafeRead out g
            VUM.unsafeWrite out g (cur + VU.unsafeIndex v row)
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
        overGroups vis offs gs ge $ \g row -> do
            cur <- VUM.unsafeRead out g
            VUM.unsafeWrite out g (combine cur (VU.unsafeIndex v row))
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
    s <- VUM.replicate nGroups (0 :: Double)
    cnt <- VUM.replicate nGroups (0 :: Int)
    forEachRange bounds caps $ \gs ge ->
        overGroups vis offs gs ge $ \g row -> do
            let !x = realToFrac (VU.unsafeIndex v row)
            cs <- VUM.unsafeRead s g
            VUM.unsafeWrite s g (cs + x)
            cc <- VUM.unsafeRead cnt g
            VUM.unsafeWrite cnt g (cc + 1)
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite out k (if c == 0 then 0 / 0 else sv / fromIntegral c)
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE meanPar #-}

{- | Per-group Welford variance/sd, parallel by group range. The recurrence is
applied in original-row order within each group, identical to the sequential
@varScatter@, so the result is byte-identical (no parallel-combine needed: each
group lives wholly inside one worker's range).
-}
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
    cnt <- VUM.replicate nGroups (0 :: Int)
    meanV <- VUM.replicate nGroups (0 :: Double)
    m2 <- VUM.replicate nGroups (0 :: Double)
    forEachRange bounds caps $ \gs ge ->
        overGroups vis offs gs ge $ \g row -> do
            let !x = realToFrac (VU.unsafeIndex v row)
            c <- VUM.unsafeRead cnt g
            mu <- VUM.unsafeRead meanV g
            mm <- VUM.unsafeRead m2 g
            let !c' = c + 1
                !delta = x - mu
                !mu' = mu + delta / fromIntegral c'
                !mm' = mm + delta * (x - mu')
            VUM.unsafeWrite cnt g c'
            VUM.unsafeWrite meanV g mu'
            VUM.unsafeWrite m2 g mm'
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
    m1 <- VUM.replicate nGroups ninf
    m2 <- VUM.replicate nGroups ninf
    forEachRange bounds caps $ \gs ge ->
        overGroups vis offs gs ge $ \g row -> do
            let !x = realToFrac (VU.unsafeIndex v row)
            a1 <- VUM.unsafeRead m1 g
            if x > a1
                then do
                    VUM.unsafeWrite m1 g x
                    VUM.unsafeWrite m2 g a1
                else do
                    a2 <- VUM.unsafeRead m2 g
                    when (x > a2) (VUM.unsafeWrite m2 g x)
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                a1 <- VUM.unsafeRead m1 k
                a2 <- VUM.unsafeRead m2 k
                let sm = (if isInfinite a1 then 0 else a1) + (if isInfinite a2 then 0 else a2)
                VUM.unsafeWrite out k sm
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE top2Par #-}

-------------------------------------------------------------------------------
-- Parallel fused two-column moments (Q9)
-------------------------------------------------------------------------------

{- | Parallel counterpart of 'momentScatter': one fused pass over both columns,
each group's six sums accumulated in original-row order inside one worker's
range. Byte-identical to 'momentScatter'; delegates to it on the sequential
path. Returns 'Nothing' unless both columns are non-null unboxed Int/Double.
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
    let bump arr g d = VUM.unsafeRead arr g >>= \c -> VUM.unsafeWrite arr g (c + d)
    forEachRange bounds caps $ \gs ge ->
        overGroups vis offs gs ge $ \g row -> do
            let !x = VU.unsafeIndex xs row
                !y = VU.unsafeIndex ys row
            VUM.unsafeRead cnt g >>= \c -> VUM.unsafeWrite cnt g (c + 1)
            bump sx g x
            bump sy g y
            bump sxx g (x * x)
            bump syy g (y * y)
            bump sxy g (x * y)
    Moments . fromUnboxedVector
        <$> VU.unsafeFreeze cnt
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxx)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze syy)
        <*> (fromUnboxedVector <$> VU.unsafeFreeze sxy)
