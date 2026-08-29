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

    -- * Group-range helpers
    -- $shared
    groupRangeBounds,
    rtgFromVis,
    overGroups,
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
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

{- $shared
Also used by "DataFrame.Internal.Aggregation.Kernel.Moments", which partitions
the group axis the same way.
-}

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
                Just Refl -> Just (reduceParTyped red vis offs nGroups v intIdent)
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> Just (reduceParTyped red vis offs nGroups v dblIdent)
                    Nothing -> Nothing
        p@(PackedText _ _) -> scatterReducePar red vis offs nGroups (materializePacked p)
        _ -> Nothing
{-# NOINLINE scatterReducePar #-}

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
    parallelBounds_ caps bounds $ \gs ge ->
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
    parallelBounds_ caps bounds $ \gs ge ->
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
    parallelBounds_ caps bounds $ \gs ge ->
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
    parallelBounds_ caps bounds $ \gs ge ->
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
