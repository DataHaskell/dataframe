{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Vectorized scatter-accumulate aggregation kernel.
module DataFrame.Internal.AggKernel (
    Reduction (..),
    scatterReduce,
    scatterColumnToDouble,
) where

import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (when)
import Control.Monad.ST (ST, runST)
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    fromUnboxedVector,
    materializePacked,
 )
import Type.Reflection (typeRep)

{- | A recognised fast-path reduction over a single value column. The element
type (Int vs Double) is resolved at scatter time; sum/min/max preserve the
column's element type, everything else produces a Double column.
-}
data Reduction
    = RSum
    | RCount
    | RMin
    | RMax
    | RMean
    | RStd
    | RVar
    | RTop2Sum
    deriving (Eq, Show)

{- | Coerce an unboxed Int or Double column to an unboxed Double vector for the
moment/mean/sd/median family. Returns 'Nothing' for boxed, nullable, or other
element types (the caller then falls back to the interpreter).
-}
scatterColumnToDouble :: Column -> Maybe (VU.Vector Double)
scatterColumnToDouble = \case
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> Just v
            Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> Just (VU.map fromIntegral v)
                Nothing -> Nothing
    p@(PackedText _ _) -> scatterColumnToDouble (materializePacked p)
    _ -> Nothing

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
                -- NaN at n = 1, matching computeVariance
                let var = if c < 2 then 0 / 0 else mm / fromIntegral (c - 1)
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
