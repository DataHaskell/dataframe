{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Vectorized scatter-accumulate aggregation kernel.

The grouping layer ('DataFrame.Internal.Grouping') hands us a dense
@rowToGroup@ vector (group id per row, canonical order) plus the number of
groups. For the common reductions this kernel replaces the per-group boxed
expression-interpreter fold with a single unboxed linear pass that scatters
each row's value into primitive per-group accumulator arrays indexed by the
group id. No boxed accumulator record, no per-element dictionary closure: the
element type is resolved once per column by a 'typeRep' switch and the inner
loop is a monomorphic primop on a 'VU.Vector'.

Result columns are length @nGroups@ in canonical group order, so they line up
with the key columns 'aggregate' gathers with @selectIndices@.

The kernel is strictly a FAST PATH: the matcher 'DataFrame.Internal.AggPlan.planAgg'
recognises a small set of expression shapes; anything it does not recognise
keeps the existing interpreter, so the general @aggregate@ API stays correct for
arbitrary expressions.
-}
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

-------------------------------------------------------------------------------
-- Column extraction
-------------------------------------------------------------------------------

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

-------------------------------------------------------------------------------
-- Scatter reductions
-------------------------------------------------------------------------------

{- | Run one fast-path reduction. Returns 'Nothing' when the value column is
not a non-null unboxed Int/Double column (then the caller falls back).
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

{- | The monomorphic reduction body. @count@ always yields an Int column;
@sum@/@min@/@max@ preserve the element type; @mean@/@std@/@var@ produce Double;
@top2Sum@ produces Double.
-}
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

{- | One pass filling running sum and count from value column @v@ over groups
@g@ into the supplied accumulator arrays.
-}
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

{- | Sample variance (or its square root for sd) via a per-group Welford
recurrence, scattered into three unboxed arrays @(count, mean, m2)@. This is the
same numerically-stable update as the interpreter's @varianceStep@, so the
result is byte-identical to the existing CollectAgg path (and the db-benchmark
checksum is unchanged); the win is the unboxed scatter replacing the per-group
boxed @VarAcc@ fold over a materialized slice. Degenerate groups (n < 2) yield
0, matching @computeVariance@.
-}
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

{- | Per-group sum of the two largest values: a 2-slot scatter holding the
running first and second maximum, then @m1 + m2@. Matches the @take 2 . sortBy
(flip compare)@ definition used by the benchmark's @top2Sum@.
-}
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
