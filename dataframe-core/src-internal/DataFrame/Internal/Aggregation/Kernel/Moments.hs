{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | The fused two-column moment kernel: one pass over @x@ and @y@ producing the
six sufficient statistics @(n, Sx, Sy, Sxx, Syy, Sxy)@ per group, from which the
whole correlation\/regression family (mean, variance, covariance, correlation,
OLS slope) is algebra requiring no further look at the rows.

The sequential and parallel passes live together because they must agree on
floating-point accumulation order. Moments are additive, so a row-range split
with a merge would be correct in exact arithmetic — but float addition is not
associative, so 'momentScatterPar' partitions by GROUP range instead: every
group accumulates start-to-finish inside a single worker, in the same order as
'momentScatter'. That is what makes the two byte-identical at any @-N@.
-}
module DataFrame.Internal.Aggregation.Kernel.Moments (
    Moments (..),
    momentScatter,
    momentScatterPar,
    momentStreamPar,
) where

import Control.Monad.ST (runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import Type.Reflection (typeRep)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)

import DataFrame.Internal.Aggregation.Kernel.Scatter (
    groupRangeBounds,
    groupSlices,
    rtgFromVis,
    streamGroupCap,
 )
import DataFrame.Internal.Aggregation.Reduction (cleanDoubleVector)
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

{- | The additive moment sums of two columns, each an @nGroups@-length column:
@(n, Sx, Sy, Sxx, Syy, Sxy)@.
-}
data Moments = Moments
    { mN :: Column
    , mSx :: Column
    , mSy :: Column
    , mSxx :: Column
    , mSyy :: Column
    , mSxy :: Column
    }

{- | One pass over two Double-coercible columns @x@ and @y@ filling the count and
five sums, collapsing the Q9 regression family's six folds into a single pass.
'Nothing' unless both columns are non-null unboxed Int/Double.
-}
momentScatter :: VU.Vector Int -> Int -> Column -> Column -> Maybe Moments
momentScatter g nGroups colX colY = do
    xs <- cleanDoubleVector colX
    ys <- cleanDoubleVector colY
    let (cnt, sx, sy, sxx, syy, sxy) = momentPass g nGroups xs ys
    pure
        Moments
            { mN = fromUnboxedVector cnt
            , mSx = fromUnboxedVector sx
            , mSy = fromUnboxedVector sy
            , mSxx = fromUnboxedVector sxx
            , mSyy = fromUnboxedVector syy
            , mSxy = fromUnboxedVector sxy
            }

momentPass ::
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    ( VU.Vector Int
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    )
momentPass g nGroups xs ys = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    sx <- VUM.replicate nGroups (0 :: Double)
    sy <- VUM.replicate nGroups (0 :: Double)
    sxx <- VUM.replicate nGroups (0 :: Double)
    syy <- VUM.replicate nGroups (0 :: Double)
    sxy <- VUM.replicate nGroups (0 :: Double)
    let n = VU.length xs
        bump arr k d = VUM.unsafeRead arr k >>= \c -> VUM.unsafeWrite arr k (c + d)
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                    !x = VU.unsafeIndex xs i
                    !y = VU.unsafeIndex ys i
                VUM.unsafeRead cnt k >>= \c -> VUM.unsafeWrite cnt k (c + 1)
                bump sx k x
                bump sy k y
                bump sxx k (x * x)
                bump syy k (y * y)
                bump sxy k (x * y)
                go (i + 1)
    go 0
    (,,,,,)
        <$> VU.unsafeFreeze cnt
        <*> VU.unsafeFreeze sx
        <*> VU.unsafeFreeze sy
        <*> VU.unsafeFreeze sxx
        <*> VU.unsafeFreeze syy
        <*> VU.unsafeFreeze sxy

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
        xs <- cleanDoubleVector colX
        ys <- cleanDoubleVector colY
        let !caps = capabilities
            !bounds = groupRangeBounds offs nGroups caps
        pure (unsafePerformIO (momentPar vis offs nGroups xs ys caps bounds))
{-# NOINLINE momentScatterPar #-}

-------------------------------------------------------------------------------

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
    parallelBounds_ caps bounds $ \gs ge ->
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
        forkJoin
            [ momentStreamChunk rtg nGroups vx vy lo hi
            | w <- [0 .. caps' - 1]
            , let lo = min n (w * per)
            , let hi = min n (lo + per)
            ]
    case parts of
        [] -> error "momentStreamTyped: no partials"
        (p0 : rest) -> do
            _ <-
                forkJoin
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

