{-# LANGUAGE BangPatterns #-}

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
) where

import Control.Monad.ST (runST)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)

import DataFrame.Internal.Aggregation.Kernel.Scatter (
    groupRangeBounds,
    overGroups,
    rtgFromVis,
 )
import DataFrame.Internal.Aggregation.Reduction (cleanDoubleVector)
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

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
    | not (shouldParallelize parThreshold (VU.length vis)) || nGroups <= 1 =
        momentScatter (rtgFromVis vis offs nGroups) nGroups colX colY
    | otherwise = do
        xs <- cleanDoubleVector colX
        ys <- cleanDoubleVector colY
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
    parallelBounds_ caps bounds $ \gs ge ->
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
