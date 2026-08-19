{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Internal.Statistics where

import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (runST)
import DataFrame.Errors (DataFrameException (..))

mean' :: (Real a, VU.Unbox a) => VU.Vector a -> Double
mean' samp
    | VU.null samp = throw $ EmptyDataSetException "mean"
    -- Widen per element: summing at 'a' wraps for Int columns.
    | otherwise =
        VU.foldl' (\acc x -> acc + rtf x) 0 samp / fromIntegral (VU.length samp)
{-# INLINE [0] mean' #-}

meanDouble' :: VU.Vector Double -> Double
meanDouble' samp
    | VU.null samp = throw $ EmptyDataSetException "mean"
    | otherwise = VU.sum samp / fromIntegral (VU.length samp)
{-# INLINE meanDouble' #-}

meanInt' :: VU.Vector Int -> Double
meanInt' samp
    | VU.null samp = throw $ EmptyDataSetException "mean"
    | otherwise =
        VU.foldl' (\acc x -> acc + fromIntegral x) 0 samp
            / fromIntegral (VU.length samp)
{-# INLINE meanInt' #-}

{-# RULES
"mean'/Double" [1] forall (xs :: VU.Vector Double).
    mean' xs =
        meanDouble' xs
"mean'/Int" [1] forall (xs :: VU.Vector Int).
    mean' xs =
        meanInt' xs
    #-}

median' :: (Real a, VU.Unbox a) => VU.Vector a -> Double
median' samp
    | VU.null samp = throw $ EmptyDataSetException "median"
    | otherwise = runST $ do
        mutableSamp <- VU.thaw samp
        VA.sort mutableSamp
        let len = VU.length samp
            middleIndex = len `div` 2
        middleElement <- VUM.read mutableSamp middleIndex
        if odd len
            then pure (rtf middleElement)
            else do
                prev <- VUM.read mutableSamp (middleIndex - 1)
                -- Widen before adding: 'a' addition wraps for Int.
                pure ((rtf middleElement + rtf prev) / 2)
{-# INLINE median' #-}

-- accumulator: count, mean, m2
data VarAcc
    = VarAcc {-# UNPACK #-} !Int {-# UNPACK #-} !Double {-# UNPACK #-} !Double
    deriving (Show)

varianceStep :: VarAcc -> Double -> VarAcc
varianceStep (VarAcc !n !meanVal !m2) !x =
    let !n' = n + 1
        !delta = x - meanVal
        !meanVal' = meanVal + delta / fromIntegral n'
        !m2' = m2 + delta * (x - meanVal')
     in VarAcc n' meanVal' m2'
{-# INLINE varianceStep #-}

computeVariance :: VarAcc -> Double
computeVariance (VarAcc !n _ !m2)
    | n == 0 = throw $ EmptyDataSetException "variance"
    -- Sample variance is undefined at n = 1: NaN, not a spurious 0.
    | n < 2 = 0 / 0
    | otherwise = m2 / fromIntegral (n - 1)
{-# INLINE computeVariance #-}

variance' :: (Real a, VU.Unbox a) => VU.Vector a -> Double
variance' = computeVariance . VU.foldl' varianceStep (VarAcc 0 0 0) . VU.map rtf
{-# INLINE variance' #-}

varianceDouble' :: VU.Vector Double -> Double
varianceDouble' = computeVariance . VU.foldl' varianceStep (VarAcc 0 0 0)
{-# INLINE varianceDouble' #-}

-- accumulator: count, mean, m2, m3
data SkewAcc = SkewAcc !Int !Double !Double !Double deriving (Show)

skewnessStep :: (VU.Unbox a, Num a, Real a) => SkewAcc -> a -> SkewAcc
skewnessStep (SkewAcc !n !meanVal !m2 !m3) !x' =
    let !n' = n + 1
        x = rtf x'
        !k = fromIntegral n'
        !delta = x - meanVal
        !meanVal' = meanVal + delta / k
        !m2' = m2 + (delta ^ (2 :: Int) * (k - 1)) / k
        !m3' =
            m3
                + (delta ^ (3 :: Int) * (k - 1) * (k - 2)) / k ^ (2 :: Int)
                - (3 * delta * m2) / k
     in SkewAcc n' meanVal' m2' m3'
{-# INLINE skewnessStep #-}

computeSkewness :: SkewAcc -> Double
computeSkewness (SkewAcc n _ m2 m3)
    | n < 3 = 0 -- or error "skewness of <3 samples"
    -- m2, m3 are raw sums, so population g1 = sqrt n * m3 / m2^(3/2).
    | otherwise = (sqrt (fromIntegral n) * m3) / sqrt (m2 ^ (3 :: Int))
{-# INLINE computeSkewness #-}

skewness' :: (VU.Unbox a, Real a, Num a) => VU.Vector a -> Double
skewness' = computeSkewness . VU.foldl' skewnessStep (SkewAcc 0 0 0 0)
{-# INLINE skewness' #-}

{- | Centered two-pass form: the one-pass @n*Sxy - Sx*Sy@ form cancels
catastrophically on low-variance columns and can report |r| > 1.
-}
correlation' :: VU.Vector Double -> VU.Vector Double -> Maybe Double
correlation' xs ys
    | n < 2 = Nothing
    | VU.length xs /= VU.length ys = Nothing
    | otherwise =
        let nf = fromIntegral n
            !mx = VU.sum xs / nf
            !my = VU.sum ys / nf
            !sxy = VU.sum (VU.zipWith (\x y -> (x - mx) * (y - my)) xs ys)
            !sxx = VU.sum (VU.map (\x -> (x - mx) * (x - mx)) xs)
            !syy = VU.sum (VU.map (\y -> (y - my) * (y - my)) ys)
         in Just (clamp (sxy / sqrt (sxx * syy)))
  where
    n = VU.length xs
    -- Cauchy-Schwarz: clamp the last rounding, never the formula. Explicit
    -- guards so the zero-variance NaN passes through ('max' would eat it).
    clamp r
        | r > 1 = 1
        | r < -1 = -1
        | otherwise = r
{-# INLINE correlation' #-}

quantiles' ::
    (VU.Unbox a, Num a, Real a) =>
    VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
quantiles' qs q samp
    | VU.null samp = throw $ EmptyDataSetException "quantiles"
    | q < 2 = throw $ WrongQuantileNumberException q
    | VU.any (\i -> i < 0 || i > q) qs = throw $ WrongQuantileIndexException qs q
    | otherwise = runST $ do
        let !n = VU.length samp
        mutableSamp <- VU.thaw samp
        VA.sort mutableSamp
        VU.mapM
            ( \i -> do
                let !p = fromIntegral i / fromIntegral q
                    !position = p * fromIntegral (n - 1) :: Double
                    !index = floor position :: Int
                    !f = position - fromIntegral index
                x <- fmap rtf (VUM.read mutableSamp index)
                if f == 0
                    then return x
                    else do
                        y <- fmap rtf (VUM.read mutableSamp (index + 1))
                        return $ (1 - f) * x + f * y
            )
            qs
{-# INLINE quantiles' #-}

percentile' :: (VU.Unbox a, Num a, Real a) => Int -> VU.Vector a -> Double
percentile' n = VU.head . quantiles' (VU.fromList [n]) 100

quantilesOrd' ::
    (Ord a, Eq a) =>
    VU.Vector Int -> Int -> V.Vector a -> V.Vector a
quantilesOrd' qs q samp
    | V.null samp = throw $ EmptyDataSetException "quantiles"
    | q < 2 = throw $ WrongQuantileNumberException q
    | VU.any (\i -> i < 0 || i > q) qs = throw $ WrongQuantileIndexException qs q
    | otherwise = runST $ do
        let !n = V.length samp
        mutableSamp <- V.thaw samp
        VA.sort mutableSamp
        V.mapM
            ( \i -> do
                let !p = fromIntegral i / fromIntegral q :: Double
                    !position = p * fromIntegral (n - 1)
                    !index = floor position :: Int
                -- This is not exact for Ord instances.
                -- Figure out how to make it so.
                VM.read mutableSamp index
            )
            (V.convert qs)

percentileOrd' :: (Ord a, Eq a) => Int -> V.Vector a -> a
percentileOrd' n = V.head . quantilesOrd' (VU.fromList [n]) 100

interQuartileRange' :: (VU.Unbox a, Num a, Real a) => VU.Vector a -> Double
interQuartileRange' samp =
    let quartiles = quantiles' (VU.fromList [1, 3]) 4 samp
     in quartiles VU.! 1 - quartiles VU.! 0
{-# INLINE interQuartileRange' #-}

meanSquaredError :: VU.Vector Double -> VU.Vector Double -> Maybe Double
meanSquaredError target prediction
    | VU.length target /= VU.length prediction = Nothing
    | VU.null target = Nothing
    | otherwise =
        Just
            ( VU.sum (VU.zipWith (\t p -> (p - t) ^ (2 :: Int)) target prediction)
                / fromIntegral (VU.length target)
            )
{-# INLINE meanSquaredError #-}

mutualInformationBinned ::
    Int -> VU.Vector Double -> VU.Vector Double -> Maybe Double
mutualInformationBinned k xs ys
    | VU.length xs /= VU.length ys = Nothing
    | VU.null xs = Nothing
    | k < 2 = Nothing
    | rx <= 0 || ry <= 0 = Just 0
    | otherwise =
        let bx = VU.map (binIndex xmin xmax k) xs
            by = VU.map (binIndex ymin ymax k) ys
            n = fromIntegral (VU.length xs) :: Double
            mx = bincount k bx
            my = bincount k by
            mxy = jointBincount k bx by
         in Just $
                sum
                    [ let !cxy = fromIntegral c
                          !pxy = cxy / n
                          !px = fromIntegral (mx VU.! i) / n
                          !py = fromIntegral (my VU.! j) / n
                       in if c == 0 then 0 else pxy * logBase 2 (pxy / (px * py))
                    | i <- [0 .. k - 1]
                    , j <- [0 .. k - 1]
                    , let !c = mxy VU.! (i * k + j)
                    ]
  where
    (xmin, xmax) = (VU.minimum xs, VU.maximum xs)
    (ymin, ymax) = (VU.minimum ys, VU.maximum ys)
    rx = xmax - xmin
    ry = ymax - ymin

binIndex :: Double -> Double -> Int -> Double -> Int
binIndex lo hi k x
    | hi == lo = 0
    | otherwise =
        let !t = (x - lo) / (hi - lo)
            !ix = floor (fromIntegral k * t) :: Int
         in max 0 (min (k - 1) ix)
{-# INLINE binIndex #-}

bincount :: Int -> VU.Vector Int -> VU.Vector Int
bincount k bs = VU.create $ do
    mv <- VU.thaw (VU.replicate k 0)
    VU.forM_ bs $ \b -> do
        let i
                | b < 0 = 0
                | b >= k = k - 1
                | otherwise = b
        x <- VUM.read mv i
        VUM.write mv i (x + 1)
    pure mv
{-# INLINE bincount #-}

jointBincount :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
jointBincount k bx by = VU.create $ do
    mv <- VU.thaw (VU.replicate (k * k) 0)
    VU.forM_ (VU.zip bx by) $ \(i, j) -> do
        let ii = clamp i 0 (k - 1)
            jj = clamp j 0 (k - 1)
            ix = ii * k + jj
        x <- VUM.read mv ix
        VUM.write mv ix (x + 1)
    pure mv
  where
    clamp z a b = max a (min b z)
{-# INLINE jointBincount #-}

rtf :: (Real a) => a -> Double
rtf = realToFrac
{-# NOINLINE [1] rtf #-}

{-# RULES
"rtf/Double" [2] forall (x :: Double). rtf x = x
    #-}
