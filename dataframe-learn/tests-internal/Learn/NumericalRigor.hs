{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Numerical-rigor suite: gradient checks, reproducibility, RNG properties.
module Learn.NumericalRigor (tests) where

import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrameApi as D

import DataFrame.GMM
import DataFrame.KMeans
import DataFrame.LinearSolver.Loss (
    SmoothLoss (..),
    logisticLoss,
    sigmoid,
    sqHingeLoss,
    squaredLoss,
 )
import DataFrame.Random
import DataFrame.SVM.RFF

import qualified Data.Vector.Unboxed as VU
import System.Random (mkStdGen)
import Test.HUnit

-- | @½ (z - y)²@  (squaredLoss).
squaredValue :: Double -> Double -> Double
squaredValue y z = 0.5 * (z - y) * (z - y)

-- | @log (1 + exp (-y z))@  (logisticLoss). softplus form, numerically stable.
logisticValue :: Double -> Double -> Double
logisticValue y z =
    let m = negate (y * z)
     in if m >= 0 then m + log1pExp (negate m) else log1pExp m
  where
    log1pExp x = log (1 + exp x)

-- | @(max 0 (1 - y z))²@  (sqHingeLoss).
sqHingeValue :: Double -> Double -> Double
sqHingeValue y z = let m = 1 - y * z in if m > 0 then m * m else 0

-- | Central finite difference of @f@ in @z@ at @(y, z)@.
centralDiff ::
    (Double -> Double -> Double) -> Double -> Double -> Double -> Double
centralDiff f eps y z = (f y (z + eps) - f y (z - eps)) / (2 * eps)

-- | Gradient-check one SmoothLoss against its reference over a grid of points.
gradCheck ::
    String ->
    SmoothLoss ->
    (Double -> Double -> Double) ->
    Double ->
    Test
gradCheck nm loss valueFn tol =
    TestCase $
        mapM_ check [(y, z) | y <- [-1, 1], z <- zs]
  where
    zs = [-3.7, -2.1, -1.25, -0.6, -0.15, 0.22, 0.55, 1.4, 1.9, 2.8, 4.3]
    check (y, z) = do
        let analytic = slGradZ loss y z
            fd = centralDiff valueFn 1e-6 y z
            ok = abs (analytic - fd) <= tol * (1 + abs fd)
        assertBool
            ( nm
                ++ " grad mismatch at (y="
                ++ show y
                ++ ", z="
                ++ show z
                ++ "): analytic="
                ++ show analytic
                ++ " finite-diff="
                ++ show fd
            )
            ok

-- | The squared-loss gradient @z - y@ checked against ½(z-y)².
testSquaredGradient :: Test
testSquaredGradient = gradCheck "squared" squaredLoss squaredValue 1e-5

-- | The logistic gradient @-y·σ(-y z)@ checked against log(1+exp(-yz)).
testLogisticGradient :: Test
testLogisticGradient = gradCheck "logistic" logisticLoss logisticValue 1e-5

-- | The squared-hinge gradient checked against its closed form.
testSqHingeGradient :: Test
testSqHingeGradient = gradCheck "squared_hinge" sqHingeLoss sqHingeValue 1e-5

-- | Sign sanity: where the loss rises in z the gradient must be positive.
testGradientSigns :: Test
testGradientSigns = TestCase $ do
    assertBool
        "squared: grad>0 when z>y (loss rising)"
        (slGradZ squaredLoss 1.0 3.0 > 0)
    assertBool
        "squared: grad<0 when z<y (loss falling)"
        (slGradZ squaredLoss 1.0 (-2.0) < 0)
    assertBool
        "logistic y=+1: grad<0 (more positive margin lowers loss)"
        (slGradZ logisticLoss 1.0 0.5 < 0)
    assertBool
        "logistic y=-1: grad>0"
        (slGradZ logisticLoss (-1.0) 0.5 > 0)
    assertBool
        "sqHinge active margin y=+1: grad<0"
        (slGradZ sqHingeLoss 1.0 0.0 < 0)
    assertBool
        "sqHinge satisfied margin: grad==0"
        (slGradZ sqHingeLoss 1.0 5.0 == 0)

-- | Draw @n@ uniforms threading the generator; returns the sample list.
drawUniforms :: Int -> Gen -> [Double]
drawUniforms n g0 = go n g0 []
  where
    go 0 _ acc = acc
    go k g acc = let (x, g') = nextDouble g in go (k - 1) g' (x : acc)

mean :: [Double] -> Double
mean xs = sum xs / fromIntegral (length xs)

variance :: [Double] -> Double
variance xs =
    let m = mean xs
     in sum [(x - m) * (x - m) | x <- xs] / fromIntegral (length xs)

-- | nextDouble is roughly uniform on [0,1): mean, variance and range checks.
testUniformDistribution :: Test
testUniformDistribution = TestCase $ do
    let n = 100000 :: Int
        xs = drawUniforms n (mkGen 20240613)
        m = mean xs
        v = variance xs
        seMean = (1 / sqrt 12) / sqrt (fromIntegral n)
    assertBool "all uniforms in [0,1)" (all (\x -> x >= 0 && x < 1) xs)
    assertBool
        ("uniform mean ~0.5, got " ++ show m)
        (abs (m - 0.5) <= 6 * seMean)
    assertBool
        ("uniform variance ~1/12, got " ++ show v)
        (abs (v - 1 / 12) <= 0.01)
    assertBool "uniform actually varies" (maximum xs - minimum xs > 0.9)

-- | Box-Muller gaussianVector yields standard normals: mean near 0, variance 1.
testGaussianMoments :: Test
testGaussianMoments = TestCase $ do
    let n = 100000 :: Int
        (vec, _) = gaussianVector n (mkGen 777)
        xs = VU.toList vec
        m = mean xs
        v = variance xs
        seMean = 1 / sqrt (fromIntegral n)
    assertBool "gaussianVector length" (VU.length vec == n)
    assertBool
        "gaussian samples finite"
        (all (\x -> not (isNaN x) && not (isInfinite x)) xs)
    assertBool
        ("gaussian mean ~0, got " ++ show m)
        (abs m <= 6 * seMean)
    assertBool
        ("gaussian variance ~1, got " ++ show v)
        (abs (v - 1) <= 0.03)
    let tail2 = fromIntegral (length (filter (\x -> abs x > 2) xs)) / fromIntegral n
    assertBool
        ("gaussian |x|>2 frequency ~0.0455, got " ++ show tail2)
        (abs (tail2 - 0.0455) <= 0.01)

-- | randomSplit train fraction matches frac and train plus test sums to input.
testSplitProportions :: Test
testSplitProportions = TestCase $ do
    let nRows = 4000
        frac = 0.7 :: Double
        df = D.fromColumns [("x", DI.fromList [1 .. nRows :: Int])]
        seeds = [1 .. 25] :: [Int]
        seFrac = sqrt (frac * (1 - frac) / fromIntegral nRows)
    mapM_
        ( \s -> do
            let (tr, te) = D.randomSplit (mkStdGen s) frac df
                nTr = fst (D.dimensions tr)
                nTe = fst (D.dimensions te)
                realized = fromIntegral nTr / fromIntegral nRows
            assertEqual
                ("split preserves rows (seed " ++ show s ++ ")")
                nRows
                (nTr + nTe)
            assertBool
                ( "split fraction ~"
                    ++ show frac
                    ++ " (seed "
                    ++ show s
                    ++ "), got "
                    ++ show realized
                )
                (abs (realized - frac) <= 5 * seFrac)
        )
        seeds

-- | k-means inertia is seed-stable and never beats the generating partition.
testKMeansInertiaStable :: Test
testKMeansInertiaStable = TestCase $ do
    let
        as = [0, 0.1, -0.1, 0.05, -0.05, 10, 10.1, 9.9, 10.05, 9.95] :: [Double]
        bs = [0, -0.1, 0.1, 0.05, -0.05, 10, 9.9, 10.1, 9.95, 10.05] :: [Double]
        df = D.fromColumns [("a", DI.fromList as), ("b", DI.fromList bs)]
        fitSeed s =
            kmInertia $
                fit
                    defaultKMeansConfig{kmK = 2, kmNInit = 1, kmSeed = s}
                    [F.double "a", F.double "b"]
                    df
        inertias = map fitSeed [0 .. 19]
        blob1 = take 5 (zip as bs)
        blob2 = zip (drop 5 as) (drop 5 bs)
        ssOf pts =
            let mx = sum (map fst pts) / 5
                my = sum (map snd pts) / 5
             in sum [(x - mx) ^ (2 :: Int) + (y - my) ^ (2 :: Int) | (x, y) <- pts]
        optimum = ssOf blob1 + ssOf blob2
        best = minimum inertias
        worst = maximum inertias
    assertBool
        "k-means inertia finite"
        (all (\i -> not (isNaN i) && not (isInfinite i)) inertias)
    assertBool
        ( "k-means inertia never below optimum ("
            ++ show optimum
            ++ "), best="
            ++ show best
        )
        (best >= optimum - 1e-9)
    assertBool
        ( "k-means inertia stable across seeds, best="
            ++ show best
            ++ " worst="
            ++ show worst
        )
        (worst - best <= 1e-6 + 1e-3 * optimum)

blobsDF :: D.DataFrame
blobsDF =
    D.fromColumns
        [
            ( "a"
            , DI.fromList ([0, 0.2, -0.1, 0.1, 8, 8.1, 7.9, 8.2, 0.05, 8.05] :: [Double])
            )
        ,
            ( "b"
            , DI.fromList ([0, -0.1, 0.2, 0.0, 5, 5.2, 4.9, 5.1, 0.1, 5.05] :: [Double])
            )
        ]

-- | Many-cluster frame so k-means++ seeding genuinely depends on the seed.
spreadDF :: D.DataFrame
spreadDF =
    D.fromColumns
        [ ("a", DI.fromList ([0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32] :: [Double]))
        , ("b", DI.fromList ([0, 1, 0, 10, 11, 10, 0, 1, 0, 10, 11, 10] :: [Double]))
        ]

-- | k-means: same seed gives an identical model; a different seed can differ.
testKMeansReproducible :: Test
testKMeansReproducible = TestCase $ do
    let cfg s = defaultKMeansConfig{kmK = 4, kmNInit = 1, kmMaxIter = 1, kmSeed = s}
        run s = fit (cfg s) [F.double "a", F.double "b"]
        a = run 1 spreadDF
        b = run 1 spreadDF
    assertEqual "k-means same seed identical model" a b
    let centersFor s = kmCenters (run s spreadDF)
        base = centersFor 1
        anyDiffer = any (\s -> centersFor s /= base) [2, 3, 5, 7, 11, 13]
    assertBool "k-means: a different seed changes the model" anyDiffer

-- | GMM: same seed gives an identical model; a different seed can move means.
testGMMReproducible :: Test
testGMMReproducible = TestCase $ do
    let cfg s = defaultGMMConfig{gmmK = 2, gmmMaxIter = 1, gmmSeed = s}
        run s = fit (cfg s) [F.double "a", F.double "b"] blobsDF
        a = run 1
        b = run 1
    assertEqual "GMM same seed identical model" a b
    let meansFor s = gmmMeans (run s)
        base = meansFor 1
        anyDiffer = any (\s -> meansFor s /= base) [2, 3, 4, 5, 6, 7, 8]
    assertBool "GMM: a different seed changes the means" anyDiffer

-- | RFF-SVM: same seed gives identical projection and SVC coefficients.
testRFFReproducible :: Test
testRFFReproducible = TestCase $ do
    let clsDF =
            D.fromColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        cfg s = defaultRFFConfig{rffD = 40, rffGamma = 0.2, rffSeed = s}
        run s = fit (cfg s) (F.int "label") clsDF
        a = run 5
        b = run 5
    assertEqual "RFF same seed: same projection B" (rffB a) (rffB b)
    assertEqual "RFF same seed: same coefficients" (rffCoef a) (rffCoef b)
    assertEqual "RFF same seed: same intercept" (rffIntercept a) (rffIntercept b)
    let projFor s = rffB (run s)
        base = projFor 5
        anyDiffer = any (\s -> projFor s /= base) [1, 2, 3, 6, 7, 8]
    assertBool "RFF: a different seed changes the projection" anyDiffer

testSplitReproducible :: Test
testSplitReproducible = TestCase $ do
    let df = D.fromColumns [("x", DI.fromList [1 .. 200 :: Int])]
        (tr1, te1) = D.randomSplit (mkStdGen 42) 0.6 df
        (tr2, te2) = D.randomSplit (mkStdGen 42) 0.6 df
    assertBool "randomSplit same seed: same train" (tr1 == tr2)
    assertBool "randomSplit same seed: same test" (te1 == te2)
    assertEqual
        "randomSplit preserves row count"
        200
        (fst (D.dimensions tr1) + fst (D.dimensions te1))
    let trainFor s = fst (D.randomSplit (mkStdGen s) 0.6 df)
        base = trainFor 42
        anyDiffer = any (\s -> trainFor s /= base) [1, 2, 3, 7, 99]
    assertBool "randomSplit: a different seed changes the partition" anyDiffer

testReferenceValueSanity :: Test
testReferenceValueSanity = TestCase $ do
    assertBool
        "squaredValue at (y=2,z=5) == 4.5"
        (abs (squaredValue 2 5 - 4.5) < 1e-12)
    assertBool
        "logisticValue at (y=1,z=0) == log 2"
        (abs (logisticValue 1 0 - log 2) < 1e-12)
    assertBool "sqHingeValue at (y=1,z=0) == 1" (abs (sqHingeValue 1 0 - 1) < 1e-12)
    assertBool "sigmoid 0 == 0.5" (abs (sigmoid 0 - 0.5) < 1e-12)

tests :: [Test]
tests =
    [ testReferenceValueSanity
    , testSquaredGradient
    , testLogisticGradient
    , testSqHingeGradient
    , testGradientSigns
    , testUniformDistribution
    , testGaussianMoments
    , testSplitProportions
    , testKMeansInertiaStable
    , testKMeansReproducible
    , testGMMReproducible
    , testRFFReproducible
    , testSplitReproducible
    ]
