{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Numerical-rigor suite: gradient checks (cat 4), reproducibility of the
stochastic models (cat 16), and statistical properties of the RNG and splitters
(cat 5). Every test is constructed to FAIL on a real bug.
-}
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

-- ---------------------------------------------------------------------------
-- Independent reference loss VALUE functions. The library exposes only the
-- gradient 'slGradZ'; these reconstruct the scalar value so the finite
-- difference is computed independently of the analytic gradient.
-- ---------------------------------------------------------------------------

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

{- | Gradient-check one 'SmoothLoss' against its reference value fn over a grid
of @(y, z)@ points. Fails if the analytic gradient differs from the finite
difference anywhere beyond @tol@.
-}
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

-- | The squared-hinge gradient @-2y·max(0,1-yz)@ checked against (max 0 (1-yz))².
testSqHingeGradient :: Test
testSqHingeGradient = gradCheck "squared_hinge" sqHingeLoss sqHingeValue 1e-5

{- | Sign sanity that a self-referential check can't pass: where the loss rises
in @z@ the gradient must be positive (and vice versa). Catches a flipped-sign
gradient even with the right magnitude.
-}
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

-- ---------------------------------------------------------------------------
-- Statistical / distributional tests (cat 5). Tolerances are CI-derived from
-- the sample size (N = 100k → 6σ band ~ 5e-3). A broken sampler — constant or
-- biased — falls well outside.
-- ---------------------------------------------------------------------------

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

{- | @nextDouble@ is ~uniform on [0,1): mean ≈ 0.5 and variance ≈ 1/12, each
within a 6σ CI for 100k samples, AND every draw is in [0,1). A constant or
out-of-range sampler fails; a biased one (mean drifts) fails.
-}
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

{- | Box-Muller @gaussianVector@ produces standard normals: sample mean ≈ 0 and
variance ≈ 1 over 100k draws (6σ band ~ 2e-2). Catches a Box-Muller bug that
shifts the mean or scales the variance.
-}
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

{- | @randomSplit frac@ over many seeds: the realized train fraction matches
@frac@ within a binomial CI, and train+test always sums to the input row count
(cat 2 invariant). A split that loses/dupes rows, or ignores @frac@, fails.
-}
testSplitProportions :: Test
testSplitProportions = TestCase $ do
    let nRows = 4000
        frac = 0.7 :: Double
        df = D.fromNamedColumns [("x", DI.fromList [1 .. nRows :: Int])]
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

{- | k-means inertia on a clean, well-separated blob is stable across seeds and
never beats the true within-cluster sum of squares of the generating partition.
A broken inertia would report values below the optimum or wildly seed-dependent.
-}
testKMeansInertiaStable :: Test
testKMeansInertiaStable = TestCase $ do
    let
        as = [0, 0.1, -0.1, 0.05, -0.05, 10, 10.1, 9.9, 10.05, 9.95] :: [Double]
        bs = [0, -0.1, 0.1, 0.05, -0.05, 10, 9.9, 10.1, 9.95, 10.05] :: [Double]
        df = D.fromNamedColumns [("a", DI.fromList as), ("b", DI.fromList bs)]
        fitSeed s =
            kmInertia $
                fit
                    defaultKMeansConfig{kmK = 2, kmNInit = 1, kmSeed = s}
                    [F.col @Double "a", F.col @Double "b"]
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

-- ---------------------------------------------------------------------------
-- Reproducibility tests (cat 16). Each compares two fits with the same seed
-- (determinism) and asserts a different seed CAN change the model, so a
-- constant-returning stub wouldn't pass.
-- ---------------------------------------------------------------------------

blobsDF :: D.DataFrame
blobsDF =
    D.fromNamedColumns
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
    D.fromNamedColumns
        [ ("a", DI.fromList ([0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32] :: [Double]))
        , ("b", DI.fromList ([0, 1, 0, 10, 11, 10, 0, 1, 0, 10, 11, 10] :: [Double]))
        ]

{- | k-means: same seed → identical KMeansModel (Eq derived); a different seed
on a multi-blob frame CAN yield different centers (the seed actually drives
k-means++ init). Single-init so the seed is not washed out by nInit restarts.
-}
testKMeansReproducible :: Test
testKMeansReproducible = TestCase $ do
    let cfg s = defaultKMeansConfig{kmK = 4, kmNInit = 1, kmMaxIter = 1, kmSeed = s}
        run s = fit (cfg s) [F.col @Double "a", F.col @Double "b"]
        a = run 1 spreadDF
        b = run 1 spreadDF
    assertEqual "k-means same seed identical model" a b
    let centersFor s = kmCenters (run s spreadDF)
        base = centersFor 1
        anyDiffer = any (\s -> centersFor s /= base) [2, 3, 5, 7, 11, 13]
    assertBool "k-means: a different seed changes the model" anyDiffer

{- | GMM: same seed → identical GMMModel (Eq derived); a different seed CAN move
the fitted means (seed drives the responsibility init via sampleIndices).
-}
testGMMReproducible :: Test
testGMMReproducible = TestCase $ do
    let cfg s = defaultGMMConfig{gmmK = 2, gmmMaxIter = 1, gmmSeed = s}
        run s = fit (cfg s) [F.col @Double "a", F.col @Double "b"] blobsDF
        a = run 1
        b = run 1
    assertEqual "GMM same seed identical model" a b
    let meansFor s = gmmMeans (run s)
        base = meansFor 1
        anyDiffer = any (\s -> meansFor s /= base) [2, 3, 4, 5, 6, 7, 8]
    assertBool "GMM: a different seed changes the means" anyDiffer

{- | RFF-SVM: same seed → identical random projection AND identical fitted SVC
coefficients; a different seed changes the random Fourier features. The model
type only derives Show, so compare representative numeric fields directly.
-}
testRFFReproducible :: Test
testRFFReproducible = TestCase $ do
    let clsDF =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        cfg s = defaultRFFConfig{rffD = 40, rffGamma = 0.2, rffSeed = s}
        run s = fit (cfg s) (F.col @Int "label") clsDF
        a = run 5
        b = run 5
    assertEqual "RFF same seed: same projection B" (rffB a) (rffB b)
    assertEqual "RFF same seed: same coefficients" (rffCoef a) (rffCoef b)
    assertEqual "RFF same seed: same intercept" (rffIntercept a) (rffIntercept b)
    let projFor s = rffB (run s)
        base = projFor 5
        anyDiffer = any (\s -> projFor s /= base) [1, 2, 3, 6, 7, 8]
    assertBool "RFF: a different seed changes the projection" anyDiffer

-- ---------------------------------------------------------------------------
-- randomSplit determinism + row-count invariant (cat 16 + 2).
-- ---------------------------------------------------------------------------

{- | @randomSplit@ with the same seed is bit-identical (compared via the
prettyPrinted frames), preserves total rows, and a different seed CAN change the
partition.
-}
testSplitReproducible :: Test
testSplitReproducible = TestCase $ do
    let df = D.fromNamedColumns [("x", DI.fromList [1 .. 200 :: Int])]
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

-- ---------------------------------------------------------------------------
-- A self-consistency sanity for the helpers above so a broken reference value
-- fn cannot make the gradient checks vacuous: the reference squared value must
-- actually be ½(z-y)² at a known point.
-- ---------------------------------------------------------------------------
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
