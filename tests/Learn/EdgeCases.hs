{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Edge-case / degeneracy tests (tests.md category 7) and numerical-stability
tests (category 8) for @dataframe-learn@.

Each test asserts the *mathematically correct* result (computed by hand, with
the derivation in a comment) or a *specific* documented degenerate behaviour --
never "it returned something". Where the library's contract is to clamp/guard a
degeneracy (e.g. @k > n@, constant columns) we assert the concrete clamped
result; where the underlying routine is stable we assert it matches the closed
form a naive implementation would get wrong.
-}
module Learn.EdgeCases (tests) where

import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.GMM
import DataFrame.KMeans
import DataFrame.LinearAlgebra (logSumExp)
import DataFrame.LinearAlgebra.Eigen (jacobiEigenSym)
import DataFrame.LinearAlgebra.Solve (choleskySolve)
import DataFrame.LinearModel
import DataFrame.LinearSolver (sigmoid)
import DataFrame.Model (fit, predict)
import DataFrame.PCA

import DataFrame.Internal.Statistics (correlation', variance')

import Test.HUnit

-- Helpers (mirrors Learn.Models) --------------------------------------------

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

interpI :: D.DataFrame -> Expr Int -> [Int]
interpI df e = case interpret @Int df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Int @VU.Vector c)
    Left err -> error (show err)

mat :: [[Double]] -> V.Vector (VU.Vector Double)
mat = V.fromList . map VU.fromList

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

finite :: Double -> Bool
finite x = not (isNaN x) && not (isInfinite x)

-- ===========================================================================
-- Category 8: numerical stability
-- ===========================================================================

{- logSumExp on a large-positive cluster.
   log(e^1000 + e^1001 + e^1002)
     = 1002 + log(e^-2 + e^-1 + 1)
   e^-2 = 0.1353352832366127, e^-1 = 0.36787944117144233
   sum = 1.5032147243...  ; log(sum) = 0.40760596444...
   => 1002.4076059644...
   A naive `log . sum . map exp` overflows to +Inf here. -}
testLogSumExpLargePositive :: Test
testLogSumExpLargePositive = TestCase $ do
    let lse = logSumExp (VU.fromList [1000, 1001, 1002])
        expected = 1002 + log (exp (-2) + exp (-1) + 1)
    assertBool "lse large-positive is finite" (finite lse)
    assertBool
        "lse [1000,1001,1002] == 1002 + log(e^-2+e^-1+1)"
        (close 1e-9 lse expected)
    -- sanity on the literal so the test pins the actual number, not just the formula
    assertBool "lse literal ~ 1002.40760596" (close 1e-7 lse 1002.4076059644443)

{- logSumExp on a large-negative cluster.
   log(e^-1000 + e^-1001 + e^-1002)
     = -1000 + log(1 + e^-1 + e^-2)  [max is -1000]
     = -1000 + 0.40760596444...
     = -999.59239403556...
   A naive implementation underflows every term to 0 -> log 0 = -Inf. -}
testLogSumExpLargeNegative :: Test
testLogSumExpLargeNegative = TestCase $ do
    let lse = logSumExp (VU.fromList [-1000, -1001, -1002])
        expected = -1000 + log (1 + exp (-1) + exp (-2))
    assertBool "lse large-negative is finite" (finite lse)
    assertBool
        "lse [-1000,-1001,-1002] == -1000 + log(1+e^-1+e^-2)"
        (close 1e-9 lse expected)
    assertBool "lse literal ~ -999.59239403" (close 1e-7 lse (-999.5923940355557))

{- logSumExp of a single element is that element exactly (m + log 1). -}
testLogSumExpSingleton :: Test
testLogSumExpSingleton = TestCase $ do
    assertBool "lse [42] == 42" (close 0 (logSumExp (VU.fromList [42])) 42)

{- sigmoid at extreme arguments must stay in [0,1] and not NaN/Inf.
   sigmoid(1000) is 1 up to rounding (1 - ~0); sigmoid(-1000) is 0.
   The naive 1/(1+exp(-z)) overflows exp(1000)=Inf at z=-1000 -> 1/Inf = 0 ok,
   but exp(-(-1000)) path; the stable branch in DataFrame.LinearSolver.sigmoid
   picks ez/(1+ez) for z<0 so e^-1000 underflows to 0 cleanly -> 0. -}
testSigmoidExtreme :: Test
testSigmoidExtreme = TestCase $ do
    let sp = sigmoid 1000
        sn = sigmoid (-1000)
    assertBool "sigmoid(1000) finite" (finite sp)
    assertBool "sigmoid(-1000) finite" (finite sn)
    assertBool "sigmoid(1000) in [0,1]" (sp >= 0 && sp <= 1)
    assertBool "sigmoid(-1000) in [0,1]" (sn >= 0 && sn <= 1)
    -- saturated values: 1 and 0 to within machine epsilon
    assertBool "sigmoid(1000) == 1" (close 1e-12 sp 1)
    assertBool "sigmoid(-1000) == 0" (close 1e-12 sn 0)
    -- sigmoid(0) is exactly 0.5
    assertBool "sigmoid(0) == 0.5" (close 1e-15 (sigmoid 0) 0.5)
    -- antisymmetry: sigmoid(-z) == 1 - sigmoid(z) at a moderate z
    assertBool
        "sigmoid antisymmetric at 3"
        (close 1e-12 (sigmoid (-3)) (1 - sigmoid 3))

{- Variance of large-but-low-variance data: [1e8+1, 1e8+2, 1e8+3].
   True sample variance (n-1) of {1,2,3} shifted by 1e8 is 1.0 exactly.
   A naive sum-of-squares-minus-square-of-sum formula loses all precision here
   (catastrophic cancellation) -> 0 or negative. Welford keeps it ~1. -}
testVarianceCatastrophicCancellation :: Test
testVarianceCatastrophicCancellation = TestCase $ do
    let xs = VU.fromList [1e8 + 1, 1e8 + 2, 1e8 + 3] :: VU.Vector Double
        v = variance' xs
    assertBool "variance finite" (finite v)
    assertBool "variance is positive" (v > 0)
    -- sample variance of {1,2,3} = ((1-2)^2 + 0 + (3-2)^2)/(3-1) = 2/2 = 1
    assertBool "variance of shifted {1,2,3} == 1.0" (close 1e-6 v 1.0)

{- Variance of an identical column is exactly 0 (not a tiny negative from
   cancellation). -}
testVarianceConstant :: Test
testVarianceConstant = TestCase $ do
    let v = variance' (VU.replicate 100 (7.0 :: Double))
    assertEqual "variance of constant column is 0" 0 v

{- Variance of fewer than two samples is defined to be 0 (computeVariance guard),
   not NaN from a /0. -}
testVarianceSingleton :: Test
testVarianceSingleton = TestCase $ do
    assertEqual
        "variance of one sample is 0"
        0
        (variance' (VU.fromList [3.5 :: Double]))

{- Correlation of a perfectly linear pair is exactly +1 (and -1 reversed),
   computed stably. y = 2x+1 over a spread of x. -}
testCorrelationPerfect :: Test
testCorrelationPerfect = TestCase $ do
    let xs = VU.fromList [1, 2, 3, 4, 5] :: VU.Vector Double
        ys = VU.map (\x -> 2 * x + 1) xs
        yneg = VU.map (\x -> -(2 * x) + 1) xs
    case correlation' xs ys of
        Just r -> assertBool "corr(x, 2x+1) == 1" (close 1e-9 r 1)
        Nothing -> assertFailure "expected a correlation"
    case correlation' xs yneg of
        Just r -> assertBool "corr(x, -2x+1) == -1" (close 1e-9 r (-1))
        Nothing -> assertFailure "expected a correlation"

{- Degeneracy: correlation against a constant column. The denominator
   sqrt(... * (n*Syy - Sy^2)) is 0, so the library returns Just NaN. This pins
   the ACTUAL behaviour: it does not throw, but it does produce a NaN that the
   caller must guard. If a future fix makes it return Nothing or 0 this test
   flags the contract change. -}
testCorrelationConstantColumnIsNaN :: Test
testCorrelationConstantColumnIsNaN = TestCase $ do
    let xs = VU.fromList [1, 2, 3, 4, 5] :: VU.Vector Double
        ys = VU.replicate 5 (9.0 :: Double)
    case correlation' xs ys of
        Just r ->
            assertBool
                "corr with a zero-variance column is NaN (degenerate denom)"
                (isNaN r)
        Nothing -> assertFailure "correlation' returned Nothing (contract changed)"

{- correlation' on n<2 returns Nothing (guarded), not a NaN. -}
testCorrelationTooFew :: Test
testCorrelationTooFew = TestCase $ do
    assertEqual
        "correlation of one point is Nothing"
        Nothing
        (correlation' (VU.fromList [1]) (VU.fromList [2]))

-- ===========================================================================
-- Category 8: stability inside the model expr layer
-- ===========================================================================

{- Logistic probability expressions at extreme feature values must remain in
   [0,1] and finite. The prob expr is 1/(1+exp(-margin)); with a separating
   model and |x| pushed to 1e6 the margin is huge, so a non-stable evaluation
   could overflow. We assert every probability is finite and in [0,1] and that
   the two one-vs-rest class probabilities are present. -}
testLogisticProbsExtremeFeatures :: Test
testLogisticProbsExtremeFeatures = TestCase $ do
    let trainDf =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.col @Int "label") trainDf
        probs = logisticProbExprs m
        -- evaluate the class-1 probability at extreme inputs
        extremeDf =
            D.fromNamedColumns
                [("x", DI.fromList ([-1e6, -1, 0, 1, 1e6] :: [Double]))]
    assertEqual "two one-vs-rest probability exprs" 2 (M.size probs)
    let allProbVals =
            concat [interpD extremeDf e | e <- M.elems probs]
    assertBool "all logistic probabilities finite" (all finite allProbVals)
    assertBool
        "all logistic probabilities in [0,1]"
        (all (\p -> p >= 0 && p <= 1) allProbVals)

-- ===========================================================================
-- Category 7: edge cases / degeneracy on models
-- ===========================================================================

{- One-row OLS: a single point (x=2, y=7). With one observation OLS is
   under-determined; the QR design [1, 2] is rank deficient (n=1 < d=2 cols of
   the intercept-augmented matrix) so olsSolve falls back to ridge(1e-8). The
   fitted line must at minimum interpolate the single training point: the
   prediction at x=2 must equal 7 (within tolerance). It must NOT be NaN/Inf. -}
testOLSOneRow :: Test
testOLSOneRow = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([2] :: [Double]))
                , ("y", DI.fromList ([7] :: [Double]))
                ]
        m = fit defaultLinearConfig (F.col @Double "y") df
        preds = interpD df (predict m)
    assertBool "single OLS prediction finite" (all finite preds)
    assertBool
        "OLS fits the single training point"
        (case preds of [p] -> close 1e-3 p 7; _ -> False)

{- All-identical labels in logistic regression: every row is class 1. There is
   exactly one class, so predict must return that class for every row (no
   division-by-zero, no crash, no spurious second class). -}
testLogisticSingleClass :: Test
testLogisticSingleClass = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-2, -1, 0, 1, 2] :: [Double]))
                , ("label", DI.fromList ([1, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.col @Int "label") df
        preds = interpI df (predict m)
    assertEqual
        "single-class logistic predicts that class everywhere"
        [1, 1, 1, 1, 1]
        preds

{- Perfectly separable logistic data with a constant (zero-variance) feature
   added. The constant column has variance 0 and is dropped by keptIndices
   (>= 1e-12 threshold); the informative column still separates the classes.
   Prediction must recover the labels exactly and not be corrupted by the
   constant column. -}
testLogisticConstantFeatureIgnored :: Test
testLogisticConstantFeatureIgnored = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("const", DI.fromList (replicate 8 (5.0 :: Double)))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.col @Int "label") df
        preds = interpI df (predict m)
    assertEqual
        "logistic ignores constant column and separates"
        [0, 0, 0, 0, 1, 1, 1, 1]
        preds

{- Linear regression with an irrelevant constant feature: the constant column is
   near-constant (variance 0) and must get a zero weight, while the informative
   coefficient is recovered. y = 3x + 4 exactly. -}
testLinearConstantFeatureZeroWeight :: Test
testLinearConstantFeatureZeroWeight = TestCase $ do
    let xs = [1, 2, 3, 4, 5, 6] :: [Double]
        df =
            D.fromNamedColumns
                [ ("x", DI.fromList xs)
                , ("const", DI.fromList (replicate 6 (5.0 :: Double)))
                , ("y", DI.fromList [3 * x + 4 | x <- xs])
                ]
        m = fit defaultLinearConfig (F.col @Double "y") df
        coefs = VU.toList (regCoef m)
        names = V.toList (regFeatureNames m)
        byName = zip names coefs
    -- find the const column weight; it must be ~0 (no information, possibly
    -- collinear with the intercept)
    case lookup "const" byName of
        Just w -> assertBool "constant feature gets ~zero weight" (close 1e-6 w 0)
        Nothing -> assertFailure "const column missing from feature names"
    let preds = interpD df (predict m)
        truth = [3 * x + 4 | x <- xs]
    assertBool
        "regression with constant feature still fits y=3x+4"
        (and (zipWith (close 1e-4) preds truth))

{- k-means with k > n: requesting more clusters than data points. The library
   clamps k = min k (max 1 n), so with 3 rows and kmK=10 we must get exactly 3
   centres (one per point), labels in range, and finite centres -- not a crash
   or empty/duplicate-degenerate centres. -}
testKMeansKGreaterThanN :: Test
testKMeansKGreaterThanN = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList ([0, 5, 10] :: [Double]))
                , ("b", DI.fromList ([0, 5, 10] :: [Double]))
                ]
        cfg = defaultKMeansConfig{kmK = 10, kmNInit = 3, kmSeed = 1}
        m = fit cfg [F.col @Double "a", F.col @Double "b"] df
    assertEqual "k clamped to n=3 centres" 3 (V.length (kmCenters m))
    let labels = VU.toList (kmLabels m)
    assertBool "all labels in [0,2]" (all (\l -> l >= 0 && l < 3) labels)
    assertBool
        "all centre coordinates finite"
        (all (VU.all finite) (V.toList (kmCenters m)))
    -- with 3 distinct points and 3 clusters, inertia must be ~0 (each point is
    -- its own centre)
    assertBool "k=n clustering has ~zero inertia" (close 1e-9 (kmInertia m) 0)

{- k-means on an all-identical (zero-variance) feature set: every row is the same
   point. Any clustering has inertia 0; centres must all equal that point and be
   finite (no NaN from dividing by an empty cluster or by a zero distance sum in
   k-means++ sampling). -}
testKMeansAllIdentical :: Test
testKMeansAllIdentical = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList (replicate 6 (4.0 :: Double)))
                , ("b", DI.fromList (replicate 6 (4.0 :: Double)))
                ]
        cfg = defaultKMeansConfig{kmK = 2, kmNInit = 3, kmSeed = 1}
        m = fit cfg [F.col @Double "a", F.col @Double "b"] df
    assertBool
        "centres finite on degenerate identical data"
        (all (VU.all finite) (V.toList (kmCenters m)))
    assertBool "inertia is 0 for identical points" (close 1e-12 (kmInertia m) 0)
    -- predict must still assign a valid (finite, in-range) label to every row
    let assigns = interpI df (predict m)
    assertBool "assignments in [0,1]" (all (\l -> l >= 0 && l < 2) assigns)

{- GMM with k > n: clamped like k-means (k = min k (max 1 n)). Two rows, gmmK=5
   must yield 2 components, finite weights summing to ~1, and a finite log
   likelihood -- not a crash or NaN. -}
testGMMKGreaterThanN :: Test
testGMMKGreaterThanN = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList ([0, 10] :: [Double]))
                , ("b", DI.fromList ([0, 10] :: [Double]))
                ]
        cfg = defaultGMMConfig{gmmK = 5, gmmSeed = 1}
        m = fit cfg [F.col @Double "a", F.col @Double "b"] df
    assertEqual "GMM k clamped to n=2" 2 (VU.length (gmmWeights m))
    assertBool "GMM weights finite" (VU.all finite (gmmWeights m))
    assertBool "GMM weights sum to ~1" (close 1e-6 (VU.sum (gmmWeights m)) 1)
    assertBool "GMM log-likelihood finite" (finite (gmmLogLikelihood m))

{- PCA on a single informative axis plus a constant column: the constant column
   carries zero variance, so its explained-variance ratio must be ~0 and the
   ratios must still sum to ~1 and stay finite. The first component should align
   with the varying axis. Catches a divide-by-zero in the ratio normalisation
   when one eigenvalue is 0. -}
testPCAConstantColumn :: Test
testPCAConstantColumn = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-2, -1, 0, 1, 2] :: [Double]))
                , ("const", DI.fromList (replicate 5 (3.0 :: Double)))
                ]
        m =
            fit
                (PCAConfig (NComp 2) False)
                [F.col @Double "x", F.col @Double "const"]
                df
        ratio = VU.toList (pcaExplainedVarianceRatio m)
    assertBool "explained ratios finite" (all finite ratio)
    assertBool "explained ratios sum to ~1" (close 1e-9 (sum ratio) 1)
    -- the variance is entirely along x, so the top ratio must be ~1
    assertBool "first PC explains ~all variance" (close 1e-9 (head ratio) 1)
    -- projection exprs must evaluate to finite numbers
    let es = map snd (pcaExprs m)
    assertBool
        "pca exprs finite on constant column"
        (all (all finite . interpD df) es)

{- PCA on extreme-scale features (1e8-shifted) without standardisation: the
   covariance entries are ~1 (variance of {1,2,3}) despite the 1e8 offset.
   Components must stay unit-length and finite -- catches cancellation in the
   covariance / eigendecomposition at large magnitudes. -}
testPCAExtremeScale :: Test
testPCAExtremeScale = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([1e8 + 1, 1e8 + 2, 1e8 + 3, 1e8 + 4] :: [Double]))
                , ("y", DI.fromList ([1e8 + 1, 1e8 + 2, 1e8 + 3, 1e8 + 4] :: [Double]))
                ]
        m =
            fit
                (PCAConfig (NComp 1) False)
                [F.col @Double "x", F.col @Double "y"]
                df
        comp0 = pcaComponents m V.! 0
        nrm = sqrt (VU.sum (VU.map (^ (2 :: Int)) comp0))
    assertBool "component finite at 1e8 scale" (VU.all finite comp0)
    assertBool "component unit length at 1e8 scale" (close 1e-6 nrm 1)
    assertBool
        "explained ratio finite at 1e8 scale"
        (VU.all finite (pcaExplainedVarianceRatio m))

-- ===========================================================================
-- Category 7/8: linear-algebra degeneracy
-- ===========================================================================

{- choleskySolve on a non-positive-definite (zero) matrix returns Nothing rather
   than crashing or producing NaNs. A 2x2 all-zero matrix has a zero pivot. -}
testCholeskyNonPD :: Test
testCholeskyNonPD = TestCase $ do
    assertEqual
        "cholesky of singular zero matrix is Nothing"
        Nothing
        (choleskySolve (mat [[0, 0], [0, 0]]) (VU.fromList [1, 1]))

{- Jacobi eigendecomposition of a 1x1 matrix (one-column degenerate case):
   eigenvalue is the single entry, eigenvector is [1] (sign-canonicalised). -}
testJacobi1x1 :: Test
testJacobi1x1 = TestCase $ do
    let (ev, vecs) = jacobiEigenSym (mat [[5]])
    assertBool "1x1 eigenvalue is the entry" (close 1e-12 (ev VU.! 0) 5)
    assertBool "1x1 eigenvector is [1]" (close 1e-12 ((vecs V.! 0) VU.! 0) 1)

{- Jacobi on an identity matrix: both eigenvalues are exactly 1 and the result is
   finite (the rotation angle code must not divide by zero when off-diagonals are
   already 0). -}
testJacobiIdentity :: Test
testJacobiIdentity = TestCase $ do
    let (ev, vecs) = jacobiEigenSym (mat [[1, 0], [0, 1]])
    assertBool "identity eigenvalues both 1" (VU.all (close 1e-12 1) ev)
    assertBool "identity eigenvectors finite" (all (VU.all finite) (V.toList vecs))

tests :: [Test]
tests =
    [ testLogSumExpLargePositive
    , testLogSumExpLargeNegative
    , testLogSumExpSingleton
    , testSigmoidExtreme
    , testVarianceCatastrophicCancellation
    , testVarianceConstant
    , testVarianceSingleton
    , testCorrelationPerfect
    , testCorrelationConstantColumnIsNaN
    , testCorrelationTooFew
    , testLogisticProbsExtremeFeatures
    , testOLSOneRow
    , testLogisticSingleClass
    , testLogisticConstantFeatureIgnored
    , testLinearConstantFeatureZeroWeight
    , testKMeansKGreaterThanN
    , testKMeansAllIdentical
    , testGMMKGreaterThanN
    , testPCAConstantColumn
    , testPCAExtremeScale
    , testCholeskyNonPD
    , testJacobi1x1
    , testJacobiIdentity
    ]
