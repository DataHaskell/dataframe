{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Edge-case and numerical-stability tests for dataframe-learn.
module Learn.EdgeCases (tests) where

import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)
import qualified DataFrameApi as D

import DataFrame.GMM
import DataFrame.KMeans
import DataFrame.LinearAlgebra (logSumExp)
import DataFrame.LinearAlgebra.Eigen (jacobiEigenSym)
import DataFrame.LinearAlgebra.Solve (choleskySolve)
import DataFrame.LinearModel
import DataFrame.LinearSolver (sigmoid)
import DataFrame.PCA

import DataFrame.Internal.Statistics (correlation', meanSquaredError, variance')

import Test.HUnit

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

-- logSumExp on a large positive cluster must not overflow to +Inf.
testLogSumExpLargePositive :: Test
testLogSumExpLargePositive = TestCase $ do
    let lse = logSumExp (VU.fromList [1000, 1001, 1002])
        expected = 1002 + log (exp (-2) + exp (-1) + 1)
    assertBool "lse large-positive is finite" (finite lse)
    assertBool
        "lse [1000,1001,1002] == 1002 + log(e^-2+e^-1+1)"
        (close 1e-9 lse expected)
    assertBool "lse literal ~ 1002.40760596" (close 1e-7 lse 1002.4076059644443)

-- logSumExp on a large negative cluster must not underflow to -Inf.
testLogSumExpLargeNegative :: Test
testLogSumExpLargeNegative = TestCase $ do
    let lse = logSumExp (VU.fromList [-1000, -1001, -1002])
        expected = -1000 + log (1 + exp (-1) + exp (-2))
    assertBool "lse large-negative is finite" (finite lse)
    assertBool
        "lse [-1000,-1001,-1002] == -1000 + log(1+e^-1+e^-2)"
        (close 1e-9 lse expected)
    assertBool "lse literal ~ -999.59239403" (close 1e-7 lse (-999.5923940355557))

-- logSumExp of a single element is that element exactly.
testLogSumExpSingleton :: Test
testLogSumExpSingleton = TestCase $ do
    assertBool "lse [42] == 42" (close 0 (logSumExp (VU.fromList [42])) 42)

-- sigmoid at extreme arguments stays in [0,1] and finite.
testSigmoidExtreme :: Test
testSigmoidExtreme = TestCase $ do
    let sp = sigmoid 1000
        sn = sigmoid (-1000)
    assertBool "sigmoid(1000) finite" (finite sp)
    assertBool "sigmoid(-1000) finite" (finite sn)
    assertBool "sigmoid(1000) in [0,1]" (sp >= 0 && sp <= 1)
    assertBool "sigmoid(-1000) in [0,1]" (sn >= 0 && sn <= 1)
    assertBool "sigmoid(1000) == 1" (close 1e-12 sp 1)
    assertBool "sigmoid(-1000) == 0" (close 1e-12 sn 0)
    assertBool "sigmoid(0) == 0.5" (close 1e-15 (sigmoid 0) 0.5)
    assertBool
        "sigmoid antisymmetric at 3"
        (close 1e-12 (sigmoid (-3)) (1 - sigmoid 3))

-- Variance of large low-variance data is 1.0, not lost to cancellation.
testVarianceCatastrophicCancellation :: Test
testVarianceCatastrophicCancellation = TestCase $ do
    let xs = VU.fromList [1e8 + 1, 1e8 + 2, 1e8 + 3] :: VU.Vector Double
        v = variance' xs
    assertBool "variance finite" (finite v)
    assertBool "variance is positive" (v > 0)
    assertBool "variance of shifted {1,2,3} == 1.0" (close 1e-6 v 1.0)

-- Variance of an identical column is exactly 0.
testVarianceConstant :: Test
testVarianceConstant = TestCase $ do
    let v = variance' (VU.replicate 100 (7.0 :: Double))
    assertEqual "variance of constant column is 0" 0 v

-- Variance of fewer than two samples is 0, not NaN.
testVarianceSingleton :: Test
testVarianceSingleton = TestCase $ do
    assertEqual
        "variance of one sample is 0"
        0
        (variance' (VU.fromList [3.5 :: Double]))

-- Correlation of a perfectly linear pair is exactly +1 (and -1 reversed).
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

-- Correlation against a constant column is Just NaN; pins current behaviour.
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

-- correlation' on n<2 returns Nothing, not NaN.
testCorrelationTooFew :: Test
testCorrelationTooFew = TestCase $ do
    assertEqual
        "correlation of one point is Nothing"
        Nothing
        (correlation' (VU.fromList [1]) (VU.fromList [2]))

-- meanSquaredError refuses length mismatches and empty inputs.
testMeanSquaredErrorGuards :: Test
testMeanSquaredErrorGuards = TestCase $ do
    assertEqual
        "mse of mismatched lengths is Nothing"
        Nothing
        (meanSquaredError (VU.fromList [0, 0, 0, 0]) (VU.fromList [2, 2]))
    assertEqual
        "mse with the longer prediction does not index out of bounds"
        Nothing
        (meanSquaredError (VU.fromList [1]) (VU.fromList [1, 2, 3]))
    assertEqual
        "mse of empty inputs is Nothing"
        Nothing
        (meanSquaredError VU.empty VU.empty)
    assertEqual
        "mse of equal-length inputs is the plain mean"
        (Just 4.0)
        (meanSquaredError (VU.fromList [0, 0]) (VU.fromList [2, 2]))

-- Logistic probabilities at extreme features stay finite and in [0,1].
testLogisticProbsExtremeFeatures :: Test
testLogisticProbsExtremeFeatures = TestCase $ do
    let trainDf =
            D.fromColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.int "label") trainDf
        probs = logisticProbExprs m
        extremeDf =
            D.fromColumns
                [("x", DI.fromList ([-1e6, -1, 0, 1, 1e6] :: [Double]))]
    assertEqual "two one-vs-rest probability exprs" 2 (M.size probs)
    let allProbVals =
            concat [interpD extremeDf e | e <- M.elems probs]
    assertBool "all logistic probabilities finite" (all finite allProbVals)
    assertBool
        "all logistic probabilities in [0,1]"
        (all (\p -> p >= 0 && p <= 1) allProbVals)

-- One-row OLS falls back to ridge and interpolates the point, never NaN.
testOLSOneRow :: Test
testOLSOneRow = TestCase $ do
    let df =
            D.fromColumns
                [ ("x", DI.fromList ([2] :: [Double]))
                , ("y", DI.fromList ([7] :: [Double]))
                ]
        m = fit defaultLinearConfig (F.double "y") df
        preds = interpD df (predict m)
    assertBool "single OLS prediction finite" (all finite preds)
    assertBool
        "OLS fits the single training point"
        (case preds of [p] -> close 1e-3 p 7; _ -> False)

-- Logistic regression with a single class predicts that class everywhere.
testLogisticSingleClass :: Test
testLogisticSingleClass = TestCase $ do
    let df =
            D.fromColumns
                [ ("x", DI.fromList ([-2, -1, 0, 1, 2] :: [Double]))
                , ("label", DI.fromList ([1, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.int "label") df
        preds = interpI df (predict m)
    assertEqual
        "single-class logistic predicts that class everywhere"
        [1, 1, 1, 1, 1]
        preds

-- A constant feature is dropped; the informative one still separates.
testLogisticConstantFeatureIgnored :: Test
testLogisticConstantFeatureIgnored = TestCase $ do
    let df =
            D.fromColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("const", DI.fromList (replicate 8 (5.0 :: Double)))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        m = fit defaultLogisticConfig (F.int "label") df
        preds = interpI df (predict m)
    assertEqual
        "logistic ignores constant column and separates"
        [0, 0, 0, 0, 1, 1, 1, 1]
        preds

-- A constant feature gets zero weight; y = 3x + 4 is recovered exactly.
testLinearConstantFeatureZeroWeight :: Test
testLinearConstantFeatureZeroWeight = TestCase $ do
    let xs = [1, 2, 3, 4, 5, 6] :: [Double]
        df =
            D.fromColumns
                [ ("x", DI.fromList xs)
                , ("const", DI.fromList (replicate 6 (5.0 :: Double)))
                , ("y", DI.fromList [3 * x + 4 | x <- xs])
                ]
        m = fit defaultLinearConfig (F.double "y") df
        coefs = VU.toList (regCoef m)
        names = V.toList (regFeatureNames m)
        byName = zip names coefs
    case lookup "const" byName of
        Just w -> assertBool "constant feature gets ~zero weight" (close 1e-6 w 0)
        Nothing -> assertFailure "const column missing from feature names"
    let preds = interpD df (predict m)
        truth = [3 * x + 4 | x <- xs]
    assertBool
        "regression with constant feature still fits y=3x+4"
        (and (zipWith (close 1e-4) preds truth))

-- k-means with k > n clamps k, giving finite centres and in-range labels.
testKMeansKGreaterThanN :: Test
testKMeansKGreaterThanN = TestCase $ do
    let df =
            D.fromColumns
                [ ("a", DI.fromList ([0, 5, 10] :: [Double]))
                , ("b", DI.fromList ([0, 5, 10] :: [Double]))
                ]
        cfg = defaultKMeansConfig{kmK = 10, kmNInit = 3, kmSeed = 1}
        m = fit cfg [F.double "a", F.double "b"] df
    assertEqual "k clamped to n=3 centres" 3 (V.length (kmCenters m))
    let labels = VU.toList (kmLabels m)
    assertBool "all labels in [0,2]" (all (\l -> l >= 0 && l < 3) labels)
    assertBool
        "all centre coordinates finite"
        (all (VU.all finite) (V.toList (kmCenters m)))
    assertBool "k=n clustering has ~zero inertia" (close 1e-9 (kmInertia m) 0)

-- k-means on identical points has inertia 0 and finite centres.
testKMeansAllIdentical :: Test
testKMeansAllIdentical = TestCase $ do
    let df =
            D.fromColumns
                [ ("a", DI.fromList (replicate 6 (4.0 :: Double)))
                , ("b", DI.fromList (replicate 6 (4.0 :: Double)))
                ]
        cfg = defaultKMeansConfig{kmK = 2, kmNInit = 3, kmSeed = 1}
        m = fit cfg [F.double "a", F.double "b"] df
    assertBool
        "centres finite on degenerate identical data"
        (all (VU.all finite) (V.toList (kmCenters m)))
    assertBool "inertia is 0 for identical points" (close 1e-12 (kmInertia m) 0)
    let assigns = interpI df (predict m)
    assertBool "assignments in [0,1]" (all (\l -> l >= 0 && l < 2) assigns)

-- GMM with k > n clamps k; weights sum to ~1 and the likelihood is finite.
testGMMKGreaterThanN :: Test
testGMMKGreaterThanN = TestCase $ do
    let df =
            D.fromColumns
                [ ("a", DI.fromList ([0, 10] :: [Double]))
                , ("b", DI.fromList ([0, 10] :: [Double]))
                ]
        cfg = defaultGMMConfig{gmmK = 5, gmmSeed = 1}
        m = fit cfg [F.double "a", F.double "b"] df
    assertEqual "GMM k clamped to n=2" 2 (VU.length (gmmWeights m))
    assertBool "GMM weights finite" (VU.all finite (gmmWeights m))
    assertBool "GMM weights sum to ~1" (close 1e-6 (VU.sum (gmmWeights m)) 1)
    assertBool "GMM log-likelihood finite" (finite (gmmLogLikelihood m))

-- PCA with a constant column: its explained ratio is ~0 and all are finite.
testPCAConstantColumn :: Test
testPCAConstantColumn = TestCase $ do
    let df =
            D.fromColumns
                [ ("x", DI.fromList ([-2, -1, 0, 1, 2] :: [Double]))
                , ("const", DI.fromList (replicate 5 (3.0 :: Double)))
                ]
        m =
            fit
                (PCAConfig (NComp 2) False)
                [F.double "x", F.double "const"]
                df
        ratio = VU.toList (pcaExplainedVarianceRatio m)
    assertBool "explained ratios finite" (all finite ratio)
    assertBool "explained ratios sum to ~1" (close 1e-9 (sum ratio) 1)
    r0 <- case ratio of
        (r : _) -> pure r
        [] -> assertFailure "expected explained-variance ratios"
    assertBool "first PC explains ~all variance" (close 1e-9 r0 1)
    let es = map snd (pcaExprs m)
    assertBool
        "pca exprs finite on constant column"
        (all (all finite . interpD df) es)

-- PCA on extreme-scale features keeps components unit-length and finite.
testPCAExtremeScale :: Test
testPCAExtremeScale = TestCase $ do
    let df =
            D.fromColumns
                [ ("x", DI.fromList ([1e8 + 1, 1e8 + 2, 1e8 + 3, 1e8 + 4] :: [Double]))
                , ("y", DI.fromList ([1e8 + 1, 1e8 + 2, 1e8 + 3, 1e8 + 4] :: [Double]))
                ]
        m =
            fit
                (PCAConfig (NComp 1) False)
                [F.double "x", F.double "y"]
                df
        comp0 = pcaComponents m V.! 0
        nrm = sqrt (VU.sum (VU.map (^ (2 :: Int)) comp0))
    assertBool "component finite at 1e8 scale" (VU.all finite comp0)
    assertBool "component unit length at 1e8 scale" (close 1e-6 nrm 1)
    assertBool
        "explained ratio finite at 1e8 scale"
        (VU.all finite (pcaExplainedVarianceRatio m))

-- choleskySolve on a non-positive-definite matrix returns Nothing.
testCholeskyNonPD :: Test
testCholeskyNonPD = TestCase $ do
    assertEqual
        "cholesky of singular zero matrix is Nothing"
        Nothing
        (choleskySolve (mat [[0, 0], [0, 0]]) (VU.fromList [1, 1]))

-- Jacobi on a 1x1 matrix: eigenvalue is the entry, eigenvector is [1].
testJacobi1x1 :: Test
testJacobi1x1 = TestCase $ do
    let (ev, vecs) = jacobiEigenSym (mat [[5]])
    assertBool "1x1 eigenvalue is the entry" (close 1e-12 (ev VU.! 0) 5)
    assertBool "1x1 eigenvector is [1]" (close 1e-12 ((vecs V.! 0) VU.! 0) 1)

-- Jacobi on an identity matrix gives eigenvalues exactly 1, all finite.
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
    , testMeanSquaredErrorGuards
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
