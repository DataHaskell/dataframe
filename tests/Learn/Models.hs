{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Learn.Models (tests) where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.DecisionTree.Model
import DataFrame.DecisionTree.Regression
import DataFrame.KMeans
import DataFrame.LinearModel
import DataFrame.LinearSolver (SolverConfig (..), defaultSolverConfig)
import DataFrame.PCA
import DataFrame.SVM
import DataFrame.Transform

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import Test.HUnit

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

interpI :: D.DataFrame -> Expr Int -> [Int]
interpI df e = case interpret @Int df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Int @VU.Vector c)
    Left err -> error (show err)

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

regDF :: D.DataFrame
regDF =
    D.fromNamedColumns
        [ ("x1", DI.fromList xs1)
        , ("x2", DI.fromList xs2)
        , ("y", DI.fromList [2 * a - 3 * b + 1 | (a, b) <- zip xs1 xs2])
        ]
  where
    xs1 = [1, 2, 3, 4, 5, 6, 7, 8] :: [Double]
    xs2 = [2, 1, 4, 3, 6, 5, 8, 7] :: [Double]

clsDF :: D.DataFrame
clsDF =
    D.fromNamedColumns
        [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
        , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
        ]

testOLS :: Test
testOLS = TestCase $ do
    let m = fit defaultLinearConfig (F.col @Double "y") regDF
    assertBool
        "OLS coef ~ [2,-3]"
        (and (zipWith (close 1e-6) (VU.toList (regCoef m)) [2, -3]))
    assertBool "OLS intercept ~ 1" (close 1e-6 (regIntercept m) 1)
    let preds = interpD regDF (predict m)
        truth = interpD regDF (F.col @Double "y")
    assertBool "OLS expr matches y" (and (zipWith (close 1e-6) preds truth))

testRidgeShrinks :: Test
testRidgeShrinks = TestCase $ do
    let r0 =
            fit (LinearConfig (Ridge 0.01) defaultSolverConfig) (F.col @Double "y") regDF
        r1 = fit (LinearConfig (Ridge 100) defaultSolverConfig) (F.col @Double "y") regDF
        norm = VU.sum . VU.map abs . regCoef
    assertBool "stronger ridge shrinks coefficients" (norm r1 < norm r0)

testLogistic :: Test
testLogistic = TestCase $ do
    let m = fit defaultLogisticConfig (F.col @Int "label") clsDF
        dec = predict m
        preds = interpI clsDF dec
        truth = [0, 0, 0, 0, 1, 1, 1, 1]
    assertEqual "logistic separates" truth preds
    let probs = logisticProbExprs m
    assertBool "prob exprs present for both classes" (M.size probs == 2)

testSVC :: Test
testSVC = TestCase $ do
    let m = fit defaultSVCConfig (F.col @Int "label") clsDF
        preds = interpI clsDF (predict m)
    assertEqual "linear SVC separates" [0, 0, 0, 0, 1, 1, 1, 1] preds

testRegressionTree :: Test
testRegressionTree = TestCase $ do
    let m = fit defaultRegTreeConfig (F.col @Double "y") regDF
        preds = interpD regDF (predict m)
        truth = interpD regDF (F.col @Double "y")
        sse = sum (zipWith (\p t -> (p - t) ^ (2 :: Int)) preds truth)
    assertBool "regression tree reduces error" (sse < 200)
    assertBool "tree has leaves" (dtrNLeaves m >= 2)

testClassifierStats :: Test
testClassifierStats = TestCase $ do
    let m = fit D.defaultTreeConfig (F.col @Int "label") clsDF
    assertBool "classifier depth >= 1" (dtcDepth m >= 1)

testPCA :: Test
testPCA = TestCase $ do
    let m =
            fit
                (PCAConfig (NComp 2) False)
                [F.col @Double "x1", F.col @Double "x2"]
                regDF
        ratio = VU.toList (pcaExplainedVarianceRatio m)
    assertBool "explained ratio sums ~1" (close 1e-9 (sum ratio) 1)
    assertEqual "two components" 2 (V.length (pcaComponents m))
    let comp0 = pcaComponents m V.! 0
        nrm = sqrt (VU.sum (VU.map (^ (2 :: Int)) comp0))
    assertBool "component is unit length" (close 1e-9 nrm 1)
    let es = map snd (pcaExprs m)
    assertBool
        "pca exprs evaluate finite"
        (not (any (any isNaN . interpD regDF) es))

testKMeans :: Test
testKMeans = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList ([0, 0.1, 0.2, 10, 10.1, 10.2] :: [Double]))
                , ("b", DI.fromList ([0, 0.1, 0, 10, 10, 10.1] :: [Double]))
                ]
        cfg = defaultKMeansConfig{kmK = 2, kmNInit = 5, kmSeed = 1}
        m = fit cfg [F.col @Double "a", F.col @Double "b"] df
        labels = VU.toList (kmLabels m)
    assertBool "two blobs split" (head labels /= last labels)
    assertEqual "two centers" 2 (V.length (kmCenters m))
    let m2 = fit cfg [F.col @Double "a", F.col @Double "b"] df
    assertEqual "kmeans deterministic" (kmCenters m) (kmCenters m2)
    let assigns = interpI df (predict m)
    assertEqual "assign expr matches labels" labels assigns

testTransformCompose :: Test
testTransformCompose = TestCase $ do
    let scaler = standardScaler ["x1", "x2"] regDF
        t = scalerTransform scaler
        scaledDf = applyTransform t regDF
        -- fit model on scaled features, then compile scaling into the expr
        m = fit defaultLinearConfig (F.col @Double "y") scaledDf
        composed = compileThrough t (predict m)
        viaCompose = interpD regDF composed
        viaStepwise = interpD scaledDf (predict m)
    assertBool
        "compileThrough == stepwise apply"
        (and (zipWith (close 1e-6) viaCompose viaStepwise))

tests :: [Test]
tests =
    [ testOLS
    , testRidgeShrinks
    , testLogistic
    , testSVC
    , testRegressionTree
    , testClassifierStats
    , testPCA
    , testKMeans
    , testTransformCompose
    ]
