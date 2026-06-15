{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Learn.MetricsTests (tests) where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI

import DataFrame.LinearModel
import DataFrame.LinearSolver (defaultSolverConfig)
import DataFrame.Metrics
import DataFrame.Metrics.Report
import DataFrame.ModelSelection
import DataFrame.PCA
import DataFrame.Transform

import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import Test.HUnit

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

preds3, truth3 :: VU.Vector Double
preds3 = VU.fromList [0, 0, 1, 1, 2, 2, 1, 0]
truth3 = VU.fromList [0, 0, 1, 2, 2, 2, 1, 0]

reg :: D.DataFrame
reg =
    D.fromNamedColumns
        [ ("x", DI.fromList ([1 .. 20] :: [Double]))
        ,
            ( "y"
            , DI.fromList ([2 * fromIntegral i + 1 | i <- [1 .. 20 :: Int]] :: [Double])
            )
        ]

testRegressionMetrics :: Test
testRegressionMetrics = TestCase $ do
    let p = VU.fromList [1, 2, 3, 4]
        t = VU.fromList [1, 2, 3, 5]
    assertBool "mse" (close 1e-9 (mse p t) 0.25)
    assertBool "rmse" (close 1e-9 (rmse p t) 0.5)
    assertBool "mae" (close 1e-9 (mae p t) 0.25)
    assertBool "r2 in range" (r2 p t <= 1)

testMulticlassMetrics :: Test
testMulticlassMetrics = TestCase $ do
    assertBool "accuracy" (close 1e-9 (accuracy preds3 truth3) 0.875)
    -- class 1: tp=2 (idx2,6), fp=1 (idx3) -> precision 2/3
    assertBool
        "binary precision class 1"
        (close 1e-9 (precision (Binary 1) preds3 truth3) (2 / 3))
    assertBool
        "macro f1 sane"
        (f1 Macro preds3 truth3 > 0.8 && f1 Macro preds3 truth3 <= 1)
    -- micro f1 == accuracy for single-label
    assertBool
        "micro f1 == accuracy"
        (close 1e-9 (f1 Micro preds3 truth3) (accuracy preds3 truth3))

testRocAuc :: Test
testRocAuc = TestCase $ do
    let scores = VU.fromList [0.1, 0.4, 0.35, 0.8]
        truth = VU.fromList [0, 0, 1, 1]
    assertBool "perfect-ish auc high" (rocAuc scores truth >= 0.75)
    assertBool "auc in [0,1]" (let a = rocAuc scores truth in a >= 0 && a <= 1)

testReports :: Test
testReports = TestCase $ do
    let cr = classificationReport preds3 truth3
    assertEqual "report covers 3 classes" 3 (length (crPerClass cr))
    assertBool "report accuracy" (close 1e-9 (crAccuracy cr) 0.875)
    let rr = regressionReport (VU.fromList [1, 2, 3]) (VU.fromList [1, 2, 4])
    assertBool "regression report rmse" (rrRMSE rr > 0)
    -- Show instances don't crash
    assertBool "classification report shows" (not (null (show cr)))
    assertBool "confusion shows" (not (null (show (confusionMatrix preds3 truth3))))

testEvaluateOneLiner :: Test
testEvaluateOneLiner = TestCase $ do
    let m = fit defaultLinearConfig (F.col @Double "y") reg
        score = evaluate rmse (predict m) (F.col @Double "y") reg
    assertBool "evaluate rmse ~ 0 on exact linear fit" (score < 1e-6)
    let r = regressionReportExpr (predict m) (F.col @Double "y") reg
    assertBool "report r2 ~ 1" (close 1e-6 (rrR2 r) 1)

testCrossValidate :: Test
testCrossValidate = TestCase $ do
    let cv =
            crossValidate
                4
                0
                r2
                (F.col @Double "y")
                (predict . fit defaultLinearConfig (F.col @Double "y"))
                reg
    assertBool "all folds high R2" (all (> 0.99) cv)
    assertEqual "four folds" 4 (length cv)

testTransformCompose :: Test
testTransformCompose = TestCase $ do
    let scaler = standardScaler ["x"] reg
        pca = fit (PCAConfig (NComp 1) False) [F.col @Double "x"] reg
        -- the whole point: scaler <> pcaTransform compose as a monoid
        pipeline = scalerTransform scaler <> pcaTransform pca
        out = applyTransform pipeline reg
    assertBool "pipeline produced a frame" (D.columnNames out /= [])
    assertBool "pipeline has pc1 column" ("pc1" `elem` D.columnNames out)

tests :: [Test]
tests =
    [ testRegressionMetrics
    , testMulticlassMetrics
    , testRocAuc
    , testReports
    , testEvaluateOneLiner
    , testCrossValidate
    , testTransformCompose
    ]
