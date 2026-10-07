{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Learn.MetricsTests (tests) where

import qualified Control.Exception as E

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI

import DataFrame.LinearModel
import DataFrame.Metrics
import DataFrame.Metrics.Report
import DataFrame.ModelSelection
import DataFrame.PCA
import DataFrame.Transform

import Data.List (sortBy)
import Data.Ord (comparing)
import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import System.Random (mkStdGen, randomRs)
import Test.HUnit

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

preds3, truth3 :: VU.Vector Double
preds3 = VU.fromList [0, 0, 1, 1, 2, 2, 1, 0]
truth3 = VU.fromList [0, 0, 1, 2, 2, 2, 1, 0]

reg :: D.DataFrame
reg =
    D.fromColumns
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
    assertBool
        "mse averages over compared pairs"
        (close 1e-9 (mse (VU.fromList [2, 2]) (VU.fromList [0, 0, 0, 0])) 4)
    assertBool
        "mae averages over compared pairs"
        (close 1e-9 (mae (VU.fromList [2, 2]) (VU.fromList [0, 0, 0, 0])) 2)
    r <- E.try (E.evaluate (mse VU.empty (VU.fromList [5, 5, 5])))
    case r of
        Left (_ :: E.SomeException) -> pure ()
        Right v -> assertFailure ("mse with no pairs returned " ++ show v)

testMulticlassMetrics :: Test
testMulticlassMetrics = TestCase $ do
    assertBool "accuracy" (close 1e-9 (accuracy preds3 truth3) 0.875)
    assertBool
        "binary precision class 1"
        (close 1e-9 (precision (Binary 1) preds3 truth3) (2 / 3))
    assertBool
        "macro f1 sane"
        (f1 Macro preds3 truth3 > 0.8 && f1 Macro preds3 truth3 <= 1)
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
        pipeline = scalerTransform scaler <> pcaTransform pca
        out = applyTransform pipeline reg
    assertBool "pipeline produced a frame" (D.columnNames out /= [])
    assertBool "pipeline has pc1 column" ("pc1" `elem` D.columnNames out)

{- | Mann-Whitney AUC from ranks computed over lists, the definition 'rocAuc'
must reproduce bit for bit.
-}
referenceAuc :: [Double] -> [Double] -> Double
referenceAuc scores truth
    | nPos == 0 || nNeg == 0 = 0.5
    | otherwise = (rankSum - nPos * (nPos + 1) / 2) / (nPos * nNeg)
  where
    sorted = sortBy (comparing snd) (zip [0 :: Int ..] scores)
    ranks = map snd (sortBy (comparing fst) (assign 1 sorted))
    assign _ [] = []
    assign start grp@((_, v) : _) =
        let (tied, rest) = span ((== v) . snd) grp
            k = length tied
            avg = fromIntegral (sum [start .. start + k - 1]) / fromIntegral k
         in [(i, avg) | (i, _) <- tied] ++ assign (start + k) rest
    rankSum = sum [r | (y, r) <- zip truth ranks, y == 1]
    nPos = fromIntegral (length (filter (== 1) truth))
    nNeg = fromIntegral (length truth) - nPos

testRocAucMatchesReference :: Test
testRocAucMatchesReference = TestCase $ do
    let cases =
            [ (n, levels, seed)
            | n <- [1, 2, 17, 1000]
            , levels <- [2, 7, 100000]
            , seed <- [1, 2, 3]
            ]
    sequence_
        [ assertEqual
            ("n=" ++ show n ++ " levels=" ++ show levels ++ " seed=" ++ show seed)
            (referenceAuc scores truth)
            (rocAuc (VU.fromList scores) (VU.fromList truth))
        | (n, levels, seed) <- cases
        , let scores = map fromIntegral (take n (randomRs (0, levels - 1 :: Int) (mkStdGen seed)))
              truth = map fromIntegral (take n (randomRs (0, 1 :: Int) (mkStdGen (seed + 100))))
        ]

testStratifiedFolds :: Test
testStratifiedFolds = TestCase $ do
    let labels =
            VU.fromList (map fromIntegral (take 1003 (randomRs (0, 1 :: Int) (mkStdGen 7))))
        folds = stratifiedFoldIds 5 42 labels
        perFold c =
            [ VU.length (VU.filter id (VU.zipWith (\f l -> f == k && l == c) folds labels))
            | k <- [0 .. 4]
            ]
        spread xs = maximum xs - minimum xs
    assertEqual "one fold per row" (VU.length labels) (VU.length folds)
    assertBool "folds in range" (VU.all (\f -> f >= 0 && f < 5) folds)
    assertBool "positives balanced" (spread (perFold 1) <= 1)
    assertBool "negatives balanced" (spread (perFold 0) <= 1)
    assertEqual "deterministic" folds (stratifiedFoldIds 5 42 labels)

tests :: [Test]
tests =
    [ testRocAucMatchesReference
    , testStratifiedFolds
    , testRegressionMetrics
    , testMulticlassMetrics
    , testRocAuc
    , testReports
    , testEvaluateOneLiner
    , testCrossValidate
    , testTransformCompose
    ]
