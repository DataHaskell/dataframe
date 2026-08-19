{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Learn.Ensembles (tests) where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.Boosting
import DataFrame.DBSCAN
import DataFrame.GMM
import DataFrame.LinearModel
import DataFrame.Metrics (r2)
import DataFrame.ModelSelection

import Data.Maybe (isJust, isNothing, listToMaybe)
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

clsDF :: D.DataFrame
clsDF =
    D.fromNamedColumns
        [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
        , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
        ]

blobs :: D.DataFrame
blobs =
    D.fromNamedColumns
        [ ("a", DI.fromList ([0, 0.2, -0.1, 0.1, 8, 8.1, 7.9, 8.2] :: [Double]))
        , ("b", DI.fromList ([0, -0.1, 0.2, 0.0, 5, 5.2, 4.9, 5.1] :: [Double]))
        ]

testGBMRegression :: Test
testGBMRegression = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([1 .. 12] :: [Double]))
                ,
                    ( "y"
                    , DI.fromList
                        ( [sin (fromIntegral i) + fromIntegral i * 0.3 | i <- [1 .. 12 :: Int]] ::
                            [Double]
                        )
                    )
                ]
        gb =
            fit
                defaultGBConfig{gbNEstimators = 60, gbMaxDepth = 2}
                (F.col @Double "y")
                df
        preds = interpD df (predict gb)
        truth = interpD df (F.col @Double "y")
        err = sum (zipWith (\p t -> (p - t) ^ (2 :: Int)) preds truth) / 12
    assertBool "GBM fits training data well" (err < 0.05)
    assertBool "trainScore recorded" (VU.length (gbTrainScore gb) == 60)

testGBMStaged :: Test
testGBMStaged = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([1 .. 8] :: [Double]))
                , ("y", DI.fromList ([1 .. 8] :: [Double]))
                ]
        gb = fit defaultGBConfig{gbNEstimators = 10} (F.col @Double "y") df
    assertBool "stage 0 valid" (isJust (gbExprAtStage 0 gb))
    assertBool "out of range stage rejected" (isNothing (gbExprAtStage 11 gb))

testAdaBoost :: Test
testAdaBoost = TestCase $ do
    let m = fit defaultAdaBoostConfig (F.col @Int "label") clsDF
        preds = interpI clsDF (predict m)
    assertEqual "AdaBoost separates" [0, 0, 0, 0, 1, 1, 1, 1] preds

testGMM :: Test
testGMM = TestCase $ do
    let m =
            fit
                defaultGMMConfig{gmmK = 2, gmmSeed = 1}
                [F.col @Double "a", F.col @Double "b"]
                blobs
        assigns = interpI blobs (predict m)
    assertBool "GMM converged" (gmmConverged m)
    assertBool
        "GMM splits the blobs"
        (listToMaybe assigns /= listToMaybe (reverse assigns))
    let m2 =
            fit
                defaultGMMConfig{gmmK = 2, gmmSeed = 1}
                [F.col @Double "a", F.col @Double "b"]
                blobs
    assertEqual "GMM deterministic" (gmmMeans m) (gmmMeans m2)
    assertBool "BIC finite" (not (isNaN (gmmBIC m)) && not (isInfinite (gmmBIC m)))

testDBSCAN :: Test
testDBSCAN = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList ([0, 0.1, 0.2, 5, 5.1, 5.2, 50] :: [Double]))
                , ("b", DI.fromList ([0, 0.1, 0.0, 5, 5.0, 5.1, 50] :: [Double]))
                ]
        m = fit (DBSCANConfig 1.0 2) [F.col @Double "a", F.col @Double "b"] df
    assertEqual "two clusters" 2 (dbNClusters m)
    assertEqual "last point is noise" (-1) (VU.last (dbLabels m))
    let surrogate =
            dbscanSurrogateExpr
                D.defaultTreeConfig
                [F.col @Double "a", F.col @Double "b"]
                m
                df
        preds = interpI df surrogate
    assertEqual
        "surrogate matches non-noise labels on cores"
        (take 6 (VU.toList (dbLabels m)))
        (take 6 preds)

testGridSearch :: Test
testGridSearch = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([1 .. 40] :: [Double]))
                ,
                    ( "y"
                    , DI.fromList ([2 * fromIntegral i + 5 | i <- [1 .. 40 :: Int]] :: [Double])
                    )
                ]
        score alpha train test' =
            let mdl =
                    fit
                        (LinearConfig (Ridge alpha) defaultSolverConfig)
                        (F.col @Double "y")
                        (train :: D.DataFrame)
             in r2
                    (VU.fromList (interpD test' (predict mdl)))
                    (VU.fromList (interpD test' (F.col @Double "y")))
        res = gridSearch 4 7 [0.0, 1.0, 100.0] score df
    assertBool "best score high" (gsBestScore res > 0.99)
    assertEqual "all configs scored" 3 (length (gsAll res))

{- | Logistic boosting must recover a known conditional probability, not merely
rank it. A leaf set to the mean gradient instead of @Σg/Σh@ understeps every
step by at least 4x, leaving probabilities shrunk toward the base rate while the
ranking — and so any accuracy or AUC check — still looks healthy.

Each @x@ carries a fixed 20 rows of which exactly @round (20 * trueP x)@ are
positive, so the empirical conditional probability at every @x@ is 'trueP' and
the target is separable in rank but not in value.
-}
sigmoidCurveDF :: D.DataFrame
sigmoidCurveDF =
    D.fromNamedColumns
        [ ("x", DI.fromList (concatMap (replicate group . fst) cells))
        , ("label", DI.fromList (concatMap snd cells))
        ]
  where
    group = 20 :: Int
    xs = [fromIntegral i / 10 - 1 | i <- [0 .. 20 :: Int]] :: [Double]
    cells = [(x, labelsAt x) | x <- xs]
    labelsAt x =
        let k = round (fromIntegral group * trueP x) :: Int
         in replicate k 1 ++ replicate (group - k) (0 :: Double)

trueP :: Double -> Double
trueP x = 1 / (1 + exp (negate (4 * x)))

testGBMCalibration :: Test
testGBMCalibration = TestCase $ do
    let m =
            fit
                defaultGBConfig
                    { gbLoss = LogisticDeviance
                    , gbNEstimators = 100
                    , gbLearningRate = 0.1
                    , gbMaxDepth = 3
                    }
                (F.col @Double "label")
                sigmoidCurveDF
        probs = interpD sigmoidCurveDF (gbProbaExpr m)
        truth = map trueP (interpD sigmoidCurveDF (F.col @Double "x"))
        err =
            sum (zipWith (\p t -> abs (p - t)) probs truth)
                / fromIntegral (length probs)
    assertBool
        ("logistic boosting recovers the conditional probability " ++ show err)
        (err < 0.03)

tests :: [Test]
tests =
    [ testGBMRegression
    , testGBMStaged
    , testGBMCalibration
    , testAdaBoost
    , testGMM
    , testDBSCAN
    , testGridSearch
    ]
