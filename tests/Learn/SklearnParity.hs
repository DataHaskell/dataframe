{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Parity tests against scikit-learn on clean Kaggle-style datasets; reference
values in @data/ml/golden.json@ come from @scripts/gen_sklearn_golden.py@.
Closed-form models get tight coefficient parity; iterative ones an accuracy floor.
-}
module Learn.SklearnParity (tests) where

import Data.Aeson (Value (..), decodeFileStrict')
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import Test.HUnit

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.Boosting (
    GBConfig (..),
    GBLoss (..),
    defaultGBConfig,
    gbProbaExpr,
 )
import DataFrame.KMeans
import DataFrame.LinearModel
import DataFrame.PCA
import DataFrame.SVM

goldenPath :: FilePath
goldenPath = "data/ml/golden.json"

loadGolden :: IO Value
loadGolden =
    fromMaybe (error "missing data/ml/golden.json") <$> decodeFileStrict' goldenPath

(.!) :: Value -> T.Text -> Value
(.!) (Object o) k = fromMaybe Null (KM.lookup (K.fromText k) o)
(.!) _ _ = Null

asNum :: Value -> Double
asNum (Number s) = realToFrac s
asNum _ = error "asNum: not a number"

asNums :: Value -> [Double]
asNums (Array a) = map asNum (V.toList a)
asNums _ = error "asNums: not an array"

asMatrix :: Value -> [[Double]]
asMatrix (Array a) = map asNums (V.toList a)
asMatrix _ = error "asMatrix: not a matrix"

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

colDoubles :: D.DataFrame -> T.Text -> [Double]
colDoubles df name = interpD df (F.col @Double name)

closeAll :: Double -> [Double] -> [Double] -> Bool
closeAll tol a b =
    length a == length b && and (zipWith (\x y -> abs (x - y) <= tol) a b)

accuracyOf :: [Double] -> [Double] -> Double
accuracyOf preds truth =
    fromIntegral (length (filter id (zipWith (==) preds truth)))
        / fromIntegral (max 1 (length truth))

testOLSParity :: Test
testOLSParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/regression.csv"
    let m = fit defaultLinearConfig (F.col @Double "target") df
        goldCoef = asNums (g .! "ols" .! "coef")
        goldB = asNum (g .! "ols" .! "intercept")
    assertBool
        "OLS coefficients match sklearn"
        (closeAll 1e-4 (VU.toList (regCoef m)) goldCoef)
    assertBool "OLS intercept matches sklearn" (abs (regIntercept m - goldB) < 1e-4)

testRidgeParity :: Test
testRidgeParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/regression.csv"
    let m =
            fit (LinearConfig (Ridge 1.0) defaultSolverConfig) (F.col @Double "target") df
        goldCoef = asNums (g .! "ridge" .! "coef")
    assertBool
        "Ridge coefficients match sklearn"
        (closeAll 1e-2 (VU.toList (regCoef m)) goldCoef)

testPCAParity :: Test
testPCAParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/iris.csv"
    let feats =
            map
                (F.col @Double)
                ["sepal_length", "sepal_width", "petal_length", "petal_width"]
        m = fit (PCAConfig (NComp 2) False) feats df
        goldEvr = asNums (g .! "pca" .! "evr")
        goldComp = asMatrix (g .! "pca" .! "components_abs")
        ourComp = map (map abs . VU.toList) (V.toList (pcaComponents m))
    assertBool
        "PCA explained variance ratio matches sklearn"
        (closeAll 1e-4 (VU.toList (pcaExplainedVarianceRatio m)) goldEvr)
    assertBool
        "PCA |components| match sklearn"
        (and (zipWith (closeAll 1e-3) ourComp goldComp))

testLogisticIrisParity :: Test
testLogisticIrisParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/iris.csv"
    let m = fit defaultLogisticConfig (F.col @Double "species") df
        preds = interpD df (predict m)
        truth = colDoubles df "species"
        acc = accuracyOf preds truth
        gold = asNum (g .! "logistic_iris" .! "accuracy")
    assertBool
        ("logistic iris accuracy within 0.06 of sklearn " ++ show (acc, gold))
        (acc >= gold - 0.06)

testSVCParity :: Test
testSVCParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/iris_binary.csv"
    let m = fit defaultSVCConfig (F.col @Double "label") df
        preds = interpD df (predict m)
        truth = colDoubles df "label"
        acc = accuracyOf preds truth
        gold = asNum (g .! "linear_svc" .! "accuracy")
    assertBool
        ("linear SVC accuracy within 0.06 of sklearn " ++ show (acc, gold))
        (acc >= gold - 0.06)

testGBMParity :: Test
testGBMParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/iris_binary.csv"
    let m =
            fit
                defaultGBConfig{gbLoss = LogisticDeviance, gbNEstimators = 100}
                (F.col @Double "label")
                df
        probs = interpD df (gbProbaExpr m)
        preds = map (\p -> if p > 0.5 then 1 else 0) probs
        truth = colDoubles df "label"
        acc = accuracyOf preds truth
        gold = asNum (g .! "gbm" .! "accuracy")
    assertBool
        ("GBM accuracy within 0.1 of sklearn " ++ show (acc, gold))
        (acc >= gold - 0.1)

testKMeansParity :: Test
testKMeansParity = TestCase $ do
    g <- loadGolden
    df <- D.readCsv "data/ml/blobs.csv"
    let m =
            fit
                defaultKMeansConfig{kmK = 3, kmNInit = 10, kmSeed = 0}
                [F.col @Double "x", F.col @Double "y"]
                df
        gold = asNum (g .! "kmeans" .! "inertia")
    assertBool
        ("k-means inertia within 10% of sklearn " ++ show (kmInertia m, gold))
        (abs (kmInertia m - gold) / gold < 0.1)

tests :: [Test]
tests =
    [ testOLSParity
    , testRidgeParity
    , testPCAParity
    , testLogisticIrisParity
    , testSVCParity
    , testGBMParity
    , testKMeansParity
    ]
