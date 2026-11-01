{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Metamorphic and invariance tests: models must obey transformation laws.
module Learn.Metamorphic (tests) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr, getColumns)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.DecisionTree.Regression (defaultRegTreeConfig)
import DataFrame.LinearModel
import DataFrame.Metrics
import DataFrame.Operations.Merge ()

-- Semigroup DataFrame (row concatenation)
import DataFrame.Transform

import Test.HUnit

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

closeList :: Double -> [Double] -> [Double] -> Bool
closeList tol as bs = length as == length bs && and (zipWith (close tol) as bs)

-- Build a regression frame from explicit feature/target lists.
mkRegDF :: [(T.Text, [Double])] -> [Double] -> D.DataFrame
mkRegDF feats ys =
    D.fromColumns
        ([(n, DI.fromList vs) | (n, vs) <- feats] ++ [("y", DI.fromList ys)])

-- Base design: a noiseless linear target so OLS recovers a unique solution.
baseX1, baseX2, baseY :: [Double]
baseX1 = [1, 2, 3, 4, 5, 6, 7, 8]
baseX2 = [2, 1, 4, 3, 6, 5, 8, 7]
baseY = [3 + 2 * a - 0.5 * b | (a, b) <- zip baseX1 baseX2]

baseDF :: D.DataFrame
baseDF = mkRegDF [("x1", baseX1), ("x2", baseX2)] baseY

fitBase :: D.DataFrame -> LinearRegressor
fitBase = fit defaultLinearConfig (F.double "y")

-- | Duplicating every row leaves the OLS coefficients unchanged.
testDuplicateRows :: Test
testDuplicateRows = TestCase $ do
    let dupDF =
            mkRegDF
                [("x1", baseX1 ++ baseX1), ("x2", baseX2 ++ baseX2)]
                (baseY ++ baseY)
        m0 = fitBase baseDF
        m1 = fitBase dupDF
    assertBool
        "duplicate rows: coefficients unchanged"
        (closeList 1e-9 (VU.toList (regCoef m0)) (VU.toList (regCoef m1)))
    assertBool
        "duplicate rows: intercept unchanged"
        (close 1e-9 (regIntercept m0) (regIntercept m1))

-- | OLS is invariant to row order.
testPermuteRows :: Test
testPermuteRows = TestCase $ do
    let permDF =
            mkRegDF
                [("x1", reverse baseX1), ("x2", reverse baseX2)]
                (reverse baseY)
        m0 = fitBase baseDF
        m1 = fitBase permDF
    assertBool
        "permute rows: coefficients unchanged"
        (closeList 1e-9 (VU.toList (regCoef m0)) (VU.toList (regCoef m1)))
    assertBool
        "permute rows: intercept unchanged"
        (close 1e-9 (regIntercept m0) (regIntercept m1))

-- | Scaling a feature by k scales its coefficient by 1/k and keeps predictions.
testScaleFeature :: Test
testScaleFeature = TestCase $ do
    let k = 10.0
        scaledDF = mkRegDF [("x1", map (* k) baseX1), ("x2", baseX2)] baseY
        m0 = fitBase baseDF
        m1 = fitBase scaledDF
        coef0 = VU.toList (regCoef m0)
        coef1 = VU.toList (regCoef m1)
    assertBool
        "scale feature: x1 coefficient scaled by 1/k"
        (close 1e-7 (VU.head (regCoef m0) / k) (VU.head (regCoef m1)))
    assertBool
        "scale feature: x2 coefficient unchanged"
        (close 1e-7 (coef0 !! 1) (coef1 !! 1))
    assertBool
        "scale feature: intercept unchanged"
        (close 1e-7 (regIntercept m0) (regIntercept m1))
    -- Predictions on each model's own (consistently transformed) frame agree.
    let p0 = interpD baseDF (predict m0)
        p1 = interpD scaledDF (predict m1)
    assertBool "scale feature: predictions invariant" (closeList 1e-6 p0 p1)

-- | Renaming feature columns only relabels coefficients and feature names.
testRenameColumns :: Test
testRenameColumns = TestCase $ do
    let renamedDF = mkRegDF [("feat_a", baseX1), ("feat_b", baseX2)] baseY
        m0 = fitBase baseDF
        m1 = fitBase renamedDF
    assertBool
        "rename: coefficients unchanged"
        (closeList 1e-9 (VU.toList (regCoef m0)) (VU.toList (regCoef m1)))
    assertBool
        "rename: intercept unchanged"
        (close 1e-9 (regIntercept m0) (regIntercept m1))
    assertEqual
        "rename: fitted feature names follow the rename"
        ["feat_a", "feat_b"]
        (V.toList (regFeatureNames m1))
    -- predict on the renamed frame yields the same numbers as on the original.
    let p0 = interpD baseDF (predict m0)
        p1 = interpD renamedDF (predict m1)
    assertBool "rename: predictions unchanged" (closeList 1e-9 p0 p1)

-- | A constant feature must not change predictions for the real features.
testConstantFeatureIgnored :: Test
testConstantFeatureIgnored = TestCase $ do
    let withConst =
            mkRegDF
                [ ("x1", baseX1)
                , ("x2", baseX2)
                , ("c", replicate (length baseX1) 7.0)
                ]
                baseY
        m0 = fitBase baseDF
        m1 = fitBase withConst
        p0 = interpD baseDF (predict m0)
        p1 = interpD withConst (predict m1)
    assertBool
        "constant feature: predictions match the no-constant model"
        (closeList 1e-6 p0 p1)
    -- And the real-feature coefficients are unchanged.
    let coef1 = VU.toList (regCoef m1)
    assertBool
        "constant feature: x1 coefficient unchanged"
        (close 1e-6 (VU.head (regCoef m0)) (VU.head (regCoef m1)))
    assertBool
        "constant feature: x2 coefficient unchanged"
        (close 1e-6 (VU.toList (regCoef m0) !! 1) (coef1 !! 1))

-- | Splitting a frame and rejoining it with <> must give an identical fit.
testSplitConcat :: Test
testSplitConcat = TestCase $ do
    let n = length baseX1
        h = n `div` 2
        half lo hi =
            mkRegDF
                [ ("x1", slice lo hi baseX1)
                , ("x2", slice lo hi baseX2)
                ]
                (slice lo hi baseY)
        slice lo hi = take (hi - lo) . drop lo
        rebuiltDF = half 0 h <> half h n
        m0 = fitBase baseDF
        m1 = fitBase rebuiltDF
    -- the concatenated frame must have exactly the original rows back
    assertEqual "split/concat: row count preserved" n (fst (D.dimensions rebuiltDF))
    assertBool
        "split/concat: coefficients unchanged"
        (closeList 1e-9 (VU.toList (regCoef m0)) (VU.toList (regCoef m1)))
    assertBool
        "split/concat: intercept unchanged"
        (close 1e-9 (regIntercept m0) (regIntercept m1))

-- | The order of feature columns must not change predictions.
testColumnOrderParity :: Test
testColumnOrderParity = TestCase $ do
    let swappedDF = mkRegDF [("x2", baseX2), ("x1", baseX1)] baseY
        m0 = fitBase baseDF
        m1 = fitBase swappedDF
        -- evaluate both fitted models on the SAME original frame.
        p0 = interpD baseDF (predict m0)
        p1 = interpD baseDF (predict m1)
    assertBool
        "column order: predictions on the same frame agree"
        (closeList 1e-7 p0 p1)

testStandardScalerLaw :: Test
testStandardScalerLaw = TestCase $ do
    let cols = ["x1", "x2"]
        scaler = standardScaler cols baseDF
        scaledDF = applyTransform (scalerTransform scaler) baseDF
        moments xs =
            let n = fromIntegral (length xs)
                mu = sum xs / n
                var = sum [(x - mu) ^ (2 :: Int) | x <- xs] / n
             in (mu, sqrt var)
    mapM_
        ( \c -> do
            let (mu, sd) = moments (interpD scaledDF (F.double c))
            assertBool ("scaler: " ++ T.unpack c ++ " mean ~ 0") (close 1e-9 mu 0)
            assertBool ("scaler: " ++ T.unpack c ++ " std ~ 1") (close 1e-9 sd 1)
        )
        cols

testScalerStatsMatchData :: Test
testScalerStatsMatchData = TestCase $ do
    let scaler = standardScaler ["x1", "x2"] baseDF
        moments xs =
            let n = fromIntegral (length xs)
                mu = sum xs / n
             in (mu, sqrt (sum [(x - mu) ^ (2 :: Int) | x <- xs] / n))
        (mu1, sd1) = moments baseX1
        (mu2, sd2) = moments baseX2
    assertBool
        "scaler means match data"
        (closeList 1e-9 (VU.toList (smMeans scaler)) [mu1, mu2])
    assertBool
        "scaler stds match data"
        (closeList 1e-9 (VU.toList (smStds scaler)) [sd1, sd2])

testAccuracyLaw :: Test
testAccuracyLaw = TestCase $ do
    let truth = VU.fromList [0, 1, 2, 1, 0, 2]
        perfect = truth
        oneWrong = VU.fromList [0, 1, 2, 1, 0, 0]
    assertBool "accuracy: perfect = 1" (close 1e-12 (accuracy perfect truth) 1.0)
    assertBool
        "accuracy: one miss out of 6 = 5/6"
        (close 1e-12 (accuracy oneWrong truth) (5 / 6))
    assertBool
        "accuracy in [0,1]"
        (let a = accuracy oneWrong truth in a >= 0 && a <= 1)

testAccuracyPermInvariant :: Test
testAccuracyPermInvariant = TestCase $ do
    let preds = VU.fromList [0, 0, 1, 1, 2, 2, 1, 0]
        truth = VU.fromList [0, 0, 1, 2, 2, 2, 1, 0]
        a0 = accuracy preds truth
        a1 = accuracy (VU.reverse preds) (VU.reverse truth)
    assertBool "accuracy: permutation invariant" (close 1e-12 a0 a1)
    -- Sanity: the metric is non-trivial here, so invariance is meaningful.
    assertBool "accuracy: non-degenerate baseline" (a0 > 0 && a0 < 1)

testR2Anchors :: Test
testR2Anchors = TestCase $ do
    let truth = VU.fromList [1, 3, 2, 8, 5, 4]
        meanT = VU.sum truth / fromIntegral (VU.length truth)
        meanPred = VU.replicate (VU.length truth) meanT
    assertBool "r2: perfect fit = 1" (close 1e-12 (r2 truth truth) 1.0)
    assertBool "r2: predicting the mean = 0" (close 1e-12 (r2 meanPred truth) 0.0)
    -- A strictly-worse-than-mean prediction gives r2 < 0 (not clamped).
    let worse = VU.map (+ 100) truth
    assertBool "r2: far-off prediction is negative" (r2 worse truth < 0)

-- | A noiseless linear fit has r2 equal to 1 and rmse equal to 0.
testR2OfFittedModel :: Test
testR2OfFittedModel = TestCase $ do
    let m = fitBase baseDF
        score = evaluate r2 (predict m) (F.double "y") baseDF
        err = evaluate rmse (predict m) (F.double "y") baseDF
    assertBool "r2 of exact linear fit ~ 1" (close 1e-9 score 1.0)
    assertBool "rmse of exact linear fit ~ 0" (err < 1e-6)

testTargetNeverAFeature :: Test
testTargetNeverAFeature = TestCase $ do
    let n = 40 :: Int
        leakDF =
            D.fromColumns
                [ ("y", DI.fromList [if even i then 1.0 else 0.0 :: Double | i <- [0 .. n - 1]])
                , ("a", DI.fromList [fromIntegral (i `div` 4) :: Double | i <- [0 .. n - 1]])
                , ("b", DI.fromList [fromIntegral (i `mod` 3) :: Double | i <- [0 .. n - 1]])
                ]
        assertNoTarget name expr =
            assertBool
                ( name
                    ++ ": target 'y' must not appear in the fitted Expr (got "
                    ++ show expr
                    ++ ")"
                )
                ("y" `notElem` getColumns expr)
    assertNoTarget
        "linear"
        (predict (fit defaultLinearConfig (F.double "y") leakDF))
    assertNoTarget
        "regression tree"
        (predict (fit defaultRegTreeConfig (F.double "y") leakDF))
    assertNoTarget
        "classification tree"
        (predict (fit D.defaultTreeConfig (F.double "y") leakDF))

tests :: [Test]
tests =
    [ testTargetNeverAFeature
    , testDuplicateRows
    , testPermuteRows
    , testScaleFeature
    , testRenameColumns
    , testConstantFeatureIgnored
    , testSplitConcat
    , testColumnOrderParity
    , testStandardScalerLaw
    , testScalerStatsMatchData
    , testAccuracyLaw
    , testAccuracyPermInvariant
    , testR2Anchors
    , testR2OfFittedModel
    ]
