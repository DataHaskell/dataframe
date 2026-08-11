{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Metamorphic, invariance, and abstraction-law tests for the ML library.

These check how the fitted model and the metrics behave under transformations of
the *input* — duplicate / permute / scale / rename / add-constant-feature — where
the math dictates the answer. Each test computes the model on two genuinely
different (transformed) frames and asserts they agree, or pins a metric/scaler to
its defining law. None compares a value to itself; each can fail if the library
is wrong.
-}
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

-- Helpers -------------------------------------------------------------------

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
    D.fromNamedColumns
        ([(n, DI.fromList vs) | (n, vs) <- feats] ++ [("y", DI.fromList ys)])

-- Base design: a noiseless linear target so OLS recovers a unique solution.
baseX1, baseX2, baseY :: [Double]
baseX1 = [1, 2, 3, 4, 5, 6, 7, 8]
baseX2 = [2, 1, 4, 3, 6, 5, 8, 7]
baseY = [3 + 2 * a - 0.5 * b | (a, b) <- zip baseX1 baseX2]

baseDF :: D.DataFrame
baseDF = mkRegDF [("x1", baseX1), ("x2", baseX2)] baseY

fitBase :: D.DataFrame -> LinearRegressor
fitBase = fit defaultLinearConfig (F.col @Double "y")

-- Metamorphic: duplicate rows ----------------------------------------------

{- | OLS minimises Σ(y-ŷ)²; duplicating every row scales the loss by 2 but leaves
the argmin (the coefficients/intercept) unchanged. Catches a solver that
weights rows by frame size or mishandles the normal equations.
-}
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

-- Metamorphic: permute rows -------------------------------------------------

{- | OLS is invariant to row order. We reverse the rows (a genuine non-identity
permutation) and require identical coefficients. Catches any order-dependence
in matrix assembly or solving.
-}
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

-- Metamorphic: scale a feature ----------------------------------------------

{- | Scaling feature x1 by k must scale that coefficient by 1/k while leaving
predictions (and every other coefficient + the intercept) invariant. Both
sides are genuine fits on different frames. Catches a regressor that drops or
mis-applies a feature's scale.
-}
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
        (close 1e-7 (head coef0 / k) (head coef1))
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

-- Metamorphic: rename columns -----------------------------------------------

{- | Renaming feature columns is a pure relabelling: coefficients, intercept, and
predictions are identical and the fitted feature names track the rename.
Catches any hidden dependence on specific column-name strings.
-}
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

-- Metamorphic: irrelevant constant feature ----------------------------------

{- | A constant feature carries no variance, so OLS predictions for the real
features must be unchanged when one is added. (Its own weight is absorbed into
the intercept and is not separately identifiable, so we pin predictions, not
the weight.) Catches a fit whose answer drifts when a degenerate column is
present.
-}
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
        (close 1e-6 (head (VU.toList (regCoef m0))) (head coef1))
    assertBool
        "constant feature: x2 coefficient unchanged"
        (close 1e-6 (VU.toList (regCoef m0) !! 1) (coef1 !! 1))

-- Metamorphic: split then concatenate --------------------------------------

{- | Genuinely split the frame into two row-range sub-frames and concatenate them
back with the DataFrame @<>@ (row concatenation). The rebuilt frame has the
same rows, so the fit must be identical — but this actually exercises the
concatenation code path, so it catches a @<>@ that drops, duplicates, or
mis-aligns rows/columns (a plain @take h ++ drop h@ would be the identity and
test nothing).
-}
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

-- Training/inference parity: column order ----------------------------------

{- | The order of feature columns in the frame must not change predictions.
We fit on a frame whose columns are declared in the opposite order and require
the prediction expression to produce the same numbers on the original frame.
(predict compiles to a name-keyed affine Expr, so it should be order-free.)
Catches a positional, rather than name-keyed, feature mapping.
-}
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
            let (mu, sd) = moments (interpD scaledDF (F.col @Double c))
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
    -- Sanity: the metric is non-trivial here (not 0 or 1), so invariance is meaningful.
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

{- | r² on a fitted noiseless linear model equals 1, and rmse equals 0. Ties the
metric law to the model: if the fit is exact, the metric must say so.
-}
testR2OfFittedModel :: Test
testR2OfFittedModel = TestCase $ do
    let m = fitBase baseDF
        score = evaluate r2 (predict m) (F.col @Double "y") baseDF
        err = evaluate rmse (predict m) (F.col @Double "y") baseDF
    assertBool "r2 of exact linear fit ~ 1" (close 1e-9 score 1.0)
    assertBool "rmse of exact linear fit ~ 0" (err < 1e-6)

testTargetNeverAFeature :: Test
testTargetNeverAFeature = TestCase $ do
    let n = 40 :: Int
        leakDF =
            D.fromNamedColumns
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
        (predict (fit defaultLinearConfig (F.col @Double "y") leakDF))
    assertNoTarget
        "regression tree"
        (predict (fit defaultRegTreeConfig (F.col @Double "y") leakDF))
    assertNoTarget
        "classification tree"
        (predict (fit D.defaultTreeConfig (F.col @Double "y") leakDF))

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
