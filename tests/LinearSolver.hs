{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module LinearSolver where

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr (..), getColumns)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.LinearSolver

import Data.List (sort)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import System.Random (StdGen, mkStdGen, randomR)
import Test.HUnit

------------------------------------------------------------------------
-- Test fixtures and helpers
------------------------------------------------------------------------

-- Generate n points with d features, each value uniform in [-1, 1], from a seed.
syntheticPoints :: Int -> Int -> Int -> V.Vector (VU.Vector Double)
syntheticPoints seed n d =
    let (rows, _) = foldr step ([], mkStdGen seed) [1 .. n]
     in V.fromList (take n rows)
  where
    step _ (acc, g) =
        let (row, g') = genRow d g
         in (row : acc, g')
    genRow k g0 = go k g0 []
      where
        go 0 g xs = (VU.fromList (reverse xs), g)
        go i g xs =
            let (v, g') = randomR (-1.0 :: Double, 1.0) g
             in go (i - 1) g' (v : xs)

-- Label each row by sign(w . x + b); +1 if score > 0, else -1.
labelsForHyperplane ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    Double ->
    VU.Vector Double
labelsForHyperplane rows w b =
    VU.generate
        (V.length rows)
        ( \i ->
            let score = dotProduct w (rows V.! i) + b
             in if score > 0 then 1 else -1
        )

-- Cosine similarity between two non-zero vectors.
cosineSim :: VU.Vector Double -> VU.Vector Double -> Double
cosineSim u v =
    let nu = sqrt (dotProduct u u)
        nv = sqrt (dotProduct v v)
     in if nu == 0 || nv == 0 then 0 else dotProduct u v / (nu * nv)

-- Predict +1 or -1 from a fitted LinearModel.
predict :: LinearModel -> VU.Vector Double -> Double
predict m x =
    let score = dotProduct (lmWeights m) x + lmIntercept m
     in if score > 0 then 1 else -1

-- Predict directly on standardized features (skipping de-standardization).
predictStandardized :: VU.Vector Double -> Double -> VU.Vector Double -> Double
predictStandardized w b x =
    if dotProduct w x + b > 0 then 1 else -1

-- Average binary logistic loss at (w, b).
logisticLoss ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    Double
logisticLoss features labels w b =
    let n = V.length features
        loss i =
            let yi = labels VU.! i
                row = features V.! i
                margin = yi * (dotProduct w row + b)
             in -- log(1 + exp(-margin)), numerically stable
                if margin >= 0
                    then log (1 + exp (-margin))
                    else (-margin) + log (1 + exp margin)
     in sum [loss i | i <- [0 .. n - 1]] / fromIntegral n

------------------------------------------------------------------------
-- A1: Recover known hyperplane with no L1
------------------------------------------------------------------------

testA1RecoverHyperplane :: Test
testA1RecoverHyperplane = TestCase $ do
    let groundTruth = VU.fromList [0.7, -0.5]
        groundBias = 0.3
        rows = syntheticPoints 1 200 2
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg = defaultSolverConfig{scL1Lambda = 0, scMaxIter = 500, scTol = 1e-6}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        cos = cosineSim (lmWeights model) groundTruth
        sameSignAll =
            all
                (\i -> predict model (rows V.! i) == labels VU.! i)
                [0 .. V.length rows - 1]
    assertBool
        ("recovered weights should align with ground truth (cos = " ++ show cos ++ ")")
        (cos > 0.99)
    assertBool "all training points predicted correctly" sameSignAll

------------------------------------------------------------------------
-- A2: L1 produces sparse weights
------------------------------------------------------------------------

testA2L1Sparsity :: Test
testA2L1Sparsity = TestCase $ do
    -- 10 features, only feature 1 and feature 4 carry signal.
    let groundTruth = VU.fromList [0, 1.2, 0, 0, -1.5, 0, 0, 0, 0, 0]
        groundBias = 0
        rows = syntheticPoints 7 500 10
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg = defaultSolverConfig{scL1Lambda = 0.1, scMaxIter = 500, scTol = 1e-6}
        names = V.fromList [T.pack ("f" ++ show i) | i <- [0 .. 9 :: Int]]
        model = fitL1Logistic cfg rows labels names
        ws = VU.toList (lmWeights model)
        nonZeroIdxs = [i | (i, w) <- zip [0 :: Int ..] ws, w /= 0]
        zeroIdxs = [i | (i, w) <- zip [0 :: Int ..] ws, w == 0]
    assertBool
        ( "informative feature 1 should have non-zero weight (got "
            ++ show (ws !! 1)
            ++ ")"
        )
        (ws !! 1 /= 0)
    assertBool
        ( "informative feature 4 should have non-zero weight (got "
            ++ show (ws !! 4)
            ++ ")"
        )
        (ws !! 4 /= 0)
    -- Of the 8 noise features (indices 0,2,3,5,6,7,8,9), expect at least 6 to be 0.
    let noiseFeatures = [0, 2, 3, 5, 6, 7, 8, 9] :: [Int]
        noiseZero = length [i | i <- noiseFeatures, i `elem` zeroIdxs]
    assertBool
        ( "at least 6 noise features zeroed (got "
            ++ show noiseZero
            ++ "; non-zero idxs = "
            ++ show nonZeroIdxs
            ++ ")"
        )
        (noiseZero >= 6)

------------------------------------------------------------------------
-- A3: Convergence on well-conditioned input
------------------------------------------------------------------------

testA3Convergence :: Test
testA3Convergence = TestCase $ do
    let groundTruth = VU.fromList [1.0, -0.5, 0.7]
        rows = syntheticPoints 2 300 3
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 1000, scTol = 1e-5}
        model = fitL1Logistic cfg rows labels (V.fromList ["a", "b", "c"])
        -- Loss at the fitted model
        (rowsStd, _, _, _) = standardize rows
        ws = lmWeights model
        b = lmIntercept model
        -- Re-standardize the weights for loss comparison on standardized data
        loss0 = logisticLoss rowsStd labels (VU.replicate 3 0) 0
        -- Fit gives raw weights; compute loss on raw rows
        lossFit = logisticLoss rows labels ws b
    assertBool
        ( "loss decreased from initial (initial="
            ++ show loss0
            ++ ", final="
            ++ show lossFit
            ++ ")"
        )
        (lossFit < loss0)

------------------------------------------------------------------------
-- A4: Final loss <= initial loss (monotone or near-monotone in FISTA)
------------------------------------------------------------------------

testA4LossNotIncreasing :: Test
testA4LossNotIncreasing = TestCase $ do
    let groundTruth = VU.fromList [0.8, 0.4]
        rows = syntheticPoints 3 100 2
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.05, scMaxIter = 100}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        loss0 = logisticLoss rows labels (VU.replicate 2 0) 0
        lossFit = logisticLoss rows labels (lmWeights model) (lmIntercept model)
    assertBool
        ( "final loss must be <= initial loss (l0="
            ++ show loss0
            ++ ", lf="
            ++ show lossFit
            ++ ")"
        )
        (lossFit <= loss0 + 1e-9)

------------------------------------------------------------------------
-- A5: Degenerate input — all labels +1
------------------------------------------------------------------------

testA5AllSameDirection :: Test
testA5AllSameDirection = TestCase $ do
    let rows = syntheticPoints 4 50 3
        labels = VU.replicate 50 1.0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 100}
        model = fitL1Logistic cfg rows labels (V.fromList ["a", "b", "c"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyNaN = any isNaN ws || isNaN b
        anyInf = any isInfinite ws || isInfinite b
        allPositive = all (\i -> predict model (rows V.! i) == 1) [0 .. V.length rows - 1]
    assertBool "no NaN in weights/intercept" (not anyNaN)
    assertBool "no Inf in weights/intercept" (not anyInf)
    assertBool
        "all-same labels should produce a positive-predicting model"
        allPositive

------------------------------------------------------------------------
-- A6: Degenerate — empty input
------------------------------------------------------------------------

testA6Empty :: Test
testA6Empty = TestCase $ do
    let cfg = defaultSolverConfig
        emptyRows = V.empty :: V.Vector (VU.Vector Double)
        emptyLabels = VU.empty :: VU.Vector Double
        names = V.fromList ["a", "b"]
        model = fitL1Logistic cfg emptyRows emptyLabels names
    assertEqual
        "empty input -> 2 zero weights"
        (VU.fromList [0, 0])
        (lmWeights model)
    assertEqual "empty input -> zero intercept" 0 (lmIntercept model)

------------------------------------------------------------------------
-- A7: Degenerate — constant feature
------------------------------------------------------------------------

testA7ConstantFeature :: Test
testA7ConstantFeature = TestCase $ do
    -- Feature 1 is informative (uniform in [-1,1]); feature 0 is constant at 0.5.
    let baseRows = syntheticPoints 5 100 1
        rows =
            V.map
                (\row -> VU.fromList (0.5 : VU.toList row))
                baseRows
        groundTruth = VU.fromList [0.0, 1.0] -- only feature 1 matters
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 300, scTol = 1e-6}
        model = fitL1Logistic cfg rows labels (V.fromList ["constant", "signal"])
        ws = VU.toList (lmWeights model)
        anyBad = any (\x -> isNaN x || isInfinite x) ws
    assertBool
        ("constant feature weight ~ 0 (got " ++ show (head ws) ++ ")")
        (abs (head ws) < 1e-6)
    assertBool
        ("signal feature non-zero (got " ++ show (ws !! 1) ++ ")")
        (ws !! 1 /= 0)
    assertBool "no NaN/Inf" (not anyBad)

------------------------------------------------------------------------
-- A8: Numerical stability with large feature values
------------------------------------------------------------------------

testA8LargeValues :: Test
testA8LargeValues = TestCase $ do
    let scale = 1000.0 :: Double
        baseRows = syntheticPoints 6 100 2
        rows = V.map (VU.map (* scale)) baseRows
        groundTruth = VU.fromList [0.5, -0.7]
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 300}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\x -> isNaN x || isInfinite x) (b : ws)
        sameSigns =
            length
                [ () | i <- [0 .. V.length rows - 1], predict model (rows V.! i) == labels VU.! i
                ]
    assertBool "no NaN/Inf with scaled features" (not anyBad)
    assertBool
        ( "should correctly classify the vast majority of rows ("
            ++ show sameSigns
            ++ "/100)"
        )
        (sameSigns >= 90)

------------------------------------------------------------------------
-- A9: Standardization round-trip — recovered weights point in the true
-- direction even when raw-feature scales differ by orders of magnitude.
-- A broken de-standardization formula would scramble the per-feature scale
-- of @wRaw@ and the cosine to ground truth would drop sharply.
------------------------------------------------------------------------

testA9StandardizationRoundTrip :: Test
testA9StandardizationRoundTrip = TestCase $ do
    let nRows = 80 :: Int
        -- Column 0 ranges 0..400 (mean ~200, std ~115).
        -- Column 1 ranges 0..0.2 (mean ~0.1, std ~0.058).
        -- True hyperplane:  (col0 - 200) + 1000 * (col1 - 0.1)  > 0
        -- True raw weights (modulo positive scaling):  [1.0, 1000.0]
        col0 = [fromIntegral i * 5 :: Double | i <- [0 .. nRows - 1]]
        col1 = [fromIntegral i * 0.0025 :: Double | i <- [0 .. nRows - 1]]
        rows = V.fromList [VU.fromList [c0, c1] | (c0, c1) <- zip col0 col1]
        labels =
            VU.fromList
                [ if (c0 - 200) + 1000 * (c1 - 0.1) > 0 then 1.0 else -1.0
                | (c0, c1) <- zip col0 col1
                ]
        cfg = defaultSolverConfig{scL1Lambda = 1.0e-4, scMaxIter = 2000, scTol = 1.0e-7}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        truthDir = VU.fromList [1.0, 1000.0]
        cs = cosineSim (lmWeights model) truthDir
        -- All training points correctly classified
        trainPreds =
            [predict model (rows V.! i) | i <- [0 .. nRows - 1]]
        trainLabs =
            [labels VU.! i | i <- [0 .. nRows - 1]]
        correct =
            length
                [() | (p, l) <- zip trainPreds trainLabs, p == l]
    assertEqual "all training points correctly classified" nRows correct
    assertBool
        ( "recovered raw weights align with ground-truth direction across "
            ++ "vastly different feature scales (cos = "
            ++ show cs
            ++ ")"
        )
        (cs > 0.95)

------------------------------------------------------------------------
-- A10: Determinism — same input -> same output
------------------------------------------------------------------------

testA10Determinism :: Test
testA10Determinism = TestCase $ do
    let groundTruth = VU.fromList [0.6, 0.4]
        rows = syntheticPoints 9 60 2
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.05, scMaxIter = 200}
        m1 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        m2 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
    assertEqual "same input -> same weights" (lmWeights m1) (lmWeights m2)
    assertEqual "same input -> same intercept" (lmIntercept m1) (lmIntercept m2)

------------------------------------------------------------------------
-- A11: Two-feature ground truth recovery (w_2/w_1 ratio)
------------------------------------------------------------------------

testA11GroundTruthRatio :: Test
testA11GroundTruthRatio = TestCase $ do
    -- y = sign(x1 + 2*x2 - 3); pull from a larger range so a non-zero intercept matters.
    let groundTruth = VU.fromList [1.0, 2.0]
        groundBias = -3.0
        n = 500
        baseRows = syntheticPoints 10 n 2
        -- Scale up so x_i can range over [-3, 3] -- gives wider coverage of the boundary
        rows = V.map (VU.map (* 3)) baseRows
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg = defaultSolverConfig{scL1Lambda = 0.001, scMaxIter = 1000, scTol = 1e-7}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        ws = lmWeights model
        b = lmIntercept model
        ratio = (ws VU.! 1) / (ws VU.! 0)
        biasRatio = b / (ws VU.! 0)
    assertBool
        ("w2/w1 should approximate 2.0 (got " ++ show ratio ++ ")")
        (ratio > 1.7 && ratio < 2.3)
    assertBool
        ("b/w1 should approximate -3.0 (got " ++ show biasRatio ++ ")")
        (biasRatio > -3.4 && biasRatio < -2.6)

------------------------------------------------------------------------
-- B1: modelToExpr produces a well-typed Expr Bool
------------------------------------------------------------------------

testB1ExprWellTyped :: Test
testB1ExprWellTyped = TestCase $ do
    let model =
            LinearModel
                { lmWeights = VU.fromList [1.0, -2.0]
                , lmIntercept = 0.5
                , lmFeatureNames = V.fromList ["x", "y"]
                }
        expr = modelToExpr model
        -- Evaluate on a 3-row DataFrame
        df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([0.0, 1.0, 2.0] :: [Double]))
                , ("y", DI.fromList ([0.0, 0.0, 5.0] :: [Double]))
                ]
        -- Manual predictions: 1*x - 2*y + 0.5 > 0 ?
        manual =
            [ (1.0 * 0.0 - 2.0 * 0.0 + 0.5) > 0
            , (1.0 * 1.0 - 2.0 * 0.0 + 0.5) > 0
            , (1.0 * 2.0 - 2.0 * 5.0 + 0.5) > 0
            ]
    case interpret @Bool df expr of
        Left e -> assertFailure ("interpret failed: " ++ show e)
        Right (DI.TColumn col) -> case DI.toVector @Bool col of
            Left e -> assertFailure ("toVector failed: " ++ show e)
            Right vals ->
                assertEqual "Expr matches manual evaluation" manual (V.toList vals)

------------------------------------------------------------------------
-- B2: Zero weights are dropped from the resulting Expr
------------------------------------------------------------------------

testB2ZeroWeightsPruned :: Test
testB2ZeroWeightsPruned = TestCase $ do
    let model =
            LinearModel
                { lmWeights = VU.fromList [0.0, 1.5, 0.0]
                , lmIntercept = 0.0
                , lmFeatureNames = V.fromList ["a", "b", "c"]
                }
        expr = modelToExpr model
        cols = sort (getColumns expr)
    assertEqual "only column b appears in the Expr" ["b"] cols

------------------------------------------------------------------------
-- A14: Constant feature at large raw value — weight must be exactly 0
-- and no NaN/Inf leaks into the rest of the fit.
------------------------------------------------------------------------

testA14ConstantHugeValue :: Test
testA14ConstantHugeValue = TestCase $ do
    let baseRows = syntheticPoints 14 100 1 -- one informative feature
    -- Prepend a constant column at 1e8 to each row.
        rows =
            V.map
                (\row -> VU.fromList (1.0e8 : VU.toList row))
                baseRows
        -- Label depends only on the informative (second) feature.
        labels = labelsForHyperplane rows (VU.fromList [0.0, 1.0]) 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 300}
        model = fitL1Logistic cfg rows labels (V.fromList ["constant", "signal"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool "no NaN/Inf with constant-at-1e8 feature" (not anyBad)
    assertEqual
        "constant feature is dropped — weight is exactly zero"
        0
        (head ws)
    assertBool
        ("signal feature has non-zero weight (got " ++ show (ws !! 1) ++ ")")
        (ws !! 1 /= 0)

------------------------------------------------------------------------
-- A15: Variance exactly zero (all rows identical for that column).
------------------------------------------------------------------------

testA15AllZeroFeature :: Test
testA15AllZeroFeature = TestCase $ do
    -- A column that is exactly 0 for every row.
    let baseRows = syntheticPoints 15 80 1
        rows =
            V.map
                (\row -> VU.fromList (0.0 : VU.toList row))
                baseRows
        labels = labelsForHyperplane rows (VU.fromList [0.0, 1.0]) 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 300}
        model = fitL1Logistic cfg rows labels (V.fromList ["zero", "signal"])
        ws = VU.toList (lmWeights model)
    assertEqual "zero-variance column has weight zero" 0 (head ws)
    assertBool ("signal weight non-zero (" ++ show (ws !! 1) ++ ")") (ws !! 1 /= 0)

------------------------------------------------------------------------
-- A16: Severely imbalanced labels (99:1) — should not collapse to a
-- constant predictor on the majority class without some learning.
------------------------------------------------------------------------

testA16ImbalancedLabels :: Test
testA16ImbalancedLabels = TestCase $ do
    let nPos = 99
        nNeg = 1
        n = nPos + nNeg
        rows = syntheticPoints 16 n 2
        labels =
            VU.fromList
                (replicate nPos 1.0 ++ replicate nNeg (-1.0))
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scMaxIter = 500}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool "no NaN/Inf with 99:1 imbalance" (not anyBad)
    -- The intercept should be positive (the easy thing for the model is to
    -- predict the majority); weights may or may not be zero depending on lambda.
    assertBool ("intercept favors majority class (got b=" ++ show b ++ ")") (b > 0)

------------------------------------------------------------------------
-- A17: Mixed per-feature raw scales — should not diverge.
------------------------------------------------------------------------

testA17ImbalancedRawScales :: Test
testA17ImbalancedRawScales = TestCase $ do
    let baseRows = syntheticPoints 17 100 3
        -- Per-row: [1e-6 * v, v, 1e6 * v] — three columns with vastly
        -- different scales but the same underlying signal.
        rows =
            V.map
                ( \row ->
                    let v0 = row VU.! 0
                        v1 = row VU.! 1
                        v2 = row VU.! 2
                     in VU.fromList [1.0e-6 * v0, v1, 1.0e6 * v2]
                )
                baseRows
        labels = labelsForHyperplane baseRows (VU.fromList [1.0, -0.5, 0.7]) 0
        cfg = defaultSolverConfig{scL1Lambda = 1.0e-4, scMaxIter = 500}
        model = fitL1Logistic cfg rows labels (V.fromList ["tiny", "unit", "huge"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool ("no NaN/Inf with mixed scales (ws=" ++ show ws ++ ")") (not anyBad)
    -- The fit should classify the training points correctly on aggregate.
    let preds = [predict model (rows V.! i) | i <- [0 .. V.length rows - 1]]
        lbls = [labels VU.! i | i <- [0 .. VU.length labels - 1]]
        correct = length [() | (p, l) <- zip preds lbls, p == l]
    -- The wild per-feature scales make the problem poorly conditioned for
    -- L1-regularized FISTA with a fixed Lipschitz upper bound. We don't
    -- expect optimal accuracy — the assertion is "not random" (>=65%),
    -- catching divergence-to-garbage rather than guaranteeing fit quality.
    assertBool
        ("non-divergent under wild scales (got " ++ show correct ++ "/100)")
        (correct >= 65)

------------------------------------------------------------------------
-- A12: maxIter = 0 returns the initial point unchanged
------------------------------------------------------------------------

testA12MaxIterZero :: Test
testA12MaxIterZero = TestCase $ do
    let rows = syntheticPoints 20 50 2
        labels = labelsForHyperplane rows (VU.fromList [1.0, -0.5]) 0
        cfg = defaultSolverConfig{scMaxIter = 0}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
    assertEqual
        "maxIter=0 returns zero weights"
        (VU.fromList [0, 0])
        (lmWeights model)
    assertEqual "maxIter=0 returns zero intercept" 0 (lmIntercept model)

------------------------------------------------------------------------
-- A13: maxIter = 1 takes exactly one prox step (results differ from
-- the initial zero point but may not be near the optimum).
------------------------------------------------------------------------

testA13MaxIterOne :: Test
testA13MaxIterOne = TestCase $ do
    let rows = syntheticPoints 21 80 2
        labels = labelsForHyperplane rows (VU.fromList [1.0, -0.5]) 0
        cfg = defaultSolverConfig{scMaxIter = 1, scL1Lambda = 0.001}
        cfg0 = cfg{scMaxIter = 0}
        m1 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        m0 = fitL1Logistic cfg0 rows labels (V.fromList ["x", "y"])
        anyNonZero v = not (VU.all (== 0) v)
    -- maxIter=0 returns zeros
    assertEqual "baseline m0 weights are zero" (VU.fromList [0, 0]) (lmWeights m0)
    -- maxIter=1 differs from maxIter=0 (one step actually happened)
    assertBool
        ("maxIter=1 must change at least one weight (got " ++ show (lmWeights m1) ++ ")")
        (anyNonZero (lmWeights m1) || lmIntercept m1 /= 0)
    -- Final value is finite
    let badW = VU.any (\x -> isNaN x || isInfinite x) (lmWeights m1)
        badB = isNaN (lmIntercept m1) || isInfinite (lmIntercept m1)
    assertBool "no NaN/Inf after one iteration" (not (badW || badB))

------------------------------------------------------------------------
-- Test list
------------------------------------------------------------------------

tests :: [Test]
tests =
    [ TestLabel "A1 recover known hyperplane" testA1RecoverHyperplane
    , TestLabel "A2 L1 sparsity" testA2L1Sparsity
    , TestLabel "A3 convergence" testA3Convergence
    , TestLabel "A4 loss not increasing" testA4LossNotIncreasing
    , TestLabel "A5 all same direction" testA5AllSameDirection
    , TestLabel "A6 empty input" testA6Empty
    , TestLabel "A7 constant feature" testA7ConstantFeature
    , TestLabel "A8 large feature values" testA8LargeValues
    , TestLabel "A9 standardization round-trip" testA9StandardizationRoundTrip
    , TestLabel "A10 determinism" testA10Determinism
    , TestLabel "A11 ground truth ratio" testA11GroundTruthRatio
    , TestLabel "A12 maxIter zero" testA12MaxIterZero
    , TestLabel "A13 maxIter one" testA13MaxIterOne
    , TestLabel "A14 constant huge value" testA14ConstantHugeValue
    , TestLabel "A15 all-zero feature" testA15AllZeroFeature
    , TestLabel "A16 imbalanced 99:1 labels" testA16ImbalancedLabels
    , TestLabel "A17 imbalanced raw scales" testA17ImbalancedRawScales
    , TestLabel "B1 Expr well-typed" testB1ExprWellTyped
    , TestLabel "B2 zero weights pruned" testB2ZeroWeightsPruned
    ]
