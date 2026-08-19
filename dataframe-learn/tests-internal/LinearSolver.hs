{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module LinearSolver where

import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (getColumns)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.LinearSolver
import qualified DataFrameApi as D

import Data.List (sort)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import System.Random (mkStdGen, randomR)
import Test.HUnit

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

-- Average binary logistic loss at (w, b); the branch on @margin@ keeps
-- @log(1 + exp(-margin))@ numerically stable.
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
             in if margin >= 0
                    then log (1 + exp (-margin))
                    else (-margin) + log (1 + exp margin)
     in sum [loss i | i <- [0 .. n - 1]] / fromIntegral n

testA1RecoverHyperplane :: Test
testA1RecoverHyperplane = TestCase $ do
    let groundTruth = VU.fromList [0.7, -0.5]
        groundBias = 0.3
        rows = syntheticPoints 1 200 2
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg =
            defaultSolverConfig
                { scL1Lambda = 0
                , scL2Lambda = 0
                , scMaxIter = 500
                , scTol = 1e-6
                }
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        cosSim = cosineSim (lmWeights model) groundTruth
        sameSignAll =
            all
                (\i -> predict model (rows V.! i) == labels VU.! i)
                [0 .. V.length rows - 1]
    assertBool
        ("recovered weights should align with ground truth (cos = " ++ show cosSim ++ ")")
        (cosSim > 0.99)
    assertBool "all training points predicted correctly" sameSignAll

testA2L1Sparsity :: Test
testA2L1Sparsity = TestCase $ do
    let groundTruth = VU.fromList [0, 1.2, 0, 0, -1.5, 0, 0, 0, 0, 0]
        groundBias = 0
        rows = syntheticPoints 7 500 10
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg =
            defaultSolverConfig
                { scL1Lambda = 0.1
                , scL2Lambda = 0
                , scMaxIter = 500
                , scTol = 1e-6
                }
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

testA3Convergence :: Test
testA3Convergence = TestCase $ do
    let groundTruth = VU.fromList [1.0, -0.5, 0.7]
        rows = syntheticPoints 2 300 3
        labels = labelsForHyperplane rows groundTruth 0
        cfg =
            defaultSolverConfig
                { scL1Lambda = 0.01
                , scL2Lambda = 0
                , scMaxIter = 1000
                , scTol = 1e-5
                }
        model = fitL1Logistic cfg rows labels (V.fromList ["a", "b", "c"])
        (rowsStd, _, _, _) = standardize rows
        ws = lmWeights model
        b = lmIntercept model
        loss0 = logisticLoss rowsStd labels (VU.replicate 3 0) 0
        lossFit = logisticLoss rows labels ws b
    assertBool
        ( "loss decreased from initial (initial="
            ++ show loss0
            ++ ", final="
            ++ show lossFit
            ++ ")"
        )
        (lossFit < loss0)

testA4LossNotIncreasing :: Test
testA4LossNotIncreasing = TestCase $ do
    let groundTruth = VU.fromList [0.8, 0.4]
        rows = syntheticPoints 3 100 2
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.05, scL2Lambda = 0, scMaxIter = 100}
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

testA5AllSameDirection :: Test
testA5AllSameDirection = TestCase $ do
    let rows = syntheticPoints 4 50 3
        labels = VU.replicate 50 1.0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scL2Lambda = 0, scMaxIter = 100}
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

testA7ConstantFeature :: Test
testA7ConstantFeature = TestCase $ do
    let baseRows = syntheticPoints 5 100 1
        rows =
            V.map
                (\row -> VU.fromList (0.5 : VU.toList row))
                baseRows
        groundTruth = VU.fromList [0.0, 1.0]
        labels = labelsForHyperplane rows groundTruth 0
        cfg =
            defaultSolverConfig
                { scL1Lambda = 0.01
                , scL2Lambda = 0
                , scMaxIter = 300
                , scTol = 1e-6
                }
        model = fitL1Logistic cfg rows labels (V.fromList ["constant", "signal"])
        ws = VU.toList (lmWeights model)
        anyBad = any (\x -> isNaN x || isInfinite x) ws
    w0 <- case ws of
        (w : _) -> pure w
        [] -> assertFailure "expected at least one weight"
    assertBool
        ("constant feature weight ~ 0 (got " ++ show w0 ++ ")")
        (abs w0 < 1e-6)
    assertBool
        ("signal feature non-zero (got " ++ show (ws !! 1) ++ ")")
        (ws !! 1 /= 0)
    assertBool "no NaN/Inf" (not anyBad)

testA8LargeValues :: Test
testA8LargeValues = TestCase $ do
    let scale = 1000.0 :: Double
        baseRows = syntheticPoints 6 100 2
        rows = V.map (VU.map (* scale)) baseRows
        groundTruth = VU.fromList [0.5, -0.7]
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scL2Lambda = 0, scMaxIter = 300}
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

testA9StandardizationRoundTrip :: Test
testA9StandardizationRoundTrip = TestCase $ do
    let nRows = 80 :: Int
        col0 = [fromIntegral i * 5 :: Double | i <- [0 .. nRows - 1]]
        col1 = [fromIntegral i * 0.0025 :: Double | i <- [0 .. nRows - 1]]
        rows = V.fromList [VU.fromList [c0, c1] | (c0, c1) <- zip col0 col1]
        labels =
            VU.fromList
                [ if (c0 - 200) + 1000 * (c1 - 0.1) > 0 then 1.0 else -1.0
                | (c0, c1) <- zip col0 col1
                ]
        cfg =
            defaultSolverConfig
                { scL1Lambda = 1.0e-4
                , scL2Lambda = 0
                , scMaxIter = 2000
                , scTol = 1.0e-7
                }
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        truthDir = VU.fromList [1.0, 1000.0]
        cs = cosineSim (lmWeights model) truthDir
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

testA10Determinism :: Test
testA10Determinism = TestCase $ do
    let groundTruth = VU.fromList [0.6, 0.4]
        rows = syntheticPoints 9 60 2
        labels = labelsForHyperplane rows groundTruth 0
        cfg = defaultSolverConfig{scL1Lambda = 0.05, scL2Lambda = 0, scMaxIter = 200}
        m1 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        m2 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
    assertEqual "same input -> same weights" (lmWeights m1) (lmWeights m2)
    assertEqual "same input -> same intercept" (lmIntercept m1) (lmIntercept m2)

testA11GroundTruthRatio :: Test
testA11GroundTruthRatio = TestCase $ do
    let groundTruth = VU.fromList [1.0, 2.0]
        groundBias = -3.0
        n = 500
        baseRows = syntheticPoints 10 n 2
        rows = V.map (VU.map (* 3)) baseRows
        labels = labelsForHyperplane rows groundTruth groundBias
        cfg =
            defaultSolverConfig
                { scL1Lambda = 0.001
                , scL2Lambda = 0
                , scMaxIter = 1000
                , scTol = 1e-7
                }
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

testB1ExprWellTyped :: Test
testB1ExprWellTyped = TestCase $ do
    let model =
            LinearModel
                { lmWeights = VU.fromList [1.0, -2.0]
                , lmIntercept = 0.5
                , lmFeatureNames = V.fromList ["x", "y"]
                }
        expr = modelToExpr model
        df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([0.0, 1.0, 2.0] :: [Double]))
                , ("y", DI.fromList ([0.0, 0.0, 5.0] :: [Double]))
                ]
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

testA14ConstantHugeValue :: Test
testA14ConstantHugeValue = TestCase $ do
    let baseRows = syntheticPoints 14 100 1
        rows =
            V.map
                (\row -> VU.fromList (1.0e8 : VU.toList row))
                baseRows
        labels = labelsForHyperplane rows (VU.fromList [0.0, 1.0]) 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scL2Lambda = 0, scMaxIter = 300}
        model = fitL1Logistic cfg rows labels (V.fromList ["constant", "signal"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool "no NaN/Inf with constant-at-1e8 feature" (not anyBad)
    w0 <- case ws of
        (w : _) -> pure w
        [] -> assertFailure "expected at least one weight"
    assertEqual
        "constant feature is dropped — weight is exactly zero"
        0
        w0
    assertBool
        ("signal feature has non-zero weight (got " ++ show (ws !! 1) ++ ")")
        (ws !! 1 /= 0)

testA15AllZeroFeature :: Test
testA15AllZeroFeature = TestCase $ do
    let baseRows = syntheticPoints 15 80 1
        rows =
            V.map
                (\row -> VU.fromList (0.0 : VU.toList row))
                baseRows
        labels = labelsForHyperplane rows (VU.fromList [0.0, 1.0]) 0
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scL2Lambda = 0, scMaxIter = 300}
        model = fitL1Logistic cfg rows labels (V.fromList ["zero", "signal"])
        ws = VU.toList (lmWeights model)
    w0 <- case ws of
        (w : _) -> pure w
        [] -> assertFailure "expected at least one weight"
    assertEqual "zero-variance column has weight zero" 0 w0
    assertBool ("signal weight non-zero (" ++ show (ws !! 1) ++ ")") (ws !! 1 /= 0)

testA16ImbalancedLabels :: Test
testA16ImbalancedLabels = TestCase $ do
    let nPos = 99
        nNeg = 1
        n = nPos + nNeg
        rows = syntheticPoints 16 n 2
        labels =
            VU.fromList
                (replicate nPos 1.0 ++ replicate nNeg (-1.0))
        cfg = defaultSolverConfig{scL1Lambda = 0.01, scL2Lambda = 0, scMaxIter = 500}
        model = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool "no NaN/Inf with 99:1 imbalance" (not anyBad)
    assertBool ("intercept favors majority class (got b=" ++ show b ++ ")") (b > 0)

testA17ImbalancedRawScales :: Test
testA17ImbalancedRawScales = TestCase $ do
    let baseRows = syntheticPoints 17 100 3
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
        cfg = defaultSolverConfig{scL1Lambda = 1.0e-4, scL2Lambda = 0, scMaxIter = 500}
        model = fitL1Logistic cfg rows labels (V.fromList ["tiny", "unit", "huge"])
        ws = VU.toList (lmWeights model)
        b = lmIntercept model
        anyBad = any (\v -> isNaN v || isInfinite v) (b : ws)
    assertBool ("no NaN/Inf with mixed scales (ws=" ++ show ws ++ ")") (not anyBad)
    let preds = [predict model (rows V.! i) | i <- [0 .. V.length rows - 1]]
        lbls = [labels VU.! i | i <- [0 .. VU.length labels - 1]]
        correct = length [() | (p, l) <- zip preds lbls, p == l]
    assertBool
        ("non-divergent under wild scales (got " ++ show correct ++ "/100)")
        (correct >= 65)

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

testA13MaxIterOne :: Test
testA13MaxIterOne = TestCase $ do
    let rows = syntheticPoints 21 80 2
        labels = labelsForHyperplane rows (VU.fromList [1.0, -0.5]) 0
        cfg = defaultSolverConfig{scMaxIter = 1, scL1Lambda = 0.001, scL2Lambda = 0}
        cfg0 = cfg{scMaxIter = 0}
        m1 = fitL1Logistic cfg rows labels (V.fromList ["x", "y"])
        m0 = fitL1Logistic cfg0 rows labels (V.fromList ["x", "y"])
        anyNonZero v = not (VU.all (== 0) v)
    assertEqual "baseline m0 weights are zero" (VU.fromList [0, 0]) (lmWeights m0)
    assertBool
        ("maxIter=1 must change at least one weight (got " ++ show (lmWeights m1) ++ ")")
        (anyNonZero (lmWeights m1) || lmIntercept m1 /= 0)
    let badW = VU.any (\x -> isNaN x || isInfinite x) (lmWeights m1)
        badB = isNaN (lmIntercept m1) || isInfinite (lmIntercept m1)
    assertBool "no NaN/Inf after one iteration" (not (badW || badB))

-- Generate two correlated features f0, f1 with correlation ρ, plus
-- noise features f2..f7. Truth is sign(f0 + f1).
correlatedPairData ::
    Int -> Double -> (V.Vector (VU.Vector Double), VU.Vector Double)
correlatedPairData seed rho =
    let n = 400 :: Int
        d = 8 :: Int
        g0 = mkStdGen seed
        drawUnit = randomR (-1.0 :: Double, 1.0)
        drawRow !gIn =
            let (z0, g1) = drawUnit gIn
                (epsRaw, g2) = drawUnit g1
                eps = epsRaw * sqrt (max 0 (1 - rho * rho))
                f0 = z0
                f1 = rho * z0 + eps
                drawNoise k g
                    | k >= d - 2 = ([], g)
                    | otherwise =
                        let (x, g') = drawUnit g
                            (xs, g'') = drawNoise (k + 1) g'
                         in (x : xs, g'')
                (noise, g3) = drawNoise 0 g2
                row = f0 : f1 : noise
             in (VU.fromList row, g3)
        go 0 _ acc = reverse acc
        go k g acc =
            let (r, g') = drawRow g
             in go (k - 1) g' (r : acc)
        rows = V.fromList (go n g0 [])
        labels =
            VU.generate n $ \i ->
                let r = rows V.! i
                    s = VU.unsafeIndex r 0 + VU.unsafeIndex r 1
                 in if s > 0 then 1.0 else -1.0
     in (rows, labels)

testA19ElasticNetRecoveryHigh :: Test
testA19ElasticNetRecoveryHigh = TestCase $ do
    let (rows, labels) = correlatedPairData 31 0.97
        names = V.fromList ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"]
        cfgEN = defaultSolverConfig{scL1Lambda = 0.05, scL2Lambda = 0.05, scMaxIter = 1000}
        men = fitL1Logistic cfgEN rows labels names
        wEN = VU.toList (lmWeights men)
        nzCount xs = length (filter (/= 0) xs)
        (aEN, bEN) = case wEN of
            (a : b : _) -> (a, b)
            _ -> error "elastic-net test: expected at least two weights"
    assertBool
        ("ρ=0.97 EN keeps f0 non-zero; wEN[:2] = " ++ show (take 2 wEN))
        (aEN /= 0)
    assertBool
        ("ρ=0.97 EN keeps f1 non-zero; wEN[:2] = " ++ show (take 2 wEN))
        (bEN /= 0)
    let ratio = abs aEN / max (abs bEN) 1e-9
    assertBool
        ("ρ=0.97 EN grouping: |w0/w1| ∈ [0.33, 3.0]; got ratio=" ++ show ratio)
        (ratio >= 0.33 && ratio <= 3.0)
    assertBool
        ("ρ=0.97 EN sparsity: total non-zero ≤ 5; got " ++ show (nzCount wEN))
        (nzCount wEN <= 5)

testA19ElasticNetRecoveryMid :: Test
testA19ElasticNetRecoveryMid = TestCase $ do
    let (rows, labels) = correlatedPairData 37 0.7
        names = V.fromList ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"]
        cfgEN = defaultSolverConfig{scL1Lambda = 0.05, scL2Lambda = 0.05, scMaxIter = 1000}
        men = fitL1Logistic cfgEN rows labels names
        wEN = VU.toList (lmWeights men)
        (aEN, bEN) = case wEN of
            (a : b : _) -> (a, b)
            _ -> error "elastic-net test: expected at least two weights"
    assertBool
        ("ρ=0.7 EN keeps f0 non-zero; wEN[:2] = " ++ show (take 2 wEN))
        (aEN /= 0)
    assertBool
        ("ρ=0.7 EN keeps f1 non-zero; wEN[:2] = " ++ show (take 2 wEN))
        (bEN /= 0)
    let ratio = abs aEN / max (abs bEN) 1e-9
    assertBool
        ("ρ=0.7 EN grouping: |w0/w1| ∈ [0.33, 3.0]; got ratio=" ++ show ratio)
        (ratio >= 0.33 && ratio <= 3.0)

testA20ClassBalancedFit :: Test
testA20ClassBalancedFit = TestCase $ do
    let n = 200 :: Int
        nPos = 190 :: Int
        g0 = mkStdGen 41
        drawN = randomR (-1.0 :: Double, 1.0)
        drawRowAt mu g =
            let (z, g') = drawN g
                x = mu + 0.6 * z
             in (VU.singleton x, g')
        rowsAndLabels =
            let go _ 0 _ acc = reverse acc
                go !pCnt k g acc =
                    let !mu = if pCnt > 0 then 0.15 else -0.15
                        (row, g') = drawRowAt mu g
                        !y = if pCnt > 0 then 1.0 else -1.0
                     in go (pCnt - 1) (k - 1) g' ((row, y) : acc)
             in go nPos n g0 []
        rows = V.fromList (map fst rowsAndLabels)
        labels = VU.fromList (map snd rowsAndLabels)
        names = V.fromList ["x"]
        cfgUnbal =
            defaultSolverConfig
                { scL1Lambda = 0.001
                , scL2Lambda = 0
                , scMaxIter = 2000
                , scTol = 1e-7
                , scSampleWeights = Nothing
                }
        nNeg = n - nPos
        balanced =
            VU.generate n $ \i ->
                let !y = VU.unsafeIndex labels i
                 in if y > 0
                        then fromIntegral n / (2 * fromIntegral nPos)
                        else fromIntegral n / (2 * fromIntegral nNeg)
        cfgBal = cfgUnbal{scSampleWeights = Just balanced}
        mUnbal = fitL1Logistic cfgUnbal rows labels names
        mBal = fitL1Logistic cfgBal rows labels names
        bUnbal = lmIntercept mUnbal
        bBal = lmIntercept mBal
        testRows =
            V.fromList
                ( replicate 100 (VU.singleton 0.15)
                    ++ replicate 100 (VU.singleton (-0.15))
                )
        predFracPos m =
            let preds = V.map (predict m) testRows
                ps = V.length (V.filter (> 0) preds)
             in fromIntegral ps / fromIntegral (V.length testRows) :: Double
        fracUnbal = predFracPos mUnbal
        fracBal = predFracPos mBal
    assertBool
        ("unbalanced |b| > 2.0; got " ++ show bUnbal)
        (abs bUnbal > 2.0)
    assertBool
        ("balanced |b| < 0.3; got " ++ show bBal)
        (abs bBal < 0.3)
    assertBool
        ("unbalanced fraction-positive on balanced test ≥ 0.90; got " ++ show fracUnbal)
        (fracUnbal >= 0.90)
    assertBool
        ( "balanced fraction-positive on balanced test ∈ [0.40, 0.60]; got "
            ++ show fracBal
        )
        (fracBal >= 0.40 && fracBal <= 0.60)

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
    , -- PR 3: Elastic Net + class-balanced weights.
      TestLabel "A19 Elastic Net grouping ρ=0.97" testA19ElasticNetRecoveryHigh
    , TestLabel "A19 Elastic Net grouping ρ=0.7" testA19ElasticNetRecoveryMid
    , TestLabel "A20 class-balanced fit on 95/5" testA20ClassBalancedFit
    ]
