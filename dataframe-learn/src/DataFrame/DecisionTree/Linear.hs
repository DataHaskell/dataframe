{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Oblique split candidates: fit an L1-regularised logistic hyperplane to the
-- care points (class-balanced) and convert it to a boolean condition, rejecting
-- all-zero and degenerate (single-side) hyperplanes.
module DataFrame.DecisionTree.Linear (
    bestLinearCandidate,
    fitLinearCandidate,
    careRowsFromFeatures,
    careLabels,
    featName,
    imputeMean,
    materializeFeatureForCare,
) where

import DataFrame.DecisionTree.Numeric (NumExpr (..), numericCols)
import DataFrame.DecisionTree.Types (CarePoint (..), Direction (..), TreeConfig (..))
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr, getColumns)
import DataFrame.Internal.Interpreter (interpret)
import qualified DataFrame.LinearSolver as LS

import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

-- | Best oblique candidate, or 'Nothing' when the linear path is disabled or
-- there are too few care points to fit on.
bestLinearCandidate :: TreeConfig -> DataFrame -> [CarePoint] -> Maybe (Expr Bool)
bestLinearCandidate cfg df carePoints
    | not (useLinearSolver cfg) = Nothing
    | length carePoints < minCarePointsForLinear cfg = Nothing
    | otherwise = fitLinearCandidate cfg df carePoints

-- | Fit an L1 logistic regression to the care points and convert the resulting
-- hyperplane to a condition, or 'Nothing' when no numeric features exist or the
-- fitted model is all-zero or degenerate.
fitLinearCandidate :: TreeConfig -> DataFrame -> [CarePoint] -> Maybe (Expr Bool)
fitLinearCandidate cfg df carePoints = case materializedFeatures df carePoints of
    [] -> Nothing
    mats -> linearFromFeatures cfg carePoints mats

materializedFeatures :: DataFrame -> [CarePoint] -> [(T.Text, VU.Vector Double)]
materializedFeatures df carePoints = mapMaybe (materializeFeatureForCare df carePoints) (numericCols df)

linearFromFeatures :: TreeConfig -> [CarePoint] -> [(T.Text, VU.Vector Double)] -> Maybe (Expr Bool)
linearFromFeatures cfg carePoints mats
    | VU.all (== 0) weights = Nothing
    | degenerateHyperplane rows weights (LS.lmIntercept model) = Nothing
    | otherwise = Just (LS.modelToExpr model)
  where
    rows = careRowsFromFeatures (length carePoints) mats
    labels = careLabels carePoints
    model = LS.fitL1Logistic (solverConfigFor cfg labels) rows labels (V.fromList (map fst mats))
    weights = LS.lmWeights model

solverConfigFor :: TreeConfig -> VU.Vector Double -> LS.SolverConfig
solverConfigFor cfg labels = (linearSolverConfig cfg){LS.scSampleWeights = classBalancedWeights labels}

-- | Class-balanced sklearn-form weights @w_i = N / (2 · N_class)@ (mean 1), or
-- 'Nothing' in the degenerate one-class case (uniform weighting).
classBalancedWeights :: VU.Vector Double -> Maybe (VU.Vector Double)
classBalancedWeights labels
    | nPos > 0 && nNeg > 0 = Just (VU.generate nCare weightAt)
    | otherwise = Nothing
  where
    nCare = VU.length labels
    nPos = VU.length (VU.filter (> 0) labels)
    nNeg = nCare - nPos
    weightAt i
        | VU.unsafeIndex labels i > 0 = fromIntegral nCare / (2 * fromIntegral nPos)
        | otherwise = fromIntegral nCare / (2 * fromIntegral nNeg)

-- | A hyperplane is degenerate when every care row scores on the same side of
-- zero (equivalent to an invalid split, caught upstream).
degenerateHyperplane :: V.Vector (VU.Vector Double) -> VU.Vector Double -> Double -> Bool
degenerateHyperplane rows weights bias =
    nCare > 0 && (VU.minimum scores > 0 || VU.maximum scores < 0)
  where
    nCare = V.length rows
    scores = VU.generate nCare (\i -> VU.sum (VU.zipWith (*) weights (V.unsafeIndex rows i)) + bias)

-- | Per-care-point feature rows from materialized columns (each of length
-- @nCare@, so indexing is in range).
careRowsFromFeatures :: Int -> [(T.Text, VU.Vector Double)] -> V.Vector (VU.Vector Double)
careRowsFromFeatures nCare mats =
    V.generate nCare (\i -> VU.generate nFeat (\j -> snd (matsVec V.! j) VU.! i))
  where
    matsVec = V.fromList mats
    nFeat = V.length matsVec

-- | Solver labels: @+1@ when 'GoLeft' is correct, @-1@ otherwise.
careLabels :: [CarePoint] -> VU.Vector Double
careLabels carePoints = VU.fromList [if cpCorrectDir cp == GoLeft then 1.0 else -1.0 | cp <- carePoints]

-- | First column referenced by an expression, or a placeholder when none.
featName :: Expr b -> T.Text
featName expr = case getColumns expr of
    (c : _) -> c
    [] -> "<feat>"

-- | Replace missing values with the mean of present ones; 'Nothing' when
-- nothing is present so the caller can drop the feature.
imputeMean :: [Maybe Double] -> Maybe (VU.Vector Double)
imputeMean careRaw = case catMaybes careRaw of
    [] -> Nothing
    present -> Just (VU.fromList [fromMaybe (mean present) mv | mv <- careRaw])
  where
    mean xs = sum xs / fromIntegral (length xs)

interpretDoubleVals :: DataFrame -> Expr Double -> Maybe (V.Vector Double)
interpretDoubleVals df expr = case interpret @Double df expr of
    Right (TColumn column) -> either (const Nothing) Just (toVector @Double column)
    _ -> Nothing

interpretMaybeDoubleVals :: DataFrame -> Expr (Maybe Double) -> Maybe (V.Vector (Maybe Double))
interpretMaybeDoubleVals df expr = case interpret @(Maybe Double) df expr of
    Right (TColumn column) -> either (const Nothing) Just (toVector @(Maybe Double) column)
    _ -> Nothing

-- | Materialize a 'NumExpr' over the care rows; 'Nothing' on interpret failure
-- or (nullable) when no care point has a present value, else mean-imputed.
materializeFeatureForCare :: DataFrame -> [CarePoint] -> NumExpr -> Maybe (T.Text, VU.Vector Double)
materializeFeatureForCare df carePoints (NDouble expr) = do
    vals <- interpretDoubleVals df expr
    Just (featName expr, VU.fromList [vals V.! cpIndex cp | cp <- carePoints])
materializeFeatureForCare df carePoints (NMaybeDouble expr) = do
    vals <- interpretMaybeDoubleVals df expr
    imputed <- imputeMean [vals V.! cpIndex cp | cp <- carePoints]
    Just (featName expr, imputed)
