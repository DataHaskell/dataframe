{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Logistic regression: binary and one-vs-rest multiclass over the FISTA
solver. 'fit' trains a 'LogisticModel'; 'predict' is the arg-max class decision.
Per-class margins and (normalized) probabilities stay available as named
auxiliary expressions.
-}
module DataFrame.LinearModel.Logistic (
    module DataFrame.Model,
    LogisticConfig (..),
    defaultLogisticConfig,
    LogisticModel (..),
    logisticMarginExprs,
    logisticProbExprs,
) where

import Data.List (sort)
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (
    affineExpr,
    argMaxExpr,
    featureNames,
    numericMatrix,
    targetValues,
 )
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.LinearSolver (
    LinearModel (..),
    SolverConfig,
    defaultSolverConfig,
    fitL1Logistic,
 )
import DataFrame.Model

-- | Hyperparameters for logistic regression (the FISTA solver config).
newtype LogisticConfig = LogisticConfig {lgSolver :: SolverConfig}
    deriving (Eq, Show)

defaultLogisticConfig :: LogisticConfig
defaultLogisticConfig = LogisticConfig defaultSolverConfig

{- | A fitted (one-vs-rest) logistic model: parallel vectors of class labels and
their binary sub-models. 'lgModels' carries sklearn's per-class @coef_@.
-}
data LogisticModel a = LogisticModel
    { lgClasses :: !(V.Vector a)
    , lgModels :: !(V.Vector LinearModel)
    }
    deriving (Eq, Show)

instance (Columnable a, Ord a) => Fit LogisticConfig (Expr a) (LogisticModel a) where
    fit = fitLogistic

instance (Columnable a, Ord a) => Predict (LogisticModel a) a where
    predict m = argMaxExpr (labelledMargins m)

-- | Fit one-vs-rest logistic regression; the target column supplies the classes.
fitLogistic ::
    (Columnable a, Ord a) =>
    LogisticConfig -> Expr a -> DataFrame -> LogisticModel a
fitLogistic (LogisticConfig cfg) target df =
    LogisticModel (V.fromList classes) (V.fromList (map fitOne classes))
  where
    names = featureNames target df
    (nameVec, mat) = numericMatrix names df
    ys = targetValues target df
    classes = sort (foldr dedup [] (V.toList ys))
    dedup x acc = if x `elem` acc then acc else x : acc
    fitOne c =
        let labels =
                VU.generate (V.length ys) (\i -> if ys V.! i == c then 1 else -1)
         in fitL1Logistic cfg mat labels nameVec

-- | The raw margin @Expr@ for each class.
logisticMarginExprs ::
    (Columnable a, Ord a) => LogisticModel a -> M.Map a (Expr Double)
logisticMarginExprs m = M.fromList (labelledMargins m)

-- | Per-class probability expressions: @1 / (1 + exp(-margin))@.
logisticProbExprs ::
    (Columnable a, Ord a) => LogisticModel a -> M.Map a (Expr Double)
logisticProbExprs = M.map sigmoidExpr . logisticMarginExprs

labelledMargins :: LogisticModel a -> [(a, Expr Double)]
labelledMargins m =
    [ (lgClasses m V.! i, marginOf (lgModels m V.! i))
    | i <- [0 .. V.length (lgClasses m) - 1]
    ]

marginOf :: LinearModel -> Expr Double
marginOf m =
    affineExpr
        (lmIntercept m)
        (zip (VU.toList (lmWeights m)) (V.toList (lmFeatureNames m)))

sigmoidExpr :: Expr Double -> Expr Double
sigmoidExpr z = F.lit 1 / (F.lit 1 + exp (negate z))
