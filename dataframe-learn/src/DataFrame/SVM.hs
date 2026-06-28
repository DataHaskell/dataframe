{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Linear support vector classification: L2-regularized squared hinge fitted
with the FISTA engine (sklearn's LinearSVC default loss). 'fit' trains a
one-vs-rest 'LinearSVCModel'; 'predict' is the arg-max class margin. There is no
@predict_proba@, matching sklearn's LinearSVC.
-}
module DataFrame.SVM (
    module DataFrame.Model,
    LinearSVCModel (..),
    SVCConfig (..),
    defaultSVCConfig,
    svcMarginExprs,
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
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.LinearSolver (LinearModel (..), SolverConfig (..), fitProx)
import DataFrame.LinearSolver.Loss (sqHingeLoss)
import DataFrame.Model

-- | Hyper-parameters. @svcC@ is the inverse regularization strength (sklearn @C@).
data SVCConfig = SVCConfig
    { svcC :: !Double
    , svcMaxIter :: !Int
    , svcTol :: !Double
    }
    deriving (Eq, Show)

defaultSVCConfig :: SVCConfig
defaultSVCConfig = SVCConfig{svcC = 1.0, svcMaxIter = 1000, svcTol = 1.0e-4}

-- | A fitted one-vs-rest linear SVC: class labels and their margin sub-models.
data LinearSVCModel a = LinearSVCModel
    { svcClasses :: !(V.Vector a)
    , svcModels :: !(V.Vector LinearModel)
    }
    deriving (Eq, Show)

instance (Columnable a, Ord a) => Fit SVCConfig (Expr a) (LinearSVCModel a) where
    fit = fitLinearSVC

instance (Columnable a, Ord a) => Predict (LinearSVCModel a) a where
    predict m = argMaxExpr (labelledMargins m)

-- | Fit a one-vs-rest linear SVC.
fitLinearSVC ::
    (Columnable a, Ord a) =>
    SVCConfig -> Expr a -> DataFrame -> LinearSVCModel a
fitLinearSVC cfg target df =
    LinearSVCModel (V.fromList classes) (V.fromList (map fitOne classes))
  where
    names = featureNames target df
    (nameVec, mat) = numericMatrix names df
    ys = targetValues target df
    classes = sort (foldr dedup [] (V.toList ys))
    dedup x acc = if x `elem` acc then acc else x : acc
    solverCfg =
        SolverConfig
            { scL1Lambda = 0
            , scL2Lambda = 1 / svcC cfg
            , scMaxIter = svcMaxIter cfg
            , scTol = svcTol cfg
            , scSampleWeights = Nothing
            }
    fitOne c =
        let labels =
                VU.generate (V.length ys) (\i -> if ys V.! i == c then 1 else -1)
         in fitProx sqHingeLoss solverCfg mat labels nameVec

-- | The raw margin expression for each class.
svcMarginExprs ::
    (Columnable a, Ord a) => LinearSVCModel a -> M.Map a (Expr Double)
svcMarginExprs m = M.fromList (labelledMargins m)

labelledMargins :: LinearSVCModel a -> [(a, Expr Double)]
labelledMargins m =
    [ (svcClasses m V.! i, marginOf (svcModels m V.! i))
    | i <- [0 .. V.length (svcClasses m) - 1]
    ]

marginOf :: LinearModel -> Expr Double
marginOf m =
    affineExpr
        (lmIntercept m)
        (zip (VU.toList (lmWeights m)) (V.toList (lmFeatureNames m)))
