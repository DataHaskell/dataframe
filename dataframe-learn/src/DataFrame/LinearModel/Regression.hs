{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}

{- | Linear regression with the standard penalties: OLS (QR), ridge (Cholesky),
and lasso\/elastic net (FISTA). 'fit' produces a 'LinearRegressor'; 'predict'
compiles it to an @Expr Double@ over the raw feature columns.
-}
module DataFrame.LinearModel.Regression (
    module DataFrame.Model,
    Penalty (..),
    LinearConfig (..),
    defaultLinearConfig,
    LinearRegressor (..),
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (
    affineExpr,
    featureNames,
    numericMatrix,
    targetDoubles,
 )
import DataFrame.Internal.Expression (Expr)
import DataFrame.LinearAlgebra (Matrix, dot, gram, tMatVec)
import DataFrame.LinearAlgebra.Solve (choleskySolve, qrLeastSquares)
import DataFrame.LinearSolver (
    LinearModel (..),
    SolverConfig (..),
    defaultSolverConfig,
    fitProx,
 )
import DataFrame.LinearSolver.Loss (squaredLoss)
import DataFrame.Model

-- | Regularization choice. @alpha@ is the penalty strength; @l1Ratio@ mixes L1/L2.
data Penalty
    = OLS
    | Ridge !Double
    | Lasso !Double
    | ElasticNet !Double !Double
    deriving (Eq, Show)

-- | Hyperparameters for linear regression: the penalty and the FISTA solver config.
data LinearConfig = LinearConfig
    { lcPenalty :: !Penalty
    , lcSolver :: !SolverConfig
    }
    deriving (Eq, Show)

defaultLinearConfig :: LinearConfig
defaultLinearConfig = LinearConfig{lcPenalty = OLS, lcSolver = defaultSolverConfig}

{- | A fitted linear regressor. @regCoef@ and @regIntercept@ are sklearn's
@coef_@ / @intercept_@ in raw feature space.
-}
data LinearRegressor = LinearRegressor
    { regCoef :: !(VU.Vector Double)
    , regIntercept :: !Double
    , regFeatureNames :: !(V.Vector T.Text)
    , regPenalty :: !Penalty
    }
    deriving (Eq, Show)

instance Fit LinearConfig (Expr Double) where
    type ModelOf LinearConfig (Expr Double) = LinearRegressor
    type FrameReq LinearConfig (Expr Double) = 'AllDoubleFrame
    fit (LinearConfig penalty cfg) target df =
        case penalty of
            OLS -> closedForm (olsSolve mat y)
            Ridge alpha -> closedForm (ridgeSolve alpha mat y)
            Lasso alpha -> proxFit alpha 1.0
            ElasticNet alpha l1r -> proxFit alpha l1r
      where
        names = featureNames target df
        (nameVec, mat) = numericMatrix names df
        y = targetDoubles target df
        closedForm (coef, intercept) =
            LinearRegressor coef intercept nameVec penalty
        proxFit alpha l1r =
            let proxCfg =
                    cfg{scL1Lambda = alpha * l1r, scL2Lambda = alpha * (1 - l1r)}
                m = fitProx squaredLoss proxCfg mat y nameVec
             in LinearRegressor (lmWeights m) (lmIntercept m) nameVec penalty

instance Predict LinearRegressor where
    type Prediction LinearRegressor = Expr Double
    predict m =
        affineExpr
            (regIntercept m)
            (zip (VU.toList (regCoef m)) (V.toList (regFeatureNames m)))

-- | OLS via QR on the intercept-augmented design matrix.
olsSolve :: Matrix -> VU.Vector Double -> (VU.Vector Double, Double)
olsSolve mat y =
    case qrLeastSquares augmented y of
        Right sol -> (VU.drop 1 sol, sol VU.! 0)
        Left _ -> ridgeSolve 1e-8 mat y
  where
    augmented = V.map (VU.cons 1) mat

{- | Ridge via Cholesky on @(XcᵀXc + αI) w = Xcᵀ yc@ over centred data; the
intercept is recovered from the column/target means.
-}
ridgeSolve :: Double -> Matrix -> VU.Vector Double -> (VU.Vector Double, Double)
ridgeSolve alpha mat y =
    case choleskySolve a rhs of
        Just w -> (w, meanY - dot w meansX)
        Nothing -> (VU.replicate d 0, meanY)
  where
    n = V.length mat
    d = if n == 0 then 0 else VU.length (V.head mat)
    meansX =
        VU.generate d $ \j ->
            sum [(mat V.! i) VU.! j | i <- [0 .. n - 1]] / fromIntegral n
    meanY = VU.sum y / fromIntegral n
    centered = V.map (\row -> VU.zipWith (-) row meansX) mat
    yc = VU.map (subtract meanY) y
    a = addDiag alpha (gram centered)
    rhs = tMatVec centered yc

-- | Add @alpha@ to the diagonal of a square matrix.
addDiag :: Double -> Matrix -> Matrix
addDiag alpha = V.imap (\i row -> row VU.// [(i, row VU.! i + alpha)])

-- | Predict the target for each row of a feature matrix.
predictLinear :: LinearRegressor -> Matrix -> VU.Vector Double
predictLinear m = VU.convert . V.map (\x -> regIntercept m + dot (regCoef m) x)
