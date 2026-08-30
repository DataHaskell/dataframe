{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Approximate RBF-kernel SVM via Random Fourier Features (Rahimi & Recht): map
each row through @z(x) = √(2/D)·cos(W x + b)@ with @W ~ N(0, 2γI)@ (seeded), then
fit a linear SVC in the random-feature space. 'predict' compiles to a closed
@Σ_r β_r·cos(…)@ expression of size @O(D·d)@, independent of the row count.
-}
module DataFrame.SVM.RFF (
    module DataFrame.Model,
    RFFConfig (..),
    defaultRFFConfig,
    RFFSVMModel (..),
) where

import Control.Exception (throw)
import Data.List (sort)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors (DataFrameException (..))

import DataFrame.Expression.Operators ((.*.), (.+.), (.>.))
import DataFrame.Featurize.Internal (featureNames, numericMatrix, targetValues)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (dot)
import DataFrame.LinearSolver (LinearModel (..), SolverConfig (..), fitProx)
import DataFrame.LinearSolver.Loss (sqHingeLoss)
import DataFrame.Model
import DataFrame.Random (Gen, gaussianVector, mkGen, nextDouble)

data RFFConfig = RFFConfig
    { rffD :: !Int
    , rffGamma :: !Double
    , rffC :: !Double
    , rffMaxIter :: !Int
    , rffTol :: !Double
    , rffSeed :: !Int
    }
    deriving (Eq, Show)

defaultRFFConfig :: RFFConfig
defaultRFFConfig =
    RFFConfig
        { rffD = 100
        , rffGamma = 0.1
        , rffC = 1.0
        , rffMaxIter = 1000
        , rffTol = 1.0e-4
        , rffSeed = 0
        }

{- | A fitted RFF SVM (binary). 'rffW' / 'rffB' are the random projection;
'rffCoef' / 'rffIntercept' the linear SVC in feature space.
-}
data RFFSVMModel a = RFFSVMModel
    { rffW :: !(V.Vector (VU.Vector Double))
    , rffB :: !(VU.Vector Double)
    , rffCoef :: !(VU.Vector Double)
    , rffIntercept :: !Double
    , rffScale :: !Double
    , rffNegClass :: !a
    , rffPosClass :: !a
    , rffFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Show)

instance (Columnable a, Ord a) => Fit RFFConfig (Expr a) where
    type ModelOf RFFConfig (Expr a) = (RFFSVMModel a)
    fit = fitRFFSVM

instance (Columnable a) => Predict (RFFSVMModel a) where
    type Prediction (RFFSVMModel a) = Expr a
    predict m =
        If (margin .>. F.lit 0) (Lit (rffPosClass m)) (Lit (rffNegClass m))
      where
        names = V.toList (rffFeatureNames m)
        margin =
            foldr (.+.) (F.lit (rffIntercept m)) $
                [ F.lit (rffCoef m VU.! r * rffScale m) .*. cosTerm r
                | r <- [0 .. V.length (rffW m) - 1]
                , rffCoef m VU.! r /= 0
                ]
        cosTerm r = cos (linComb (rffW m V.! r) (rffB m VU.! r))
        linComb w b =
            foldr (.+.) (F.lit b) $
                [ F.lit (w VU.! j) .*. (Col n :: Expr Double)
                | (j, n) <- zip [0 ..] names
                ]

-- | Fit a binary RFF SVM. Targets with more than two classes are rejected.
fitRFFSVM ::
    (Columnable a, Ord a) => RFFConfig -> Expr a -> DataFrame -> RFFSVMModel a
fitRFFSVM cfg target df =
    case classes of
        [neg, pos] -> build neg pos
        _ ->
            throw
                ( InternalException
                    "fitRFFSVM: binary classification only, but the target has /= 2 classes"
                )
  where
    names = featureNames target df
    (nameVec, mat) = numericMatrix names df
    ys = targetValues target df
    classes = sort (foldr dedup [] (V.toList ys))
    dedup x acc = if x `elem` acc then acc else x : acc
    d = if V.null mat then 0 else VU.length (V.head mat)
    bigD = max 1 (rffD cfg)
    (ws, bs) = sampleRFF bigD d (rffGamma cfg) (mkGen (rffSeed cfg))
    scale = sqrt (2 / fromIntegral bigD)
    z = V.map (featureRow ws bs scale) mat
    featNames = V.fromList ["rff" <> T.pack (show r) | r <- [0 .. bigD - 1]]
    build neg pos =
        let labels = VU.generate (V.length ys) (\i -> if ys V.! i == pos then 1 else -1)
            solverCfg =
                SolverConfig
                    { scL1Lambda = 0
                    , scL2Lambda = 1 / rffC cfg
                    , scMaxIter = rffMaxIter cfg
                    , scTol = rffTol cfg
                    , scSampleWeights = Nothing
                    }
            model = fitProx sqHingeLoss solverCfg z labels featNames
         in RFFSVMModel ws bs (lmWeights model) (lmIntercept model) scale neg pos nameVec

sampleRFF ::
    Int -> Int -> Double -> Gen -> (V.Vector (VU.Vector Double), VU.Vector Double)
sampleRFF bigD d gamma g0 = (V.fromList ws, VU.fromList bs)
  where
    sigma = sqrt (2 * gamma)
    (ws, g1) = goW bigD g0 []
    goW 0 g acc = (reverse acc, g)
    goW k g acc =
        let (vec, g') = gaussianVector d g
         in goW (k - 1) g' (VU.map (* sigma) vec : acc)
    bs = take bigD (goB g1)
    goB g = let (u, g') = nextDouble g in (u * 2 * pi) : goB g'

featureRow ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    Double ->
    VU.Vector Double ->
    VU.Vector Double
featureRow ws bs scale x =
    VU.generate (V.length ws) $ \r ->
        scale * cos (dot (ws V.! r) x + bs VU.! r)
