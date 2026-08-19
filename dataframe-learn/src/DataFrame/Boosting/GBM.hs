{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

{- | Gradient boosting of regression trees (Friedman). Trees are fitted to the
negative gradient of the loss each round and accumulated with a shrinkage
factor; squared error gives regression, logistic deviance gives binary
classification. 'predict' is the additive score; 'gbProbaExpr' /
'gbDecisionExpr' give the classification probability / decision.
-}
module DataFrame.Boosting.GBM (
    module DataFrame.Model,
    GBLoss (..),
    GBConfig (..),
    defaultGBConfig,
    GBModel (..),
    gbExprAtStage,
    gbProbaExpr,
    gbDecisionExpr,
) where

import Control.Exception (throw)
import Data.Either (fromRight)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors (DataFrameException (..))

import DataFrame.DecisionTree.Cart (cartFeatures)
import DataFrame.DecisionTree.Fit (treeToExpr)
import DataFrame.DecisionTree.Regression (RegTreeConfig (..), fitRegTreeOn)
import DataFrame.DecisionTree.Types (Tree)
import DataFrame.Featurize.Internal (targetDoubles)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), getColumns)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Model
import DataFrame.Operators ((.*.), (.+.), (.>.))

-- | The boosting loss.
data GBLoss = SquaredError | LogisticDeviance
    deriving (Eq, Show)

data GBConfig = GBConfig
    { gbLoss :: !GBLoss
    , gbNEstimators :: !Int
    , gbLearningRate :: !Double
    , gbMaxDepth :: !Int
    , gbSeed :: !Int
    }
    deriving (Eq, Show)

defaultGBConfig :: GBConfig
defaultGBConfig =
    GBConfig
        { gbLoss = SquaredError
        , gbNEstimators = 100
        , gbLearningRate = 0.1
        , gbMaxDepth = 3
        , gbSeed = 0
        }

{- | A fitted gradient-boosting model. 'gbInit' is the constant initial score
(mean, or log-odds for classification); 'gbTrees' are the staged regression
trees.
-}
data GBModel = GBModel
    { gbInit :: !Double
    , gbTrees :: !(V.Vector (Tree Double))
    , gbRate :: !Double
    , gbModelLoss :: !GBLoss
    , gbTrainScore :: !(VU.Vector Double)
    , gbFeatureUsage :: !(M.Map T.Text Int)
    }
    deriving (Show)

instance Fit GBConfig (Expr Double) where
    type ModelOf GBConfig (Expr Double) = GBModel
    fit = fitGBM

instance Predict GBModel where
    type Prediction GBModel = Expr Double
    predict = gbExpr

-- | Fit a gradient-boosting ensemble predicting @target@ from the other columns.
fitGBM :: GBConfig -> Expr Double -> DataFrame -> GBModel
fitGBM cfg target@(Col name) df =
    GBModel
        f0
        (V.fromList (reverse trees))
        lr
        (gbLoss cfg)
        (VU.fromList (reverse scores))
        usage
  where
    feats = V.fromList (cartFeatures name df)
    y = targetDoubles target df
    n = VU.length y
    lr = gbLearningRate cfg
    rtCfg =
        RegTreeConfig
            { rtMaxDepth = gbMaxDepth cfg
            , rtMinSamplesSplit = 2
            , rtMinLeafSize = 1
            , rtMinImpurityDecrease = 0.0
            }
    f0 = case gbLoss cfg of
        SquaredError -> VU.sum y / fromIntegral (max 1 n)
        LogisticDeviance ->
            let p = clamp01 (VU.sum y / fromIntegral (max 1 n))
             in log (p / (1 - p))
    (trees, scores, usage) = boost 0 (VU.replicate n f0) [] [] M.empty
    boost !m fScores ts ss usageAcc
        | m >= gbNEstimators cfg = (ts, ss, usageAcc)
        | otherwise =
            let (target', weights) = newtonStep (gbLoss cfg) y fScores
                tree = fitRegTreeOn rtCfg feats target' weights
                pred = predictTree df tree
                fScores' = VU.zipWith (\f p -> f + lr * p) fScores pred
                score = lossValue (gbLoss cfg) y fScores'
                usage' = foldr (\c -> M.insertWith (+) c 1) usageAcc (treeColumns tree)
             in boost (m + 1) fScores' (tree : ts) (score : ss) usage'
fitGBM _ expr _ =
    throw (NonColumnReferenceException ("fitGBM: " <> T.pack (show expr)))

newtonStep ::
    GBLoss ->
    VU.Vector Double ->
    VU.Vector Double ->
    (VU.Vector Double, Maybe (VU.Vector Double))
newtonStep SquaredError y f = (negGradient SquaredError y f, Nothing)
newtonStep LogisticDeviance y f = (z, Just h)
  where
    p = VU.map sigmoid f
    h = VU.map (\pi' -> max hFloor (pi' * (1 - pi'))) p
    z = VU.zipWith3 (\yi pi' hi -> (yi - pi') / hi) y p h

-- | Floor on the Hessian, so a saturated row cannot produce an unbounded step.
hFloor :: Double
hFloor = 1e-6

negGradient ::
    GBLoss -> VU.Vector Double -> VU.Vector Double -> VU.Vector Double
negGradient SquaredError y f = VU.zipWith (-) y f
negGradient LogisticDeviance y f =
    VU.zipWith (\yi fi -> yi - sigmoid fi) y f

lossValue :: GBLoss -> VU.Vector Double -> VU.Vector Double -> Double
lossValue SquaredError y f =
    VU.sum (VU.zipWith (\yi fi -> (yi - fi) ^ (2 :: Int)) y f)
        / fromIntegral (max 1 (VU.length y))
lossValue LogisticDeviance y f =
    VU.sum
        ( VU.zipWith
            ( \yi fi -> let p = clamp01 (sigmoid fi) in negate (yi * log p + (1 - yi) * log (1 - p))
            )
            y
            f
        )
        / fromIntegral (max 1 (VU.length y))

sigmoid :: Double -> Double
sigmoid z
    | z >= 0 = 1 / (1 + exp (-z))
    | otherwise = let e = exp z in e / (1 + e)

clamp01 :: Double -> Double
clamp01 p = max 1e-12 (min (1 - 1e-12) p)

predictTree :: DataFrame -> Tree Double -> VU.Vector Double
predictTree df t = case interpret @Double df (treeToExpr t) of
    Right (TColumn c) -> fromRight VU.empty (toVector @Double @VU.Vector c)
    Left e -> throw e

treeColumns :: Tree Double -> [T.Text]
treeColumns = getColumns . treeToExpr

-- | The full additive prediction expression: @f0 + lr · Σ treeᵢ@.
gbExpr :: GBModel -> Expr Double
gbExpr m = stageExpr (V.length (gbTrees m)) m

-- | The prediction expression using only the first @k@ trees (staged predict).
gbExprAtStage :: Int -> GBModel -> Maybe (Expr Double)
gbExprAtStage k m
    | k < 0 || k > V.length (gbTrees m) = Nothing
    | otherwise = Just (stageExpr k m)

stageExpr :: Int -> GBModel -> Expr Double
stageExpr k m =
    foldr ((.+.) . scaled) (F.lit (gbInit m)) (take k (V.toList (gbTrees m)))
  where
    scaled t = F.lit (gbRate m) .*. treeToExpr t

-- | Probability expression for classification: @sigmoid(score)@.
gbProbaExpr :: GBModel -> Expr Double
gbProbaExpr m = F.lit 1 / (F.lit 1 + exp (negate (gbExpr m)))

-- | Decision expression for classification: positive class when score > 0.
gbDecisionExpr :: GBModel -> Expr Bool
gbDecisionExpr m = gbExpr m .>. F.lit 0
