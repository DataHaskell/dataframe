{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

{- | Gradient boosting of regression trees (Friedman). Trees are fitted to the
negative gradient of the loss each round and accumulated with a shrinkage
factor; squared error gives regression, logistic deviance gives binary
classification. Split search runs on per-feature histograms (see
'gbMaxBins'), so a tree costs O(rows) rather than a sweep per node.
'predict' is the additive score; 'gbProbaExpr' /
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
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors (DataFrameException (..))

import Data.Maybe (fromMaybe)
import DataFrame.DecisionTree.Cart (cartFeaturesByColumn)
import DataFrame.DecisionTree.Fit (treeToExpr)
import DataFrame.DecisionTree.Histogram (
    TreeLimits (..),
    binFeatures,
    fitBinnedTreeOn,
 )
import DataFrame.DecisionTree.Types (Tree)
import DataFrame.Expression.Operators ((.*.), (.+.), (.>.))
import DataFrame.Featurize.Internal (targetDoubles)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), getColumns)
import DataFrame.Model
import System.Random (StdGen, mkStdGen, randoms)

-- | The boosting loss.
data GBLoss = SquaredError | LogisticDeviance
    deriving (Eq, Show)

data GBConfig = GBConfig
    { gbLoss :: !GBLoss
    , gbNEstimators :: !Int
    , gbLearningRate :: !Double
    , gbMaxDepth :: !Int
    , gbMaxBins :: !Int
    {- ^ Bins per feature for split finding. Features with at most this many
    distinct values split exactly as on the raw values.
    -}
    , gbSeed :: !Int
    , gbL2 :: !Double
    -- ^ L2 penalty on leaf values (XGBoost's @lambda@).
    , gbMinChildWeight :: !Double
    -- ^ Smallest Hessian sum a leaf may have (XGBoost's @min_child_weight@).
    , gbFeatureGroups :: ![[T.Text]]
    {- ^ Tree @m@ splits only on columns of group @m mod (number of groups)@.
    Empty: every tree may use every column.
    -}
    , gbBaseScore :: !(Maybe (Expr Double))
    -- ^ Boost from these per-row scores instead of a constant; the prediction adds them back.
    , gbSubsample :: !Double
    -- ^ Fraction of rows each tree is fitted on (1 = all); drawn per tree from 'gbSeed'.
    , gbColsample :: !Double
    -- ^ Fraction of the allowed features each tree may split on (1 = all); drawn per tree from 'gbSeed'.
    }
    deriving (Show)

defaultGBConfig :: GBConfig
defaultGBConfig =
    GBConfig
        { gbLoss = SquaredError
        , gbNEstimators = 100
        , gbLearningRate = 0.1
        , gbMaxDepth = 3
        , gbMaxBins = 1024
        , gbSeed = 0
        , gbL2 = 0
        , gbMinChildWeight = 0
        , gbFeatureGroups = []
        , gbBaseScore = Nothing
        , gbSubsample = 1
        , gbColsample = 1
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
    , gbBase :: !(Maybe (Expr Double))
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
        (gbBaseScore cfg)
  where
    byColumn = cartFeaturesByColumn name df
    binned = binFeatures (gbMaxBins cfg) (V.fromList (map snd byColumn))
    allFeatures = VU.enumFromN 0 (length byColumn)
    groupFeatures =
        [ VU.fromList [j | (j, (c, _)) <- zip [0 ..] byColumn, c `elem` g]
        | g <- gbFeatureGroups cfg
        ]
    featuresFor m =
        let allowed = case groupFeatures of
                [] -> allFeatures
                gs -> gs !! (m `mod` length gs)
         in if gbColsample cfg >= 1
                then allowed
                else
                    keepFraction
                        (gbColsample cfg)
                        (mkStdGen (7919 * gbSeed cfg + 2 * m + 1))
                        allowed
    rowMask m
        | gbSubsample cfg >= 1 = Nothing
        | otherwise =
            Just
                ( VU.fromList
                    ( take
                        n
                        ( map
                            (\u -> if u < gbSubsample cfg then 1 else 0)
                            (randoms (mkStdGen (7919 * gbSeed cfg + 2 * m)) :: [Double])
                        )
                    )
                )
    y = targetDoubles target df
    n = VU.length y
    lr = gbLearningRate cfg
    limits =
        TreeLimits
            { tlMaxDepth = gbMaxDepth cfg
            , tlMinSamplesSplit = 2
            , tlMinLeafSize = 1
            , tlMinImpurityDecrease = 0.0
            , tlL2 = gbL2 cfg
            , tlMinChildWeight = gbMinChildWeight cfg
            }
    baseScores = fmap (`targetDoubles` df) (gbBaseScore cfg)
    f0 = case (baseScores, gbLoss cfg) of
        (Just _, _) -> 0
        (Nothing, SquaredError) -> VU.sum y / fromIntegral (max 1 n)
        (Nothing, LogisticDeviance) ->
            let p = clamp01 (VU.sum y / fromIntegral (max 1 n))
             in log (p / (1 - p))
    (trees, scores, usage) = boost 0 (fromMaybe (VU.replicate n f0) baseScores) [] [] M.empty
    boost !m fScores ts ss usageAcc
        | m >= gbNEstimators cfg = (ts, ss, usageAcc)
        | otherwise =
            let (target', weights0) = newtonStep (gbLoss cfg) y fScores
                weights = case rowMask m of
                    Nothing -> weights0
                    Just mask -> Just (VU.zipWith (*) mask (fromMaybe (VU.replicate n 1) weights0))
                (tree, pred) = fitBinnedTreeOn (featuresFor m) limits binned target' weights
                fScores' = VU.zipWith (\f p -> f + lr * p) fScores pred
                !score = lossValue (gbLoss cfg) y fScores'
                !usage' = foldr (\c -> M.insertWith (+) c 1) usageAcc (treeColumns tree)
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
    foldr ((.+.) . scaled) start (take k (V.toList (gbTrees m)))
  where
    start = maybe (F.lit (gbInit m)) (\b -> b .+. F.lit (gbInit m)) (gbBase m)
    scaled t = F.lit (gbRate m) .*. treeToExpr t

-- | Probability expression for classification: @sigmoid(score)@.
gbProbaExpr :: GBModel -> Expr Double
gbProbaExpr m = F.lit 1 / (F.lit 1 + exp (negate (gbExpr m)))

-- | Decision expression for classification: positive class when score > 0.
gbDecisionExpr :: GBModel -> Expr Bool
gbDecisionExpr m = gbExpr m .>. F.lit 0

-- | A uniformly chosen subset of the given size fraction (at least one element), in order.
keepFraction :: Double -> StdGen -> VU.Vector Int -> VU.Vector Int
keepFraction frac g xs =
    let k = max 1 (round (frac * fromIntegral (VU.length xs)))
        keyed = zip (take (VU.length xs) (randoms g :: [Double])) (VU.toList xs)
        chosen = map snd (take k (M.toAscList (M.fromList keyed)))
     in VU.fromList (M.keys (M.fromList [(x, ()) | x <- chosen]))
