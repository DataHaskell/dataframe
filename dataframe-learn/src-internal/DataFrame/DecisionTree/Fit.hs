{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Top-level fitting: assemble the candidate pool, seed from CART, run TAO,
and convert the result to an expression. Also the probability-tree variant
('fitProbTree') that annotates leaves with class distributions.
-}
module DataFrame.DecisionTree.Fit (
    treeToExpr,
    fitDecisionTree,
    buildTree,
    pruneTree,
    partitionDataFrame,
    calculateGini,
    majorityValue,
    getCounts,
    percentile,
    ProbTree,
    probsFromIndices,
    buildProbTree,
    fitProbTree,
    probExprs,
) where

import DataFrame.DecisionTree.Cart (buildCartTree)
import DataFrame.DecisionTree.Categorical (
    TargetInfo (..),
    discreteCondVecs,
    discreteConditions,
    mkTargetInfo,
 )
import DataFrame.DecisionTree.CondVec (CondVec)
import DataFrame.DecisionTree.Numeric (numericCondVecs, numericConditions)
import DataFrame.DecisionTree.Pool (dedupCVByExpr, nubByExpr)
import DataFrame.DecisionTree.Predict (partitionIndices)
import DataFrame.DecisionTree.Prune (pruneDead, pruneExpr)
import DataFrame.DecisionTree.Tao (taoOptimize, taoOptimizeCV)
import DataFrame.DecisionTree.Types (Tree (..), TreeConfig (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable, TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Operations.Core (nRows)
import DataFrame.Operations.Subset (exclude, filterWhere)

import Control.Exception (throw)
import Data.Function (on)
import Data.List (foldl', maximumBy, nub, sort)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Vector as V

-- | Convert a fitted tree to a nested-conditional expression.
treeToExpr :: (Columnable a) => Tree a -> Expr a
treeToExpr (Leaf v) = Lit v
treeToExpr (Branch cond left right) = F.ifThenElse cond (treeToExpr left) (treeToExpr right)

-- | Fit a TAO decision tree (CART-seeded) and return it as an expression.
fitDecisionTree ::
    forall a. (Columnable a, Ord a) => TreeConfig -> Expr a -> DataFrame -> Expr a
fitDecisionTree cfg (Col target) df =
    pruneExpr
        (treeToExpr (taoOptimizeCV @a cfg target condVecs df indices initialTree))
  where
    condVecs = candidatePool @a cfg target df
    initialTree = buildCartTree @a cfg target df
    indices = V.enumFromN 0 (nRows df)
fitDecisionTree _ expr _ = error ("Cannot create tree for compound expression: " ++ show expr)

-- | The deduplicated numeric + discrete candidate pool for a target column.
candidatePool ::
    forall a.
    (Columnable a, Ord a) => TreeConfig -> T.Text -> DataFrame -> [CondVec]
candidatePool cfg target df = dedupCVByExpr (numericCVs ++ discreteCVs)
  where
    dfNoTarget = exclude [target] df
    numericCVs = numericCondVecs cfg dfNoTarget df
    discreteCVs = discreteCondVecs (targetInfoOrEmpty @a target df) cfg dfNoTarget

targetInfoOrEmpty ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> TargetInfo a
targetInfoOrEmpty target df = fromMaybe (TargetInfo False Nothing V.empty) (mkTargetInfo @a target df)

-- | Fit a tree at a given depth from a raw condition list (CART + TAO + prune).
buildTree ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig -> Int -> T.Text -> [Expr Bool] -> DataFrame -> Expr a
buildTree cfg depth target conds df =
    pruneExpr (treeToExpr (taoOptimize @a cfg target conds df indices tree))
  where
    tree = buildCartTree @a cfg{maxTreeDepth = depth} target df
    indices = V.enumFromN 0 (nRows df)

pruneTree :: forall a. (Columnable a) => Expr a -> Expr a
pruneTree = pruneExpr

partitionDataFrame :: Expr Bool -> DataFrame -> (DataFrame, DataFrame)
partitionDataFrame cond df = (filterWhere cond df, filterWhere (F.not cond) df)

-- | Laplace-smoothed Gini impurity of the target distribution.
calculateGini ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> Double
calculateGini target df
    | n == 0 = 0
    | otherwise = 1 - sum (map (^ (2 :: Int)) probs)
  where
    n = fromIntegral (nRows df)
    counts = getCounts @a target df
    numClasses = fromIntegral (M.size counts)
    probs = map (\c -> (fromIntegral c + 1) / (n + numClasses)) (M.elems counts)

majorityValue :: forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> a
majorityValue target df
    | M.null counts = error "Empty DataFrame in leaf"
    | otherwise = fst (maximumBy (compare `on` snd) (M.toList counts))
  where
    counts = getCounts @a target df

getCounts ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> M.Map a Int
getCounts target df = case interpret @a df (Col target) of
    Left e -> throw e
    Right (TColumn column) -> case toVector @a column of
        Left e -> throw e
        Right vals -> foldl' (\acc x -> M.insertWith (+) x 1 acc) M.empty (V.toList vals)

-- | The @p@-th percentile of an expression's values (@0@ on failure/empty).
percentile :: Int -> Expr Double -> DataFrame -> Double
percentile p expr df = case interpret @Double df expr of
    Right (TColumn column) -> either (const 0) (percentileOfVec p) (toVector @Double column)
    _ -> 0

percentileOfVec :: Int -> V.Vector Double -> Double
percentileOfVec p vals
    | n == 0 = 0
    | otherwise = sorted V.! min (n - 1) (max 0 ((p * n) `div` 100))
  where
    sorted = V.fromList (sort (V.toList vals))
    n = V.length sorted

-- | A tree whose leaves hold class-probability distributions.
type ProbTree a = Tree (M.Map a Double)

-- | Normalised class probabilities over a subset of training rows.
probsFromIndices ::
    forall a.
    (Columnable a, Ord a) => T.Text -> DataFrame -> V.Vector Int -> M.Map a Double
probsFromIndices target df indices = case interpret @a df (Col target) of
    Right (TColumn column) -> either (const M.empty) (normaliseCounts indices) (toVector @a column)
    _ -> M.empty

normaliseCounts :: (Ord a) => V.Vector Int -> V.Vector a -> M.Map a Double
normaliseCounts indices vals = M.map (\c -> fromIntegral c / total) counts
  where
    counts =
        V.foldl'
            (\acc i -> M.insertWith (+) (vals V.! i) (1 :: Int) acc)
            M.empty
            indices
    total = fromIntegral (V.length indices) :: Double

{- | Re-label a fitted tree's leaves with class distributions, routing the
training data through the (unchanged) split conditions.
-}
buildProbTree ::
    forall a.
    (Columnable a, Ord a) =>
    Tree a -> T.Text -> DataFrame -> V.Vector Int -> ProbTree a
buildProbTree (Leaf _) target df indices = Leaf (probsFromIndices @a target df indices)
buildProbTree (Branch cond left right) target df indices =
    Branch
        cond
        (buildProbTree @a left target df l)
        (buildProbTree @a right target df r)
  where
    (l, r) = partitionIndices cond df indices

-- | Fit a TAO tree and return one probability expression per class.
fitProbTree ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig -> Expr a -> DataFrame -> M.Map a (Expr Double)
fitProbTree cfg (Col target) df = probExprs (buildProbTree @a pruned target df indices)
  where
    conds =
        nubByExpr
            ( numericConditions cfg dfNoTarget
                ++ discreteConditions (targetInfoOrEmpty @a target df) cfg dfNoTarget
            )
    dfNoTarget = exclude [target] df
    indices = V.enumFromN 0 (nRows df)
    pruned =
        pruneDead
            (taoOptimize @a cfg target conds df indices (buildCartTree @a cfg target df))
fitProbTree _ expr _ = error ("Cannot create prob tree for compound expression: " ++ show expr)

-- | Convert a 'ProbTree' into one @Expr Double@ per class.
probExprs ::
    forall a. (Columnable a, Ord a) => ProbTree a -> M.Map a (Expr Double)
probExprs tree = M.fromList [(c, classExpr c tree) | c <- nub (allClasses tree)]

allClasses :: ProbTree a -> [a]
allClasses (Leaf m) = M.keys m
allClasses (Branch _ l r) = allClasses l ++ allClasses r

classExpr :: (Ord a) => a -> ProbTree a -> Expr Double
classExpr c (Leaf m) = Lit (M.findWithDefault 0.0 c m)
classExpr c (Branch cond l r) = F.ifThenElse cond (classExpr c l) (classExpr c r)
