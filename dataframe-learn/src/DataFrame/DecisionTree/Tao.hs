{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Tree Alternating Optimization: hold the tree fixed and re-optimize one node
-- at a time, bottom-up, minimizing care-point misroutes. Sibling subtrees at a
-- depth level are independent and optimized in parallel.
module DataFrame.DecisionTree.Tao (
    taoOptimize,
    taoOptimizeCV,
    taoIteration,
    taoIterationCV,
    optimizeNode,
    findBestSplitTAO,
) where

import DataFrame.DecisionTree.CondVec
import DataFrame.DecisionTree.Linear (bestLinearCandidate)
import DataFrame.DecisionTree.Pool (bestDiscreteCandidate, candidateParChunk, evalWithPenaltyVec)
import DataFrame.DecisionTree.Predict
import DataFrame.DecisionTree.Prune (pruneDead)
import DataFrame.DecisionTree.Types
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)

import Control.Parallel (par, pseq)
import Control.Parallel.Strategies (parListChunk, rdeepseq, using)
import Data.Function (on)
import Data.List (foldl', minimumBy)
import Data.Maybe (catMaybes, mapMaybe)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

-- | The constant per-fit context threaded through the node-optimization
-- recursion (the cache is rebuilt each iteration).
data TaoEnv = TaoEnv
    { teCache :: !CondCache
    , teCfg :: !TreeConfig
    , teTarget :: !T.Text
    , teConds :: ![CondVec]
    , teDf :: !DataFrame
    }

-- | Public TAO entry point over raw conditions; materializes each once.
taoOptimize :: forall a. (Columnable a, Ord a) => TreeConfig -> T.Text -> [Expr Bool] -> DataFrame -> V.Vector Int -> Tree a -> Tree a
taoOptimize cfg target conds df =
    taoOptimizeCV @a cfg target (mapMaybe (materializeCondVec df) conds) df

-- | TAO outer loop over pre-evaluated candidates: iterate until the iteration
-- budget or convergence tolerance is reached, then prune dead branches.
taoOptimizeCV :: forall a. (Columnable a, Ord a) => TreeConfig -> T.Text -> [CondVec] -> DataFrame -> V.Vector Int -> Tree a -> Tree a
taoOptimizeCV cfg target condVecs df rootIndices initialTree =
    go 0 initialTree (lossWith baseCache initialTree)
  where
    baseCache = condCacheFromVecs condVecs
    lossWith cache = computeTreeLossCached @a cache target df rootIndices
    go iter tree prevLoss
        | iter >= taoIterations cfg = pruneDead tree
        | prevLoss - newLoss < taoConvergenceTol cfg = pruneDead tree'
        | otherwise = go (iter + 1) tree' newLoss
      where
        cache = addTreeCondsToCache df tree baseCache
        tree' = taoIterationCV @a cache cfg target condVecs df rootIndices tree
        newLoss = lossWith cache tree'

-- | Public single-iteration entry point.
taoIteration :: forall a. (Columnable a, Ord a) => TreeConfig -> T.Text -> [Expr Bool] -> DataFrame -> V.Vector Int -> Tree a -> Tree a
taoIteration cfg target conds df rootIndices tree =
    let condVecs = mapMaybe (materializeCondVec df) conds
        cache = addTreeCondsToCache df tree (condCacheFromVecs condVecs)
     in taoIterationCV @a cache cfg target condVecs df rootIndices tree

-- | One bottom-to-top sweep: re-optimize every node level by level.
taoIterationCV :: forall a. (Columnable a, Ord a) => CondCache -> TreeConfig -> T.Text -> [CondVec] -> DataFrame -> V.Vector Int -> Tree a -> Tree a
taoIterationCV cache cfg target condVecs df rootIndices tree =
    foldl' (optimizeDepthLevel env rootIndices) tree [treeDepth tree, treeDepth tree - 1 .. 0]
  where
    env = TaoEnv cache cfg target condVecs df

optimizeDepthLevel :: forall a. (Columnable a, Ord a) => TaoEnv -> V.Vector Int -> Tree a -> Int -> Tree a
optimizeDepthLevel env rootIndices tree = optimizeAtDepth @a env rootIndices tree 0

optimizeAtDepth :: forall a. (Columnable a, Ord a) => TaoEnv -> V.Vector Int -> Tree a -> Int -> Int -> Tree a
optimizeAtDepth env indices tree currentDepth targetDepth
    | currentDepth == targetDepth = optimizeNode @a env indices tree
    | otherwise = case tree of
        Leaf v -> Leaf v
        Branch cond left right -> optimizeChildren @a env indices cond left right currentDepth targetDepth

-- | Optimize the two subtrees over their disjoint index sets, scoring the left
-- in parallel with the right (the cache is read-only, so this is pure).
optimizeChildren :: forall a. (Columnable a, Ord a) => TaoEnv -> V.Vector Int -> Expr Bool -> Tree a -> Tree a -> Int -> Int -> Tree a
optimizeChildren env indices cond left right currentDepth targetDepth =
    forceTreeWork left' `par` (forceTreeWork right' `pseq` Branch cond left' right')
  where
    (indicesL, indicesR) = partitionIndicesCached (teCache env) cond (teDf env) indices
    left' = optimizeAtDepth @a env indicesL left (currentDepth + 1) targetDepth
    right' = optimizeAtDepth @a env indicesR right (currentDepth + 1) targetDepth

-- | Force a subtree's optimization work to WHNF so the parallel scheduler has
-- something substantial to evaluate; pure and value-preserving.
forceTreeWork :: Tree a -> ()
forceTreeWork (Leaf v) = v `seq` ()
forceTreeWork (Branch c l r) = c `seq` forceTreeWork l `seq` forceTreeWork r

-- | Re-optimize one node: pick its best split, or collapse to a leaf when the
-- node is empty or the chosen split underflows 'minLeafSize'.
optimizeNode :: forall a. (Columnable a, Ord a) => TaoEnv -> V.Vector Int -> Tree a -> Tree a
optimizeNode env indices tree
    | V.null indices = tree
    | otherwise = case tree of
        Leaf _ -> leaf
        Branch oldCond left right -> rebuiltBranch env indices oldCond left right leaf
  where
    leaf = Leaf (majorityValueFromIndices @a (teTarget env) (teDf env) indices)

rebuiltBranch :: forall a. (Columnable a, Ord a) => TaoEnv -> V.Vector Int -> Expr Bool -> Tree a -> Tree a -> Tree a -> Tree a
rebuiltBranch env indices oldCond left right leaf
    | underflows = leaf
    | otherwise = Branch newCond left right
  where
    newCond = findBestSplitTAO @a env indices left right oldCond
    (l, r) = partitionIndicesCached (teCache env) newCond (teDf env) indices
    underflows = V.length l < minLeafSize (teCfg env) || V.length r < minLeafSize (teCfg env)

-- | The lowest-penalty replacement condition for a node, falling back to the
-- current condition when no valid candidate beats it.
findBestSplitTAO :: forall a. (Columnable a) => TaoEnv -> V.Vector Int -> Tree a -> Tree a -> Expr Bool -> Expr Bool
findBestSplitTAO env indices leftTree rightTree currentCond
    | V.null indices || null carePoints = currentCond
    | pureReplacementLinear cfg, Just c <- linearCandidate, isValidAtNode cfg (teDf env) indices c = c
    | otherwise = bestOfPool penaltyCV currentCond pool
  where
    cfg = teCfg env
    carePoints = identifyCarePointsCached @a (teCache env) (teTarget env) (teDf env) indices leftTree rightTree
    penaltyCV = evalWithPenaltyVec cfg carePoints
    linearCandidate = bestLinearCandidate cfg (teDf env) carePoints
    valid = filterValidCandidates cfg indices (teConds env)
    pool = candidatePool env indices currentCond (bestDiscreteCandidate cfg penaltyCV valid) linearCandidate

bestOfPool :: (CondVec -> (Int, Int)) -> Expr Bool -> [CondVec] -> Expr Bool
bestOfPool _ currentCond [] = currentCond
bestOfPool penaltyCV _ pool = cvExpr (minimumBy (compare `on` penaltyCV) pool)

-- | Validity-filtered candidates the node could split on: both children must
-- keep at least 'minLeafSize'. Scored in parallel chunks, order preserved.
filterValidCandidates :: TreeConfig -> V.Vector Int -> [CondVec] -> [CondVec]
filterValidCandidates cfg indices condVecs = map snd (filter fst (zip validity condVecs))
  where
    validity = map (validAtNode cfg indices) condVecs `using` parListChunk candidateParChunk rdeepseq

validAtNode :: TreeConfig -> V.Vector Int -> CondVec -> Bool
validAtNode cfg indices cv = nTrue >= minLeaf && (V.length indices - nTrue) >= minLeaf
  where
    minLeaf = minLeafSize cfg
    nTrue = V.foldl' (\ !acc i -> if cvVec cv VU.! i then acc + 1 else acc) (0 :: Int) indices

-- | The candidate pool to minimize over: the current condition, the best
-- discrete candidate, and the linear candidate, each kept only if valid.
candidatePool :: TaoEnv -> V.Vector Int -> Expr Bool -> Maybe CondVec -> Maybe (Expr Bool) -> [CondVec]
candidatePool env indices currentCond discreteCV linearCandidate =
    filter (isValidAtNode (teCfg env) (teDf env) indices . cvExpr) (catMaybes [currentCV, discreteCV, linearCV])
  where
    currentCV = CondVec currentCond <$> lookupCondVec (teCache env) (teDf env) currentCond
    linearCV = linearCandidate >>= materializeCondVec (teDf env)
