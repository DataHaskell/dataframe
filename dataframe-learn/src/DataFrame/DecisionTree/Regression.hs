{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Variance-reduction (weighted-SSE) regression trees, reusing the CART
feature machinery. Leaves predict the (weighted) mean of their rows. The
matrix-level 'fitRegTreeOn' lets gradient boosting refit on residuals without
re-extracting features each round.
-}
module DataFrame.DecisionTree.Regression (
    RegTreeConfig (..),
    defaultRegTreeConfig,
    fitRegTreeOn,
) where

import Control.Parallel (par, pseq)
import Data.Maybe (maybeToList)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.DecisionTree.Cart (CartFeature (..), sortIndicesByValue)
import DataFrame.DecisionTree.Types (Tree (..))

-- | Stopping criteria for the regression tree.
data RegTreeConfig = RegTreeConfig
    { rtMaxDepth :: !Int
    , rtMinSamplesSplit :: !Int
    , rtMinLeafSize :: !Int
    , rtMinImpurityDecrease :: !Double
    }
    deriving (Eq, Show)

defaultRegTreeConfig :: RegTreeConfig
defaultRegTreeConfig =
    RegTreeConfig
        { rtMaxDepth = 3
        , rtMinSamplesSplit = 2
        , rtMinLeafSize = 1
        , rtMinImpurityDecrease = 0.0
        }

{- | Fit on pre-extracted features, a target vector, and optional per-row
weights (length @n@). Used by gradient boosting on residual targets.
-}
fitRegTreeOn ::
    RegTreeConfig ->
    V.Vector CartFeature ->
    VU.Vector Double ->
    Maybe (VU.Vector Double) ->
    Tree Double
fitRegTreeOn cfg feats y mw = buildNode 0 (VU.enumFromN 0 n) featSorted
  where
    n = VU.length y
    weightAt i = maybe 1 (VU.! i) mw
    -- Sort every feature's rows once; each node inherits its subset by an
    -- order-preserving filter, so no node re-sorts (cf. CART's 'buildCartNode').
    featSorted = V.map (sortIndicesByValue . cfValues) feats

    buildNode depth idxs sortedByFeat
        | depth >= rtMaxDepth cfg || VU.length idxs < rtMinSamplesSplit cfg = leaf
        | otherwise =
            maybe leaf (splitNode depth idxs sortedByFeat) (bestSplit idxs sortedByFeat)
      where
        leaf = Leaf (weightedMean idxs)

    -- A degenerate (all-to-one-side) partition collapses back to a leaf.
    splitNode depth idxs sortedByFeat (fj, thr)
        | VU.null lefts || VU.null rights = Leaf (weightedMean idxs)
        | otherwise =
            forceTree l `par` (forceTree r `pseq` Branch (cfPred (feats V.! fj) thr) l r)
      where
        vals = cfValues (feats V.! fj)
        goesLeft i = vals VU.! i <= thr
        lefts = VU.filter goesLeft idxs
        rights = VU.filter (not . goesLeft) idxs
        l = buildNode (depth + 1) lefts (V.map (VU.filter goesLeft) sortedByFeat)
        r =
            buildNode (depth + 1) rights (V.map (VU.filter (not . goesLeft)) sortedByFeat)

    weightedMean idxs =
        let (w, sy) = VU.foldl' step (0, 0) idxs
            step (!a, !b) i = (a + weightAt i, b + weightAt i * (y VU.! i))
         in if w == 0 then 0 else sy / w

    bestSplit idxs sortedByFeat
        | null candidates = Nothing
        | red > 0 && red >= rtMinImpurityDecrease cfg = Just (fj, thr)
        | otherwise = Nothing
      where
        (totW, totSY, totSY2) = moments idxs
        nodeSSE = sse totSY totSY2 totW
        candidates =
            [ (red', fj', thr')
            | fj' <- [0 .. V.length feats - 1]
            , (thr', red') <-
                bestThreshold fj' (sortedByFeat V.! fj') totW totSY totSY2 nodeSSE
            ]
        (red, fj, thr) = maximumByFst candidates

    -- Prefix-scan the sorted rows, carrying the left side's weight and Σy / Σy².
    bestThreshold fj sorted totW totSY totSY2 nodeSSE = maybeToList (go 0 0 0 0 Nothing)
      where
        vals = cfValues (feats V.! fj)
        m = VU.length sorted
        go !k !wl !syl !syl2 best
            | k >= m - 1 = best
            | otherwise = go (k + 1) wl' syl' syl2' best'
          where
            i = sorted VU.! k
            next = sorted VU.! (k + 1)
            wi = weightAt i
            yi = y VU.! i
            wl' = wl + wi
            syl' = syl + wi * yi
            syl2' = syl2 + wi * yi * yi
            wr = totW - wl'
            leafSizesOk = k + 1 >= rtMinLeafSize cfg && m - (k + 1) >= rtMinLeafSize cfg
            splittable = vals VU.! i /= vals VU.! next && leafSizesOk && wl' > 0 && wr > 0
            reduction = nodeSSE - (sse syl' syl2' wl' + sse (totSY - syl') (totSY2 - syl2') wr)
            best'
                | splittable && maybe True ((reduction >) . snd) best =
                    Just ((vals VU.! i + vals VU.! next) / 2, reduction)
                | otherwise = best

    moments = VU.foldl' step (0, 0, 0)
      where
        step (!w, !sy, !sy2) i =
            let wi = weightAt i; yi = y VU.! i
             in (w + wi, sy + wi * yi, sy2 + wi * yi * yi)

safeDiv :: Double -> Double -> Double
safeDiv a b = if b == 0 then 0 else a / b

-- | Weighted SSE of a node from its Σy, Σy², and total weight: @Σy² − (Σy)²/w@.
sse :: Double -> Double -> Double -> Double
sse sumY sumSq w = sumSq - safeDiv (sumY * sumY) w

{- | Force a subtree to WHNF throughout so the spark scoring the sibling has
substantial work to evaluate; pure and value-preserving (cf. 'Tao').
-}
forceTree :: Tree Double -> ()
forceTree (Leaf v) = v `seq` ()
forceTree (Branch _ l r) = forceTree l `seq` forceTree r

maximumByFst :: (Ord a) => [(a, b, c)] -> (a, b, c)
maximumByFst = foldr1 (\x@(a, _, _) y@(b, _, _) -> if a >= b then x else y)
