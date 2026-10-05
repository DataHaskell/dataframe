{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Variance-reduction (weighted-SSE) regression trees over the CART feature
machinery; leaves predict the weighted mean of their rows. 'fitRegTree' lets
gradient boosting refit on residuals without re-extracting features.
-}
module DataFrame.DecisionTree.Regression (
    RegTreeConfig (..),
    defaultRegTreeConfig,
    RegFit (..),
    -- | Implementation verb used by the fit\/predict instances and boosting.
    fitRegTree,
) where

import Control.Parallel (par, pseq)
import Data.Maybe (maybeToList)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.DecisionTree.Cart (
    CartFeature (..),
    NullSide (..),
    sortIndicesByValue,
    splitMidpoint,
 )
import DataFrame.DecisionTree.Types (Tree (..))
import DataFrame.Internal.Expression (Expr)

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

data RegFit = RegFit
    { rfTree :: !(Tree Double)
    , rfFitted :: !(VU.Vector Double)
    }

{- | Fit on pre-extracted features, a target vector, and optional per-row
weights (length @n@). Used by gradient boosting on residual targets.
-}
fitRegTree ::
    RegTreeConfig ->
    V.Vector CartFeature ->
    VU.Vector Double ->
    Maybe (VU.Vector Double) ->
    RegFit
fitRegTree cfg feats y mw = RegFit (toTree root) inSample
  where
    root = buildNode 0 (VU.enumFromN 0 n) featSorted
    inSample = VU.update (VU.replicate n 0) (VU.concat (leafRows root))
    leafRows (FLeaf v idxs) = [VU.map (\i -> (i, v)) idxs]
    leafRows (FBranch _ l r) = leafRows l ++ leafRows r
    n = VU.length y
    weightAt i = maybe 1 (VU.! i) mw
    -- For each feature, row indices sorted by value, with null rows left out.
    featSorted = V.map presentSorted feats
    presentSorted f = case cfNulls f of
        Nothing -> sortIndicesByValue (cfValues f)
        Just nulls -> VU.filter (not . (nulls VU.!)) (sortIndicesByValue (cfValues f))

    buildNode depth idxs sortedByFeat
        | depth >= rtMaxDepth cfg || VU.length idxs < rtMinSamplesSplit cfg = leaf
        | otherwise =
            maybe leaf (splitNode depth idxs sortedByFeat) (bestSplit idxs sortedByFeat)
      where
        leaf = FLeaf (weightedMean idxs) idxs

    splitNode depth idxs sortedByFeat (fj, thr, side)
        | VU.null lefts || VU.null rights = FLeaf (weightedMean idxs) idxs
        | otherwise =
            forceTree l `par`
                (forceTree r `pseq` FBranch (cfSplit feat thr side) l r)
      where
        feat = feats V.! fj
        vals = cfValues feat
        goesLeft i = case cfNulls feat of
            Just nulls | nulls VU.! i -> side == NullsLeft
            _ -> vals VU.! i <= thr
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
        | red > 0 && red >= rtMinImpurityDecrease cfg = Just split
        | otherwise = Nothing
      where
        nNode = VU.length idxs
        (totW, totSY, totSY2) = moments idxs
        nodeSSE = sse totSY totSY2 totW
        candidates =
            concat
                [ bestThreshold fj (sortedByFeat V.! fj) nNode totW totSY totSY2 nodeSSE
                | fj <- [0 .. V.length feats - 1]
                ]
        (red, split) = maximumByFst candidates

    {- Best (threshold, null side) for feature @fj@ at this node. The sweep
    covers non-null rows; each threshold is scored twice, with the node's null
    rows added to the left and to the right. -}
    bestThreshold fj sorted nNode totW totSY totSY2 nodeSSE =
        [(r, (fj, thr, dir)) | (thr, dir, r) <- maybeToList (go 0 0 0 0 Nothing)]
      where
        vals = cfValues (feats V.! fj)
        m = VU.length sorted
        nNull = nNode - m
        (pW, pSY, pSY2) = moments sorted
        (nW, nSY, nSY2) = (totW - pW, totSY - pSY, totSY2 - pSY2)
        hasNulls = nNull > 0
        -- Last split position to try. With nulls, putting every non-null row
        -- left is a valid null vs non-null split; without nulls it splits nothing.
        lastK = if hasNulls then m - 1 else m - 2
        score nl wl syl syl2 =
            let wr = totW - wl
                ok =
                    nl >= rtMinLeafSize cfg && nNode - nl >= rtMinLeafSize cfg && wl > 0 && wr > 0
             in if ok
                    then Just (nodeSSE - (sse syl syl2 wl + sse (totSY - syl) (totSY2 - syl2) wr))
                    else Nothing
        go !k !wl !syl !syl2 best
            | k > lastK || k >= m = best
            | otherwise = go (k + 1) wl' syl' syl2' best'
          where
            i = sorted VU.! k
            wi = weightAt i
            yi = y VU.! i
            wl' = wl + wi
            syl' = syl + wi * yi
            syl2' = syl2 + wi * yi * yi
            boundary
                | k + 1 < m = vals VU.! i /= vals VU.! (sorted VU.! (k + 1))
                | otherwise = True
            thr
                | k + 1 < m = splitMidpoint (vals VU.! i) (vals VU.! (sorted VU.! (k + 1)))
                | otherwise = vals VU.! i
            right = (,) NullsRight <$> score (k + 1) wl' syl' syl2'
            left
                | hasNulls =
                    (,) NullsLeft <$> score (k + 1 + nNull) (wl' + nW) (syl' + nSY) (syl2' + nSY2)
                | otherwise = Nothing
            consider b (dir, r)
                | maybe True (\(_, _, rb) -> r > rb) b = Just (thr, dir, r)
                | otherwise = b
            best'
                | boundary = foldl consider best (maybeToList right ++ maybeToList left)
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

data FitNode
    = FLeaf !Double !(VU.Vector Int)
    | FBranch !(Expr Bool) !FitNode !FitNode

toTree :: FitNode -> Tree Double
toTree (FLeaf v _) = Leaf v
toTree (FBranch c l r) = Branch c (toTree l) (toTree r)

{- | Force a subtree to WHNF throughout so the spark scoring the sibling has
substantial work to evaluate; pure and value-preserving (cf. 'Tao').
-}
forceTree :: FitNode -> ()
forceTree (FLeaf v _) = v `seq` ()
forceTree (FBranch _ l r) = forceTree l `seq` forceTree r

maximumByFst :: (Ord a) => [(a, b)] -> (a, b)
maximumByFst = foldr1 (\x@(a, _) y@(b, _) -> if a >= b then x else y)
