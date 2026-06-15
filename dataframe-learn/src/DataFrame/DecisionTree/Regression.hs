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

import Data.Function (on)
import Data.Maybe (maybeToList)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Unboxed as VU

import DataFrame.DecisionTree.Cart (CartFeature (..))
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
fitRegTreeOn cfg feats y mw = go 0 (VU.enumFromN 0 n)
  where
    n = VU.length y
    wt i = maybe 1 (VU.! i) mw
    go depth idxs
        | VU.length idxs < rtMinSamplesSplit cfg
            || depth >= rtMaxDepth cfg =
            Leaf (nodeMean idxs)
        | otherwise = case bestSplit idxs of
            Nothing -> Leaf (nodeMean idxs)
            Just (fj, thr) ->
                let vals = cfValues (feats V.! fj)
                    (lefts, rights) = VU.partition (\i -> vals VU.! i <= thr) idxs
                 in if VU.null lefts || VU.null rights
                        then Leaf (nodeMean idxs)
                        else
                            Branch
                                (cfPred (feats V.! fj) thr)
                                (go (depth + 1) lefts)
                                (go (depth + 1) rights)
    nodeMean idxs =
        let (sw, sy) = VU.foldl' (\(!a, !b) i -> (a + wt i, b + wt i * (y VU.! i))) (0, 0) idxs
         in if sw == 0 then 0 else sy / sw
    bestSplit idxs =
        let (totW, totSY, totSY2) = moments idxs
            nodeSSE = totSY2 - safeDiv (totSY * totSY) totW
            candidates =
                [ (red, fj, thr)
                | fj <- [0 .. V.length feats - 1]
                , (thr, red) <- featureSplits idxs fj totW totSY totSY2 nodeSSE
                ]
         in case candidates of
                [] -> Nothing
                _ ->
                    let (red, fj, thr) = maximumByFst candidates
                     in if red >= rtMinImpurityDecrease cfg && red > 0
                            then Just (fj, thr)
                            else Nothing
    featureSplits idxs fj totW totSY totSY2 nodeSSE =
        let vals = cfValues (feats V.! fj)
            sorted = sortByVal vals idxs
         in sweep sorted vals totW totSY totSY2 nodeSSE
    sweep sorted vals totW totSY totSY2 nodeSSE = go0 0 0 0 0 Nothing
      where
        m = VU.length sorted
        go0 !k !wl !syl !syl2 best
            | k >= m - 1 = maybeToList best
            | otherwise =
                let i = sorted VU.! k
                    wi = wt i
                    yi = y VU.! i
                    wl' = wl + wi
                    syl' = syl + wi * yi
                    syl2' = syl2 + wi * yi * yi
                    vCur = vals VU.! i
                    vNext = vals VU.! (sorted VU.! (k + 1))
                    nl = k + 1
                    nr = m - nl
                    wr = totW - wl'
                    valid =
                        vCur /= vNext
                            && nl >= rtMinLeafSize cfg
                            && nr >= rtMinLeafSize cfg
                            && wl' > 0
                            && wr > 0
                    red =
                        nodeSSE
                            - ( (syl2' - safeDiv (syl' * syl') wl')
                                    + ( (totSY2 - syl2')
                                            - safeDiv ((totSY - syl') * (totSY - syl')) wr
                                      )
                              )
                    thr = (vCur + vNext) / 2
                    best' =
                        if valid && maybe True (\(_, b) -> red > b) best
                            then Just (thr, red)
                            else best
                 in go0 (k + 1) wl' syl' syl2' best'
    moments =
        VU.foldl'
            ( \(!w, !sy, !sy2) i ->
                let wi = wt i; yi = y VU.! i
                 in (w + wi, sy + wi * yi, sy2 + wi * yi * yi)
            )
            (0, 0, 0)

safeDiv :: Double -> Double -> Double
safeDiv a b = if b == 0 then 0 else a / b

sortByVal :: VU.Vector Double -> VU.Vector Int -> VU.Vector Int
sortByVal vals = VU.modify (VA.sortBy (compare `on` (vals VU.!)))

maximumByFst :: (Ord a) => [(a, b, c)] -> (a, b, c)
maximumByFst = foldr1 (\x@(a, _, _) y@(b, _, _) -> if a >= b then x else y)
