{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

{- | AdaBoost (SAMME) over short, sample-weighted classification trees. The
weighted-Gini stump fitter here is self-contained (it reuses the CART feature
encoding but not the unweighted CART recursion), so the existing decision-tree
path is untouched. 'predict' is the arg-max of weighted votes.
-}
module DataFrame.Boosting.AdaBoost (
    module DataFrame.Model,
    AdaBoostConfig (..),
    defaultAdaBoostConfig,
    AdaBoostModel (..),
) where

import Data.List (sort)
import Data.Maybe (fromMaybe, maybeToList)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.DecisionTree.Cart (
    CartFeature (..),
    cartFeatures,
    sortIndicesByValue,
 )
import DataFrame.DecisionTree.Fit (treeToExpr)
import DataFrame.DecisionTree.Types (Tree (..))
import DataFrame.Featurize.Internal (argMaxExpr, targetValues)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable, TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Model
import DataFrame.Operators ((.*.), (.+.), (.==.))

data AdaBoostConfig = AdaBoostConfig
    { abNEstimators :: !Int
    , abMaxDepth :: !Int
    }
    deriving (Eq, Show)

defaultAdaBoostConfig :: AdaBoostConfig
defaultAdaBoostConfig = AdaBoostConfig{abNEstimators = 50, abMaxDepth = 1}

-- | A fitted SAMME model: per-stage weights and stumps over the class set.
data AdaBoostModel a = AdaBoostModel
    { abAlphas :: !(VU.Vector Double)
    , abStumps :: !(V.Vector (Tree a))
    , abClasses :: !(V.Vector a)
    }
    deriving (Show)

instance (Columnable a, Ord a) => Fit AdaBoostConfig (Expr a) (AdaBoostModel a) where
    fit = fitAdaBoost

instance (Columnable a, Ord a) => Predict (AdaBoostModel a) a where
    predict = adaBoostExpr

-- | Fit an AdaBoost-SAMME classifier.
fitAdaBoost ::
    (Columnable a, Ord a) =>
    AdaBoostConfig -> Expr a -> DataFrame -> AdaBoostModel a
fitAdaBoost cfg target@(Col name) df =
    AdaBoostModel
        (VU.fromList (reverse alphas))
        (V.fromList (reverse stumps))
        classesV
  where
    feats = V.fromList (cartFeatures name df)
    ys = targetValues target df
    n = V.length ys
    classes = sort (foldr dedup [] (V.toList ys))
    dedup x acc = if x `elem` acc then acc else x : acc
    classesV = V.fromList classes
    kClasses = length classes
    codes = VU.generate n (\i -> classIndex (ys V.! i))
    classIndex v = length (takeWhile (< v) classes)
    (alphas, stumps) = boost 0 (VU.replicate n (1 / fromIntegral (max 1 n))) [] []
    boost !m w as ts
        | m >= abNEstimators cfg = (as, ts)
        | otherwise =
            let stump = fitWeightedTree (abMaxDepth cfg) feats classesV codes kClasses w
                pred = predictCodes df classesV stump
                wrong :: VU.Vector Int
                wrong = VU.generate n (\i -> if pred VU.! i /= codes VU.! i then 1 else 0)
                err = clamp (VU.sum (VU.zipWith (*) w (VU.map fromIntegral wrong)) / VU.sum w)
                alpha = log ((1 - err) / err) + log (fromIntegral (max 1 (kClasses - 1)))
                w' = normalize (VU.zipWith (\wi e -> wi * exp (alpha * fromIntegral e)) w wrong)
             in if err <= 0 || err >= 1 - 1 / fromIntegral kClasses
                    then (alpha : as, stump : ts)
                    else boost (m + 1) w' (alpha : as) (stump : ts)
    clamp e = max 1e-10 (min (1 - 1e-10) e)
    normalize v = let s = VU.sum v in if s == 0 then v else VU.map (/ s) v
fitAdaBoost _ expr _ =
    error ("fitAdaBoost: target must be a column, got " ++ show expr)

predictCodes ::
    forall a.
    (Columnable a, Ord a) =>
    DataFrame -> V.Vector a -> Tree a -> VU.Vector Int
predictCodes df classesV stump =
    VU.fromList (map toCode preds)
  where
    preds :: [a]
    preds = case interpret df (treeToExpr stump) of
        Right (TColumn c) -> either (const []) V.toList (toVector @a @V.Vector c)
        Left e -> error (show e)
    toCode v = fromMaybe 0 (V.findIndex (== v) classesV)

-- | A depth-bounded weighted classification tree (weighted Gini splits).
fitWeightedTree ::
    (Columnable a) =>
    Int ->
    V.Vector CartFeature ->
    V.Vector a ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    Tree a
fitWeightedTree maxDepth feats classesV codes kClasses weights =
    go 0 (VU.enumFromN 0 (VU.length codes))
  where
    go depth idxs
        | depth >= maxDepth || VU.length idxs < 2 || isPure idxs =
            Leaf (classesV V.! majority idxs)
        | otherwise = case bestSplit idxs of
            Nothing -> Leaf (classesV V.! majority idxs)
            Just (fj, thr) ->
                let vals = cfValues (feats V.! fj)
                    (l, r) = VU.partition (\i -> vals VU.! i <= thr) idxs
                 in if VU.null l || VU.null r
                        then Leaf (classesV V.! majority idxs)
                        else
                            Branch
                                (cfPred (feats V.! fj) thr)
                                (go (depth + 1) l)
                                (go (depth + 1) r)
    classWeights idxs =
        VU.accumulate
            (+)
            (VU.replicate kClasses 0)
            (VU.map (\i -> (codes VU.! i, weights VU.! i)) idxs)
    majority idxs = VU.maxIndex (classWeights idxs)
    isPure idxs = VU.length (VU.filter (> 0) (classWeights idxs)) <= 1
    bestSplit idxs =
        let cands =
                [ (score, fj, thr)
                | fj <- [0 .. V.length feats - 1]
                , (thr, score) <- featureSplits idxs fj
                ]
         in case cands of
                [] -> Nothing
                _ -> let (_, fj, thr) = minimum3 cands in Just (fj, thr)
    featureSplits idxs fj =
        let vals = cfValues (feats V.! fj)
            member =
                VU.replicate (VU.length codes) False
                    VU.// [(i, True) | i <- VU.toList idxs]
            sorted = VU.filter (member VU.!) (sortIndicesByValue vals)
         in sweep vals sorted (classWeights idxs)
    sweep vals sorted totW = go0 0 (VU.replicate kClasses 0) Nothing
      where
        m = VU.length sorted
        totWsum = VU.sum totW
        go0 !k leftW best
            | k >= m - 1 = maybeToList best
            | otherwise =
                let i = sorted VU.! k
                    leftW' = leftW VU.// [(codes VU.! i, leftW VU.! (codes VU.! i) + weights VU.! i)]
                    vCur = vals VU.! i
                    vNext = vals VU.! (sorted VU.! (k + 1))
                    wl = VU.sum leftW'
                    wr = totWsum - wl
                    score = wl * gini leftW' + wr * gini (VU.zipWith (-) totW leftW')
                    valid = vCur /= vNext && wl > 0 && wr > 0
                    best' =
                        if valid && maybe True (\(_, s) -> score < s) best
                            then Just ((vCur + vNext) / 2, score)
                            else best
                 in go0 (k + 1) leftW' best'

gini :: VU.Vector Double -> Double
gini cw =
    let total = VU.sum cw
     in if total == 0
            then 0
            else 1 - VU.sum (VU.map (\c -> (c / total) ^ (2 :: Int)) cw)

minimum3 :: (Ord a) => [(a, b, c)] -> (a, b, c)
minimum3 = foldr1 (\x@(a, _, _) y@(b, _, _) -> if a <= b then x else y)

{- | Compile to an arg-max-of-weighted-votes expression over the class set:
@argmax_c Σ_m αₘ·[stumpₘ = c]@.
-}
adaBoostExpr :: (Columnable a, Ord a) => AdaBoostModel a -> Expr a
adaBoostExpr m = argMaxExpr (zip classes scores)
  where
    classes = V.toList (abClasses m)
    stumpExprs = map treeToExpr (V.toList (abStumps m))
    alphas = VU.toList (abAlphas m)
    scores =
        [ foldr (.+.) (F.lit 0) (zipWith (vote c) alphas stumpExprs)
        | c <- classes
        ]
    vote c a se = F.lit a .*. indicator (se .==. F.lit c)
    indicator cond = F.ifThenElse cond (F.lit 1.0) (F.lit 0.0)
