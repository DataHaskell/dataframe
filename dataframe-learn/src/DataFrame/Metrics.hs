{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Evaluation metrics for fitted models. The everyday entry point is
'evaluate', which applies a model's prediction expression and a truth column to
a frame and folds a metric — no manual @interpret@/extract plumbing. Metrics are
plain functions (@type Metric = Vector -> Vector -> Double@), so you pass @mse@
or @accuracy@ directly. Classification metrics handle multiclass via 'Average';
'classificationReport' / 'regressionReport' bundle the common numbers with a
scikit-learn-style 'Show'.
-}
module DataFrame.Metrics (
    -- * Metric type + evaluation
    Metric,
    evaluate,
    predictColumn,
    columnOf,

    -- * Regression metrics
    mse,
    rmse,
    mae,
    r2,

    -- * Classification metrics
    accuracy,
    logLoss,
    Average (..),
    precision,
    recall,
    f1,
    rocAuc,

    -- * Per-class helpers (for reports)
    classCounts,
    precOf,
    recOf,
    f1Of,
) where

import Control.Exception (throw)
import Data.Either (fromRight)
import Data.List (nub, sort, sortBy)
import Data.Ord (comparing)
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Operations.Transformations (derive)

-- | A metric maps predictions and ground truth to a scalar score.
type Metric = VU.Vector Double -> VU.Vector Double -> Double

{- | Evaluate a model's prediction expression against a truth column on a frame.

> evaluate rmse (linearExpr model) (F.col @Double "target") df
> evaluate accuracy (logisticDecisionExpr model) (F.col @Double "label") df
-}
evaluate :: Metric -> Expr Double -> Expr Double -> DataFrame -> Double
evaluate metric predExpr truthExpr df =
    metric (columnOf df predExpr) (columnOf df truthExpr)

-- | Add a model's prediction expression to a frame as a named column.
predictColumn :: T.Text -> Expr Double -> DataFrame -> DataFrame
predictColumn = derive

-- | Interpret an expression to a @Double@ vector over a frame.
columnOf :: DataFrame -> Expr Double -> VU.Vector Double
columnOf df e = case interpret @Double df e of
    Right (TColumn c) -> fromRight VU.empty (toVector @Double @VU.Vector c)
    Left err -> throw err

{- | Compared pairs: 'VU.zipWith' truncates to the shorter vector, so every
mean below divides by this, never by the length of 'truth' alone.
-}
nCompared :: VU.Vector Double -> VU.Vector Double -> Double
nCompared preds truth = fromIntegral (min (VU.length preds) (VU.length truth))

-- | Mean squared error.
mse :: Metric
mse preds truth
    | VU.null truth = 0
    -- No predictions is not a perfect score.
    | n == 0 = 0 / 0
    | otherwise =
        VU.sum (VU.zipWith (\p t -> (p - t) ^ (2 :: Int)) preds truth) / n
  where
    n = nCompared preds truth

-- | Root mean squared error.
rmse :: Metric
rmse preds truth = sqrt (mse preds truth)

-- | Mean absolute error.
mae :: Metric
mae preds truth
    | VU.null truth = 0
    | n == 0 = 0 / 0
    | otherwise = VU.sum (VU.zipWith (\p t -> abs (p - t)) preds truth) / n
  where
    n = nCompared preds truth

-- | Coefficient of determination @R²@.
r2 :: Metric
r2 preds truth
    | VU.null truth = 0
    | n == 0 = 0 / 0
    | ssTot == 0 = 0
    | otherwise = 1 - ssRes / ssTot
  where
    n = nCompared preds truth
    truth' = VU.take (min (VU.length preds) (VU.length truth)) truth
    mean = VU.sum truth' / n
    ssRes = VU.sum (VU.zipWith (\p t -> (t - p) ^ (2 :: Int)) preds truth')
    ssTot = VU.sum (VU.map (\t -> (t - mean) ^ (2 :: Int)) truth')

-- | Fraction of exact matches.
accuracy :: Metric
accuracy preds truth
    | VU.null truth = 0
    | n == 0 = 0 / 0
    | otherwise =
        fromIntegral (VU.length (VU.filter id (VU.zipWith (==) preds truth))) / n
  where
    n = nCompared preds truth

-- | Binary log loss; probabilities clamped away from @0@/@1@.
logLoss :: Metric
logLoss probs truth
    | VU.null truth = 0
    | n == 0 = 0 / 0
    | otherwise =
        negate
            ( VU.sum
                ( VU.zipWith
                    (\p y -> let q = clampP p in y * log q + (1 - y) * log (1 - q))
                    probs
                    truth
                )
            )
            / n
  where
    n = nCompared probs truth
    clampP p = max 1e-15 (min (1 - 1e-15) p)

-- | Averaging strategy for multiclass precision/recall/F1.
data Average
    = -- | one class is positive; the rest negative
      Binary Double
    | -- | unweighted mean over classes
      Macro
    | -- | pool per-class counts (equals accuracy for single-label)
      Micro
    | -- | support-weighted mean over classes
      Weighted
    deriving (Eq, Show)

-- | Per-class @(tp, fp, fn, support)@ over the class set of @truth ∪ preds@.
classCounts ::
    VU.Vector Double -> VU.Vector Double -> [(Double, (Int, Int, Int, Int))]
classCounts preds truth =
    [(c, countsFor c) | c <- classes]
  where
    classes = sort (nub (VU.toList truth ++ VU.toList preds))
    countsFor c =
        VU.foldl'
            ( \(tp, fp, fn, sup) (p, y) ->
                ( if p == c && y == c then tp + 1 else tp
                , if p == c && y /= c then fp + 1 else fp
                , if p /= c && y == c then fn + 1 else fn
                , if y == c then sup + 1 else sup
                )
            )
            (0, 0, 0, 0)
            (VU.zip preds truth)

safeDiv :: Int -> Int -> Double
safeDiv a b = if b == 0 then 0 else fromIntegral a / fromIntegral b

precOf, recOf :: (Int, Int, Int, Int) -> Double
precOf (tp, fp, _, _) = safeDiv tp (tp + fp)
recOf (tp, _, fn, _) = safeDiv tp (tp + fn)

f1Of :: (Int, Int, Int, Int) -> Double
f1Of cs =
    let p = precOf cs; r = recOf cs in if p + r == 0 then 0 else 2 * p * r / (p + r)

averaged ::
    ((Int, Int, Int, Int) -> Double) ->
    Average ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double
averaged stat avg preds truth =
    case avg of
        Binary pos -> maybe 0 stat (lookup pos cc)
        Macro -> meanOf [stat c | (_, c) <- cc]
        Weighted ->
            let total = sum [sup | (_, (_, _, _, sup)) <- cc]
             in if total == 0
                    then 0
                    else
                        sum [fromIntegral sup * stat c | (_, c@(_, _, _, sup)) <- cc]
                            / fromIntegral total
        Micro ->
            let tp = sum [t | (_, (t, _, _, _)) <- cc]
                fp = sum [x | (_, (_, x, _, _)) <- cc]
                fn = sum [x | (_, (_, _, x, _)) <- cc]
             in stat (tp, fp, fn, 0)
  where
    cc = classCounts preds truth
    meanOf xs = if null xs then 0 else sum xs / fromIntegral (length xs)

-- | Precision with the given averaging.
precision :: Average -> VU.Vector Double -> VU.Vector Double -> Double
precision = averaged precOf

-- | Recall with the given averaging.
recall :: Average -> VU.Vector Double -> VU.Vector Double -> Double
recall = averaged recOf

-- | F1 with the given averaging.
f1 :: Average -> VU.Vector Double -> VU.Vector Double -> Double
f1 = averaged f1Of

{- | Binary ROC-AUC (Mann–Whitney). @scores@ are predicted probabilities, @truth@
is @0@/@1@.
-}
rocAuc :: VU.Vector Double -> VU.Vector Double -> Double
rocAuc scores truth
    | nPos == 0 || nNeg == 0 = 0.5
    | otherwise = (rankSum - nPos * (nPos + 1) / 2) / (nPos * nNeg)
  where
    ranked = rankAverages (VU.toList scores)
    pairs = zip (VU.toList truth) ranked
    rankSum = sum [r | (y, r) <- pairs, y == 1]
    nPos = fromIntegral (length (filter (== 1) (VU.toList truth)))
    nNeg = fromIntegral (VU.length truth) - nPos

-- | Average ranks (ties share the mean rank), returned in input order.
rankAverages :: [Double] -> [Double]
rankAverages xs =
    let indexed = zip [0 :: Int ..] xs
        sorted = sortBy (comparing snd) indexed
        ranked = assignRanks 1 sorted
     in map snd (sortBy (comparing fst) ranked)
  where
    assignRanks _ [] = []
    assignRanks start grp =
        let v = snd (head grp)
            (tied, rest) = span ((== v) . snd) grp
            k = length tied
            avgRank = fromIntegral (sum [start .. start + k - 1]) / fromIntegral k
         in [(i, avgRank) | (i, _) <- tied] ++ assignRanks (start + k) rest
