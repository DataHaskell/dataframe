{- | Cross-validation and grid search for hyperparameter tuning. The model
fitters have heterogeneous types, so these helpers are parameterized by a
user-supplied @train -> test -> score@ closure; the search maximizes the mean
cross-validated score (use a negated error metric to minimize). Splitting reuses
the deterministic 'kFolds' from @dataframe-operations@.
-}
module DataFrame.ModelSelection (
    crossValScore,
    crossValidate,
    stratifiedFoldIds,
    outOfFold,
    GridSearchResult (..),
    gridSearch,
) where

import Data.List (maximumBy, sortOn)
import Data.Ord (comparing)
import qualified Data.Set as S
import qualified Data.Vector.Unboxed as VU
import System.Random (mkStdGen, randoms)

import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Metrics (Metric, evaluate)
import DataFrame.Operations.Merge ()
import DataFrame.Operations.Subset (kFolds, rowsAtIndices)

{- | Per-fold scores from k-fold cross-validation. @scoreFn train test@ fits on
the training rows and returns a score on the held-out fold.
-}
crossValScore ::
    Int -> Int -> (DataFrame -> DataFrame -> Double) -> DataFrame -> [Double]
crossValScore folds seed scoreFn df =
    [ scoreFn (combine (others i)) (fs !! i)
    | i <- [0 .. length fs - 1]
    , not (null (others i))
    ]
  where
    fs = kFolds (mkStdGen seed) folds df
    others i = [f | (j, f) <- zip [0 ..] fs, j /= i]
    combine = foldr1 (<>)

{- | scikit-learn @cross_val_score@: fit a model on each training fold and score
its prediction expression against a truth column on the held-out fold.

@fitPredict train@ fits on the training frame and returns the prediction
expression; @truth@ is the target column. Returns the per-fold metric values.

> crossValidate 5 0 rmse (F.col @Double "target")
>   (\tr -> predict (fit defaultLinearConfig (F.col @Double "target") tr)) df
-}
crossValidate ::
    Int ->
    Int ->
    Metric ->
    Expr Double ->
    (DataFrame -> Expr Double) ->
    DataFrame ->
    [Double]
crossValidate folds seed metric truth fitPredict =
    crossValScore folds seed score
  where
    score train = evaluate metric (fitPredict train) truth

{- | A fold number in @[0, k)@ for every row. Each label's rows are shuffled by
the seed and dealt round-robin, so every fold has the same class balance.
-}
stratifiedFoldIds :: Int -> Int -> VU.Vector Double -> VU.Vector Int
stratifiedFoldIds k seed labels = VU.update (VU.replicate n 0) (VU.fromList assigned)
  where
    n = VU.length labels
    shuffled = map snd (sortOn fst (zip (randoms (mkStdGen seed) :: [Double]) [0 .. n - 1]))
    assigned =
        concat
            [ zip [i | i <- shuffled, labels VU.! i == c] (cycle [0 .. k - 1])
            | c <- S.toList (S.fromList (VU.toList labels))
            ]

{- | Out-of-fold scores: row @i@ is scored by @fitScore train test@ fitted on
every fold except row @i@'s.
-}
outOfFold ::
    VU.Vector Int ->
    (DataFrame -> DataFrame -> VU.Vector Double) ->
    DataFrame ->
    VU.Vector Double
outOfFold folds fitScore df =
    VU.update
        (VU.replicate (VU.length folds) 0)
        (VU.concat (map score [0 .. k - 1]))
  where
    k = if VU.null folds then 0 else VU.maximum folds + 1
    score f =
        let test = VU.findIndices (== f) folds
            train = VU.findIndices (/= f) folds
         in VU.zip test (fitScore (rowsAtIndices train df) (rowsAtIndices test df))

-- | The outcome of a grid search: the best config, its score, and all results.
data GridSearchResult c = GridSearchResult
    { gsBest :: !c
    , gsBestScore :: !Double
    , gsAll :: ![(c, Double)]
    }
    deriving (Show)

{- | Search configurations by mean cross-validated score, returning the
maximizer. @scoreFn cfg train test@ fits @cfg@ on @train@ and scores on @test@.
-}
gridSearch ::
    Int ->
    Int ->
    [c] ->
    (c -> DataFrame -> DataFrame -> Double) ->
    DataFrame ->
    GridSearchResult c
gridSearch folds seed configs scoreFn df =
    GridSearchResult bestC bestS scored
  where
    scored = [(c, mean (crossValScore folds seed (scoreFn c) df)) | c <- configs]
    (bestC, bestS) = maximumBy (comparing snd) scored
    mean xs = if null xs then -(1 / 0) else sum xs / fromIntegral (length xs)
