{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Prediction, care-point identification, node validity, and tree loss. The
batched, cache-aware variants resolve each branch condition's truth vector
once per call instead of once per row.
-}
module DataFrame.DecisionTree.Predict (
    predictWithTree,
    predictManyWithTree,
    predictManyWithTreeCached,
    identifyCarePoints,
    identifyCarePointsCached,
    countCarePointErrors,
    partitionIndices,
    partitionIndicesCached,
    majorityValueFromIndices,
    computeTreeLoss,
    computeTreeLossCached,
    isValidAtNode,
) where

import DataFrame.DecisionTree.CondVec (
    CondCache,
    countErrorsByVec,
    lookupCondVec,
 )
import DataFrame.DecisionTree.Types (
    CarePoint (..),
    Direction (..),
    Tree (..),
    TreeConfig (..),
 )
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column (Columnable, TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)

import Control.Exception (throw)
import Control.Monad.ST (ST)
import Data.Function (on)
import Data.List (maximumBy)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU

{- | A condition's truth vector over the DataFrame, or 'Nothing' on a
type/interpret failure (callers default such rows to the left child).
-}
branchBool :: DataFrame -> Expr Bool -> Maybe (VU.Vector Bool)
branchBool df cond = case interpret @Bool df cond of
    Right (TColumn column) -> either (const Nothing) Just (toVector @Bool @VU.Vector column)
    _ -> Nothing

-- | The target column as a label vector, or 'Nothing' on failure.
interpretLabelCol ::
    forall a. (Columnable a) => DataFrame -> T.Text -> Maybe (V.Vector a)
interpretLabelCol df target = case interpret @a df (Col target) of
    Right (TColumn column) -> either (const Nothing) Just (toVector @a column)
    _ -> Nothing

-- | Predict the label for a single row by walking a fixed tree (@True@ → left).
predictWithTree ::
    forall a. (Columnable a) => T.Text -> DataFrame -> Int -> Tree a -> a
predictWithTree _ _ _ (Leaf v) = v
predictWithTree target df idx (Branch cond left right) =
    predictWithTree @a target df idx (childFor cond left right idx df)

childFor :: Expr Bool -> Tree a -> Tree a -> Int -> DataFrame -> Tree a
childFor cond left right idx df = case branchBool df cond of
    Nothing -> left
    Just boolVals -> if boolVals VU.! idx then left else right

predictManyWithTree ::
    forall a. (Columnable a) => Tree a -> DataFrame -> V.Vector Int -> V.Vector a
predictManyWithTree = predictManyWithTreeCached @a M.empty

{- | 'predictManyWithTree' resolving each branch condition through a 'CondCache'.
Each condition is read at most once per call rather than once per row.
-}
predictManyWithTreeCached ::
    forall a.
    (Columnable a) => CondCache -> Tree a -> DataFrame -> V.Vector Int -> V.Vector a
predictManyWithTreeCached cache tree df indices = V.create $ do
    mv <- VM.new (V.length indices)
    fill mv (V.zip (V.enumFromN 0 (V.length indices)) indices) tree
    pure mv
  where
    fill :: VM.MVector s a -> V.Vector (Int, Int) -> Tree a -> ST s ()
    fill mv prs (Leaf v) = V.mapM_ (\(p, _) -> VM.write mv p v) prs
    fill mv prs (Branch cond left right) = case lookupCondVec cache df cond of
        Nothing -> fill mv prs left
        Just boolVals -> fillSplit mv (V.partition (\(_, i) -> boolVals VU.! i) prs) left right

    fillSplit ::
        VM.MVector s a ->
        (V.Vector (Int, Int), V.Vector (Int, Int)) ->
        Tree a ->
        Tree a ->
        ST s ()
    fillSplit mv (leftPrs, rightPrs) left right = fill mv leftPrs left >> fill mv rightPrs right

identifyCarePoints ::
    forall a.
    (Columnable a) =>
    T.Text -> DataFrame -> V.Vector Int -> Tree a -> Tree a -> [CarePoint]
identifyCarePoints = identifyCarePointsCached @a M.empty

{- | Rows the parent must route to a specific child for the (fixed) subtrees to
classify correctly; a 'CondCache' avoids re-interpreting subtree conditions.
-}
identifyCarePointsCached ::
    forall a.
    (Columnable a) =>
    CondCache ->
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a ->
    [CarePoint]
identifyCarePointsCached cache target df indices leftTree rightTree =
    maybe [] carePoints (interpretLabelCol @a df target)
  where
    leftPreds = predictManyWithTreeCached cache leftTree df indices
    rightPreds = predictManyWithTreeCached cache rightTree df indices
    carePoints targetVals = V.toList (V.imapMaybe (checkPoint targetVals leftPreds rightPreds) indices)

checkPoint ::
    (Eq a) =>
    V.Vector a -> V.Vector a -> V.Vector a -> Int -> Int -> Maybe CarePoint
checkPoint targetVals leftPreds rightPreds k idx =
    case (leftPreds V.! k == trueLabel, rightPreds V.! k == trueLabel) of
        (True, False) -> Just (CarePoint idx GoLeft)
        (False, True) -> Just (CarePoint idx GoRight)
        _ -> Nothing
  where
    trueLabel = targetVals V.! idx

-- | Care points a free condition misroutes (uncached; for the linear path).
countCarePointErrors :: Expr Bool -> DataFrame -> [CarePoint] -> Int
countCarePointErrors cond df carePoints =
    maybe (length carePoints) (`countErrorsByVec` carePoints) (branchBool df cond)

partitionIndices ::
    Expr Bool -> DataFrame -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionIndices = partitionIndicesCached M.empty

{- | 'partitionIndices' resolving the condition through a 'CondCache'; a miss
routes every index left (matching the uncached fallback).
-}
partitionIndicesCached ::
    CondCache ->
    Expr Bool ->
    DataFrame ->
    V.Vector Int ->
    (V.Vector Int, V.Vector Int)
partitionIndicesCached cache cond df indices = case lookupCondVec cache df cond of
    Nothing -> (indices, V.empty)
    Just boolVals -> V.partition (boolVals VU.!) indices

-- | A split is valid at a node when both children keep at least 'minLeafSize'.
isValidAtNode :: TreeConfig -> DataFrame -> V.Vector Int -> Expr Bool -> Bool
isValidAtNode cfg df indices c =
    V.length t >= minLeafSize cfg && V.length f >= minLeafSize cfg
  where
    (t, f) = partitionIndices c df indices

majorityValueFromIndices ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> V.Vector Int -> a
majorityValueFromIndices target df indices = majorityOf (countLabels (labelColOrThrow @a df target) indices)

labelColOrThrow :: forall a. (Columnable a) => DataFrame -> T.Text -> V.Vector a
labelColOrThrow df target = case interpret @a df (Col target) of
    Left e -> throw e
    Right (TColumn column) -> either throw id (toVector @a column)

countLabels :: (Ord a) => V.Vector a -> V.Vector Int -> M.Map a Int
countLabels vals = V.foldl' (\acc i -> M.insertWith (+) (vals V.! i) (1 :: Int) acc) M.empty

majorityOf :: M.Map a Int -> a
majorityOf counts
    | M.null counts = throw (EmptyDataSetException "majorityValueFromIndices")
    | otherwise = fst (maximumBy (compare `on` snd) (M.toList counts))

computeTreeLoss ::
    forall a.
    (Columnable a) => T.Text -> DataFrame -> V.Vector Int -> Tree a -> Double
computeTreeLoss = computeTreeLossCached @a M.empty

-- | 0/1 loss of a tree over @indices@, with a 'CondCache' for the predictions.
computeTreeLossCached ::
    forall a.
    (Columnable a) =>
    CondCache -> T.Text -> DataFrame -> V.Vector Int -> Tree a -> Double
computeTreeLossCached cache target df indices tree
    | V.null indices = 0
    | otherwise =
        maybe 1.0 (treeLoss cache tree df indices) (interpretLabelCol @a df target)

treeLoss ::
    (Columnable a) =>
    CondCache -> Tree a -> DataFrame -> V.Vector Int -> V.Vector a -> Double
treeLoss cache tree df indices targetVals =
    fromIntegral (countMismatches targetVals indices preds)
        / fromIntegral (V.length indices)
  where
    preds = predictManyWithTreeCached cache tree df indices

countMismatches :: (Eq a) => V.Vector a -> V.Vector Int -> V.Vector a -> Int
countMismatches targetVals indices preds =
    V.length
        (V.ifilter (\k _ -> targetVals V.! (indices V.! k) /= preds V.! k) preds)
