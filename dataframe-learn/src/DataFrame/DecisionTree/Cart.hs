{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | sklearn-faithful CART initializer used to seed TAO. One-hot encodes
categoricals, splits on exact (unsmoothed) Gini over midpoint thresholds
(@<=@ routes left), and emits a @Tree@ predicting identically to
@DecisionTreeClassifier(criterion='gini')@ on continuous features.
-}
module DataFrame.DecisionTree.Cart (
    CartFeature (..),
    CartNode (..),
    sortIndicesByValue,
    buildCartTree,
    cartFeatures,
    cartTargetLabels,
) where

import DataFrame.DecisionTree.Types (Tree (..), TreeConfig (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame, columnNames, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Types
import DataFrame.Operations.Core (nRows)
import DataFrame.Operators

import Data.Either (fromRight)
import Data.Function (on)
import Data.List (foldl')
import qualified Data.Map.Strict as M
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

{- | A one-hot feature column: per-row Double values plus the sklearn LEFT
predicate (@x <= threshold@) over the ORIGINAL DataFrame.
-}
data CartFeature = CartFeature
    { cfValues :: !(VU.Vector Double)
    , cfPred :: !(Double -> Expr Bool)
    }

-- | Pre-'Tree' CART node: a leaf class id, or a split on feature @j@.
data CartNode = CLeaf !Int | CSplit !Int !Double !CartNode !CartNode

-- | Immutable per-fit context for the CART recursion.
data CartCtx = CartCtx
    { ctxFeats :: !(V.Vector CartFeature)
    , ctxNFeats :: !Int
    , ctxCodes :: !(VU.Vector Int)
    , ctxNClasses :: !Int
    , ctxMaxDepth :: !Int
    , ctxMinLeaf :: !Int
    }

{- | Indices @0..n-1@ stably sorted by their value (ascending), ties keeping
ascending index. In-place unboxed merge sort — no boxed-list allocation.
-}
sortIndicesByValue :: VU.Vector Double -> VU.Vector Int
sortIndicesByValue vs =
    VU.create $ do
        mv <- VU.thaw (VU.enumFromN 0 (VU.length vs))
        VA.sortBy (compare `on` (vs VU.!)) mv
        pure mv

buildCartTree ::
    forall a. (Columnable a, Ord a) => TreeConfig -> T.Text -> DataFrame -> Tree a
buildCartTree cfg target df =
    cartToTree feats classes (buildCartNode ctx 0 (VU.enumFromN 0 nAll) featSorted)
  where
    nAll = nRows df
    feats = V.fromList (cartFeatures target df)
    featSorted = V.map (sortIndicesByValue . cfValues) feats
    labels = cartLabels @a df target
    classes = cartClasses labels
    ctx =
        CartCtx
            feats
            (V.length feats)
            (classCodes classes labels)
            (V.length classes)
            (maxTreeDepth cfg)
            (max 1 (minLeafSize cfg))

cartLabels :: forall a. (Columnable a) => DataFrame -> T.Text -> V.Vector a
cartLabels df target = case interpret @a df (Col target) of
    Right (TColumn column) -> fromRight err (toVector @a column)
    _ -> err
  where
    err = error "buildCartTree: cannot interpret target column"

cartClasses :: (Ord a) => V.Vector a -> V.Vector a
cartClasses = V.fromList . Set.toList . Set.fromList . V.toList

classCodes :: (Ord a) => V.Vector a -> V.Vector a -> VU.Vector Int
classCodes classes labels = VU.generate (V.length labels) (\i -> M.findWithDefault 0 (labels V.! i) ix)
  where
    ix = M.fromList (zip (V.toList classes) [0 ..])

cartToTree :: V.Vector CartFeature -> V.Vector a -> CartNode -> Tree a
cartToTree feats classes = go
  where
    go (CLeaf cid) = Leaf (classes V.! cid)
    go (CSplit fj thr l r) = Branch (cfPred (feats V.! fj) thr) (go l) (go r)

classCounts :: CartCtx -> VU.Vector Int -> VU.Vector Int
classCounts ctx idxs =
    VU.accumulate
        (+)
        (VU.replicate (ctxNClasses ctx) 0)
        (VU.map (\i -> (ctxCodes ctx VU.! i, 1)) idxs)

isPure :: VU.Vector Int -> Bool
isPure counts = VU.length (VU.filter (> 0) counts) <= 1

buildCartNode ::
    CartCtx -> Int -> VU.Vector Int -> V.Vector (VU.Vector Int) -> CartNode
buildCartNode ctx depth idxs sortedByFeat
    | VU.length idxs < 2 || depth >= ctxMaxDepth ctx || isPure counts = leaf
    | otherwise =
        maybe
            leaf
            (splitNode ctx depth idxs sortedByFeat)
            (bestSplit ctx sortedByFeat counts n)
  where
    n = VU.length idxs
    counts = classCounts ctx idxs
    leaf = CLeaf (VU.maxIndex counts)

splitNode ::
    CartCtx ->
    Int ->
    VU.Vector Int ->
    V.Vector (VU.Vector Int) ->
    (Int, Double) ->
    CartNode
splitNode ctx depth idxs sortedByFeat (fj, thr) =
    CSplit fj thr (rec leftIdx leftSorted) (rec rightIdx rightSorted)
  where
    vals = cfValues (ctxFeats ctx V.! fj)
    leftIdx = VU.filter (\i -> vals VU.! i <= thr) idxs
    rightIdx = VU.filter (\i -> vals VU.! i > thr) idxs
    leftSorted = V.map (VU.filter (\i -> vals VU.! i <= thr)) sortedByFeat
    rightSorted = V.map (VU.filter (\i -> vals VU.! i > thr)) sortedByFeat
    rec = buildCartNode ctx (depth + 1)

{- | Minimum weighted-child-Gini @(feature, threshold)@; the first feature wins
ties; 'Nothing' when no feature has a leaf-size-respecting threshold.
-}
bestSplit ::
    CartCtx ->
    V.Vector (VU.Vector Int) ->
    VU.Vector Int ->
    Int ->
    Maybe (Int, Double)
bestSplit ctx sortedByFeat counts n =
    fmap (\(_, j, t) -> (j, t)) (foldl' consider Nothing [0 .. ctxNFeats ctx - 1])
  where
    total = VU.toList counts
    consider acc fj = case sweepFeature ctx total (sortedByFeat V.! fj) (ctxFeats ctx V.! fj) n of
        Just (g, thr) | maybe True (\(gB, _, _) -> g < gB) acc -> Just (g, fj, thr)
        _ -> acc

{- | Accumulator while sweeping a feature's sorted rows: best @(gini, thr)@ so
far, per-class left counts, rows moved left, and the previous value seen.
-}
data Sweep = Sweep
    { swBest :: !(Maybe (Double, Double))
    , swLeft :: ![Int]
    , swMoved :: !Int
    , swPrev :: !Double
    }

sweepFeature ::
    CartCtx ->
    [Int] ->
    VU.Vector Int ->
    CartFeature ->
    Int ->
    Maybe (Double, Double)
sweepFeature ctx total si feat n =
    swBest
        ( foldl'
            step
            (Sweep Nothing (replicate (ctxNClasses ctx) 0) 0 (0 / 0))
            [0 .. VU.length si - 1]
        )
  where
    vals = cfValues feat
    step s k = advance ctx total n (vals VU.! i) (ctxCodes ctx VU.! i) s
      where
        i = si VU.! k

advance :: CartCtx -> [Int] -> Int -> Double -> Int -> Sweep -> Sweep
advance ctx total n v c s =
    Sweep
        (considerThreshold ctx total n v s)
        (bumpClass c (swLeft s))
        (swMoved s + 1)
        v

considerThreshold ::
    CartCtx -> [Int] -> Int -> Double -> Sweep -> Maybe (Double, Double)
considerThreshold ctx total n v s
    | swMoved s >= ctxMinLeaf ctx
    , n - swMoved s >= ctxMinLeaf ctx
    , v > swPrev s + 1e-7 =
        keepBetter
            (swBest s)
            (weightedGini total (swLeft s) (swMoved s) n)
            ((swPrev s + v) / 2)
    | otherwise = swBest s

keepBetter ::
    Maybe (Double, Double) -> Double -> Double -> Maybe (Double, Double)
keepBetter best g thr = case best of
    Just (wb, _) | wb <= g -> best
    _ -> Just (g, thr)

weightedGini :: [Int] -> [Int] -> Int -> Int -> Double
weightedGini total leftAcc nl n =
    ( fromIntegral nl * giniImpurity leftAcc nl
        + fromIntegral nr * giniImpurity rightAcc nr
    )
        / fromIntegral n
  where
    nr = n - nl
    rightAcc = zipWith (-) total leftAcc

-- | Gini impurity @1 - Σ (c/m)²@ of a class-count list of total @m@.
giniImpurity :: [Int] -> Int -> Double
giniImpurity _ 0 = 0
giniImpurity cs m = 1 - sum [let p = fromIntegral c / fromIntegral m in p * p | c <- cs]

bumpClass :: Int -> [Int] -> [Int]
bumpClass c = zipWith (\j x -> if j == c then x + 1 else x) [0 ..]

-- | One-hot features in @pd.get_dummies(drop_first=False)@ column order.
cartFeatures :: T.Text -> DataFrame -> [CartFeature]
cartFeatures target df = concatMap (featuresOfColumn df) (filter (/= target) (columnNames df))

featuresOfColumn :: DataFrame -> T.Text -> [CartFeature]
featuresOfColumn df c = case unsafeGetColumn c df of
    UnboxedColumn _ (v :: VU.Vector b) -> numericFeature @b c v
    BoxedColumn _ (v :: V.Vector b) -> oneHotFeatures @b (nRows df) c v

numericFeature ::
    forall b. (Columnable b, VU.Unbox b) => T.Text -> VU.Vector b -> [CartFeature]
numericFeature c v = case testEquality (typeRep @b) (typeRep @Double) of
    Just Refl -> [CartFeature v (\t -> F.col @Double c .<=. F.lit t)]
    Nothing -> case sIntegral @b of
        STrue ->
            [ CartFeature (VU.map fromIntegral v) (\t -> F.toDouble (F.col @b c) .<=. F.lit t)
            ]
        SFalse -> []

oneHotFeatures ::
    forall b. (Columnable b) => Int -> T.Text -> V.Vector b -> [CartFeature]
oneHotFeatures nAll c v = case testEquality (typeRep @b) (typeRep @T.Text) of
    Just Refl -> [oneHot nAll c v cat | cat <- Set.toList (Set.fromList (V.toList v))]
    Nothing -> []

oneHot :: Int -> T.Text -> V.Vector T.Text -> T.Text -> CartFeature
oneHot nAll c v cat =
    CartFeature
        (VU.generate nAll (\i -> if v V.! i == cat then 1 else 0))
        (const (F.col @T.Text c ./=. F.lit cat))

-- | Target column as string labels (matches pandas @y.astype(str)@).
cartTargetLabels :: T.Text -> DataFrame -> V.Vector T.Text
cartTargetLabels target df = case unsafeGetColumn target df of
    BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @T.Text) of
        Just Refl -> v
        Nothing -> V.map (T.pack . show) v
    UnboxedColumn _ (v :: VU.Vector b) -> V.map (T.pack . show) (V.convert v)
