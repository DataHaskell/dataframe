{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.DecisionTree.Cart (
    CartFeature (..),
    NullSide (..),
    cfPred,
    splitMidpoint,
    CartNode (..),
    sortIndicesByValue,
    buildCartTree,
    cartFeatures,
    cartFeaturesByColumn,
    cartTargetLabels,
) where

import DataFrame.DecisionTree.Types (Tree (..), TreeConfig (..))
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import DataFrame.Expression.Operators
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    getColumn,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Operations.Core (nRows)

import Control.Exception (throw)
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
import Type.Reflection (TypeRep, typeRep)

{- | A one-hot feature column: per-row Double values plus the sklearn LEFT
predicate (@x <= threshold@) over the ORIGINAL DataFrame.
-}
data CartFeature = CartFeature
    { cfValues :: !(VU.Vector Double)
    -- ^ One value per row. Null rows hold @+Infinity@, so they sort last.
    , cfNulls :: !(Maybe (VU.Vector Bool))
    -- ^ Which rows are null; 'Nothing' if the training data has no nulls here.
    , cfSplit :: !(Double -> NullSide -> Expr Bool)
    -- ^ The expression @value <= threshold@, sending null rows to the given side.
    }

data NullSide = NullsLeft | NullsRight
    deriving (Eq, Show)

{- | The split expression with null rows sent right. For learners that ignore
'cfNulls': their @+Infinity@ values also fall right of any 'splitMidpoint'.
-}
cfPred :: CartFeature -> Double -> Expr Bool
cfPred f t = cfSplit f t NullsRight

{- | The threshold between two adjacent sorted values. If the upper one is
@+Infinity@ (a null row) it returns the lower one, keeping nulls on the right.
-}
splitMidpoint :: Double -> Double -> Double
splitMidpoint lo hi
    | isInfinite hi = lo
    | otherwise = (lo + hi) / 2

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

{- | Read the target column at the type the tree is being fitted at. Names the
column and both types on failure: a bare @fromIntegral@ defaults to 'Integer'
and lands here, and the old message said only that something went wrong.
-}
cartLabels :: forall a. (Columnable a) => DataFrame -> T.Text -> V.Vector a
cartLabels df target = case interpret @a df (Col target) of
    Right (TColumn column) -> fromRight (throw err) (toVector @a column)
    Left e -> throw e
  where
    err =
        TypeMismatchException
            ( MkTypeErrorContext
                (Right (typeRep @a))
                ( Left (maybe "missing" columnTypeString (getColumn target df)) ::
                    Either String (TypeRep a)
                )
                (Just (T.unpack target))
                (Just "buildCartTree")
            )

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
            (splitMidpoint (swPrev s) v)
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
cartFeatures target df = map snd (cartFeaturesByColumn target df)

-- | 'cartFeatures', each paired with the column it was built from.
cartFeaturesByColumn :: T.Text -> DataFrame -> [(T.Text, CartFeature)]
cartFeaturesByColumn target df =
    [(c, f) | c <- filter (/= target) (columnNames df), f <- featuresOfColumn df c]

featuresOfColumn :: DataFrame -> T.Text -> [CartFeature]
featuresOfColumn df c = case unsafeGetColumn c df of
    UnboxedColumn bm (v :: VU.Vector b) -> numericFeature @b c bm v
    BoxedColumn bm (v :: V.Vector b) -> oneHotFeatures @b c bm v
    pt@(PackedText _ _) -> case materializePacked pt of
        BoxedColumn bm (v :: V.Vector b) -> oneHotFeatures @b c bm v
        _ -> []
    mc@(MergedColumn _ _) -> case materializeMerged mc of
        BoxedColumn bm (v :: V.Vector b) -> oneHotFeatures @b c bm v
        _ -> []

nullFlags :: Int -> Bitmap -> VU.Vector Bool
nullFlags n bm = VU.generate n (not . bitmapTestBit bm)

{- | 'Nothing' if no row is null, so learners split a null-free column the plain
way. Its split expression still handles nulls that appear at prediction time.
-}
trainingNulls :: VU.Vector Bool -> Maybe (VU.Vector Bool)
trainingNulls nulls
    | VU.or nulls = Just nulls
    | otherwise = Nothing

numericFeature ::
    forall b.
    (Columnable b, VU.Unbox b) =>
    T.Text -> Maybe Bitmap -> VU.Vector b -> [CartFeature]
numericFeature c Nothing v = case testEquality (typeRep @b) (typeRep @Double) of
    Just Refl -> [CartFeature v Nothing (\t _ -> F.col @Double c .<=. F.lit t)]
    Nothing -> case sIntegral @b of
        STrue ->
            [ CartFeature
                (VU.map fromIntegral v)
                Nothing
                (\t _ -> F.toDouble (F.col @b c) .<=. F.lit t)
            ]
        SFalse -> []
numericFeature c (Just bm) v = case nullableLeq @b c of
    Just (toD, leq) ->
        [ CartFeature
            (VU.imap (\i x -> if nulls VU.! i then 1 / 0 else toD x) v)
            (trainingNulls nulls)
            (\t side -> F.fromMaybe (side == NullsLeft) (leq t))
        ]
    Nothing -> []
  where
    nulls = nullFlags (VU.length v) bm

nullableLeq ::
    forall b.
    (Columnable b) =>
    T.Text -> Maybe (b -> Double, Double -> Expr (Maybe Bool))
nullableLeq c
    | Just Refl <- testEquality (typeRep @b) (typeRep @Double) =
        Just (id, \t -> F.col @(Maybe Double) c .<= F.lit t)
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int) =
        Just (fromIntegral, \t -> F.col @(Maybe Int) c .<= F.lit t)
    | otherwise = Nothing

oneHotFeatures ::
    forall b.
    (Columnable b) => T.Text -> Maybe Bitmap -> V.Vector b -> [CartFeature]
oneHotFeatures c bm v = case testEquality (typeRep @b) (typeRep @T.Text) of
    Just Refl -> [oneHot c nulls v cat | cat <- Set.toList (Set.fromList present)]
    Nothing -> []
  where
    nulls = fmap (nullFlags (V.length v)) bm
    present = [x | (i, x) <- zip [0 ..] (V.toList v), maybe True (not . (VU.! i)) nulls]

oneHot ::
    T.Text -> Maybe (VU.Vector Bool) -> V.Vector T.Text -> T.Text -> CartFeature
oneHot c Nothing v cat =
    CartFeature
        (VU.generate (V.length v) (\i -> if v V.! i == cat then 1 else 0))
        Nothing
        (\_ _ -> F.col @T.Text c ./=. F.lit cat)
oneHot c (Just nulls) v cat =
    CartFeature
        (VU.generate (V.length v) value)
        (trainingNulls nulls)
        cond
  where
    value i
        | nulls VU.! i = 1 / 0
        | v V.! i == cat = 1
        | otherwise = 0
    col' = F.col @(Maybe T.Text) c
    -- The feature is 1 for this category, 0 for others. A threshold below 1
    -- means "not this category"; a threshold of 1 lets every non-null row
    -- pass, so the split is null vs non-null.
    cond t side
        | t < 1 = F.fromMaybe (side == NullsLeft) (col' ./= F.lit cat)
        | side == NullsLeft = F.lit True
        | otherwise = F.isJust col'

-- | Target column as string labels (matches pandas @y.astype(str)@).
cartTargetLabels :: T.Text -> DataFrame -> V.Vector T.Text
cartTargetLabels target df = case unsafeGetColumn target df of
    BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @T.Text) of
        Just Refl -> v
        Nothing -> V.map (T.pack . show) v
    UnboxedColumn _ (v :: VU.Vector b) -> V.map (T.pack . show) (V.convert v)
    pt@(PackedText _ _) -> case materializePacked pt of
        BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @T.Text) of
            Just Refl -> v
            Nothing -> V.map (T.pack . show) v
        _ -> V.empty
    mc@(MergedColumn _ _) -> case materializeMerged mc of
        BoxedColumn _ (v :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @T.Text) of
            Just Refl -> v
            Nothing -> V.map (T.pack . show) v
        _ -> V.empty
