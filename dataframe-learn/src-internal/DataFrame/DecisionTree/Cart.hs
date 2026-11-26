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
    cartFeaturesWith,
    cartFeaturesByColumn,
    cartFeaturesByColumnWith,
    cartTargetLabels,
) where

import qualified Data.List as L
import DataFrame.DecisionTree.ColumnKind (
    AsDouble (..),
    ColumnKind (..),
    columnKind,
    withColumnVector,
 )
import DataFrame.DecisionTree.Types (
    ColumnOrdering,
    Tree (..),
    TreeConfig (..),
    defaultColumnOrdering,
 )
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
import qualified Data.Map.Strict as M
import Data.Maybe (isNothing)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (TypeRep, typeRep)

-- | A one-hot feature column with its sklearn LEFT predicate.
data CartFeature = CartFeature
    { cfValues :: !(VU.Vector Double)
    -- ^ One value per row. Null rows hold @+Infinity@, so they sort last.
    , cfNulls :: !(Maybe (VU.Vector Bool))
    -- ^ Which rows are null; 'Nothing' if the training data has no nulls here.
    , cfSplit :: !(Double -> NullSide -> Expr Bool)
    -- ^ The split expression, sending null rows to the given side.
    }

data NullSide = NullsLeft | NullsRight
    deriving (Eq, Show)

-- | The split expression with null rows sent right.
cfPred :: CartFeature -> Double -> Expr Bool
cfPred f t = cfSplit f t NullsRight

-- | The threshold between two adjacent sorted values, keeping nulls right.
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

-- | Indices 0..n-1 stably sorted by value, via in-place merge sort.
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
    feats = V.fromList (cartFeaturesWith (columnOrdering cfg) target df)
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

-- | Read the target column at the fitted type, naming both types on failure.
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

-- | Minimum weighted-child-Gini split; the first feature wins ties.
bestSplit ::
    CartCtx ->
    V.Vector (VU.Vector Int) ->
    VU.Vector Int ->
    Int ->
    Maybe (Int, Double)
bestSplit ctx sortedByFeat counts n =
    fmap (\(_, j, t) -> (j, t)) (L.foldl' consider Nothing [0 .. ctxNFeats ctx - 1])
  where
    total = VU.toList counts
    consider acc fj = case sweepFeature ctx total (sortedByFeat V.! fj) (ctxFeats ctx V.! fj) n of
        Just (g, thr) | maybe True (\(gB, _, _) -> g < gB) acc -> Just (g, fj, thr)
        _ -> acc

-- | Accumulator while sweeping a feature's sorted rows.
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
        ( L.foldl'
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
cartFeatures = cartFeaturesWith defaultColumnOrdering

-- | 'cartFeatures' using a custom category ordering registry.
cartFeaturesWith :: ColumnOrdering -> T.Text -> DataFrame -> [CartFeature]
cartFeaturesWith ords target df = map snd (cartFeaturesByColumnWith ords target df)

-- | 'cartFeatures', each paired with the column it was built from.
cartFeaturesByColumn :: T.Text -> DataFrame -> [(T.Text, CartFeature)]
cartFeaturesByColumn = cartFeaturesByColumnWith defaultColumnOrdering

-- | 'cartFeaturesByColumn' using a custom category ordering registry.
cartFeaturesByColumnWith ::
    ColumnOrdering -> T.Text -> DataFrame -> [(T.Text, CartFeature)]
cartFeaturesByColumnWith ords target df =
    [(c, f) | c <- filter (/= target) (columnNames df), f <- featuresOfColumn ords df c]

-- | A column's split features, chosen by 'columnKind', never by storage.
featuresOfColumn :: ColumnOrdering -> DataFrame -> T.Text -> [CartFeature]
featuresOfColumn ords df c = case unsafeGetColumn c df of
    -- Read unboxed numbers and categories directly without boxing.
    UnboxedColumn bm (v :: VU.Vector b) -> case columnKind @b ords of
        Ordered num -> [orderedFeature num c bm (VU.length v) (v VU.!)]
        Discrete -> levelFeatures c bm (VU.length v) (v VU.!)
        Unsplittable -> []
    col -> withColumnVector col $ \bm (v :: V.Vector b) ->
        case columnKind @b ords of
            Ordered num -> [orderedFeature num c bm (V.length v) (v V.!)]
            Discrete -> levelFeatures c bm (V.length v) (v V.!)
            Unsplittable -> []

nullFlags :: Int -> Bitmap -> VU.Vector Bool
nullFlags n bm = VU.generate n (not . bitmapTestBit bm)

-- | Nothing if no row is null, so null-free columns split the plain way.
trainingNulls :: VU.Vector Bool -> Maybe (VU.Vector Bool)
trainingNulls nulls
    | VU.or nulls = Just nulls
    | otherwise = Nothing

-- | Threshold feature @x <= t@; reads row @i@ only when non-null.
orderedFeature :: AsDouble b -> T.Text -> Maybe Bitmap -> Int -> (Int -> b) -> CartFeature
orderedFeature num c Nothing n at =
    CartFeature
        (VU.generate n (valueAsDouble num . at))
        Nothing
        (\t _ -> columnAsDouble num c .<=. F.lit t)
orderedFeature num c (Just bm) n at =
    CartFeature
        (VU.generate n (\i -> if nulls VU.! i then 1 / 0 else valueAsDouble num (at i)))
        (trainingNulls nulls)
        (\t side -> F.fromMaybe (side == NullsLeft) (nullableAsDouble num c .<= F.lit t))
  where
    nulls = nullFlags n bm

-- | One-hot features per level (one feature for a non-null binary column).
levelFeatures ::
    forall b.
    (Columnable b, Ord b) =>
    T.Text -> Maybe Bitmap -> Int -> (Int -> b) -> [CartFeature]
levelFeatures c bm n at =
    [levelFeature c nulls trainNulls n at lvl | lvl <- chosen]
  where
    nulls = fmap (nullFlags n) bm
    trainNulls = nulls >>= trainingNulls
    present = [at i | i <- [0 .. n - 1], maybe True (not . (VU.! i)) nulls]
    allLevels = Set.toAscList (Set.fromList present)
    chosen = case allLevels of
        [lvl, _] | isNothing trainNulls -> [lvl]
        _ -> allLevels

-- | One level's feature: 1 on rows equal to it, 0 elsewhere.
levelFeature ::
    forall b.
    (Columnable b) =>
    T.Text ->
    Maybe (VU.Vector Bool) ->
    Maybe (VU.Vector Bool) ->
    Int ->
    (Int -> b) ->
    b ->
    CartFeature
levelFeature c Nothing _ n at lvl =
    CartFeature
        (VU.generate n (\i -> if at i == lvl then 1 else 0))
        Nothing
        (\_ _ -> Col @b c ./=. Lit lvl)
levelFeature c (Just nulls) trainNulls n at lvl =
    CartFeature
        (VU.generate n value)
        trainNulls
        cond
  where
    value i
        | nulls VU.! i = 1 / 0
        | at i == lvl = 1
        | otherwise = 0
    col' = Col @(Maybe b) c
    -- Null is unequal to the level, so NullsRight filters it out.
    notLevel = col' ./=. Lit (Just lvl)
    -- Threshold 1 passes every non-null row, splitting null vs non-null.
    cond t side
        | t < 1 = if side == NullsLeft then notLevel else F.isJust col' .&&. notLevel
        | side == NullsLeft = F.lit True
        | otherwise = F.isJust col'

-- | Target column as string labels (matches pandas @y.astype(str)@).
cartTargetLabels :: T.Text -> DataFrame -> V.Vector T.Text
cartTargetLabels target df =
    withColumnVector (unsafeGetColumn target df) $ \_ (v :: V.Vector b) ->
        case testEquality (typeRep @b) (typeRep @T.Text) of
            Just Refl -> v
            Nothing -> V.map (T.pack . show) v
