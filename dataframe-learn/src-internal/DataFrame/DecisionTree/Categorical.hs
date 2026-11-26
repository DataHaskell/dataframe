{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Categorical split candidates: Breiman prefixes, subsets and equality.
module DataFrame.DecisionTree.Categorical (
    TargetInfo (..),
    mkTargetInfo,
    distinctValuesUpTo,
    validBoxedValues,
    orEqs,
    subsetSplits,
    subsetLists,
    singletonSplits,
    singletonLists,
    breimanPrefixSplits,
    breimanPrefixLists,
    catValueLists,
    membershipVec,
    crossColumnConds,
    discreteConditions,
    discreteCondVecs,
) where

import DataFrame.DecisionTree.ColumnKind (ColumnKind (..), columnKind, withColumnVector)
import DataFrame.DecisionTree.CondVec (CondVec (..), materializeCondVec)
import DataFrame.DecisionTree.Types (
    ColumnOrdering,
    SynthConfig (..),
    TreeConfig (..),
    withOrdFrom,
 )
import DataFrame.Expression.Operators
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame, columnNames, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)

import Data.Either (fromRight)
import Data.Function (on)
import Data.List (inits, sort, sortBy, subsequences)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, mapMaybe)
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

-- | Valid-slot view of a nullable boxed column (null slots hold crash-thunks).
validBoxedValues :: Bitmap -> V.Vector a -> V.Vector a
validBoxedValues bm = V.ifilter (\i _ -> bitmapTestBit bm i)

-- | Target-column summary driving the categorical generator.
data TargetInfo target = TargetInfo
    { tiIsBinary :: !Bool
    , tiPositiveClass :: !(Maybe target)
    , tiValues :: !(V.Vector target)
    }

-- | Compute TargetInfo once per fit.
mkTargetInfo ::
    forall target.
    (Columnable target, Ord target) =>
    T.Text -> DataFrame -> Maybe (TargetInfo target)
mkTargetInfo target df = case interpret @target df (Col target) of
    Right (TColumn column) ->
        either (const Nothing) (Just . targetInfoFromValues) (toVector @target column)
    _ -> Nothing

targetInfoFromValues :: (Ord target) => V.Vector target -> TargetInfo target
targetInfoFromValues vals = TargetInfo isBinary posClass vals
  where
    distinct = Set.toAscList (Set.fromList (V.toList vals))
    isBinary = length distinct == 2
    posClass = case distinct of
        (p : _) | isBinary -> Just p
        _ -> Nothing

-- | Distinct values up to a cap, else the count so far.
distinctValuesUpTo :: (Ord a) => Int -> V.Vector a -> Either Int [a]
distinctValuesUpTo cap values = go Set.empty 0
  where
    n = V.length values
    go !s !i
        | i >= n = Right (Set.toAscList s)
        | Set.size s > cap = Left (Set.size s)
        | otherwise = go (Set.insert (V.unsafeIndex values i) s) (i + 1)

-- | OR-of-equalities for a value-list, shared by both discrete paths.
orEqs :: (a -> Expr Bool) -> [a] -> Expr Bool
orEqs eqLit = foldr1 (.||.) . map eqLit

subsetSplits :: (a -> Expr Bool) -> [a] -> [Expr Bool]
subsetSplits eqLit = map (orEqs eqLit) . subsetLists

-- | Proper non-empty, non-full subsets of the values.
subsetLists :: [a] -> [[a]]
subsetLists vs = drop 1 (init (subsequences vs))

singletonSplits :: (a -> Expr Bool) -> [a] -> [Expr Bool]
singletonSplits = map

singletonLists :: [a] -> [[a]]
singletonLists = map (: [])

breimanPrefixSplits ::
    (Ord a, Ord target) =>
    target ->
    V.Vector a ->
    V.Vector target ->
    [a] ->
    (a -> Expr Bool) ->
    [Expr Bool]
breimanPrefixSplits pc values targetVals distinctVals eqLit =
    map (orEqs eqLit) (breimanPrefixLists pc values targetVals distinctVals)

-- | Breiman's binary-target split set: contiguous prefixes by positive rate.
breimanPrefixLists ::
    (Ord a, Ord target) => target -> V.Vector a -> V.Vector target -> [a] -> [[a]]
breimanPrefixLists pc values targetVals distinctVals =
    nonTrivialPrefixes (sortByRate (levelCounts pc values targetVals) distinctVals)

levelCounts ::
    (Ord a, Eq target) =>
    target -> V.Vector a -> V.Vector target -> M.Map a (Int, Int)
levelCounts pc values targetVals = V.ifoldl' add M.empty values
  where
    add acc i v = M.insertWith plus v (indicator (V.unsafeIndex targetVals i == pc), 1) acc
    plus (p1, n1) (p2, n2) = (p1 + p2, n1 + n2)
    indicator b = if b then 1 else 0

laplaceRate :: (Ord a) => M.Map a (Int, Int) -> a -> Double
laplaceRate counts v = case M.lookup v counts of
    Nothing -> 0.5
    Just (pos, n) -> (fromIntegral pos + 1) / (fromIntegral n + 2)

sortByRate :: (Ord a) => M.Map a (Int, Int) -> [a] -> [a]
sortByRate counts = sortBy (compare `on` (\v -> (laplaceRate counts v, v)))

nonTrivialPrefixes :: [a] -> [[a]]
nonTrivialPrefixes = drop 1 . init . inits

-- | Value-lists a categorical column contributes, in a fixed order.
catValueLists ::
    (Ord a, Ord target) =>
    Bool -> Maybe target -> V.Vector target -> Int -> V.Vector a -> [[a]]
catValueLists isBinary posClass targetVals subsetCap values
    | V.null values = []
    | isBinary, Just pc <- posClass = binaryLists pc targetVals values
    | otherwise = multiclassLists subsetCap values

binaryLists ::
    (Ord a, Ord target) => target -> V.Vector target -> V.Vector a -> [[a]]
binaryLists pc targetVals values
    | length distinct < 2 = []
    | otherwise = breimanPrefixLists pc values targetVals distinct
  where
    distinct = fromRight (ascDistinct values) (distinctValuesUpTo 64 values)

multiclassLists :: (Ord a) => Int -> V.Vector a -> [[a]]
multiclassLists subsetCap values = case distinctValuesUpTo subsetCap values of
    Right vs | length vs >= 2 -> subsetLists vs
    Right _ -> []
    Left _ -> singletonLists (ascDistinct values)

ascDistinct :: (Ord a) => V.Vector a -> [a]
ascDistinct = Set.toAscList . Set.fromList . V.toList

-- | Truth vector of col in values, read directly from the column.
membershipVec :: (Ord a) => V.Vector a -> [a] -> VU.Vector Bool
membershipVec colVals vs =
    let !s = Set.fromList vs
     in VU.generate (V.length colVals) (\i -> Set.member (colVals `V.unsafeIndex` i) s)

-- | Per-fit categorical generation context.
data CatCtx target = CatCtx
    { ccBinary :: !Bool
    , ccPos :: !(Maybe target)
    , ccTargets :: !(V.Vector target)
    , ccSubsetCap :: !Int
    , ccOrds :: !ColumnOrdering
    }

catCtx :: TargetInfo target -> TreeConfig -> CatCtx target
catCtx ti cfg =
    CatCtx
        (tiIsBinary ti)
        (tiPositiveClass ti)
        (tiValues ti)
        (maxCategoricalSubsetCardinality (synthConfig cfg))
        (columnOrdering cfg)

catValueListsFor :: (Ord a, Ord target) => CatCtx target -> V.Vector a -> [[a]]
catValueListsFor ctx = catValueLists (ccBinary ctx) (ccPos ctx) (ccTargets ctx) (ccSubsetCap ctx)

-- | All equality-based candidate splits from non-numeric columns.
discreteConditions ::
    forall target.
    (Columnable target, Ord target) =>
    TargetInfo target -> TreeConfig -> DataFrame -> [Expr Bool]
discreteConditions targetInfo cfg df =
    concatMap (columnConds (catCtx targetInfo cfg) df) (columnNames df)
        ++ crossColumnConds cfg df

columnConds ::
    (Columnable target, Ord target) =>
    CatCtx target -> DataFrame -> T.Text -> [Expr Bool]
columnConds ctx df colName = withCategory ctx df colName $ \bm (column :: V.Vector a) ->
    maybe
        (nonNullColConds ctx colName column)
        (\b -> nullableColConds ctx colName b column)
        bm

nonNullColConds ::
    forall a target.
    (Columnable a, Ord target) =>
    CatCtx target -> T.Text -> V.Vector a -> [Expr Bool]
nonNullColConds ctx colName column =
    fromMaybe
        []
        ( withOrdFrom @a
            (ccOrds ctx)
            (map (orEqs (eqExprFor @a colName)) (catValueListsFor ctx column))
        )

nullableColConds ::
    forall a target.
    (Columnable a, Ord target) =>
    CatCtx target -> T.Text -> Bitmap -> V.Vector a -> [Expr Bool]
nullableColConds ctx colName bm column
    | V.null valid = []
    | otherwise =
        fromMaybe
            []
            ( withOrdFrom @a
                (ccOrds ctx)
                (map (orEqs (eqJustFor @a colName)) (catValueListsFor ctx valid))
            )
  where
    valid = validBoxedValues bm column

eqExprFor :: forall a. (Columnable a) => T.Text -> a -> Expr Bool
eqExprFor colName v = Col @a colName .==. Lit v

eqJustFor :: forall a. (Columnable a) => T.Text -> a -> Expr Bool
eqJustFor colName v = Col @(Maybe a) colName .==. Lit (Just v)

-- | Cross-column equality/order conditions over pairs of same-typed columns.
crossColumnConds :: TreeConfig -> DataFrame -> [Expr Bool]
crossColumnConds cfg df = concatMap (pairConds (columnOrdering cfg) df) (allowedPairs cfg df)

allowedPairs :: TreeConfig -> DataFrame -> [(T.Text, T.Text)]
allowedPairs cfg df =
    [ (l, r)
    | l <- columnNames df
    , r <- columnNames df
    , l /= r
    , not (isDisallowedPair cfg l r)
    ]

isDisallowedPair :: TreeConfig -> T.Text -> T.Text -> Bool
isDisallowedPair cfg l r =
    any
        (\(l', r') -> sort [l', r'] == sort [l, r])
        (disallowedCombinations (synthConfig cfg))

pairConds :: ColumnOrdering -> DataFrame -> (T.Text, T.Text) -> [Expr Bool]
pairConds ords df (l, r) =
    withColumnVector (unsafeGetColumn l df) $ \bmL (_ :: V.Vector a) ->
        withColumnVector (unsafeGetColumn r df) $ \bmR (_ :: V.Vector b) ->
            case (columnKind @a ords, bmL, bmR) of
                (Discrete, Nothing, Nothing) -> strictPairConds @a @b l r
                (Discrete, Just _, Just _) -> nullablePairConds @a @b ords l r
                _ -> []

strictPairConds ::
    forall a b. (Columnable a, Columnable b) => T.Text -> T.Text -> [Expr Bool]
strictPairConds l r = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> [Col @a l .==. Col @a r]
    Nothing -> []

nullablePairConds ::
    forall a b.
    (Columnable a, Columnable b) =>
    ColumnOrdering -> T.Text -> T.Text -> [Expr Bool]
nullablePairConds ords l r = case testEquality (typeRep @a) (typeRep @b) of
    Nothing -> []
    Just Refl -> nullableEqOrLe @a ords l r

nullableEqOrLe ::
    forall a. (Columnable a) => ColumnOrdering -> T.Text -> T.Text -> [Expr Bool]
nullableEqOrLe ords l r
    | isTextType @a = eqOnly
    | otherwise =
        maybe
            eqOnly
            (++ eqOnly)
            (withOrdFrom @a ords [Col @(Maybe a) l .<=. Col @(Maybe a) r])
  where
    eqOnly = [Col @(Maybe a) l .==. Col @(Maybe a) r]

isTextType :: forall a. (Columnable a) => Bool
isTextType = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> True
    Nothing -> False

-- | discreteConditions materialized with shared per-column reads.
discreteCondVecs ::
    forall target.
    (Columnable target, Ord target) =>
    TargetInfo target -> TreeConfig -> DataFrame -> [CondVec]
discreteCondVecs targetInfo cfg df =
    concatMap (columnCondVecs (catCtx targetInfo cfg) df) (columnNames df)
        ++ mapMaybe (materializeCondVec df) (crossColumnConds cfg df)

columnCondVecs ::
    (Columnable target, Ord target) =>
    CatCtx target -> DataFrame -> T.Text -> [CondVec]
columnCondVecs ctx df colName = withCategory ctx df colName $ \bm (column :: V.Vector a) ->
    maybe
        (nonNullColCondVecs ctx colName column)
        (\b -> mapMaybe (materializeCondVec df) (nullableColConds ctx colName b column))
        bm

{- | Run @k@ on a column's elements when its element type is a category
('columnKind'), whatever its storage; numbers belong to the numeric pool.
-}
withCategory ::
    CatCtx target ->
    DataFrame ->
    T.Text ->
    (forall a. (Columnable a) => Maybe Bitmap -> V.Vector a -> [r]) ->
    [r]
withCategory ctx df colName k = withColumnVector (unsafeGetColumn colName df) $ \bm (column :: V.Vector a) ->
    case columnKind @a (ccOrds ctx) of
        Discrete -> k bm column
        _ -> []

nonNullColCondVecs ::
    forall a target.
    (Columnable a, Ord target) => CatCtx target -> T.Text -> V.Vector a -> [CondVec]
nonNullColCondVecs ctx colName column =
    fromMaybe
        []
        ( withOrdFrom @a
            (ccOrds ctx)
            (map (membershipCondVec colName column) (catValueListsFor ctx column))
        )

membershipCondVec ::
    forall a. (Columnable a, Ord a) => T.Text -> V.Vector a -> [a] -> CondVec
membershipCondVec colName column vs = CondVec (orEqs (eqExprFor @a colName) vs) (membershipVec column vs)
