{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Cached condition truth vectors and the per-fit cache keyed by structural
form. A condition's truth over a fixed DataFrame is invariant for a whole
fit, so it is materialized once and reused.
-}
module DataFrame.DecisionTree.CondVec (
    CondVec (..),
    materializeCondVec,
    CondCache,
    condCacheKey,
    condCacheFromVecs,
    addTreeCondsToCache,
    lookupCondVec,
    partitionByVec,
    countErrorsByVec,
    consolidateThreshold,
    combineAndVec,
    combineOrVec,
) where

import DataFrame.DecisionTree.Types (CarePoint (..), Direction (..), Tree (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (
    BinaryOp (binaryName),
    Expr (..),
    eqExpr,
    normalize,
 )
import DataFrame.Internal.Interpreter (interpret)

import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

-- | A boolean condition paired with its truth vector over the full DataFrame.
data CondVec = CondVec
    { cvExpr :: !(Expr Bool)
    , cvVec :: !(VU.Vector Bool)
    }

{- | Interpret a condition once over the DataFrame; 'Nothing' on a
type/interpret failure so the candidate is silently dropped.
-}
materializeCondVec :: DataFrame -> Expr Bool -> Maybe CondVec
materializeCondVec df cond = case interpret @Bool df cond of
    Left _ -> Nothing
    Right (TColumn column) -> CondVec cond <$> eitherToMaybe (toVector @Bool @VU.Vector column)

eitherToMaybe :: Either e a -> Maybe a
eitherToMaybe = either (const Nothing) Just

{- | Full-DataFrame truth vectors keyed by structural form, read-only once
built. Seeded for free from the candidate pool plus the initial tree so the
predict/loss passes index a vector instead of re-interpreting per node.
-}
type CondCache = M.Map T.Text (VU.Vector Bool)

{- | Structural key matching the candidate-dedup key, so a tree branch whose
condition came from the pool hits the cache (equal keys ⟹ equal vector).
-}
condCacheKey :: Expr Bool -> T.Text
condCacheKey = T.pack . show . normalize

-- | Seed a cache from already-materialized candidate 'CondVec's (no interpret).
condCacheFromVecs :: [CondVec] -> CondCache
condCacheFromVecs cvs = M.fromList [(condCacheKey (cvExpr cv), cvVec cv) | cv <- cvs]

{- | Add a tree's branch-condition vectors to a cache (one interpret per
distinct, not-yet-cached condition).
-}
addTreeCondsToCache :: DataFrame -> Tree a -> CondCache -> CondCache
addTreeCondsToCache df = go
  where
    go (Leaf _) c = c
    go (Branch cond l r) c = go r (go l (insertCond df cond c))

insertCond :: DataFrame -> Expr Bool -> CondCache -> CondCache
insertCond df cond c
    | M.member k c = c
    | otherwise =
        maybe c (\cv -> M.insert k (cvVec cv) c) (materializeCondVec df cond)
  where
    k = condCacheKey cond

{- | A condition's truth vector: a cache hit, else interpret over the
DataFrame. 'Nothing' mirrors the interpret-failure fallback (route left).
-}
lookupCondVec :: CondCache -> DataFrame -> Expr Bool -> Maybe (VU.Vector Bool)
lookupCondVec cache df cond = case M.lookup (condCacheKey cond) cache of
    hit@(Just _) -> hit
    Nothing -> cvVec <$> materializeCondVec df cond

-- | Partition row indices by a truth vector: @True@ → left, @False@ → right.
partitionByVec :: VU.Vector Bool -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionByVec boolVals = V.partition (boolVals VU.!)

-- | Count care points the truth vector routes to the wrong child.
countErrorsByVec :: VU.Vector Bool -> [CarePoint] -> Int
countErrorsByVec boolVals = length . filter misrouted
  where
    misrouted cp = (boolVals VU.! cpIndex cp) /= (cpCorrectDir cp == GoLeft)

{- | A same-column same-direction Double threshold comparison, with a rebuild
function to swap in a new threshold.
-}
data ThreshCmp = ThreshCmp
    { tcCol :: !T.Text
    , tcName :: !T.Text
    , tcThr :: !Double
    , tcRebuild :: Double -> Expr Bool
    }

asDoubleThreshold :: Expr Bool -> Maybe ThreshCmp
asDoubleThreshold (Binary op (Col c :: Expr cc) (Lit (t :: tt))) =
    case ( testEquality (typeRep @cc) (typeRep @Double)
         , testEquality (typeRep @tt) (typeRep @Double)
         ) of
        (Just Refl, Just Refl) -> Just (ThreshCmp c (binaryName op) t (Binary op (Col c) . Lit))
        _ -> Nothing
asDoubleThreshold _ = Nothing

directionalNames :: [T.Text]
directionalNames = ["lt", "leq", "gt", "geq"]

{- | Tighter (AND) or looser (OR) of two same-direction thresholds: @<@/@<=@
are left-half-spaces (AND = min), @>@/@>=@ are right-half-spaces (AND = max).
-}
chooseThreshold :: Bool -> T.Text -> Double -> Double -> Double
chooseThreshold isAnd name t1 t2
    | leftDir = if isAnd then min t1 t2 else max t1 t2
    | otherwise = if isAnd then max t1 t2 else min t1 t2
  where
    leftDir = name == "lt" || name == "leq"

{- | Collapse two same-column same-direction strict-Double comparisons into one
comparison (the @True@ argument selects AND, @False@ OR); 'Nothing' otherwise.
-}
consolidateThreshold :: Bool -> Expr Bool -> Expr Bool -> Maybe (Expr Bool)
consolidateThreshold isAnd ea eb = do
    a <- asDoubleThreshold ea
    b <- asDoubleThreshold eb
    if tcCol a == tcCol b && tcName a == tcName b && tcName a `elem` directionalNames
        then Just (tcRebuild a (chooseThreshold isAnd (tcName a) (tcThr a) (tcThr b)))
        else Nothing

{- | AND-combine two cached conditions: idempotence and threshold consolidation
first, else the generic @F.and@; the vector is always the elementwise AND.
-}
combineAndVec :: CondVec -> CondVec -> CondVec
combineAndVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | otherwise = CondVec expr (VU.zipWith (&&) (cvVec a) (cvVec b))
  where
    expr =
        fromMaybe
            (F.and (cvExpr a) (cvExpr b))
            (consolidateThreshold True (cvExpr a) (cvExpr b))

{- | OR-combine two cached conditions (see 'combineAndVec'; AND/OR direction
differs in 'consolidateThreshold').
-}
combineOrVec :: CondVec -> CondVec -> CondVec
combineOrVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | otherwise = CondVec expr (VU.zipWith (||) (cvVec a) (cvVec b))
  where
    expr =
        fromMaybe
            (F.or (cvExpr a) (cvExpr b))
            (consolidateThreshold False (cvExpr a) (cvExpr b))
