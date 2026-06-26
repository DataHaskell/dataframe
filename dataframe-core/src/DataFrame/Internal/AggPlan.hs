{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | The aggregation fast-path planner and the two-column moment scatter.

'planAgg' inspects a named output expression and, on a recognised shape over a
clean (non-null, unboxed Int/Double) column, returns the 'AggPlan' the caller
runs through the scatter kernel ('DataFrame.Internal.AggKernel'); anything it
does not recognise returns 'Nothing' so the caller keeps the existing
interpreter. Recognition is by the 'AggStrategy' name tag plus the shape of the
inner 'Expr' — no new constructor is needed, and the general @aggregate@ API is
unchanged.

'momentScatter' fuses the additive moment sums of two columns (count, Sx, Sy,
Sxx, Syy, Sxy) into one pass — the sufficient statistics for the Q9 regression
family. It is exposed for callers that want to collapse the six separate folds
into a single pass.
-}
module DataFrame.Internal.AggPlan (
    AggPlan (..),
    planAgg,
    Moments (..),
    momentScatter,
    MomentPlan (..),
    planMoments,
) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad.ST (runST)
import DataFrame.Internal.AggKernel (Reduction (..), scatterColumnToDouble)
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.DataFrame (
    DataFrame (derivingExpressions),
    GroupedDataFrame (..),
    getColumn,
 )
import DataFrame.Internal.Expression (
    AggStrategy (..),
    BinaryOp (binaryCommutative, binaryName),
    Expr (..),
    UExpr (..),
 )
import Type.Reflection (Typeable, typeRep)

{- | The plan 'planAgg' produces for a recognised output expression. The median
plan carries only the column name (the holistic grouped sort lives in the
operations layer, where @vector-algorithms@ is available).
-}
data AggPlan
    = -- | A single scatter reduction over one named column.
      PlanScatter Reduction T.Text
    | -- | @max a - min b@ (Q7): two scatters then a vectorized combine.
      PlanMaxMinusMin T.Text T.Text
    | -- | Holistic median over one named column.
      PlanMedian T.Text

{- | Inspect a named output expression. On a recognised shape over a present
clean column return @Just plan@; otherwise 'Nothing'. Nullable (bitmap) or
non-Int/Double value columns are rejected here so the scatter only ever sees a
clean unboxed vector.
-}
planAgg :: GroupedDataFrame -> UExpr -> Maybe AggPlan
planAgg gdf (UExpr (expr :: Expr a)) = case expr of
    Agg (FoldAgg tag _ _) (Col name) -> foldPlan tag name
    Agg (MergeAgg tag _ _ _ _) (Col name) -> mergePlan tag name
    Agg (CollectAgg tag _) (Col name) -> collectPlan tag name
    Binary
        op
        (Agg (FoldAgg lt Nothing _) (Col a))
        (Agg (FoldAgg rt Nothing _) (Col b)) ->
            if binaryName op == "sub" && lt == "maximum" && rt == "minimum"
                then requireBoth a b (PlanMaxMinusMin a b)
                else Nothing
    _ -> Nothing
  where
    foldPlan tag name = case tag of
        "sum" -> require name (PlanScatter RSum name)
        "minimum" -> require name (PlanScatter RMin name)
        "maximum" -> require name (PlanScatter RMax name)
        _ -> Nothing
    mergePlan tag name = case tag of
        "mean" -> outputType @Double >> require name (PlanScatter RMean name)
        "count" -> outputType @Int >> require name (PlanScatter RCount name)
        _ -> Nothing
    outputType :: forall t. (Typeable t) => Maybe ()
    outputType = case testEquality (typeRep @a) (typeRep @t) of
        Just Refl -> Just ()
        Nothing -> Nothing
    collectPlan tag name = case tag of
        "stddev" -> require name (PlanScatter RStd name)
        "variance" -> require name (PlanScatter RVar name)
        "top2Sum" -> require name (PlanScatter RTop2Sum name)
        "median" -> require name (PlanMedian name)
        _ -> Nothing
    require name plan = colUnboxedNumeric name >> Just plan
    requireBoth a b plan = colUnboxedNumeric a >> colUnboxedNumeric b >> Just plan
    colUnboxedNumeric name = case getColumn name (fullDataframe gdf) of
        Just c | isUnboxedNumeric c -> Just ()
        _ -> Nothing

-- | The matcher only fires on non-null unboxed Int/Double columns.
isUnboxedNumeric :: Column -> Bool
isUnboxedNumeric = \case
    UnboxedColumn Nothing (_ :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> True
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> True
                Nothing -> False
    _ -> False

{- | A recognised moment (Q9 regression) aggregate group: six output columns
that together form the sufficient statistics of two base columns @x@ and @y@.
The caller runs the fused 'momentScatter'/'momentScatterPar' once over
@(colX, colY)@ and binds each output name to the named field of the result.
-}
data MomentPlan = MomentPlan
    { mpColX :: T.Text
    , mpColY :: T.Text
    , mpNName :: T.Text
    , mpSxName :: T.Text
    , mpSyName :: T.Text
    , mpSxxName :: T.Text
    , mpSyyName :: T.Text
    , mpSxyName :: T.Text
    }

{- | The shape of a sum's argument once unary coercions are peeled and derived
columns are resolved through @derivingExpressions@: either linear in one base
column or the product of two base columns (sorted).
-}
data Term
    = Lin T.Text
    | Prod T.Text T.Text
    deriving (Eq, Ord, Show)

{- | Recognise the multi-aggregate moment shape across a whole @aggregate@ list:
exactly @count(_)@, @sum(x)@, @sum(y)@, @sum(x*x)@, @sum(y*y)@, @sum(x*y)@ over
two distinct base columns @x@ and @y@ (after resolving derived product columns
through @derivingExpressions@). Returns 'Nothing' on any other set so the caller
falls back to the per-expression planner. Both base columns must be clean
unboxed Int/Double, the same gate the single-column scatter uses.
-}
planMoments :: GroupedDataFrame -> [(T.Text, UExpr)] -> Maybe MomentPlan
planMoments gdf aggs
    | length aggs /= 6 = Nothing
    | otherwise = do
        let exprs = derivingExpressions (fullDataframe gdf)
        roles <- traverse (classify exprs) aggs
        let names = M.fromList [(r, nm) | (nm, r) <- roles]
        nName <- M.lookup RoleN names
        (x, y) <- pickBaseColumns roles
        sxName <- M.lookup (RoleLin x) names
        syName <- M.lookup (RoleLin y) names
        sxxName <- M.lookup (RoleProd x x) names
        syyName <- M.lookup (RoleProd y y) names
        sxyName <- M.lookup (RoleProd x y) names
        _ <- if x /= y then Just () else Nothing
        _ <- colUnboxedNumeric x
        _ <- colUnboxedNumeric y
        pure
            MomentPlan
                { mpColX = x
                , mpColY = y
                , mpNName = nName
                , mpSxName = sxName
                , mpSyName = syName
                , mpSxxName = sxxName
                , mpSyyName = syyName
                , mpSxyName = sxyName
                }
  where
    colUnboxedNumeric name = case getColumn name (fullDataframe gdf) of
        Just c | isUnboxedNumeric c -> Just ()
        _ -> Nothing

-- | The output role each named aggregation plays in the moment shape.
data Role
    = RoleN
    | RoleLin T.Text
    | RoleProd T.Text T.Text
    deriving (Eq, Ord, Show)

-- | Tag a single named aggregation with its moment role, or reject the group.
classify :: M.Map T.Text UExpr -> (T.Text, UExpr) -> Maybe (T.Text, Role)
classify exprs (name, UExpr expr) = case expr of
    Agg (MergeAgg "count" _ _ _ _) _ -> Just (name, RoleN)
    Agg (FoldAgg "sum" _ _) arg -> (\t -> (name, termRole t)) <$> resolveTerm exprs (UExpr arg)
    _ -> Nothing

termRole :: Term -> Role
termRole (Lin a) = RoleLin a
termRole (Prod a b) = RoleProd a b

{- | Resolve a (sum-argument) expression to its 'Term'. Peels @toDouble@-style
unary coercions, follows a derived column to its stored expression, and
recognises a commutative product of two linear terms.
-}
resolveTerm :: M.Map T.Text UExpr -> UExpr -> Maybe Term
resolveTerm exprs = go (8 :: Int)
  where
    go 0 _ = Nothing
    go fuel (UExpr e) = case e of
        Col nm -> case M.lookup nm exprs of
            Just ue -> go (fuel - 1) ue
            Nothing -> Just (Lin nm)
        Unary _ inner -> go (fuel - 1) (UExpr inner)
        Binary op l r
            | binaryName op == "mult" && binaryCommutative op -> do
                Lin a <- go (fuel - 1) (UExpr l)
                Lin b <- go (fuel - 1) (UExpr r)
                Just (sortProd a b)
        _ -> Nothing

-- | Products are unordered: store the pair sorted so @x*y@ and @y*x@ unify.
sortProd :: T.Text -> T.Text -> Term
sortProd a b
    | a <= b = Prod a b
    | otherwise = Prod b a

{- | From the classified roles, find the unordered pair of base columns that the
linear sums name. There must be exactly two distinct linear-sum columns.
-}
pickBaseColumns :: [(T.Text, Role)] -> Maybe (T.Text, T.Text)
pickBaseColumns roles =
    case lins of
        [a, b] | a /= b -> Just (a, b)
        _ -> Nothing
  where
    lins = M.keys (M.fromList [(c, ()) | (_, RoleLin c) <- roles])

{- | The additive moment sums of two columns, each an @nGroups@-length column:
@(n, Sx, Sy, Sxx, Syy, Sxy)@.
-}
data Moments = Moments
    { mN :: Column
    , mSx :: Column
    , mSy :: Column
    , mSxx :: Column
    , mSyy :: Column
    , mSxy :: Column
    }

{- | One pass over two Double-coercible columns @x@ and @y@ filling the count
and the five sums. Collapses the Q9 regression family's six independent folds
(and three derive passes) into a single fused pass. Returns 'Nothing' unless
both columns are non-null unboxed Int/Double.
-}
momentScatter :: VU.Vector Int -> Int -> Column -> Column -> Maybe Moments
momentScatter g nGroups colX colY = do
    xs <- scatterColumnToDouble colX
    ys <- scatterColumnToDouble colY
    let (cnt, sx, sy, sxx, syy, sxy) = momentPass g nGroups xs ys
    pure
        Moments
            { mN = fromUnboxedVector cnt
            , mSx = fromUnboxedVector sx
            , mSy = fromUnboxedVector sy
            , mSxx = fromUnboxedVector sxx
            , mSyy = fromUnboxedVector syy
            , mSxy = fromUnboxedVector sxy
            }

momentPass ::
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    ( VU.Vector Int
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    )
momentPass g nGroups xs ys = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    sx <- VUM.replicate nGroups (0 :: Double)
    sy <- VUM.replicate nGroups (0 :: Double)
    sxx <- VUM.replicate nGroups (0 :: Double)
    syy <- VUM.replicate nGroups (0 :: Double)
    sxy <- VUM.replicate nGroups (0 :: Double)
    let n = VU.length xs
        bump arr k d = VUM.unsafeRead arr k >>= \c -> VUM.unsafeWrite arr k (c + d)
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                    !x = VU.unsafeIndex xs i
                    !y = VU.unsafeIndex ys i
                VUM.unsafeRead cnt k >>= \c -> VUM.unsafeWrite cnt k (c + 1)
                bump sx k x
                bump sy k y
                bump sxx k (x * x)
                bump syy k (y * y)
                bump sxy k (x * y)
                go (i + 1)
    go 0
    (,,,,,)
        <$> VU.unsafeFreeze cnt
        <*> VU.unsafeFreeze sx
        <*> VU.unsafeFreeze sy
        <*> VU.unsafeFreeze sxx
        <*> VU.unsafeFreeze syy
        <*> VU.unsafeFreeze sxy
