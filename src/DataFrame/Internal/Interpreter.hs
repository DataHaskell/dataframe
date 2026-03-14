{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Interpreter (
    -- * New core API
    Value (..),
    Ctx (..),
    eval,
    materialize,

    -- * Backward-compatible API
    interpret,
    interpretAggregation,
    AggregationResult (..),
) where

import Data.Bifunctor (first)
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression
import DataFrame.Internal.Types
import Type.Reflection (Typeable, typeRep)

-------------------------------------------------------------------------------
-- Value: the unified result type
-------------------------------------------------------------------------------

{- | The result of interpreting an expression.  Keeps literals as scalars
until the point where a concrete column is needed, avoiding premature
broadcast allocations.
-}
data Value a where
    -- | A single value, not yet broadcast to any length.
    Scalar :: (Columnable a) => a -> Value a
    {- | A flat column (one element per row in the flat case, or one
    element per group after aggregation).
    -}
    Flat :: (Columnable a) => Column -> Value a
    {- | A grouped column: one 'Column' slice per group.  Only produced
    when interpreting inside a 'GroupCtx'.
    -}
    Group :: (Columnable a) => V.Vector Column -> Value a

-- | The interpretation context.
data Ctx
    = FlatCtx DataFrame
    | GroupCtx GroupedDataFrame

-------------------------------------------------------------------------------
-- Materialisation
-------------------------------------------------------------------------------

{- | Force a 'Value' into a flat 'Column' of the given length.  Scalars
are broadcast; flat columns are returned as-is.
-}
materialize :: forall a. (Columnable a) => Int -> Value a -> Column
materialize n (Scalar v) = broadcastScalar @a n v
materialize _ (Flat c) = c
materialize _ (Group _) =
    error "materialize: cannot flatten a grouped value to a single column"

{- | Replicate a scalar to a column of length @n@, choosing the most
efficient representation.
-}
broadcastScalar :: forall a. (Columnable a) => Int -> a -> Column
broadcastScalar n v = case sUnbox @a of
    STrue -> fromUnboxedVector (VU.replicate n v)
    SFalse -> fromVector (V.replicate n v)

-------------------------------------------------------------------------------
-- Lifting: the core combinators
-------------------------------------------------------------------------------

-- | Apply a pure function to a 'Value'.
liftValue ::
    (Columnable b, Columnable a) =>
    (b -> a) -> Value b -> Either DataFrameException (Value a)
liftValue f (Scalar v) = Right (Scalar (f v))
liftValue f (Flat col) = Flat <$> mapColumn f col
liftValue f (Group gs) = Group <$> V.mapM (mapColumn f) gs

{- | Apply a binary function to two 'Value's.  When one side is a
'Scalar' the operation degenerates to a 'liftValue' — this is how the
old @Binary op (Lit l) right@ special cases are recovered without
explicit pattern matches in the evaluator.
-}
liftValue2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) ->
    Value c ->
    Value b ->
    Either DataFrameException (Value a)
liftValue2 f (Scalar l) (Scalar r) = Right (Scalar (f l r))
liftValue2 f (Scalar l) v = liftValue (f l) v
liftValue2 f v (Scalar r) = liftValue (`f` r) v
liftValue2 f (Flat l) (Flat r) = Flat <$> zipWithColumns f l r
liftValue2 f (Group ls) (Group rs)
    | V.length ls == V.length rs =
        Group <$> V.zipWithM (zipWithColumns f) ls rs
-- Shape mismatches: aggregated vs. non-aggregated.
liftValue2 _ (Flat _) (Group _) =
    Left $ AggregatedAndNonAggregatedException "aggregated" "non-aggregated"
liftValue2 _ (Group _) (Flat _) =
    Left $ AggregatedAndNonAggregatedException "non-aggregated" "aggregated"
liftValue2 _ (Group _) (Group _) =
    Left $ InternalException "Group count mismatch in binary operation"

-- | Branch on a boolean 'Value', selecting from two same-typed 'Value's.
branchValue ::
    forall a.
    (Columnable a) =>
    Value Bool ->
    Value a ->
    Value a ->
    Either DataFrameException (Value a)
branchValue (Scalar True) l _ = Right l
branchValue (Scalar False) _ r = Right r
branchValue cond (Scalar l) (Scalar r) =
    liftValue (\c -> if c then l else r) cond
branchValue cond (Scalar l) r =
    liftValue2 (\c rv -> if c then l else rv) cond r
branchValue cond l (Scalar r) =
    liftValue2 (\c lv -> if c then lv else r) cond l
branchValue (Flat cc) (Flat lc) (Flat rc) =
    Flat <$> branchColumn @a cc lc rc
branchValue (Group cgs) (Group lgs) (Group rgs)
    | V.length cgs == V.length lgs
        && V.length lgs == V.length rgs =
        Group
            <$> V.generateM
                (V.length cgs)
                ( \i ->
                    branchColumn @a (cgs V.! i) (lgs V.! i) (rgs V.! i)
                )
branchValue _ _ _ =
    Left $
        AggregatedAndNonAggregatedException
            "if-then-else branches"
            "mismatched shapes"

{- | Low-level column branch: given a boolean column and two same-typed
columns, produce the element-wise selection.
-}
branchColumn ::
    forall a.
    (Columnable a) =>
    Column ->
    Column ->
    Column ->
    Either DataFrameException Column
branchColumn cc lc rc = do
    cs <- toVector @Bool @V.Vector cc
    ls <- toVector @a @V.Vector lc
    rs <- toVector @a @V.Vector rc
    pure $
        fromVector @a $
            V.zipWith3 (\c l r -> if c then l else r) cs ls rs

-------------------------------------------------------------------------------
-- Error enrichment
-------------------------------------------------------------------------------

{- | Wrap an interpretation step so that any 'TypeMismatchException' gets
annotated with the expression that was being evaluated.
-}
addContext ::
    (Show a) => Expr a -> Either DataFrameException b -> Either DataFrameException b
addContext expr = first (enrichError (show expr))

enrichError :: String -> DataFrameException -> DataFrameException
enrichError loc (TypeMismatchException ctx) =
    TypeMismatchException
        ctx
            { callingFunctionName =
                callingFunctionName ctx <|+> Just "eval"
            , errorColumnName =
                errorColumnName ctx <|+> Just loc
            }
  where
    -- Prefer the existing value; fall back to the new one.
    Nothing <|+> b = b
    a <|+> _ = a
enrichError _ e = e

-------------------------------------------------------------------------------
-- Group slicing
-------------------------------------------------------------------------------

{- | Given a flat column and grouping metadata, produce one 'Column' per
group.  Each result column is an O(1) slice into a sorted copy of the
input — the sort happens once, not per-group.
-}
sliceGroups :: Column -> VU.Vector Int -> VU.Vector Int -> V.Vector Column
sliceGroups col os indices = case col of
    BoxedColumn vec ->
        let !sorted = V.unsafeBackpermute vec (V.convert indices)
         in V.generate nGroups $ \i ->
                BoxedColumn (V.unsafeSlice (start i) (len i) sorted)
    UnboxedColumn vec ->
        let !sorted = VU.unsafeBackpermute vec indices
         in V.generate nGroups $ \i ->
                UnboxedColumn (VU.unsafeSlice (start i) (len i) sorted)
    OptionalColumn vec ->
        let !sorted = V.unsafeBackpermute vec (V.convert indices)
         in V.generate nGroups $ \i ->
                OptionalColumn (V.unsafeSlice (start i) (len i) sorted)
  where
    !nGroups = VU.length os - 1
    start i = os `VU.unsafeIndex` i
    len i = os `VU.unsafeIndex` (i + 1) - start i
{-# INLINE sliceGroups #-}

numGroups :: GroupedDataFrame -> Int
numGroups gdf = VU.length (offsets gdf) - 1

-------------------------------------------------------------------------------
-- promoteColumn: numeric / text coercion for Cast
-------------------------------------------------------------------------------

{- | Coerce a column to type @a@, using @def@ for Nothing in Optional columns.
Numeric coercion handles Double, Float, and Int targets via the singleton
dispatch for floating and integral types.  Text columns (String / T.Text) are
parsed via 'reads'; parse failures substitute @def@.  Any other mismatch
returns 'Left TypeMismatchException'.
-}
promoteColumn ::
    forall a. (Columnable a) => a -> Column -> Either DataFrameException Column
promoteColumn def col
    | hasElemType @a col = Right col
    | otherwise =
        case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> promoteToDouble def col
            Nothing ->
                case testEquality (typeRep @a) (typeRep @Float) of
                    Just Refl -> promoteToFloat def col
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Int) of
                            Just Refl -> promoteToInt def col
                            Nothing -> tryParse @a def col

promoteToDouble :: Double -> Column -> Either DataFrameException Column
promoteToDouble def col = case col of
    UnboxedColumn (v :: VU.Vector b) ->
        case sFloating @b of
            STrue -> Right $ fromUnboxedVector @Double (VU.map (realToFrac :: b -> Double) v)
            SFalse -> case sIntegral @b of
                STrue -> Right $ fromUnboxedVector @Double (VU.map (fromIntegral :: b -> Double) v)
                SFalse -> castMismatch @b @Double
    OptionalColumn (v :: V.Vector (Maybe b)) ->
        case sFloating @b of
            STrue ->
                Right $
                    fromUnboxedVector @Double
                        (VG.convert $ V.map (maybe def (realToFrac :: b -> Double)) v)
            SFalse -> case sIntegral @b of
                STrue ->
                    Right $
                        fromUnboxedVector @Double
                            (VG.convert $ V.map (maybe def (fromIntegral :: b -> Double)) v)
                SFalse -> tryParse @Double def col
    BoxedColumn _ -> tryParse @Double def col

promoteToFloat :: Float -> Column -> Either DataFrameException Column
promoteToFloat def col = case col of
    UnboxedColumn (v :: VU.Vector b) ->
        case sFloating @b of
            STrue -> Right $ fromUnboxedVector @Float (VU.map (realToFrac :: b -> Float) v)
            SFalse -> case sIntegral @b of
                STrue -> Right $ fromUnboxedVector @Float (VU.map (fromIntegral :: b -> Float) v)
                SFalse -> castMismatch @b @Float
    OptionalColumn (v :: V.Vector (Maybe b)) ->
        case sFloating @b of
            STrue ->
                Right $
                    fromUnboxedVector @Float
                        (VG.convert $ V.map (maybe def (realToFrac :: b -> Float)) v)
            SFalse -> case sIntegral @b of
                STrue ->
                    Right $
                        fromUnboxedVector @Float
                            (VG.convert $ V.map (maybe def (fromIntegral :: b -> Float)) v)
                SFalse -> tryParse @Float def col
    BoxedColumn _ -> tryParse @Float def col

promoteToInt :: Int -> Column -> Either DataFrameException Column
promoteToInt def col = case col of
    UnboxedColumn (v :: VU.Vector b) ->
        case sFloating @b of
            STrue ->
                Right $
                    fromUnboxedVector @Int
                        (VU.map (round . (realToFrac :: b -> Double)) v)
            SFalse -> case sIntegral @b of
                STrue -> Right $ fromUnboxedVector @Int (VU.map (fromIntegral :: b -> Int) v)
                SFalse -> castMismatch @b @Int
    OptionalColumn (v :: V.Vector (Maybe b)) ->
        case sFloating @b of
            STrue ->
                Right $
                    fromUnboxedVector @Int
                        (VG.convert $ V.map (maybe def (round . (realToFrac :: b -> Double))) v)
            SFalse -> case sIntegral @b of
                STrue ->
                    Right $
                        fromUnboxedVector @Int
                            (VG.convert $ V.map (maybe def (fromIntegral :: b -> Int)) v)
                SFalse -> tryParse @Int def col
    BoxedColumn _ -> tryParse @Int def col

tryParse ::
    forall a. (Columnable a) => a -> Column -> Either DataFrameException Column
tryParse def col = case col of
    BoxedColumn (v :: V.Vector b) ->
        case testEquality (typeRep @b) (typeRep @String) of
            Just Refl -> Right $ fromVector @a $ V.map (parseValue def) v
            Nothing ->
                case testEquality (typeRep @b) (typeRep @T.Text) of
                    Just Refl -> Right $ fromVector @a $ V.map (parseValue def . T.unpack) v
                    Nothing -> castMismatch @b @a
    OptionalColumn (v :: V.Vector (Maybe b)) ->
        case testEquality (typeRep @b) (typeRep @String) of
            Just Refl -> Right $ fromVector @a $ V.map (maybe def (parseValue def)) v
            Nothing ->
                case testEquality (typeRep @b) (typeRep @T.Text) of
                    Just Refl ->
                        Right $
                            fromVector @a $
                                V.map (maybe def (parseValue def . T.unpack)) v
                    Nothing -> castMismatch @b @a
    UnboxedColumn (_ :: VU.Vector b) -> castMismatch @b @a

parseValue :: (Read a) => a -> String -> a
parseValue def s = case reads s of
    [(x, "")] -> x
    _ -> def

castMismatch ::
    forall src tgt.
    (Typeable src, Typeable tgt) =>
    Either DataFrameException Column
castMismatch =
    Left $
        TypeMismatchException
            MkTypeErrorContext
                { userType = Right (typeRep @tgt)
                , expectedType = Right (typeRep @src)
                , callingFunctionName = Just "cast"
                , errorColumnName = Nothing
                }

-------------------------------------------------------------------------------
-- eval: the unified interpreter
-------------------------------------------------------------------------------

{- | Evaluate an expression in a given context, producing a 'Value'.
This single function replaces both the old @interpret@ (flat) and
@interpretAggregation@ (grouped) code paths.
-}
eval ::
    forall a.
    (Columnable a) =>
    Ctx -> Expr a -> Either DataFrameException (Value a)
-- Leaves -----------------------------------------------------------------

eval _ (Lit v) = Right (Scalar v)
eval (FlatCtx df) (Col name) =
    case getColumn name df of
        Nothing ->
            Left $
                ColumnNotFoundException name "" (M.keys $ columnIndices df)
        Just c -> Right (Flat c)
eval (GroupCtx gdf) (Col name) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnNotFoundException
                    name
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c ->
            Right
                ( Group
                    (sliceGroups c (offsets gdf) (valueIndices gdf))
                )
-- Cast -------------------------------------------------------------------

eval (FlatCtx df) (Cast name def) =
    case getColumn name df of
        Nothing ->
            Left $
                ColumnNotFoundException name "" (M.keys $ columnIndices df)
        Just c -> Flat <$> promoteColumn @a def c
eval (GroupCtx gdf) (Cast name def) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnNotFoundException
                    name
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c -> do
            promoted <- promoteColumn @a def c
            Right $ Group (sliceGroups promoted (offsets gdf) (valueIndices gdf))

-- Unary ------------------------------------------------------------------

eval ctx expr@(Unary (op :: UnaryOp b a) inner) = addContext expr $ do
    v <- eval @b ctx inner
    liftValue (unaryFn op) v

-- Binary -----------------------------------------------------------------

eval ctx expr@(Binary (op :: BinaryOp c b a) left right) =
    addContext expr $ do
        l <- eval @c ctx left
        r <- eval @b ctx right
        liftValue2 (binaryFn op) l r

-- If ---------------------------------------------------------------------

eval ctx expr@(If cond l r) = addContext expr $ do
    c <- eval @Bool ctx cond
    lv <- eval @a ctx l
    rv <- eval @a ctx r
    branchValue c lv rv

-- Fast path: FoldAgg (seeded) on a bare Col in GroupCtx.
-- Avoids the O(n) backpermute in sliceGroups by folding directly over
-- permuted indices.  Only matches when inner is exactly (Col name).

eval (GroupCtx gdf) expr@(Agg (FoldAgg _ (Just seed) (f :: a -> b -> a)) (Col name :: Expr b)) =
    addContext expr $
        case getColumn name (fullDataframe gdf) of
            Nothing ->
                Left $
                    ColumnNotFoundException
                        name
                        ""
                        (M.keys $ columnIndices $ fullDataframe gdf)
            Just col ->
                Flat <$> foldLinearGroups @b @a f seed col (rowToGroup gdf) (numGroups gdf)
-- Fast path: FoldAgg (seedless) on a bare Col in GroupCtx.

eval (GroupCtx gdf) expr@(Agg (FoldAgg _ Nothing (f :: a -> b -> a)) (Col name :: Expr b)) =
    addContext expr $
        case testEquality (typeRep @a) (typeRep @b) of
            Nothing ->
                Left $
                    InternalException
                        "Type mismatch in seedless fold: \
                        \accumulator and element types must match"
            Just Refl ->
                case getColumn name (fullDataframe gdf) of
                    Nothing ->
                        Left $
                            ColumnNotFoundException
                                name
                                ""
                                (M.keys $ columnIndices $ fullDataframe gdf)
                    Just col ->
                        Flat . fromVector
                            <$> foldl1DirectGroups @b f col (valueIndices gdf) (offsets gdf)
-- Fast path: MergeAgg on a bare Col in GroupCtx.

eval
    (GroupCtx gdf)
    expr@( Agg
                (MergeAgg _ seed (step :: acc -> b -> acc) _ (finalize :: acc -> a))
                (Col name :: Expr b)
            ) =
        addContext expr $
            case getColumn name (fullDataframe gdf) of
                Nothing ->
                    Left $
                        ColumnNotFoundException
                            name
                            ""
                            (M.keys $ columnIndices $ fullDataframe gdf)
                Just col ->
                    Flat
                        <$> ( foldLinearGroups @b step seed col (rowToGroup gdf) (numGroups gdf)
                                >>= mapColumn finalize
                            )
-- Aggregation: CollectAgg ------------------------------------------------

eval ctx expr@(Agg (CollectAgg _ (f :: v b -> a)) inner) =
    addContext expr $ do
        v <- eval @b ctx inner
        case v of
            Scalar _ ->
                Left $
                    InternalException
                        "Cannot apply a collection aggregation to a scalar"
            Flat col ->
                Scalar <$> applyCollect @v @b @a f col
            Group gs ->
                Flat . fromVector
                    <$> V.mapM (applyCollect @v @b @a f) gs

-- Aggregation: FoldAgg with seed -----------------------------------------

eval ctx expr@(Agg (FoldAgg _ (Just seed) (f :: a -> b -> a)) inner) =
    addContext expr $ do
        v <- eval @b ctx inner
        case v of
            Scalar _ ->
                Left $
                    InternalException
                        "Cannot apply a fold aggregation to a scalar"
            Flat col ->
                Scalar <$> foldlColumn @b @a f seed col
            Group gs ->
                Flat . fromVector
                    <$> V.mapM (foldlColumn @b @a f seed) gs

-- Aggregation: MergeAgg --------------------------------------------------

eval
    ctx
    expr@( Agg
                (MergeAgg _ seed (step :: acc -> b -> acc) _ (finalize :: acc -> a))
                (inner :: Expr b)
            ) =
        addContext expr $ do
            v <- eval @b ctx inner
            case v of
                Scalar _ ->
                    Left $
                        InternalException
                            "Cannot apply a merge aggregation to a scalar"
                Flat col ->
                    Scalar . finalize <$> foldlColumnWith @b step seed col
                Group gs ->
                    Flat . fromVector
                        <$> V.mapM (fmap finalize . foldlColumnWith @b step seed) gs

-- Aggregation: FoldAgg without seed (fold1) ------------------------------

eval ctx expr@(Agg (FoldAgg _ Nothing (f :: a -> b -> a)) inner) =
    addContext expr $
        case testEquality (typeRep @a) (typeRep @b) of
            Nothing ->
                Left $
                    InternalException
                        "Type mismatch in seedless fold: \
                        \accumulator and element types must match"
            Just Refl -> do
                v <- eval @b ctx inner
                case v of
                    Scalar _ ->
                        Left $
                            InternalException
                                "Cannot apply a fold aggregation to a scalar"
                    Flat col ->
                        Scalar <$> foldl1Column @a f col
                    Group gs ->
                        Flat . fromVector
                            <$> V.mapM (foldl1Column @a f) gs

-------------------------------------------------------------------------------
-- Aggregation helpers
-------------------------------------------------------------------------------

{- | Apply a 'CollectAgg' function to a single column, extracting the
appropriate vector type and applying the aggregation function.
-}
applyCollect ::
    forall v b a.
    (VG.Vector v b, Typeable v, Columnable b, Columnable a) =>
    (v b -> a) -> Column -> Either DataFrameException a
applyCollect f col = f <$> toVector @b @v col

-------------------------------------------------------------------------------
-- Backward-compatible wrappers
-------------------------------------------------------------------------------

{- | Result of interpreting an expression in a grouped context.
Retained for backward compatibility with 'aggregate' and friends.
-}
data AggregationResult a
    = UnAggregated Column
    | Aggregated (TypedColumn a)

{- | Interpret an expression against a flat 'DataFrame', producing a
typed column.  This is the original top-level entry point; internally
it calls 'eval' and materialises the result.

NOTE: unlike the old implementation, 'Lit' values are no longer
eagerly broadcast.  The broadcast happens here, at the boundary,
via 'materialize'.
-}
interpret ::
    forall a.
    (Columnable a) =>
    DataFrame -> Expr a -> Either DataFrameException (TypedColumn a)
interpret df expr = do
    v <- eval (FlatCtx df) expr
    pure $ TColumn $ materialize @a (fst (dataframeDimensions df)) v

{- | Interpret an expression against a 'GroupedDataFrame',
distinguishing aggregated results from bare column references.
Internally calls 'eval'.
-}
interpretAggregation ::
    forall a.
    (Columnable a) =>
    GroupedDataFrame ->
    Expr a ->
    Either DataFrameException (AggregationResult a)
interpretAggregation gdf expr = do
    v <- eval (GroupCtx gdf) expr
    case v of
        Scalar a ->
            Right $
                Aggregated $
                    TColumn $
                        broadcastScalar @a (numGroups gdf) a
        Flat col ->
            Right $ Aggregated $ TColumn col
        Group _ ->
            -- The Column payload is intentionally unused — the only
            -- call-site ('aggregate') immediately throws
            -- 'UnaggregatedException' on this constructor.
            Right $ UnAggregated $ BoxedColumn @T.Text V.empty
