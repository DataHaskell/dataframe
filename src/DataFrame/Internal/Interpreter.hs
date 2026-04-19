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
{-# OPTIONS_GHC -Wno-orphans #-}

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
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression
import qualified DataFrame.Internal.Grouping as G
import DataFrame.Internal.Types
import Type.Reflection (
    Typeable,
    typeRep,
 )

import Data.Int (Int16, Int32, Int64, Int8)

-- Specializations for common aggregation types to avoid dictionary overhead.
-- foldLinearGroups: mean accumulator
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Double -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Float -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Int -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Int8 -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Int16 -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Int32 -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (MeanAcc -> Int64 -> MeanAcc) ->
    MeanAcc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
-- foldLinearGroups: count accumulator
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Double -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Float -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int8 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int16 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int32 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int64 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
-- foldLinearGroups: sum/min/max (acc == elem)
{-# SPECIALIZE foldLinearGroups ::
    (Double -> Double -> Double) ->
    Double ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Float -> Float -> Float) ->
    Float ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int8 -> Int8 -> Int8) ->
    Int8 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int16 -> Int16 -> Int16) ->
    Int16 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int32 -> Int32 -> Int32) ->
    Int32 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int64 -> Int64 -> Int64) ->
    Int64 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}

-- mapColumn: finalize
{-# SPECIALIZE mapColumn ::
    (MeanAcc -> Double) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Double -> Double) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Float -> Float) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Int -> Int) -> Column -> Either DataFrameException Column
    #-}

-- zipWithColumns: binary ops
{-# SPECIALIZE zipWithColumns ::
    (Double -> Double -> Double) ->
    Column ->
    Column ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Float -> Float -> Float) ->
    Column ->
    Column ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Int -> Int -> Int) -> Column -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Int8 -> Int8 -> Int8) -> Column -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Int16 -> Int16 -> Int16) ->
    Column ->
    Column ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Int32 -> Int32 -> Int32) ->
    Column ->
    Column ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE zipWithColumns ::
    (Int64 -> Int64 -> Int64) ->
    Column ->
    Column ->
    Either DataFrameException Column
    #-}

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
    BoxedColumn bm vec ->
        let !sorted =
                V.generate
                    (VU.length indices)
                    ((vec `V.unsafeIndex`) . (indices `VU.unsafeIndex`))
         in V.generate nGroups $ \i ->
                BoxedColumn
                    (fmap (bitmapSlice (start i) (len i)) bm)
                    (V.unsafeSlice (start i) (len i) sorted)
    UnboxedColumn bm vec ->
        let !sorted = VU.unsafeBackpermute vec indices
         in V.generate nGroups $ \i ->
                UnboxedColumn
                    (fmap (bitmapSlice (start i) (len i)) bm)
                    (VU.unsafeSlice (start i) (len i) sorted)
  where
    !nGroups = VU.length os - 1
    start i = os `VU.unsafeIndex` i
    len i = os `VU.unsafeIndex` (i + 1) - start i
{-# INLINE sliceGroups #-}

numGroups :: GroupedDataFrame -> Int
numGroups gdf = VU.length (offsets gdf) - 1

-- | Build the inverse of a permutation vector.
invertPermutation :: VU.Vector Int -> VU.Vector Int
invertPermutation perm = VU.create $ do
    let !n = VU.length perm
    inv <- VUM.new n
    VU.imapM_ (flip (VUM.unsafeWrite inv)) perm
    return inv
{-# INLINE invertPermutation #-}

-------------------------------------------------------------------------------
-- promoteColumnWith: unified numeric / text coercion for CastWith
-------------------------------------------------------------------------------

{- | Apply a result-handler @onResult@ to each element of a column after
coercing it to type @a@.  Covers three modes in one:

* @onResult = either (const Nothing) Just@  → like @cast@   (returns @Maybe a@)
* @onResult = either (const def) id@         → like @castWithDefault@ (returns @a@)
* @onResult = either (Left . T.pack) Right@  → like @castEither@       (returns @Either T.Text a@)

Numeric coercion handles Double, Float, and Int targets.  Text columns
(String / T.Text) are parsed via 'reads'.  Any other mismatch returns
'Left TypeMismatchException'.
-}
promoteColumnWith ::
    forall a b.
    (Columnable a, Columnable b, Read a) =>
    (Either String a -> b) -> Column -> Either DataFrameException Column
promoteColumnWith onResult col
    | hasElemType @b col = Right col
    | hasElemType @a col = mapColumn @a (onResult . Right) col
    | Just result <- tryMaybeWrap @a @b onResult col = result
    | otherwise =
        case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> promoteToDoubleWith onResult col
            Nothing ->
                case testEquality (typeRep @a) (typeRep @Float) of
                    Just Refl -> promoteToFloatWith onResult col
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Int) of
                            Just Refl -> promoteToIntWith onResult col
                            Nothing -> tryParseWith @a onResult col

promoteToDoubleWith ::
    forall b.
    (Columnable b) =>
    (Either String Double -> b) -> Column -> Either DataFrameException Column
promoteToDoubleWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (realToFrac :: c -> Double)) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Double)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (realToFrac (VU.unsafeIndex v i) :: Double))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Double))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Double onResult col

promoteToFloatWith ::
    forall b.
    (Columnable b) =>
    (Either String Float -> b) -> Column -> Either DataFrameException Column
promoteToFloatWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (realToFrac :: c -> Float)) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Float)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (realToFrac (VU.unsafeIndex v i) :: Float))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Float))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Float onResult col

promoteToIntWith ::
    forall b.
    (Columnable b) =>
    (Either String Int -> b) -> Column -> Either DataFrameException Column
promoteToIntWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (round . (realToFrac :: c -> Double))) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Int)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (round (realToFrac (VU.unsafeIndex v i) :: Double)))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Int))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Int onResult col

-- | Single parse primitive: apply @onResult@ to the result of 'reads'.
parseWith :: (Read a) => (Either String a -> b) -> String -> b
parseWith f s = case reads s of
    [(x, "")] -> f (Right x)
    _ -> f (Left s)

tryParseWith ::
    forall a b.
    (Columnable a, Columnable b, Read a) =>
    (Either String a -> b) -> Column -> Either DataFrameException Column
tryParseWith onResult col = case col of
    BoxedColumn bm (v :: V.Vector c) ->
        case testEquality (typeRep @c) (typeRep @String) of
            Just Refl -> case bm of
                Nothing -> Right $ fromVector @b $ V.map (parseWith onResult) v
                Just bitmap ->
                    Right $
                        fromVector @b $
                            V.imap
                                ( \i x ->
                                    if bitmapTestBit bitmap i then parseWith onResult x else onResult (Left "null")
                                )
                                v
            Nothing ->
                case testEquality (typeRep @c) (typeRep @T.Text) of
                    Just Refl -> case bm of
                        Nothing -> Right $ fromVector @b $ V.map (parseWith onResult . T.unpack) v
                        Just bitmap ->
                            Right $
                                fromVector @b $
                                    V.imap
                                        ( \i x ->
                                            if bitmapTestBit bitmap i
                                                then parseWith onResult (T.unpack x)
                                                else onResult (Left "null")
                                        )
                                        v
                    Nothing -> castMismatch @c @b
    UnboxedColumn _ (_ :: VU.Vector c) -> castMismatch @c @b

{- | When the output type @b@ is @Maybe c@ (or @Maybe (Maybe c)@) and the
column stores plain @c@ values, wrap each element in 'Just'.
The @Maybe (Maybe c)@ case applies join semantics: instead of producing
a double-wrapped column, a @Maybe c@ column is returned, so
@castExpr \@(Maybe Double)@ on a @Double@ column yields @Maybe Double@
rather than @Maybe (Maybe Double)@.
Returns 'Nothing' when neither condition holds.
-}
tryMaybeWrap ::
    forall a b.
    (Columnable a, Columnable b) =>
    (Either String a -> b) -> Column -> Maybe (Either DataFrameException Column)
tryMaybeWrap _onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        let wrapped = V.map Just (VG.convert v) :: V.Vector (Maybe c)
         in case testEquality (typeRep @b) (typeRep @(Maybe c)) of
                Just Refl -> Just $ Right $ fromVector @b wrapped
                Nothing ->
                    case testEquality (typeRep @b) (typeRep @(Maybe (Maybe c))) of
                        Just _ -> Just $ Right $ fromVector @(Maybe c) wrapped
                        Nothing -> Nothing
    BoxedColumn Nothing (v :: V.Vector c) ->
        let wrapped = V.map Just v :: V.Vector (Maybe c)
         in case testEquality (typeRep @b) (typeRep @(Maybe c)) of
                Just Refl -> Just $ Right $ fromVector @b wrapped
                Nothing ->
                    case testEquality (typeRep @b) (typeRep @(Maybe (Maybe c))) of
                        Just _ -> Just $ Right $ fromVector @(Maybe c) wrapped
                        Nothing -> Nothing
    _ -> Nothing

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
            Left $ ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
        Just c
            | hasElemType @a c -> Right (Flat c)
            | otherwise ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @a)
                            , expectedType = Left (columnTypeString c)
                            , errorColumnName = Just (T.unpack name)
                            , callingFunctionName = Just "col"
                            } ::
                            TypeErrorContext a ()
                        )
eval (GroupCtx gdf) (Col name) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnsNotFoundException
                    [name]
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c
            | hasElemType @a c ->
                Right (Group (sliceGroups c (offsets gdf) (valueIndices gdf)))
            | otherwise ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @a)
                            , expectedType = Left (columnTypeString c)
                            , errorColumnName = Just (T.unpack name)
                            , callingFunctionName = Just "col"
                            } ::
                            TypeErrorContext a ()
                        )
-- CastWith ---------------------------------------------------------------

eval (FlatCtx df) (CastWith name _tag onResult) =
    case getColumn name df of
        Nothing ->
            Left $
                ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
        Just c -> Flat <$> promoteColumnWith onResult c
eval (GroupCtx gdf) (CastWith name _tag onResult) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnsNotFoundException
                    [name]
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c -> do
            promoted <- promoteColumnWith onResult c
            Right $ Group (sliceGroups promoted (offsets gdf) (valueIndices gdf))
-- CastExprWith -----------------------------------------------------------

eval ctx (CastExprWith _tag onResult (inner :: Expr src)) = do
    v <- eval @src ctx inner
    case v of
        Scalar s ->
            Flat <$> promoteColumnWith onResult (fromList @src [s])
        Flat col ->
            Flat <$> promoteColumnWith onResult col
        Group gs ->
            Group <$> V.mapM (promoteColumnWith onResult) gs
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

-- Over (window function) -------------------------------------------------

eval (FlatCtx df) expr@(Over keys inner) = addContext expr $ do
    let gdf = G.groupBy keys df
    v <- eval (GroupCtx gdf) inner
    case v of
        Scalar s ->
            Right (Scalar s)
        Flat groupCol ->
            -- Scalar agg (mean, sum, median): one value per group.
            -- Broadcast via rowToGroup: row i gets value at group rowToGroup[i].
            Right (Flat (atIndicesStable (rowToGroup gdf) groupCol))
        Group groupCols -> do
            -- Concatenate in sorted order, then unsort to original row order.
            sorted <- V.fold1M' concatColumns groupCols
            let inv = invertPermutation (valueIndices gdf)
            Right (Flat (atIndicesStable inv sorted))
eval (GroupCtx _) expr@(Over _ _) =
    addContext expr $
        Left
            ( InternalException
                "Over (window function) is not supported inside a grouped context"
            )
-- Fast path: FoldAgg (seeded) on a bare Col in GroupCtx.
-- Avoids the O(n) backpermute in sliceGroups by folding directly over
-- permuted indices.  Only matches when inner is exactly (Col name).

eval (GroupCtx gdf) expr@(Agg (FoldAgg _ (Just seed) (f :: a -> b -> a)) (Col name :: Expr b)) =
    addContext expr $
        case getColumn name (fullDataframe gdf) of
            Nothing ->
                Left $
                    ColumnsNotFoundException
                        [name]
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
                            ColumnsNotFoundException
                                [name]
                                ""
                                (M.keys $ columnIndices $ fullDataframe gdf)
                    Just col ->
                        Flat <$> foldl1DirectGroups @b f col (valueIndices gdf) (offsets gdf)
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
                        ColumnsNotFoundException
                            [name]
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
                    Scalar . finalize <$> foldlColumn @b step seed col
                Group gs ->
                    Flat . fromVector
                        <$> V.mapM (fmap finalize . foldlColumn @b step seed) gs

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
            Right $ UnAggregated $ BoxedColumn @T.Text Nothing V.empty
