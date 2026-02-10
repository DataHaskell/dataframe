{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Interpreter where

import Control.Monad.ST (runST)
import Data.Bifunctor
import qualified Data.Map as M
import Data.Maybe (fromMaybe, isJust)
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression
import DataFrame.Internal.Types
import Type.Reflection (TypeRep, Typeable, typeOf, typeRep, pattern App)

interpret ::
    forall a.
    (Columnable a) =>
    DataFrame -> Expr a -> Either DataFrameException (TypedColumn a)
interpret df (Lit value) = case sUnbox @a of
    -- Specialize the creation of unboxed columns to avoid an extra allocation.
    STrue -> pure $ TColumn $ fromUnboxedVector $ VU.replicate (numRows df) value
    SFalse -> pure $ TColumn $ fromVector $ V.replicate (numRows df) value
interpret df (Col name) = maybe columnNotFound (pure . TColumn) (getColumn name df)
  where
    columnNotFound = Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
-- Unary operations.
interpret df expr@(Unary (op :: UnaryOp b c) value) = first (handleInterpretException (show expr)) $ do
    (TColumn value') <- interpret df value
    fmap TColumn (mapColumn (unaryFn op) value')
-- Variations of binary operations.
interpret df expr@(Binary (op :: BinaryOp c b a) left right) = first (handleInterpretException (show expr)) $ case (left, right) of
    (Lit left, Lit right) -> interpret df (Lit (binaryFn op left right))
    (Lit left, right) -> do
        -- If we have a literal then we don't have to materialise
        -- the column.
        (TColumn value') <- interpret df right
        fmap TColumn (mapColumn (binaryFn op left) value')
    (left, Lit right) -> do
        -- Same as the above except the right side is the
        -- literl.
        (TColumn value') <- interpret df left
        fmap TColumn (mapColumn (flip (binaryFn op) right) value')
    (_, _) -> do
        -- In the general case we interpret and zip.
        (TColumn left') <- interpret df left
        (TColumn right') <- interpret df right
        fmap TColumn (zipWithColumns (binaryFn op) left' right')
-- Conditionals
interpret df expr@(If cond l r) = first (handleInterpretException (show expr)) $ do
    (TColumn conditions) <- interpret df cond
    (TColumn left) <- interpret df l
    (TColumn right) <- interpret df r
    let branch (c :: Bool) (l' :: a, r' :: a) = if c then l' else r'
    fmap TColumn (zipWithColumns branch conditions (zipColumns left right))
interpret df expression@(Agg (CollectAgg op (f :: v b -> c)) expr) = do
    (TColumn column) <- interpret df expr
    -- Helper for errors. Should probably find a way of throwing this
    -- without leaking the fact that we use `Vector` to users.
    let aggTypeError expected =
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @(v b))
                    , expectedType = Left expected :: Either String (TypeRep ())
                    , callingFunctionName = Just "interpret"
                    , errorColumnName = Nothing
                    }
                )
    let processColumn ::
            (Columnable d) => d -> Either DataFrameException (TypedColumn a)
        processColumn col = case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpret @c df (Lit (f col))
            Nothing -> Left $ aggTypeError (show (typeOf col))
    case column of
        (BoxedColumn col) -> processColumn col
        (OptionalColumn col) -> processColumn col
        (UnboxedColumn col) -> processColumn col
interpret df expression@(Agg (FoldAgg op (Just v) f) expr) = first (handleInterpretException (show expr)) $ do
    (TColumn column) <- interpret df expr
    value <- foldlColumn f v column
    pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df expression@(Agg (FoldAgg op Nothing (f :: a -> b -> a)) expr) = first (handleInterpretException (show expr)) $ do
    (TColumn column) <- interpret df expr
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> do
            value <- foldl1Column f column
            pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
        Nothing -> error "Type error"

data AggregationResult a
    = UnAggregated Column
    | Aggregated (TypedColumn a)

interpretAggregation ::
    forall a.
    (Columnable a) =>
    GroupedDataFrame -> Expr a -> Either DataFrameException (AggregationResult a)
interpretAggregation gdf (Lit value) =
    Right $
        Aggregated $
            TColumn $
                fromVector $
                    V.replicate (VU.length (offsets gdf) - 1) value
interpretAggregation gdf@(Grouped df names indices os) (Col name) = case getColumn name df of
    Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just (BoxedColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnBoxed col os indices
    Just (OptionalColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnBoxed col os indices
    Just (UnboxedColumn col) ->
        Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnUnboxed col os indices
interpretAggregation gdf expression@(Unary (op :: UnaryOp b a) expr) =
    case interpretAggregation @b gdf expr of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left e -> Left e
        Right (UnAggregated unaggregated) -> case unaggregated of
            BoxedColumn (col :: V.Vector c) -> case testEquality (typeRep @c) (typeRep @(V.Vector b)) of
                Just Refl -> case sUnbox @a of
                    SFalse -> Right $ UnAggregated $ fromVector $ V.map (V.map (unaryFn op)) col
                    STrue ->
                        Right $
                            UnAggregated $
                                fromVector $
                                    V.map (V.convert @V.Vector @a @VU.Vector . V.map (unaryFn op)) col
                Nothing -> case testEquality (typeRep @c) (typeRep @(VU.Vector b)) of
                    Nothing -> Left $ nestedTypeException @c @a (show expression)
                    Just Refl -> case (sUnbox @b, sUnbox @a) of
                        (SFalse, _) -> Left $ InternalException "Boxed type inside an unboxed column"
                        (STrue, STrue) -> Right $ UnAggregated $ fromVector $ V.map (VU.map (unaryFn op)) col
                        (STrue, _) ->
                            Right $ UnAggregated $ fromVector $ V.map (V.map (unaryFn op) . VU.convert) col
            _ -> Left $ InternalException "Aggregated into a non-boxed column"
        Right (Aggregated (TColumn aggregated)) -> case mapColumn (unaryFn op) aggregated of
            Left e -> Left e
            Right col -> Right $ Aggregated $ TColumn col
interpretAggregation gdf expression@(Binary (op :: t c b a) left (Lit (right :: b))) =
    interpretAggregation
        gdf
        (Unary (MkUnaryOp (flip (binaryFn op) right) "udf" Nothing) left)
interpretAggregation gdf expression@(Binary (op :: BinaryOp c b a) (Lit (left :: c)) right) =
    interpretAggregation
        gdf
        (Unary (MkUnaryOp (binaryFn op left) "udf" Nothing) right)
interpretAggregation gdf expression@(Binary (op :: BinaryOp c b a) left right) =
    case (interpretAggregation gdf left, interpretAggregation gdf right) of
        (Right (Aggregated (TColumn left')), Right (Aggregated (TColumn right'))) -> case zipWithColumns (binaryFn op) left' right' of
            Left e -> Left e
            Right col -> Right $ Aggregated $ TColumn col
        (Right (UnAggregated left'), Right (UnAggregated right')) -> case (left', right') of
            (BoxedColumn (l :: V.Vector m), BoxedColumn (r :: V.Vector n)) -> case testEquality (typeRep @m) (typeRep @(VU.Vector c)) of
                Just Refl -> case testEquality (typeRep @n) (typeRep @(VU.Vector b)) of
                    Just Refl -> case (sUnbox @c, sUnbox @b, sUnbox @a) of
                        (STrue, STrue, STrue) ->
                            Right $ UnAggregated $ fromVector $ V.zipWith (VU.zipWith (binaryFn op)) l r
                        (STrue, STrue, SFalse) ->
                            Right $
                                UnAggregated $
                                    fromVector $
                                        V.zipWith (\l' r' -> V.zipWith (binaryFn op) (V.convert l') (V.convert r')) l r
                        (_, _, _) -> Left $ InternalException "Boxed vectors contain unboxed types"
                    Nothing -> case testEquality (typeRep @n) (typeRep @(V.Vector b)) of
                        Just Refl -> case sUnbox @c of
                            STrue ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith (V.zipWith (binaryFn op) . V.convert) l r
                            SFalse -> Left $ InternalException "Unboxed vectors contain boxed types"
                        Nothing -> Left $ nestedTypeException @n @b (show right)
                Nothing -> case testEquality (typeRep @m) (typeRep @(V.Vector c)) of
                    Nothing -> Left $ nestedTypeException @m @c (show left)
                    Just Refl -> case testEquality (typeRep @n) (typeRep @(VU.Vector b)) of
                        Just Refl -> case (sUnbox @b, sUnbox @a) of
                            (STrue, STrue) ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith
                                                ( \l' r' ->
                                                    V.convert @V.Vector @a @VU.Vector $ V.zipWith (binaryFn op) l' (V.convert r')
                                                )
                                                l
                                                r
                            (STrue, SFalse) ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith (\l' r' -> V.zipWith (binaryFn op) l' (V.convert r')) l r
                            (_, _) -> Left $ InternalException "Unboxed vectors contain boxed types"
                        Nothing -> case testEquality (typeRep @n) (typeRep @(V.Vector b)) of
                            Just Refl -> case sUnbox @a of
                                SFalse ->
                                    Right $
                                        UnAggregated $
                                            fromVector $
                                                V.zipWith (V.zipWith (binaryFn op) . V.convert) l r
                                STrue ->
                                    Right $
                                        UnAggregated $
                                            fromVector $
                                                V.zipWith
                                                    (\l' r' -> V.convert @V.Vector @a @VU.Vector $ V.zipWith (binaryFn op) l' r')
                                                    l
                                                    r
                            Nothing -> Left $ nestedTypeException @n @b (show right)
            _ -> Left $ InternalException "Aggregated into a non-boxed column"
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show left) (T.pack $ show right)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show left)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show right)
                        }
                    )
        (_, Left e) -> Left e
interpretAggregation gdf expression@(If cond (Lit l) (Lit r)) =
    case interpretAggregation @Bool gdf cond of
        Right (Aggregated (TColumn conditions)) -> case mapColumn
            (\(c :: Bool) -> if c then l else r)
            conditions of
            Left e -> Left e
            Right v -> Right $ Aggregated (TColumn v)
        Right (UnAggregated conditions) -> case sUnbox @a of
            STrue -> case mapColumn
                (\(c :: VU.Vector Bool) -> VU.map (\c' -> if c' then l else r) c)
                conditions of
                Left (TypeMismatchException context) ->
                    Left $
                        TypeMismatchException
                            ( context
                                { callingFunctionName = Just "interpretAggregation"
                                , errorColumnName = Just (show expression)
                                }
                            )
                Left e -> Left e
                Right v -> Right $ UnAggregated v
            SFalse -> case mapColumn
                (\(c :: VU.Vector Bool) -> V.map (\c' -> if c' then l else r) (VU.convert c))
                conditions of
                Left (TypeMismatchException context) ->
                    Left $
                        TypeMismatchException
                            ( context
                                { callingFunctionName = Just "interpretAggregation"
                                , errorColumnName = Just (show expression)
                                }
                            )
                Left e -> Left e
                Right v -> Right $ UnAggregated v
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        Left e -> Left e
interpretAggregation gdf expression@(If cond (Lit l) r) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf r
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn right))
            ) -> case zipWithColumns
                (\(c :: Bool) (r' :: a) -> if c then l else r')
                conditions
                right of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated right@(BoxedColumn (right' :: V.Vector c)))
            ) -> case testEquality (typeRep @(V.Vector a)) (typeRep @c) of
                Just Refl -> case zipWithColumns
                    ( \(c :: VU.Vector Bool) (r' :: V.Vector a) ->
                        V.zipWith
                            (\c' r'' -> if c' then l else r'')
                            (V.convert c)
                            r'
                    )
                    conditions
                    right of
                    Left (TypeMismatchException context) ->
                        Left $
                            TypeMismatchException
                                ( context
                                    { callingFunctionName = Just "interpretAggregation"
                                    , errorColumnName = Just (show expression)
                                    }
                                )
                    Left e -> Left e
                    Right v -> Right $ UnAggregated v
                Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @c) of
                    Nothing -> Left $ nestedTypeException @c @a (show expression)
                    Just Refl -> case sUnbox @a of
                        SFalse -> Left $ InternalException "Boxed type in unboxed column"
                        STrue -> case zipWithColumns
                            ( \(c :: VU.Vector Bool) (r' :: VU.Vector a) ->
                                VU.zipWith
                                    (\c' r'' -> if c' then l else r'')
                                    c
                                    r'
                            )
                            conditions
                            right of
                            Left (TypeMismatchException context) ->
                                Left $
                                    TypeMismatchException
                                        ( context
                                            { callingFunctionName = Just "interpretAggregation"
                                            , errorColumnName = Just (show expression)
                                            }
                                        )
                            Left e -> Left e
                            Right v -> Right $ UnAggregated v
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
                        }
                    )
        (_, Left e) -> Left e
interpretAggregation gdf expression@(If cond l (Lit r)) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf l
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn left))
            ) -> case zipWithColumns
                (\(c :: Bool) (l' :: a) -> if c then l' else r)
                conditions
                left of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated left@(BoxedColumn (left' :: V.Vector c)))
            ) -> case testEquality (typeRep @(V.Vector a)) (typeRep @c) of
                Just Refl -> case zipWithColumns
                    ( \(c :: VU.Vector Bool) (l' :: V.Vector a) ->
                        V.zipWith
                            (\c' l'' -> if c' then l'' else r)
                            (V.convert c)
                            l'
                    )
                    conditions
                    left of
                    Left (TypeMismatchException context) ->
                        Left $
                            TypeMismatchException
                                ( context
                                    { callingFunctionName = Just "interpretAggregation"
                                    , errorColumnName = Just (show expression)
                                    }
                                )
                    Left e -> Left e
                    Right v -> Right $ UnAggregated v
                Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @c) of
                    Nothing -> Left $ nestedTypeException @c @a (show expression)
                    Just Refl -> case sUnbox @a of
                        SFalse -> Left $ InternalException "Boxed type in unboxed column"
                        STrue -> case zipWithColumns
                            ( \(c :: VU.Vector Bool) (l' :: VU.Vector a) ->
                                VU.zipWith
                                    (\c' l'' -> if c' then l'' else r)
                                    c
                                    l'
                            )
                            conditions
                            left of
                            Left (TypeMismatchException context) ->
                                Left $
                                    TypeMismatchException
                                        ( context
                                            { callingFunctionName = Just "interpretAggregation"
                                            , errorColumnName = Just (show expression)
                                            }
                                        )
                            Left e -> Left e
                            Right v -> Right $ UnAggregated v
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
                        }
                    )
        (_, Left e) -> Left e
interpretAggregation gdf expression@(If cond l r) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf l
         , interpretAggregation @a gdf r
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn left))
            , Right (Aggregated (TColumn right))
            ) -> case zipWithColumns
                (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                conditions
                (zipColumns left right) of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated left@(BoxedColumn (left' :: V.Vector b)))
            , Right (UnAggregated right@(BoxedColumn (right' :: V.Vector c)))
            ) -> case testEquality (typeRep @b) (typeRep @c) of
                Nothing ->
                    Left $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right (typeRep @b)
                                , expectedType = Right (typeRep @c)
                                , callingFunctionName = Just "interpretAggregation"
                                , errorColumnName = Just (show expression)
                                }
                            )
                Just Refl -> case testEquality (typeRep @(V.Vector a)) (typeRep @b) of
                    Just Refl -> case zipWithColumns
                        ( \(c :: VU.Vector Bool) (l' :: V.Vector a, r' :: V.Vector a) ->
                            V.zipWith
                                (\c' (l'', r'') -> if c' then l'' else r'')
                                (V.convert c)
                                (V.zip l' r')
                        )
                        conditions
                        (zipColumns left right) of
                        Left (TypeMismatchException context) ->
                            Left $
                                TypeMismatchException
                                    ( context
                                        { callingFunctionName = Just "interpretAggregation"
                                        , errorColumnName = Just (show expression)
                                        }
                                    )
                        Left e -> Left e
                        Right v -> Right $ UnAggregated v
                    Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @b) of
                        Nothing -> Left $ nestedTypeException @b @a (show expression)
                        Just Refl -> case sUnbox @a of
                            SFalse -> Left $ InternalException "Boxed type in unboxed column"
                            STrue -> case zipWithColumns
                                ( \(c :: VU.Vector Bool) (l' :: VU.Vector a, r' :: VU.Vector a) ->
                                    VU.zipWith
                                        (\c' (l'', r'') -> if c' then l'' else r'')
                                        c
                                        (VU.zip l' r')
                                )
                                conditions
                                (zipColumns left right) of
                                Left (TypeMismatchException context) ->
                                    Left $
                                        TypeMismatchException
                                            ( context
                                                { callingFunctionName = Just "interpretAggregation"
                                                , errorColumnName = Just (show expression)
                                                }
                                            )
                                Left e -> Left e
                                Right v -> Right $ UnAggregated v
        (Right _, Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _, _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _, _) -> Left e
        (_, Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show l)
                        }
                    )
        (_, Left e, _) -> Left e
        (_, _, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
                        }
                    )
        (_, _, Left e) -> Left e
interpretAggregation gdf@(Grouped df names indices os) expression@(Agg (CollectAgg op (f :: v b -> c)) expr) =
    case interpretAggregation @b gdf expr of
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(v b)) (typeRep @d) of
            Nothing -> Left $ nestedTypeException @d @b (show expr)
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Nothing -> Right $ Aggregated $ TColumn $ fromVector $ V.map (f . V.convert) col
                Just Refl -> Right $ Aggregated $ TColumn $ fromVector $ V.map f col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn (BoxedColumn (col :: V.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . V.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (UnboxedColumn (col :: VU.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @VU.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . VU.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (OptionalColumn (col :: V.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . V.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
interpretAggregation gdf@(Grouped df names indices os) expression@(Agg (FoldAgg op Nothing (f :: a -> b -> a)) (Col name)) = case testEquality (typeRep @a) (typeRep @b) of
    Nothing -> error "Type mismatch"
    Just Refl -> case getColumn name df of
        Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
        Just (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                mkReducedColumnBoxed col os indices f
        Just (OptionalColumn (col :: V.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                mkReducedColumnBoxed col os indices f
        Just (UnboxedColumn (col :: VU.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromUnboxedVector $
                                mkReducedColumnUnboxed col os indices f
            Nothing -> error "Type mismatch"
interpretAggregation gdf@(Grouped df names indices os) expression@(Agg (FoldAgg op Nothing (f :: a -> b -> a)) expr) = case testEquality (typeRep @a) (typeRep @b) of
    Nothing -> error "Type mismatch"
    Just Refl -> case interpretAggregation @a gdf expr of
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(V.Vector a)) (typeRep @d) of
            Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @d) of
                Nothing -> Left $ nestedTypeException @d @a (show expr)
                Just Refl -> case sUnbox @a of
                    STrue ->
                        Right $
                            Aggregated $
                                TColumn $
                                    fromVector $
                                        V.map (VU.foldl1' f) col
                    SFalse -> Left $ InternalException "Boxed type inside an unboxed column"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                V.map (V.foldl1' f) col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn column)) -> case foldl1Column f column of
            Left e -> Left e
            Right value -> interpretAggregation @a gdf (Lit value)
interpretAggregation gdf@(Grouped df names indices os) expression@(Agg (FoldAgg op (Just s) (f :: a -> b -> a)) expr) =
    case interpretAggregation gdf expr of
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(V.Vector b)) (typeRep @d) of
            Just Refl -> Right $ Aggregated $ TColumn $ fromVector $ V.map (V.foldl' f s) col
            Nothing -> case testEquality (typeRep @(VU.Vector b)) (typeRep @d) of
                Just Refl -> case sUnbox @b of
                    STrue ->
                        Right $ Aggregated $ TColumn $ fromVector $ V.map (VU.foldl' f s) col
                    SFalse -> Left $ InternalException "Boxed type inside an unboxed column"
                Nothing -> Left $ nestedTypeException @d @b (show expr)
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn column)) -> case foldlColumn f s column of
            Left e -> Left e
            Right value -> interpretAggregation @a gdf (Lit value)

handleInterpretException :: String -> DataFrameException -> DataFrameException
handleInterpretException errorLocation (TypeMismatchException context) = mkTypeMismatchException (Just "interpret") (Just errorLocation) context
handleInterpretException _ e = e

numRows :: DataFrame -> Int
numRows df = fst (dataframeDimensions df)

mkUnaggregatedColumnBoxed ::
    forall a.
    (Columnable a) =>
    V.Vector a -> VU.Vector Int -> VU.Vector Int -> V.Vector (V.Vector a)
mkUnaggregatedColumnBoxed col os indices =
    let
        sorted = V.unsafeBackpermute col (V.convert indices)
        n i = os `VU.unsafeIndex` (i + 1) - (os `VU.unsafeIndex` i)
        start i = os `VU.unsafeIndex` i
     in
        V.generate
            (VU.length os - 1)
            ( \i ->
                V.unsafeSlice (start i) (n i) sorted
            )

mkUnaggregatedColumnUnboxed ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    VU.Vector a -> VU.Vector Int -> VU.Vector Int -> V.Vector (VU.Vector a)
mkUnaggregatedColumnUnboxed col os indices =
    let
        sorted = VU.unsafeBackpermute col indices
        n i = os `VU.unsafeIndex` (i + 1) - (os `VU.unsafeIndex` i)
        start i = os `VU.unsafeIndex` i
     in
        V.generate
            (VU.length os - 1)
            ( \i ->
                VU.unsafeSlice (start i) (n i) sorted
            )

mkAggregatedColumnUnboxed ::
    forall a b.
    (Columnable a, VU.Unbox a, Columnable b, VU.Unbox b) =>
    VU.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (VU.Vector a -> b) ->
    VU.Vector b
mkAggregatedColumnUnboxed col os indices f =
    let
        sorted = VU.unsafeBackpermute col indices
        n i = os `VU.unsafeIndex` (i + 1) - (os `VU.unsafeIndex` i)
        start i = os `VU.unsafeIndex` i
     in
        VU.generate
            (VU.length os - 1)
            ( \i ->
                f (VU.unsafeSlice (start i) (n i) sorted)
            )

mkReducedColumnUnboxed ::
    forall a.
    (VU.Unbox a) =>
    VU.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (a -> a -> a) ->
    VU.Vector a
mkReducedColumnUnboxed col os indices f = runST $ do
    let len = VU.length os - 1
    mvec <- VUM.unsafeNew len

    let loopOut i
            | i == len = return ()
            | otherwise = do
                let !start = os `VU.unsafeIndex` i
                let !end = os `VU.unsafeIndex` (i + 1)
                let !initVal = col `VU.unsafeIndex` (indices `VU.unsafeIndex` start)

                let loopIn !acc !idx
                        | idx == end = acc
                        | otherwise =
                            let val = col `VU.unsafeIndex` (indices `VU.unsafeIndex` idx)
                             in loopIn (f acc val) (idx + 1)
                let !finalVal = loopIn initVal (start + 1)
                VUM.unsafeWrite mvec i finalVal
                loopOut (i + 1)

    loopOut 0
    VU.unsafeFreeze mvec
{-# INLINE mkReducedColumnUnboxed #-}

mkReducedColumnBoxed ::
    V.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (a -> a -> a) ->
    V.Vector a
mkReducedColumnBoxed col os indices f = runST $ do
    let len = VU.length os - 1
    mvec <- VM.unsafeNew len

    let loopOut i
            | i == len = return ()
            | otherwise = do
                let start = os `VU.unsafeIndex` i
                let end = os `VU.unsafeIndex` (i + 1)
                let initVal = col `V.unsafeIndex` (indices `VU.unsafeIndex` start)

                let loopIn !acc idx
                        | idx == end = acc
                        | otherwise =
                            let val = col `V.unsafeIndex` (indices `VU.unsafeIndex` idx)
                             in loopIn (f acc val) (idx + 1)
                let !finalVal = loopIn initVal (start + 1)
                VM.unsafeWrite mvec i finalVal
                loopOut (i + 1)

    loopOut 0
    V.unsafeFreeze mvec
{-# INLINE mkReducedColumnBoxed #-}

nestedTypeException ::
    forall a b. (Typeable a, Typeable b) => String -> DataFrameException
nestedTypeException expression = case typeRep @a of
    App t1 t2 ->
        TypeMismatchException
            ( MkTypeErrorContext
                { userType = Left (show (typeRep @b)) :: Either String (TypeRep ())
                , expectedType = Left (show (typeRep @a)) :: Either String (TypeRep ())
                , callingFunctionName = Just "interpretAggregation"
                , errorColumnName = Just expression
                }
            )
    t ->
        TypeMismatchException
            ( MkTypeErrorContext
                { userType = Right (typeRep @(VU.Vector b))
                , expectedType = Right (typeRep @b)
                , callingFunctionName = Just "interpretAggregation"
                , errorColumnName = Just expression
                }
            )

mkTypeMismatchException ::
    (Typeable a, Typeable b) =>
    Maybe String -> Maybe String -> TypeErrorContext a b -> DataFrameException
mkTypeMismatchException callPoint errorLocation context =
    TypeMismatchException
        ( context
            { callingFunctionName = callPoint
            , errorColumnName = errorLocation
            }
        )
