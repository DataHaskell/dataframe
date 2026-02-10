{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Transformations where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V

import Control.Exception (throw)
import Data.Maybe
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    TypedColumn (..),
    ifoldrColumn,
    imapColumn,
    mapColumn,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), getColumn)
import DataFrame.Internal.Expression
import DataFrame.Internal.Interpreter
import DataFrame.Operations.Core

-- | O(k) Apply a function to a given column in a dataframe.
apply ::
    forall b c.
    (Columnable b, Columnable c) =>
    -- | function to apply
    (b -> c) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    DataFrame
apply f columnName d = case safeApply f columnName d of
    Left (TypeMismatchException context) ->
        throw $ TypeMismatchException (context{callingFunctionName = Just "apply"})
    Left exception -> throw exception
    Right df -> df

-- | O(k) Safe version of the apply function. Returns (instead of throwing) the error.
safeApply ::
    forall b c.
    (Columnable b, Columnable c) =>
    -- | function to apply
    (b -> c) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    Either DataFrameException DataFrame
safeApply f columnName d = case getColumn columnName d of
    Nothing -> Left $ ColumnNotFoundException columnName "apply" (M.keys $ columnIndices d)
    Just column -> do
        column' <- mapColumn f column
        pure $ insertColumn columnName column' d

{- | O(k) Apply a function to an expression in a dataframe and
add the result into `alias` column.
-}
derive :: forall a. (Columnable a) => T.Text -> Expr a -> DataFrame -> DataFrame
derive name expr df = case interpret @a df (normalize expr) of
    Left e -> throw e
    Right (TColumn value) ->
        (insertColumn name value df)
            { derivingExpressions = M.insert name (UExpr expr) (derivingExpressions df)
            }

{- | O(k) Apply a function to an expression in a dataframe and
add the result into `alias` column but

==== __Examples__

>>> (z, df') = deriveWithExpr "z" (F.col @Int "x" + F.col "y") df
>>> filterWhere (z .>= 50)
-}
deriveWithExpr ::
    forall a. (Columnable a) => T.Text -> Expr a -> DataFrame -> (Expr a, DataFrame)
deriveWithExpr name expr df = case interpret @a df (normalize expr) of
    Left e -> throw e
    Right (TColumn value) -> (Col name, insertColumn name value df)

deriveMany :: [NamedExpr] -> DataFrame -> DataFrame
deriveMany exprs df =
    let
        f (name, UExpr (expr :: Expr a)) d =
            case interpret @a df expr of
                Left e -> throw e
                Right (TColumn value) -> insertColumn name value d
     in
        fold f exprs df

-- | O(k * n) Apply a function to given column names in a dataframe.
applyMany ::
    (Columnable b, Columnable c) =>
    (b -> c) ->
    [T.Text] ->
    DataFrame ->
    DataFrame
applyMany f names df = L.foldl' (flip (apply f)) df names

-- | O(k) Convenience function that applies to an int column.
applyInt ::
    (Columnable b) =>
    -- | function to apply
    (Int -> b) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    DataFrame
applyInt = apply

-- | O(k) Convenience function that applies to an double column.
applyDouble ::
    (Columnable b) =>
    -- | function to apply
    (Double -> b) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    DataFrame
applyDouble = apply

{- | O(k * n) Apply a function to a column only if there is another column
value that matches the given criterion.

> applyWhere (<20) "Age" (const "Gen-Z") "Generation" df
-}
applyWhere ::
    forall a b.
    (Columnable a, Columnable b) =>
    -- | Filter condition
    (a -> Bool) ->
    -- | Criterion Column
    T.Text ->
    -- | function to apply
    (b -> b) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    DataFrame
applyWhere condition filterColumnName f columnName df = case getColumn filterColumnName df of
    Nothing ->
        throw $
            ColumnNotFoundException
                filterColumnName
                "applyWhere"
                (M.keys $ columnIndices df)
    Just column -> case ifoldrColumn
        (\i val acc -> if condition val then V.cons i acc else acc)
        V.empty
        column of
        Left e -> throw e
        Right indexes ->
            if V.null indexes
                then df
                else L.foldl' (\d i -> applyAtIndex i f columnName d) df indexes

-- | O(k) Apply a function to the column at a given index.
applyAtIndex ::
    forall a.
    (Columnable a) =>
    -- | Index
    Int ->
    -- | function to apply
    (a -> a) ->
    -- | Column name
    T.Text ->
    -- | DataFrame to apply operation to
    DataFrame ->
    DataFrame
applyAtIndex i f columnName df = case getColumn columnName df of
    Nothing ->
        throw $
            ColumnNotFoundException columnName "applyAtIndex" (M.keys $ columnIndices df)
    Just column -> case imapColumn (\index value -> if index == i then f value else value) column of
        Left e -> throw e
        Right column' -> insertColumn columnName column' df

-- | Replace all instances of `Nothing` in a column with the given value.
impute ::
    forall b.
    (Columnable b) =>
    Expr (Maybe b) ->
    b ->
    DataFrame ->
    DataFrame
impute (Col columnName) value df = case getColumn columnName df of
    Nothing ->
        throw $ ColumnNotFoundException columnName "impute" (M.keys $ columnIndices df)
    Just (OptionalColumn _) -> case safeApply (fromMaybe value) columnName df of
        Left (TypeMismatchException context) -> throw $ TypeMismatchException (context{callingFunctionName = Just "impute"})
        Left exception -> throw exception
        Right res -> res
    _ -> error $ "Cannot impute to a non-Empty column: " ++ T.unpack columnName
impute _ _ df = df
