{-# LANGUAGE DataKinds #-}

{- |
Module      : DataFrame.Typed
Copyright   : (c) 2025
License     : MIT
Maintainer  : mschavinda@gmail.com
Stability   : experimental

A type-safe layer over the @dataframe@ library.

This module provides 'TypedDataFrame', a phantom-typed wrapper around
the untyped 'DataFrame' that tracks column names and types at compile time.
All operations delegate to the untyped core at runtime; the phantom type
is updated at compile time to reflect schema changes.

== Key difference from untyped API: TExpr

All expression-taking operations use 'TExpr' (typed expressions) instead
of raw @Expr@. Column references are validated at compile time:

@
{\-\# LANGUAGE DataKinds, TypeApplications, TypeOperators \#-\}
import qualified DataFrame.Typed as T

type People = '[T.Column \"name\" Text, T.Column \"age\" Int]

main = do
    raw <- D.readCsv \"people.csv\"
    case T.freeze \@People raw of
        Nothing -> putStrLn \"Schema mismatch!\"
        Just df -> do
            let adults = T.filterWhere (T.col \@\"age\" T..>=. T.lit 18) df
            let names  = T.columnAsList \@\"name\" adults  -- :: [Text]
            print names
@

Column references like @T.col \@\"age\"@ are checked at compile time — if the
column doesn't exist or has the wrong type, you get a type error, not a
runtime exception.

== filterAllJust tracks Maybe-stripping

@
df :: TypedDataFrame '[Column \"x\" (Maybe Double), Column \"y\" Int]
T.filterAllJust df :: TypedDataFrame '[Column \"x\" Double, Column \"y\" Int]
@

== Typed aggregation (Option B)

@
result = T.aggregate
    (T.agg \@\"total\" (T.tsum (T.col \@\"salary\"))
   $ T.agg \@\"count\" (T.tcount (T.col \@\"salary\"))
   $ T.aggNil)
    (T.groupBy \@'[\"dept\"] employees)
@
-}
module DataFrame.Typed (
    -- * Core types
    TypedDataFrame,
    Column,
    TypedGrouped,
    These (..),

    -- * Typed expressions
    TExpr (..),
    col,
    lit,
    ifThenElse,
    lift,
    lift2,

    -- * Comparison operators
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),

    -- * Logical operators
    (.&&.),
    (.||.),
    DataFrame.Typed.Expr.not,

    -- * Aggregation expression combinators
    DataFrame.Typed.Expr.sum,
    mean,
    count,
    DataFrame.Typed.Expr.minimum,
    DataFrame.Typed.Expr.maximum,
    collect,

    -- * Typed sort orders
    TSortOrder (..),
    asc,
    desc,

    -- * Named expression helper
    DataFrame.Typed.Expr.as,

    -- * Freeze / thaw boundary
    freeze,
    freezeWithError,
    thaw,
    unsafeFreeze,

    -- * Typed column access
    columnAsVector,
    columnAsList,

    -- * Schema-preserving operations
    filterWhere,
    filter,
    filterBy,
    filterAllJust,
    filterJust,
    filterNothing,
    sortBy,
    take,
    takeLast,
    drop,
    dropLast,
    range,
    cube,
    distinct,
    sample,
    shuffle,

    -- * Schema-modifying operations
    derive,
    impute,
    select,
    exclude,
    rename,
    renameMany,
    insert,
    insertColumn,
    insertVector,
    cloneColumn,
    dropColumn,
    replaceColumn,

    -- * Metadata
    dimensions,
    nRows,
    nColumns,
    columnNames,

    -- * Vertical merge
    append,

    -- * Joins
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,

    -- * GroupBy and Aggregation (Option B)
    groupBy,
    agg,
    aggNil,
    aggregate,
    aggregateUntyped,

    -- * Template Haskell
    deriveSchema,
    deriveSchemaFromCsvFile,

    -- * Schema type families (for advanced use)
    Lookup,
    HasName,
    SubsetSchema,
    ExcludeSchema,
    RenameInSchema,
    RemoveColumn,
    Impute,
    Append,
    Reverse,
    StripAllMaybe,
    StripMaybeAt,
    GroupKeyColumns,
    InnerJoinSchema,
    LeftJoinSchema,
    RightJoinSchema,
    FullOuterJoinSchema,
    AssertAbsent,
    AssertPresent,

    -- * Constraints
    KnownSchema (..),
    AllKnownSymbol (..),

    -- * Pipe operator
    (|>),
) where

import Prelude hiding (drop, filter, take)

import DataFrame.Typed.Access (columnAsList, columnAsVector)
import DataFrame.Typed.Aggregate (
    agg,
    aggNil,
    aggregate,
    aggregateUntyped,
    groupBy,
 )
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (freeze, freezeWithError, thaw, unsafeFreeze)
import DataFrame.Typed.Join (fullOuterJoin, innerJoin, leftJoin, rightJoin)
import DataFrame.Typed.Operations
import DataFrame.Typed.Schema
import DataFrame.Typed.TH (deriveSchema, deriveSchemaFromCsvFile)
import DataFrame.Typed.Types (
    Column,
    TSortOrder (..),
    These (..),
    TypedDataFrame,
    TypedGrouped,
 )
