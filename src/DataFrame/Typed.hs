{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

{- |
Module      : DataFrame.Typed
Copyright   : (c) 2024 - 2026 Michael Chavinda
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

type People = '[ '(\"name\", Text), '(\"age\", Int)]

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
df :: TypedDataFrame '[ '(\"x\", Maybe Double), '(\"y\", Int)]
T.filterAllJust df :: TypedDataFrame '[ '(\"x\", Double), '(\"y\", Int)]
@

== Typed aggregation

@
result = T.aggregate
    ( T.as \@\"total\" (T.sum   (T.col \@\"salary\"))
    . T.as \@\"count\" (T.count (T.col \@\"salary\"))
    )
    (T.groupBy \@'[\"dept\"] employees)
@
-}
module DataFrame.Typed (
    -- * Core types
    TypedDataFrame,
    TypedGrouped,
    These (..),

    -- * Typed expressions
    TExpr (..),
    col,
    lit,
    ifThenElse,
    lift,
    lift2,
    nullLift,
    nullLift2,

    -- * Same-type comparison operators
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),

    -- * Nullable-aware arithmetic operators
    (.+),
    (.-),
    (.*),
    (./),

    -- * Nullable-aware comparison operators (three-valued logic)
    (.==),
    (./=),
    (.<),
    (.<=),
    (.>=),
    (.>),

    -- * Logical operators
    (.&&.),
    (.||.),
    DataFrame.Typed.Expr.not,

    -- * Aggregation expression combinators
    DataFrame.Typed.Expr.sum,
    mean,
    median,
    count,
    countAll,
    DataFrame.Typed.Expr.minimum,
    DataFrame.Typed.Expr.maximum,
    collect,
    over,

    -- * Expression combinators (full DataFrame.Functions parity)
    DataFrame.Typed.Expr.div,
    DataFrame.Typed.Expr.mod,
    mode,
    sumMaybe,
    DataFrame.Typed.Expr.meanMaybe,
    DataFrame.Typed.Expr.variance,
    DataFrame.Typed.Expr.medianMaybe,
    DataFrame.Typed.Expr.percentile,
    stddev,
    stddevMaybe,
    zScore,
    pow,
    relu,
    DataFrame.Typed.Expr.min,
    DataFrame.Typed.Expr.max,
    reduce,
    toMaybe,
    fromMaybe,
    isJust,
    isNothing,
    fromJust,
    whenPresent,
    whenBothPresent,
    recode,
    recodeWithCondition,
    recodeWithDefault,
    firstOrNothing,
    lastOrNothing,
    splitOn,
    match,
    matchAll,
    parseDate,
    daysBetween,
    bind,

    -- * Cast / coercion expressions
    castExpr,
    castExprWithDefault,
    castExprEither,
    unsafeCastExpr,
    toDouble,

    -- * Typed sort orders
    TSortOrder (..),
    asc,
    desc,

    -- * Freeze / thaw boundary
    freeze,
    freezeWithError,
    thaw,
    unsafeFreeze,

    -- * Typed column access
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,

    -- * Schema-preserving operations
    filterWhere,
    filter,
    filterBy,
    filterAllJust,
    filterJust,
    filterNothing,
    filterAllNothing,
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

    -- * Set algebra (topos operations)
    union,
    intersect,
    difference,
    symmetricDifference,

    -- * Joins
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,

    -- * GroupBy and Aggregation
    groupBy,
    as,
    aggregate,
    aggregateUntyped,

    -- * Column transformations
    applyColumn,
    applyMany,
    applyWhere,
    applyAtIndex,
    safeApply,
    deriveWithExpr,
    insertWithDefault,
    insertVectorWithDefault,
    insertUnboxedVector,
    (|||),

    -- * Sampling and splitting
    randomSplit,
    kFolds,
    selectRows,
    stratifiedSample,
    stratifiedSplit,

    -- * Frequencies
    valueCounts,
    valueProportions,

#ifdef WITH_TH
    -- * Template Haskell
    deriveSchema,
#ifdef WITH_CSV_TH
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
#endif
#ifdef WITH_PARQUET_TH
    deriveSchemaFromParquetFile,
#endif
    deriveSchemaFromType,
    deriveSchemaFromTypeWith,
    SchemaOptions (..),
    defaultSchemaOptions,
#endif

    -- * Record bridge (ADT <-> TypedDataFrame)
    HasSchema (..),
    fromRecordsTyped,
    toRecordsTyped,

    -- * Generics opt-in for schema derivation
    SchemaOf,
    SchemaOfRaw,
    NameCase (..),
    genericToColumns,
    genericFromColumns,

    -- * Schema type families (for advanced use)
    Lookup,
    SafeLookup,
    HasName,
    SubsetSchema,
    ExcludeSchema,
    RenameInSchema,
    RenameManyInSchema,
    RemoveColumn,
    Impute,
    SetColumnType,
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
    AssertAllPresent,
    AssertPresent,
    AssertDisjoint,
    AssertRealColumn,
    AllColumnsReal,
    IsRealType,

    -- * Constraints
    KnownSchema (..),
    schemaColumnNames,
    AllKnownSymbol (..),
) where

import Prelude hiding (drop, filter, take)

import DataFrame.Typed.Access (
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsIntVector,
    columnAsList,
    columnAsUnboxedVector,
    columnAsVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,
 )
import DataFrame.Typed.Aggregate (
    aggregate,
    aggregateUntyped,
    as,
    groupBy,
 )
import DataFrame.Typed.Apply (
    applyAtIndex,
    applyColumn,
    applyMany,
    applyWhere,
    deriveWithExpr,
    insertUnboxedVector,
    insertVectorWithDefault,
    insertWithDefault,
    safeApply,
    (|||),
 )
import DataFrame.Typed.Sampling (
    kFolds,
    randomSplit,
    selectRows,
    stratifiedSample,
    stratifiedSplit,
 )
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (freeze, freezeWithError, thaw, unsafeFreeze)
import DataFrame.Typed.Generic (
    NameCase (..),
    SchemaOf,
    SchemaOfRaw,
    genericFromColumns,
    genericToColumns,
 )
import DataFrame.Typed.Join (fullOuterJoin, innerJoin, leftJoin, rightJoin)
import DataFrame.Typed.Operations
import DataFrame.Typed.Record (
    HasSchema (..),
    fromRecordsTyped,
    toRecordsTyped,
 )
import DataFrame.Typed.Schema
#ifdef WITH_TH
import DataFrame.Typed.TH (
    SchemaOptions (..),
    defaultSchemaOptions,
    deriveSchema,
#ifdef WITH_CSV_TH
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
#endif
#ifdef WITH_PARQUET_TH
    deriveSchemaFromParquetFile,
#endif
    deriveSchemaFromType,
    deriveSchemaFromTypeWith,
 )
#endif
import DataFrame.Typed.Types (
    TSortOrder (..),
    These (..),
    TypedDataFrame,
    TypedGrouped,
 )
