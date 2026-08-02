{-# LANGUAGE CPP #-}

{- |
Module      : DataFrame
Copyright   : (c) 2024 - 2026 Michael Chavinda
License     : GPL-3.0
Maintainer  : mschavinda@gmail.com
Stability   : experimental
Portability : POSIX

Batteries-included entry point for @dataframe@: re-exports the most commonly
used pieces for GHCi and scripts. Use the @D.@ prefix for core table operations
and @F.@ for the expression DSL.
-}
module DataFrame (
    -- * Core data structures
    module CoreTypes,

    -- * Operator symbols.
    module Operators,

    -- * Display operations
    module Display,

    -- * Core dataframe operations
    module Core,

    -- * Types
    module Schema,
#ifdef WITH_TH
    module SchemaTH,
#endif

    -- * I/O
#ifdef WITH_CSV
    module CSV,
#endif
#ifdef WITH_PARQUET
    module Parquet,
#endif
    module JSON,

    -- * Lazy query engine
#ifdef WITH_LAZY
    module Lazy,
#endif

    -- * Feature synthesis & decision trees
    module Synthesis,
    module DecisionTree,

    -- * Type conversion
    module Typing,

    -- * Operations
    module Subset,
    module Transformations,
    module Aggregation,
    module Permutation,
    module Merge,
    module SetOps,
    module Join,
    module Statistics,

    -- * Errors
    module Errors,

    -- * Record bridge
    module Record,
    module TypedSchema,

    -- * Template Haskell column-binding splices
#ifdef WITH_TH
    module TH,
#endif

    -- * Plotting
    module Plot,
)
where

-- DecisionTree's own `percentile`/`percentiles` are hidden here; the public
-- versions come from Operations.Statistics / DataFrame.Synthesis, avoiding
-- ambiguous-export errors.
import DataFrame.DecisionTree as DecisionTree
import DataFrame.Display as Display (
    DisplayOptions (..),
    defaultDisplayOptions,
    display,
 )
import DataFrame.Display.Terminal.Plot as Plot
import DataFrame.Errors as Errors
#ifdef WITH_CSV
import DataFrame.IO.CSV as CSV (
    HeaderSpec (..),
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
    fromCsv,
    fromCsvBytes,
    readCsv,
    readCsvWithOpts,
    readCsvWithSchema,
    readSeparated,
    readTsv,
    writeCsv,
    writeSeparated,
 )
#endif
import DataFrame.IO.JSON as JSON (
    readJSON,
    readJSONEither,
 )
-- Marks the dataframe-expr-serializer dependency as used (the codec modules are
-- surfaced to consumers via this library's @reexported-modules@).
import DataFrame.Expr.Serialize ()
#ifdef WITH_PARQUET
import DataFrame.IO.Parquet as Parquet (
    ParquetReadOptions (..),
    defaultParquetReadOptions,
    readParquet,
    readParquetFiles,
    readParquetFilesWithOpts,
    readParquetWithOpts,
 )
#endif
import DataFrame.Core as CoreTypes (
    Any,
    Column,
    Columnable,
    Columnable',
    DataFrame,
    Expr,
    GroupedDataFrame,
    NamedExpr,
    Row,
    TruncateConfig (..),
    columnNames,
    defaultTruncateConfig,
    toNamedExpr,
    toUExpr,
    fromUExpr,
    eSize,
    empty,
    fromAny,
    fromList,
    fromNamedColumns,
    fromUnboxedVector,
    fromVector,
    hasElemType,
    hasMissing,
    insertColumn,
    isNumeric,
    mkRandom,
    null,
    prettyPrint,
    prettyPrintWidth,
    rowValue,
    toAny,
    toCsv,
    toCsv',
    toList,
    toMarkdown,
    toMarkdown',
    toRowList,
    toRowVector,
    toSeparated,
    toVector,
 )
import DataFrame.Schema as Schema (
    SchemaType (..),
    makeSchema,
    schemaType,
 )
#ifdef WITH_TH
import DataFrame.Typed.TH.Records as SchemaTH (deriveSchemaFromType, deriveSchemaValues)
#endif
#ifdef WITH_LAZY
-- Re-export only the lazy engine's types and source constructors; its
-- operator surface (filter/select/derive/...) collides with the eager API,
-- so users wanting full lazy access `import qualified DataFrame.Lazy as L`.
import DataFrame.Lazy as Lazy (
    LazyDataFrame,
    fromDataFrame,
    runDataFrame,
    scanCsv,
    scanCsvWith,
    scanParquet,
    scanSeparated,
    scanSeparatedWith,
 )
#endif
import DataFrame.Operations.Aggregation as Aggregation (
    aggregate,
    distinct,
    groupBy,
 )
import DataFrame.Operations.Core as Core
import DataFrame.Operations.Join as Join (
    JoinType (..),
    fullOuterJoin,
    innerJoin,
    join,
    leftJoin,
    rightJoin,
 )
import DataFrame.Operations.Merge as Merge
import DataFrame.Operations.SetOps as SetOps (
    difference,
    intersect,
    symmetricDifference,
    union,
 )
import DataFrame.Operations.Permutation as Permutation (
    SortOrder (..),
    shuffle,
    sortBy,
 )
import DataFrame.Operations.Statistics as Statistics (
    correlation,
    frequencies,
    genericPercentile,
    imputeWith,
    interQuartileRange,
    mean,
    meanMaybe,
    median,
    medianMaybe,
    percentile,
    skewness,
    standardDeviation,
    sum,
    summarize,
    variance,
 )
import DataFrame.Synthesis as Synthesis
import DataFrame.Operations.Subset as Subset (
    SelectionCriteria,
    byIndexRange,
    byName,
    byNameProperty,
    byNameRange,
    byProperty,
    cube,
    drop,
    dropLast,
    exclude,
    filter,
    filterAllJust,
    filterAllNothing,
    filterBy,
    filterJust,
    filterNothing,
    filterWhere,
    kFolds,
    randomSplit,
    range,
    sample,
    select,
    selectBy,
    selectRows,
    stratifiedSample,
    stratifiedSplit,
    take,
    takeLast,
 )
import DataFrame.Operations.Transformations as Transformations (
    apply,
    applyAtIndex,
    applyDouble,
    applyInt,
    applyMany,
    applyWhere,
    derive,
    deriveMany,
    deriveWithExpr,
    impute,
    safeApply,
 )
import DataFrame.Operations.Typing as Typing (
    ParseOptions (..),
    SafeReadMode (..),
    defaultParseOptions,
    effectiveSafeRead,
    parseDefaults,
 )
import DataFrame.Operators as Operators
#ifdef WITH_TH
import DataFrame.TH as TH (
    declareColumns,
#ifdef WITH_CSV_TH
    declareColumnsFromCsvFile,
    declareColumnsFromCsvWithOpts,
#endif
#ifdef WITH_PARQUET_TH
    declareColumnsFromParquetFile,
#endif
    declareColumnsWithPrefix,
    declareColumnsWithPrefix',
 )
#endif
import DataFrame.Typed.Record as Record (
    HasSchema (..),
    fromRecords,
    toRecords,
 )
import DataFrame.Typed.Schema as TypedSchema (schemaColumnNames)
