{- |
Module      : DataFrame
Copyright   : (c) 2025
License     : GPL-3.0
Maintainer  : mschavinda@gmail.com
Stability   : experimental
Portability : POSIX

Batteries-included entry point for the DataFrame library.

This module re-exports the most commonly used pieces of the @dataframe@ library so you
can get productive fast in GHCi, IHaskell, or scripts.

__Naming convention__

* Use the @D.@ (\"DataFrame\") prefix for core table operations.
* Use the @F.@ (\"Functions\") prefix for the expression DSL (columns, math, aggregations).

Example session:

We provide a script that imports the core functionality and defines helpful
macros for writing safe code.

@
\$ cabal update
\$ cabal install dataframe
\$ dataframe
Configuring library for fake-package-0...
Warning: No exposed modules
GHCi, version 9.6.7: https:\/\/www.haskell.org\/ghc\/  :? for help
Loaded GHCi configuration from \/tmp\/cabal-repl.-242816\/setcwd.ghci
========================================
              📦Dataframe
========================================

✨  Modules were automatically imported.

💡  Use prefix 'D' for core functionality.
        ● E.g. D.readCsv \"\/path\/to\/file\"
💡  Use prefix 'F' for expression functions.
        ● E.g. F.sum (F.col \@Int \"value\")

✅ Ready.
Loaded GHCi configuration from ./dataframe.ghci
ghci>
@

= Quick start
Load a CSV, select a few columns, filter, derive a column, then group + aggregate:

@
-- 1) Load data
ghci> df0 <- D.readCsv "data/housing.csv"
ghci> D.describeColumns df0
-------------------------------------------------------------------------------------------------------------
    Column Name     | # Non-null Values | # Null Values | # Partially parsed | # Unique Values |     Type
--------------------|-------------------|---------------|--------------------|-----------------|-------------
        Text        |        Int        |      Int      |        Int         |       Int       |     Text
--------------------|-------------------|---------------|--------------------|-----------------|-------------
 ocean_proximity    | 20640             | 0             | 0                  | 5               | Text
 median_house_value | 20640             | 0             | 0                  | 3842            | Double
 median_income      | 20640             | 0             | 0                  | 12928           | Double
 households         | 20640             | 0             | 0                  | 1815            | Double
 population         | 20640             | 0             | 0                  | 3888            | Double
 total_bedrooms     | 20640             | 0             | 0                  | 1924            | Maybe Double
 total_rooms        | 20640             | 0             | 0                  | 5926            | Double
 housing_median_age | 20640             | 0             | 0                  | 52              | Double
 latitude           | 20640             | 0             | 0                  | 862             | Double
 longitude          | 20640             | 0             | 0                  | 844             | Double

-- 2) Project & filter
ghci> :declareColumns df
ghci> df1 = D.filterWhere (ocean_proximity .== \"ISLAND\") df0 D.|> D.select [F.name median_house_value, F.name median_income, F.name ocean_proximity]

-- 3) Add a derived column using the expression DSL
--    (col types are explicit via TypeApplications)
ghci> df2 = D.derive "rooms_per_household" (total_rooms / households) df0

-- 4) Group + aggregate
ghci> import DataFrame.Operators
ghci> let grouped   = D.groupBy ["ocean_proximity"] df0
ghci> let summary   =
         D.aggregate
             [ F.maximum median_house_value \`as\` "max_house_value"]
             grouped
ghci> D.take 5 summary
----------------------------------
 ocean_proximity | max_house_value
-----------------|----------------
      Text       |     Double
-----------------|----------------
 <1H OCEAN       | 500001.0
 INLAND          | 500001.0
 ISLAND          | 450000.0
 NEAR BAY        | 500001.0
 NEAR OCEAN      | 500001.0
@

== Simple operations (cheat sheet)
Most users only need a handful of verbs:

__I/O__

  * @D.readCsv :: FilePath -> IO DataFrame@
  * @D.readTsv :: FilePath -> IO DataFrame@
  * @D.writeCsv :: FilePath -> DataFrame -> IO ()@
  * @D.readParquet :: FilePath -> IO DataFrame@
  * @D.readParquetWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame@

__Exploration__

  * @D.take :: Int -> DataFrame -> DataFrame@
  * @D.takeLast :: Int -> DataFrame -> DataFrame@
  * @D.describeColumns :: DataFrame -> DataFrame@
  * @D.summarize :: DataFrame -> DataFrame@

__Row ops__

  * @D.filter :: Expr a -> (a -> Bool) -> DataFrame -> DataFrame@
  * @D.filterWhere :: Expr Bool -> DataFrame -> DataFrame@
  * @D.sortBy :: SortOrder -> [Text] -> DataFrame -> DataFrame@

__Column ops__

  * @D.select :: [Text] -> DataFrame -> DataFrame@
  * @D.exclude :: [Text] -> DataFrame -> DataFrame@
  * @D.rename :: [(Text,Text)] -> DataFrame -> DataFrame@
  * @D.derive :: Text -> D.Expr a -> DataFrame -> DataFrame@

__Group & aggregate__

  * @D.groupBy :: [Text] -> DataFrame -> GroupedDataFrame@
  * @D.aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame@

__Joins__

  * @D.innerJoin \/ D.leftJoin \/ D.rightJoin \/ D.fullOuterJoin@

== Expression DSL (F.*) at a glance
Columns (typed):

@
F.col \@Text   "ocean_proximity"
F.col \@Double "total_rooms"
F.lit \@Double 1.0
@

Math & comparisons (overloaded by type):

@
(+), (-), (*), (/), abs, log, exp, round
(F.eq), (F.gt), (F.geq), (F.lt), (F.leq)
(.==), (.>), (.>=), (.<), (.<=)
@

Aggregations (for D.'aggregate'):

@
F.count \@a (F.col \@a "c")
F.sum   \@Double (F.col \@Double "x")
F.mean  \@Double (F.col \@Double "x")
F.min   \@t (F.col \@t "x")
F.max   \@t (F.col \@t "x")
@

== REPL power-tool: ':declareColumns'

Use @:declareColumns <df>@ in GHCi/IHaskell to turn each column of a bound 'DataFrame'
into a local binding with the same (mangled if needed) name and the column's concrete
vector type. This is great for quick ad-hoc analysis, plotting, or hand-rolled checks.

@
-- Suppose df has columns: "passengers" :: Int, "fare" :: Double, "payment" :: Text
ghci> :set -XTemplateHaskell
ghci> :declareColumns df

-- Now you have in scope:
ghci> :type passengers
passengers :: Expr Int

ghci> :type fare
fare :: Expr Double

ghci> :type payment
payment :: Expr Text

-- You can use them directly:
ghci> D.derive "fare_with_tip" (fare * 1.2)
@

Notes:

* Name mangling: spaces and non-identifier characters are replaced (e.g. @"trip id"@ -> @trip_id@).
* Optional/nullable columns are exposed as @Expr (Maybe a)@.
-}
module DataFrame (
    -- * Core data structures
    module Dataframe,
    module Column,
    module Row,
    module Expression,

    -- * Operator symbols.
    module Operators,

    -- * Display operations
    module Display,

    -- * Core dataframe operations
    module Core,

    -- * Types
    module Schema,

    -- * I/O
    module CSV,
    module UnstableCSV,
    module Parquet,

    -- * Operations
    module Subset,
    module Transformations,
    module Aggregation,
    module Permutation,
    module Merge,
    module Join,
    module Statistics,

    -- * Errors
    module Errors,

    -- * Plotting
    module Plot,
)
where

import DataFrame.Display as Display (
    DisplayOptions (..),
    defaultDisplayOptions,
    display,
 )
import DataFrame.Display.Terminal.Plot as Plot
import DataFrame.Errors as Errors
import DataFrame.IO.CSV as CSV (
    HeaderSpec (..),
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
    readCsv,
    readCsvWithOpts,
    readSeparated,
    readTsv,
    writeCsv,
    writeSeparated,
 )
import DataFrame.IO.Parquet as Parquet (
    ParquetReadOptions (..),
    defaultParquetReadOptions,
    readParquet,
    readParquetFiles,
    readParquetFilesWithOpts,
    readParquetWithOpts,
 )
import DataFrame.IO.Unstable.CSV as UnstableCSV (
    fastReadCsvUnstable,
    fastReadTsvUnstable,
    readCsvUnstable,
    readTsvUnstable,
 )
import DataFrame.Internal.Column as Column (
    Column,
    fromList,
    fromUnboxedVector,
    fromVector,
    hasElemType,
    hasMissing,
    isNumeric,
    toList,
    toVector,
 )
import DataFrame.Internal.DataFrame as Dataframe (
    DataFrame,
    GroupedDataFrame,
    empty,
    null,
    toMarkdownTable,
 )
import DataFrame.Internal.Expression as Expression (Expr, prettyPrint)
import DataFrame.Internal.Row as Row (
    Any,
    Row,
    fromAny,
    rowValue,
    toAny,
    toRowList,
    toRowVector,
 )
import DataFrame.Internal.Schema as Schema (
    schemaType,
 )
import DataFrame.Operations.Aggregation as Aggregation (
    aggregate,
    distinct,
    groupBy,
 )
import DataFrame.Operations.Core as Core hiding (
    ColumnInfo (..),
    nulls,
    partiallyParsed,
    renameSafe,
 )
import DataFrame.Operations.Join as Join
import DataFrame.Operations.Merge as Merge
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
    take,
    takeLast,
 )
import DataFrame.Operations.Transformations as Transformations
import DataFrame.Operators as Operators
