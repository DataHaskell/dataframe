<h1 align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">
    <img width="100" height="100" src="https://raw.githubusercontent.com/mchav/dataframe/master/docs/_static/haskell-logo.svg" alt="dataframe logo">
  </a>
</h1>

<div align="center">
  <a href="https://hackage.haskell.org/package/dataframe">
    <img src="https://img.shields.io/hackage/v/dataframe" alt="hackage Latest Release"/>
  </a>
  <a href="https://github.com/mchav/dataframe/actions/workflows/haskell-ci.yml">
    <img src="https://github.com/mchav/dataframe/actions/workflows/haskell-ci.yml/badge.svg" alt="C/I"/>
  </a>
</div>

<p align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">User guide</a>
  |
  <a href="https://discord.gg/8u8SCWfrNC">Discord</a>
</p>

# DataFrame

Tabular data analysis in Haskell. Read CSV, Parquet, and JSON files, transform columns with a typed expression DSL, and optionally lock down your entire schema at the type level for compile-time safety.

The library ships three API layers — all operating on the same underlying `DataFrame` type at runtime:

- **Untyped** (`import qualified DataFrame as D`) — string-based column names, great for exploration and scripting.
- **Typed** (`import qualified DataFrame.Typed as T`) — phantom-type schema tracking with compile-time column validation.
- **Monadic API** — write your transformation as a self contained pipeline.

## Why this library?

* Concise, declarative, composable data pipelines using the `|>` pipe operator.
* Choose your level of type safety: keep it lightweight for quick analysis, or lock it down for production pipelines.
* High performance from Haskell's optimizing compiler and an efficient columnar memory model with bitmap-backed nullability.
* Designed for interactivity: a custom REPL, IHaskell notebook support, terminal and web plotting, and helpful error messages.

## Install

```bash
cabal update
cabal install dataframe
```

To use as a dependency in a project:

```
build-depends: base >= 4, dataframe
```

Works with GHC 9.4 through 9.12. A custom REPL with all imports pre-loaded is available after installing:

```bash
dataframe
```

## Quick Start

Save this as `Example.hs` and run with `cabal run Example.hs`:

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operators

main :: IO ()
main = do
    let sales = D.fromNamedColumns
            [ ("product", D.fromList [1, 1, 2, 2, 3, 3 :: Int])
            , ("amount",  D.fromList [100, 120, 50, 20, 40, 30 :: Int])
            ]

    -- Group by product and compute totals
    print $ sales
        |> D.groupBy ["product"]
        |> D.aggregate [ F.sum (F.col @Int "amount") `as` "total"
                       , F.count (F.col @Int "amount") `as` "orders"
                       ]
```

```
-----------------------
product | total | orders
--------|-------|-------
  Int   |  Int  |  Int
--------|-------|-------
1       | 220   | 2
2       | 70    | 2
3       | 70    | 2
```

Reading from files works the same way:

```haskell
df <- D.readCsv "data.csv"
df <- D.readParquet "data.parquet"

-- Hugging Face datasets
df <- D.readParquet "hf://datasets/scikit-learn/iris/default/train/0000.parquet"
```

## Interactive REPL

The `dataframe` REPL comes with all imports pre-loaded. Here's a typical exploration session:

```haskell
dataframe> df <- D.readCsv "./data/housing.csv"
dataframe> D.dimensions df
(20640, 10)

dataframe> D.describeColumns df
------------------------------------------------------------------------
    Column Name     | ## Non-null Values | ## Null Values |     Type
--------------------|--------------------|----------------|-------------
        Text        |         Int        |      Int       |     Text
--------------------|--------------------|----------------|-------------
 total_bedrooms     | 20433              | 207            | Maybe Double
 ocean_proximity    | 20640              | 0              | Text
 median_house_value | 20640              | 0              | Double
 median_income      | 20640              | 0              | Double
 households         | 20640              | 0              | Double
 population         | 20640              | 0              | Double
 total_rooms        | 20640              | 0              | Double
 housing_median_age | 20640              | 0              | Double
 latitude           | 20640              | 0              | Double
 longitude          | 20640              | 0              | Double
```

The `:declareColumns` macro generates typed column references from a dataframe, so you can use column names directly in expressions instead of writing `F.col @Double "median_income"` every time:

```haskell
dataframe> :declareColumns df
"longitude :: Expr Double"
"latitude :: Expr Double"
"housing_median_age :: Expr Double"
"total_rooms :: Expr Double"
"total_bedrooms :: Expr (Maybe Double)"
"population :: Expr Double"
"households :: Expr Double"
"median_income :: Expr Double"
"median_house_value :: Expr Double"
"ocean_proximity :: Expr Text"

dataframe> df |> D.groupBy ["ocean_proximity"]
              |> D.aggregate [F.mean median_house_value `as` "avg_value"]
-------------------------------------
 ocean_proximity |     avg_value
-----------------|-------------------
      Text       |       Double
-----------------|-------------------
 <1H OCEAN       | 240084.28546409807
 INLAND          | 124805.39200122119
 ISLAND          | 380440.0
 NEAR BAY        | 259212.31179039303
 NEAR OCEAN      | 249433.97742663656
```

Create new columns from existing ones:

```haskell
dataframe> df |> D.derive "rooms_per_household" (total_rooms / households) |> D.take 3
-----------------------------------------------------------------------------------------------------------------
 longitude | latitude | housing_median_age | total_rooms | ... | ocean_proximity | rooms_per_household
-----------|----------|--------------------|-------------|-----|-----------------|--------------------
  Double   |  Double  |       Double       |   Double    | ... |      Text       |       Double
-----------|----------|--------------------|-------------|-----|-----------------|--------------------
 -122.23   | 37.88    | 41.0               | 880.0       | ... | NEAR BAY        | 6.984126984126984
 -122.22   | 37.86    | 21.0               | 7099.0      | ... | NEAR BAY        | 6.238137082601054
 -122.24   | 37.85    | 52.0               | 1467.0      | ... | NEAR BAY        | 8.288135593220339
```

Type mismatches are caught as compile errors — adding a `Double` column to a `Text` column won't silently produce garbage:

```haskell
dataframe> df |> D.derive "nonsense" (latitude + ocean_proximity)

<interactive>:14:47: error: [GHC-83865]
    • Couldn't match type 'Text' with 'Double'
        Expected: Expr Double
          Actual: Expr Text
    • In the second argument of '(+)', namely 'ocean_proximity'
      In the second argument of 'derive', namely
        '(latitude + ocean_proximity)'
```

## Template Haskell

For scripts and projects, Template Haskell can generate column bindings at compile time.

### Generate column references from a CSV

`declareColumnsFromCsvFile` reads your CSV at compile time and generates typed `Expr` bindings for every column:

```haskell
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Operators

-- Reads housing.csv at compile time and generates:
--   latitude :: Expr Double
--   total_rooms :: Expr Double
--   ocean_proximity :: Expr Text
--   ... one binding per column
$(F.declareColumnsFromCsvFile "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    print $ df
        |> D.derive "rooms_per_household" (total_rooms / households)
        |> D.filterWhere (median_income .>. 5)
        |> D.groupBy ["ocean_proximity"]
        |> D.aggregate [F.mean median_house_value `as` "avg_value"]
```

Compare this to the manual version which requires spelling out every column name and type:

```haskell
-- Without TH — every column needs its name and type spelled out
df |> D.derive "rooms_per_household"
        (F.col @Double "total_rooms" / F.col @Double "households")
   |> D.filterWhere (F.col @Double "median_income" .>. F.lit 5)
```

### Generate a schema type from a CSV

`deriveSchemaFromCsvFile` generates a type synonym for use with the typed API — instead of manually writing out every column name and type:

```haskell
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DataKinds #-}

import qualified DataFrame.Typed as T

-- Generates:
-- type HousingSchema = '[ T.Column "longitude" Double
--                        , T.Column "latitude" Double
--                        , T.Column "total_rooms" Double
--                        , ...
--                        ]
$(T.deriveSchemaFromCsvFile "HousingSchema" "./data/housing.csv")
```

## Typed API

When you want compile-time guarantees that column names exist and types match, wrap your `DataFrame` in a `TypedDataFrame`:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified DataFrame as D
import qualified DataFrame.Typed as T
import Data.Text (Text)
import DataFrame.Operators

type EmployeeSchema =
    '[ T.Column "name"       Text
     , T.Column "department" Text
     , T.Column "salary"     Double
     ]

main :: IO ()
main = do
    df <- D.readCsv "employees.csv"
    case T.freeze @EmployeeSchema df of
        Nothing  -> putStrLn "Schema mismatch!"
        Just tdf -> do
            let result = tdf
                    |> T.derive @"bonus" (T.col @"salary" * T.lit 0.1)
                    |> T.filterWhere (T.col @"salary" .>. T.lit 50000)
                    |> T.select @'["name", "bonus"]
            print (T.thaw result)
```

`T.freeze` validates the runtime `DataFrame` against your schema once at the boundary. After that, every column access is checked at compile time:

```haskell
-- Typo in column name → compile error
tdf |> T.filterWhere (T.col @"slary" .>. T.lit 50000)
-- error: Column "slary" not found in schema

-- Wrong type → compile error
tdf |> T.filterWhere (T.col @"name" .>. T.lit 50000)
-- error: Couldn't match type 'Text' with 'Double'
```

`filterAllJust` goes further — it strips `Maybe` from every column in the schema type, so downstream code can't accidentally treat cleaned columns as nullable:

```haskell
-- Before: TypedDataFrame '[Column "score" (Maybe Double), Column "name" Text]
let cleaned = T.filterAllJust tdf
-- After:  TypedDataFrame '[Column "score" Double, Column "name" Text]

cleaned |> T.derive @"scaled" (T.col @"score" * T.lit 100)
```

## Features

**I/O**: CSV, TSV, Parquet (Snappy, ZSTD, Gzip), JSON. Read Parquet from HTTP URLs and Hugging Face datasets (`hf://` URIs). Column projection and predicate pushdown for Parquet reads.

**Operations**: filter, select, derive, groupBy, aggregate, joins (inner, left, right, full outer), sort, sample, stratified sample, distinct, k-fold splits.

**Expressions**: typed column references (`F.col @Double "x"`), arithmetic, comparisons, logical operators, nullable-aware three-valued logic (`.==`, `.&&`), string matching (`like`, `regex`), casting, and user-defined functions via `lift`/`lift2`.

**Statistics**: mean, median, mode, variance, standard deviation, percentiles, inter-quartile range, correlation, skewness, frequency tables, imputation.

**Plotting**: terminal plots (histogram, scatter, line, bar, box, pie, heatmap, stacked bar, correlation matrix) and interactive HTML plots.

**Lazy engine**: streaming query execution for files that don't fit in memory. Rule-based optimizer with filter fusion, predicate pushdown, and dead column elimination. Pull-based executor with configurable batch sizes.

**Interop**: Arrow C Data Interface for zero-copy round-trips with Python and Polars.

**ML**: decision trees (TAO algorithm), feature synthesis, k-fold cross-validation, stratified sampling.

**Notebooks**: IHaskell integration with [pre-built Binder examples](https://mybinder.org/v2/gh/mchav/ihaskell-dataframe/HEAD).

## Lazy Queries

For files too large to fit in memory, `DataFrame.Lazy` provides a streaming query engine. Declare a schema, build a query plan with the same familiar operations, and `runDataFrame` runs it through an optimizer before streaming results batch-by-batch:

```haskell
import qualified DataFrame.Lazy as L
import qualified DataFrame.Functions as F
import DataFrame.Operators
import DataFrame.Internal.Schema (Schema, schemaType)
import Data.Text (Text)

mySchema :: Schema
mySchema = [ ("name",   schemaType @Text)
           , ("weight", schemaType @Double)
           , ("height", schemaType @Double)
           ]

main :: IO ()
main = do
    result <- L.runDataFrame $
        L.scanCsv mySchema "large_file.csv"
        |> L.filter  (F.col @Double "height" .>. F.lit 1.7)
        |> L.select  ["name", "weight", "height"]
        |> L.derive  "bmi" (F.col @Double "weight"
                           / (F.col @Double "height" * F.col @Double "height"))
        |> L.take 1000
    print result
```

The optimizer pushes the filter into the scan, drops unreferenced columns before reading, and stops pulling batches once 1000 rows have been collected.

## Documentation

* User guide: https://dataframe.readthedocs.io/en/latest/
* API reference: https://hackage.haskell.org/package/dataframe/docs/DataFrame.html
* [Coming from pandas, Polars, dplyr, or Frames?](docs/coming_from_other_implementations.md)
* [Cookbook (SQL-style patterns)](docs/cookbook.md)
* [Tutorials](docs/tutorial.md)
* Discord: https://discord.gg/8u8SCWfrNC
