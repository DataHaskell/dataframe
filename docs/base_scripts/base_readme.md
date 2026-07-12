<!--
  This file is the runnable scripths source for the project README.
  Every ```haskell block below executes in order in a single shared session
  (later blocks see bindings from earlier blocks), and scripths inserts each
  block's output beneath it as a blockquote.

  Regenerate the rendered README with:
      scripths docs/base_scripts/base_readme.md -o README.md
  (run from the repo root, so the ./data/... paths resolve).

  Blocks tagged ```text are intentional compile-error demos and are NOT run.
-->
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

> **This README is a runnable [scripths](https://github.com/DataHaskell/scripths) notebook.** Every Haskell block runs top-to-bottom in one shared session against the datasets in [`./data`](./data). Reproduce every output below with `scripths docs/base_scripts/base_readme.md -o README.md` from the repo root.

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

Group sales by product and compute totals. The first block carries the
`scripths` cabal directives and the imports shared by the rest of the document;
you can also drop the same code into an `Example.hs` and run it with
`cabal run Example.hs` after adding a `#!/usr/bin/env cabal` header.

```haskell
-- cabal: build-depends: dataframe, text
-- cabal: default-extensions: OverloadedStrings, TypeApplications, TemplateHaskell, DataKinds, TypeFamilies, FlexibleInstances, FlexibleContexts, ScopedTypeVariables, DeriveGeneric, UndecidableInstances
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Typed as DT
import DataFrame.Operators
import Data.Text (Text)
import Data.Int (Int64)

sales = D.fromNamedColumns
    [ ("product", D.fromList [1, 1, 2, 2, 3, 3 :: Int])
    , ("amount",  D.fromList [100, 120, 50, 20, 40, 30 :: Int])
    ]

-- Group by product and compute totals
sales
    |> D.groupBy ["product"]
    |> D.aggregate [ F.sum (F.col @Int "amount") `as` "total"
                   , F.count (F.col @Int "amount") `as` "orders"
                   ]
    |> D.toMarkdown'
```

Reading from files works the same way:

```haskell
fileDf <- D.readCsv "./data/housing.csv"
fileDf <- D.readParquet "./data/mtcars.parquet"

-- Hugging Face datasets (needs network access, via the dataframe-huggingface package):
-- import qualified DataFrame.IO.HuggingFace as HF
-- fileDf <- HF.readParquet "hf://datasets/scikit-learn/iris/default/train/0000.parquet"

D.dimensions fileDf
```

## Interactive REPL

The `dataframe` REPL comes with all imports pre-loaded. Here's a typical exploration session (each block runs as a cell):

```haskell
df <- D.readCsv "./data/housing.csv"
D.dimensions df
```

```haskell
D.describeColumns df |> D.toMarkdown'
```

The `:declareColumns` macro (`$(D.declareColumns df)` outside the REPL) generates typed column references from a dataframe, so you can use column names directly in expressions instead of writing `F.col @Double "median_income"` every time:

```haskell
$(D.declareColumns df)
```

```haskell
df |> D.groupBy ["ocean_proximity"]
   |> D.aggregate [F.mean median_house_value `as` "avg_value"]
   |> D.toMarkdown'
```

Create new columns from existing ones:

```haskell
df |> D.derive "rooms_per_household" (total_rooms / households) |> D.take 3 |> D.toMarkdown'
```

Type mismatches are caught as compile errors — adding a `Double` column to a `Text` column won't silently produce garbage:

```text
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

`declareColumnsFromCsvFile` (in `DataFrame.TH`, also re-exported from `DataFrame`)
reads your CSV at compile time and generates typed `Expr` bindings for every column:

```haskell
-- Reads housing.csv at compile time and generates:
--   latitude :: Expr Double
--   total_rooms :: Expr Double
--   ocean_proximity :: Expr Text
--   ... one binding per column
$(D.declareColumnsFromCsvFile "./data/housing.csv")

df <- D.readCsv "./data/housing.csv"
df |> D.derive "rooms_per_household" (total_rooms / households)
   |> D.filterWhere (median_income .>. 5)
   |> D.groupBy ["ocean_proximity"]
   |> D.aggregate [F.mean median_house_value `as` "avg_value"]
   |> D.toMarkdown'
```

Compare this to the manual version which requires spelling out every column name and type:

```haskell
-- Without TH — every column needs its name and type spelled out
df |> D.derive "rooms_per_household"
        (F.col @Double "total_rooms" / F.col @Double "households")
   |> D.filterWhere (F.col @Double "median_income" .>. F.lit 5)
   |> D.take 5
   |> D.toMarkdown'
```

### Generate a schema type from a CSV

`deriveSchemaFromCsvFile` generates a type synonym for use with the typed API — instead of manually writing out every column name and type:

```haskell
-- Generates:
-- type HousingSchema = '[ DT.Column "longitude" Double
--                       , DT.Column "latitude" Double
--                       , DT.Column "total_rooms" Double
--                       , ...
--                       ]
$(DT.deriveSchemaFromCsvFile "HousingSchema" "./data/housing.csv")
```

### Generate a schema (and a row bridge) from a record ADT

When the canonical row shape lives in your code as a Haskell record,
`deriveSchemaFromType` produces both the typed schema and a `HasSchema`
instance that converts between `[Order]` and a `DataFrame` (or
`TypedDataFrame OrderSchema`) at runtime:

```haskell
data Order = Order
    { orderId :: Int64
    , region  :: Text
    , amount  :: Double
    } deriving (Show, Eq)

$(DT.deriveSchemaFromType ''Order)
-- expands to:
--   type OrderSchema =
--     '[DT.Column "order_id" Int64, DT.Column "region" Text, DT.Column "amount" Double]
--   instance DT.HasSchema Order where
--     type Schema Order = OrderSchema
--     toColumns   = ...
--     fromColumns = ...

xs :: [Order]
xs = [Order 1 "us" 10.0, Order 2 "eu" 20.5]

-- Untyped: [Order] -> DataFrame
ordersDf :: D.DataFrame
ordersDf = D.fromRecords xs

ordersDf |> D.toMarkdown'
```

The runtime-checked round-trip back to records:

```haskell
D.toRecords ordersDf :: Either Text [Order]
```

And the typed bridge — `[Order]` to `TypedDataFrame OrderSchema` and back:

```haskell
DT.thaw (DT.fromRecordsTyped xs :: DT.TypedDataFrame OrderSchema) |> D.toMarkdown'
```

Field names are translated `camelCase → snake_case` by default; override
the translation with `deriveSchemaFromTypeWith
defaultSchemaOptions{nameTransform = id}` (or any `String -> String`).

If all you need is a runtime `Schema` to drive `readCsvWithSchema` (no
typed-dataframe machinery), there's a companion splice in
`DataFrame.Internal.Schema` (re-exported from `DataFrame`):

```haskell
$(D.deriveSchema ''Order)
-- emits:
--   orderSchema     :: Schema
--   orderSchema     = makeSchema [("order_id", schemaType @Int64), ...]
--   orderOrderId    :: Expr Int64
--   orderOrderId    = col "order_id"
--   orderRegion     :: Expr Text
--   orderRegion     = col "region"
--   orderAmount     :: Expr Double
--   orderAmount     = col "amount"

orders :: IO D.DataFrame
orders = do
    raw <- D.readCsvWithSchema orderSchema "./data/orders.csv"
    pure (D.filter orderAmount (> 100) raw)
```

Each record field gets a typed accessor named `<lower-first TyConName><UpperFirst FieldName>`,
so `data Order { customerId :: Int }` yields `orderCustomerId :: Expr Int = col "customer_id"`.
That's the same shape as `$(D.declareColumns df)` produces from a runtime
`DataFrame`, but driven off the ADT instead of an existing frame.

If you'd rather not depend on Template Haskell, the same schema is
available via `GHC.Generics` (shown here on an equivalent record):

```haskell
import GHC.Generics (Generic)
import DataFrame.Typed (Schema)

data OrderG = OrderG
    { orderGId :: Int64
    , regionG  :: Text
    , amountG  :: Double
    } deriving (Generic)

type OrderGSchema = DT.SchemaOf OrderG

instance DT.HasSchema OrderG where
    type Schema OrderG = OrderGSchema
    toColumns   = DT.genericToColumns
    fromColumns = DT.genericFromColumns
```

## Typed API

When you want compile-time guarantees that column names exist and types match, wrap your `DataFrame` in a `TypedDataFrame`:

```haskell
type EmployeeSchema =
    '[ DT.Column "name"       Text
     , DT.Column "department" Text
     , DT.Column "salary"     Double
     ]

employees <- D.readCsv "./data/employees.csv"
case DT.freeze @EmployeeSchema employees of
    Nothing  -> "Schema mismatch!"
    Just tdf -> tdf
        |> DT.derive @"bonus" (DT.col @"salary" * DT.lit 0.1)
        |> DT.filterWhere (DT.col @"salary" DT..>. DT.lit 50000)
        |> DT.select @'["name", "bonus"]
        |> DT.thaw
        |> D.toMarkdown'
```

`DT.freeze` validates the runtime `DataFrame` against your schema once at the boundary. After that, every column access is checked at compile time:

```text
-- Typo in column name -> compile error
tdf |> DT.filterWhere (DT.col @"slary" DT..>. DT.lit 50000)
-- error: Column "slary" not found in schema

-- Wrong type -> compile error
tdf |> DT.filterWhere (DT.col @"name" DT..>. DT.lit 50000)
-- error: Couldn't match type 'Text' with 'Double'
```

`filterAllJust` goes further — it strips `Maybe` from every column in the schema type, so downstream code can't accidentally treat cleaned columns as nullable:

```haskell
type ScoreSchema = '[ DT.Column "name" Text, DT.Column "score" (Maybe Double) ]

scoresDf = D.fromNamedColumns
    [ ("name",  D.fromList ["a", "b", "c" :: Text])
    , ("score", D.fromList [Just 1.0, Nothing, Just 3.0 :: Maybe Double])
    ]

Just stdf = DT.freeze @ScoreSchema scoresDf

-- filterAllJust drops the null row and changes the column type from
-- (Maybe Double) to Double, so `scaled` can multiply it directly.
DT.thaw (DT.filterAllJust stdf |> DT.derive @"scaled" (DT.col @"score" * DT.lit 100)) |> D.toMarkdown'
```

## Features

**I/O**: CSV, TSV, Parquet (Snappy, ZSTD, Gzip), JSON. Read Parquet from Hugging Face datasets (`hf://` URIs) via the `dataframe-huggingface` package. Column projection and predicate pushdown for Parquet reads.

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
import DataFrame.Schema (schemaType, makeSchema)

housingSchema = makeSchema
    [ ("longitude",          schemaType @Double)
    , ("latitude",           schemaType @Double)
    , ("housing_median_age", schemaType @Double)
    , ("total_rooms",        schemaType @Double)
    , ("total_bedrooms",     schemaType @(Maybe Double))
    , ("population",         schemaType @Double)
    , ("households",         schemaType @Double)
    , ("median_income",      schemaType @Double)
    , ("median_house_value", schemaType @Double)
    , ("ocean_proximity",    schemaType @Text)
    ]

lazyResult <- L.runDataFrame $
    L.scanCsv housingSchema "./data/housing.csv"
    |> L.filter  (F.col @Double "median_income" .>. F.lit 5)
    |> L.derive  "value_per_income"
                 (F.col @Double "median_house_value" / F.col @Double "median_income")
    |> L.select  ["ocean_proximity", "median_house_value", "value_per_income"]
    |> L.take 1000

D.take 10 lazyResult |> D.toMarkdown'
```

The optimizer pushes the filter into the scan, drops unreferenced columns before reading, and stops pulling batches once 1000 rows have been collected.

## Documentation

* User guide: https://dataframe.readthedocs.io/en/latest/
* API reference: https://hackage.haskell.org/package/dataframe/docs/DataFrame.html
* [Coming from pandas, Polars, dplyr, or Frames?](docs/coming_from_other_implementations.md)
* [Cookbook (SQL-style patterns)](docs/cookbook.md)
* [Tutorials](docs/tutorial.md)
* Discord: https://discord.gg/8u8SCWfrNC
