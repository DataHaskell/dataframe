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
import DataFrame.Expression.Operators
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

> <!-- sabela:mime text/plain -->
> | product<br>Int | total<br>Int | orders<br>Int |
> | ---------------|--------------|-------------- |
> | 1              | 220          | 2             |
> | 3              | 70           | 2             |
> | 2              | 70           | 2             |


Reading from files works the same way:


```haskell
fileDf <- D.readCsv "./data/housing.csv"
fileDf <- D.readParquet "./data/mtcars.parquet"

-- Hugging Face datasets (needs network access, via the dataframe-huggingface package):
-- import qualified DataFrame.IO.HuggingFace as HF
-- fileDf <- HF.readParquet "hf://datasets/scikit-learn/iris/default/train/0000.parquet"

D.dimensions fileDf
```

> <!-- sabela:mime text/plain -->
> (32,12)


## Interactive REPL

The `dataframe` REPL comes with all imports pre-loaded. Here's a typical exploration session (each block runs as a cell):


```haskell
df <- D.readCsv "./data/housing.csv"
D.dimensions df
```

> <!-- sabela:mime text/plain -->
> (20640,10)



```haskell
D.describeColumns df |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | Column Name<br>Text | # Non-null Values<br>Int | # Null Values<br>Int | Type<br>Text |
> | --------------------|--------------------------|----------------------|------------- |
> | total_bedrooms      | 20433                    | 207                  | Maybe Double |
> | ocean_proximity     | 20640                    | 0                    | Text         |
> | median_house_value  | 20640                    | 0                    | Double       |
> | median_income       | 20640                    | 0                    | Double       |
> | households          | 20640                    | 0                    | Double       |
> | population          | 20640                    | 0                    | Double       |
> | total_rooms         | 20640                    | 0                    | Double       |
> | housing_median_age  | 20640                    | 0                    | Double       |
> | latitude            | 20640                    | 0                    | Double       |
> | longitude           | 20640                    | 0                    | Double       |


The `:declareColumns` macro (`$(D.declareColumns df)` outside the REPL) generates typed column references from a dataframe, so you can use column names directly in expressions instead of writing `F.col @Double "median_income"` every time:


```haskell
$(D.declareColumns df)
```

> <!-- sabela:mime text/plain -->



```haskell
df |> D.groupBy ["ocean_proximity"]
   |> D.aggregate [F.mean median_house_value `as` "avg_value"]
   |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | ocean_proximity<br>Text | avg_value<br>Double |
> | ------------------------|-------------------- |
> | NEAR BAY                | 259212.31179039303  |
> | NEAR OCEAN              | 249433.97742663656  |
> | INLAND                  | 124805.39200122119  |
> | <1H OCEAN               | 240084.28546409807  |
> | ISLAND                  | 380440.0            |


Create new columns from existing ones:


```haskell
df |> D.derive "rooms_per_household" (total_rooms / households) |> D.take 3 |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | longitude<br>Double | latitude<br>Double | housing_median_age<br>Double | total_rooms<br>Double | total_bedrooms<br>Maybe Double | population<br>Double | households<br>Double | median_income<br>Double | median_house_value<br>Double | ocean_proximity<br>Text | rooms_per_household<br>Double |
> | --------------------|--------------------|------------------------------|-----------------------|--------------------------------|----------------------|----------------------|-------------------------|------------------------------|-------------------------|------------------------------ |
> | -122.23             | 37.88              | 41.0                         | 880.0                 | Just 129.0                     | 322.0                | 126.0                | 8.3252                  | 452600.0                     | NEAR BAY                | 6.984126984126984             |
> | -122.22             | 37.86              | 21.0                         | 7099.0                | Just 1106.0                    | 2401.0               | 1138.0               | 8.3014                  | 358500.0                     | NEAR BAY                | 6.238137082601054             |
> | -122.24             | 37.85              | 52.0                         | 1467.0                | Just 190.0                     | 496.0                | 177.0                | 7.2574                  | 352100.0                     | NEAR BAY                | 8.288135593220339             |


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

> <!-- sabela:mime text/plain -->
> | ocean_proximity<br>Text | avg_value<br>Double |
> | ------------------------|-------------------- |
> | NEAR BAY                | 361441.9354304636   |
> | NEAR OCEAN              | 380041.63071895426  |
> | INLAND                  | 234817.86695906433  |
> | <1H OCEAN               | 333411.75125531096  |


Compare this to the manual version which requires spelling out every column name and type:


```haskell
-- Without TH — every column needs its name and type spelled out
df |> D.derive "rooms_per_household"
        (F.col @Double "total_rooms" / F.col @Double "households")
   |> D.filterWhere (F.col @Double "median_income" .>. F.lit 5)
   |> D.take 5
   |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | longitude<br>Double | latitude<br>Double | housing_median_age<br>Double | total_rooms<br>Double | total_bedrooms<br>Maybe Double | population<br>Double | households<br>Double | median_income<br>Double | median_house_value<br>Double | ocean_proximity<br>Text | rooms_per_household<br>Double |
> | --------------------|--------------------|------------------------------|-----------------------|--------------------------------|----------------------|----------------------|-------------------------|------------------------------|-------------------------|------------------------------ |
> | -122.23             | 37.88              | 41.0                         | 880.0                 | Just 129.0                     | 322.0                | 126.0                | 8.3252                  | 452600.0                     | NEAR BAY                | 6.984126984126984             |
> | -122.22             | 37.86              | 21.0                         | 7099.0                | Just 1106.0                    | 2401.0               | 1138.0               | 8.3014                  | 358500.0                     | NEAR BAY                | 6.238137082601054             |
> | -122.24             | 37.85              | 52.0                         | 1467.0                | Just 190.0                     | 496.0                | 177.0                | 7.2574                  | 352100.0                     | NEAR BAY                | 8.288135593220339             |
> | -122.25             | 37.85              | 52.0                         | 1274.0                | Just 235.0                     | 558.0                | 219.0                | 5.6431000000000004      | 341300.0                     | NEAR BAY                | 5.8173515981735155            |
> | -122.29             | 37.82              | 49.0                         | 135.0                 | Just 29.0                      | 86.0                 | 23.0                 | 6.1183                  | 75000.0                      | NEAR BAY                | 5.869565217391305             |


### Generate a schema type from a CSV

`deriveSchemaFromCsvFile` generates a type synonym for use with the typed API — instead of manually writing out every column name and type:


```haskell
-- Generates:
-- type HousingSchema = '[ '("longitude", Double)
--                       , '("latitude", Double)
--                       , '("total_rooms", Double)
--                       , ...
--                       ]
$(DT.deriveSchemaFromCsvFile "HousingSchema" "./data/housing.csv")
```

> <!-- sabela:mime text/plain -->


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
--     '[ '("order_id", Int64), '("region", Text), '("amount", Double)]
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

> <!-- sabela:mime text/plain -->
> | order_id<br>Int64 | region<br>Text | amount<br>Double |
> | ------------------|----------------|----------------- |
> | 1                 | us             | 10.0             |
> | 2                 | eu             | 20.5             |


The runtime-checked round-trip back to records:


```haskell
D.toRecords ordersDf :: Either Text [Order]
```

> <!-- sabela:mime text/plain -->
> Right [Order {orderId = 1, region = "us", amount = 10.0},Order {orderId = 2, region = "eu", amount = 20.5}]


And the typed bridge — `[Order]` to `TypedDataFrame OrderSchema` and back:


```haskell
DT.thaw (DT.fromRecordsTyped xs :: DT.TypedDataFrame OrderSchema) |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | order_id<br>Int64 | region<br>Text | amount<br>Double |
> | ------------------|----------------|----------------- |
> | 1                 | us             | 10.0             |
> | 2                 | eu             | 20.5             |


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

> <!-- sabela:mime text/plain -->


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

> <!-- sabela:mime text/plain -->


## Typed API

When you want compile-time guarantees that column names exist and types match, wrap your `DataFrame` in a `TypedDataFrame`:


```haskell
type EmployeeSchema =
    '[ '("name", Text)
     , '("department", Text)
     , '("salary", Double)
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

> <!-- sabela:mime text/plain -->
> | name<br>Text | bonus<br>Double |
> | -------------|---------------- |
> | Alice        | 8500.0          |
> | Carol        | 12000.0         |
> | Dave         | 5200.0          |
> | Frank        | 6700.0          |


`DT.freeze` validates the runtime `DataFrame` against your schema once at the boundary. After that, every column access is checked at compile time:


```text
-- Typo in column name -> compile error
tdf |> DT.filterWhere (DT.col @"slary" DT..>. DT.lit 50000)
-- error: Column 'slary' not found in schema

-- Wrong type -> compile error
tdf |> DT.filterWhere (DT.col @"name" DT..>. DT.lit 50000)
-- error: Couldn't match type 'Text' with 'Double'
```


`filterAllJust` goes further — it strips `Maybe` from every column in the schema type, so downstream code can't accidentally treat cleaned columns as nullable:


```haskell
type ScoreSchema = '[ '("name", Text), '("score", Maybe Double)]

scoresDf = D.fromNamedColumns
    [ ("name",  D.fromList ["a", "b", "c" :: Text])
    , ("score", D.fromList [Just 1.0, Nothing, Just 3.0 :: Maybe Double])
    ]

Just stdf = DT.freeze @ScoreSchema scoresDf

-- filterAllJust drops the null row and changes the column type from
-- (Maybe Double) to Double, so `scaled` can multiply it directly.
DT.thaw (DT.filterAllJust stdf |> DT.derive @"scaled" (DT.col @"score" * DT.lit 100)) |> D.toMarkdown'
```

> <!-- sabela:mime text/plain -->
> | name<br>Text | score<br>Double | scaled<br>Double |
> | -------------|-----------------|----------------- |
> | a            | 1.0             | 100.0            |
> | c            | 3.0             | 300.0            |


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
import DataFrame.Internal.Schema (schemaType, makeSchema)

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

> <!-- sabela:mime text/plain -->
> | ocean_proximity<br>Text | median_house_value<br>Double | value_per_income<br>Double |
> | ------------------------|------------------------------|--------------------------- |
> | NEAR BAY                | 452600.0                     | 54365.06029885168          |
> | NEAR BAY                | 358500.0                     | 43185.48678536151          |
> | NEAR BAY                | 352100.0                     | 48515.997464656764         |
> | NEAR BAY                | 341300.0                     | 60480.94132657581          |
> | NEAR BAY                | 75000.0                      | 12258.307046074891         |
> | NEAR BAY                | 262500.0                     | 51554.49064163246          |
> | NEAR BAY                | 327600.0                     | 55908.25312308007          |
> | NEAR BAY                | 347600.0                     | 65748.65703260951          |
> | NEAR BAY                | 366100.0                     | 61467.42780389524          |
> | NEAR BAY                | 373600.0                     | 58895.860264211624         |


The optimizer pushes the filter into the scan, drops unreferenced columns before reading, and stops pulling batches once 1000 rows have been collected.

## Documentation

* User guide: https://dataframe.readthedocs.io/en/latest/
* API reference: https://hackage.haskell.org/package/dataframe/docs/DataFrame.html
* [Coming from pandas, Polars, dplyr, or Frames?](docs/coming_from_other_implementations.md)
* [Cookbook (SQL-style patterns)](docs/cookbook.md)
* [Tutorials](docs/tutorial.md)
* Discord: https://discord.gg/8u8SCWfrNC
