# Haskell for Data Analysis

This guide ports and extends Wes McKinney's [Python for Data Analysis](https://wesmckinney.com/book/). Examples and organisation are drawn from there. No prior Haskell knowledge is assumed.

```haskell
-- cabal: build-depends: dataframe, text, granite
-- cabal: default-extensions: TemplateHaskell, TypeApplications, OverloadedStrings, DataKinds
:set -package granite
:set -package dataframe
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified Data.Text.IO as TIO
import qualified Data.Text as T
import Data.Text (Text)
import Granite.Svg
import DataFrame.Operators
import DataFrame.Operations.Merge ()
```

---

## Chapter 1: Getting Started

### What is a dataframe?

A DataFrame is like a spreadsheet or a table — it organises data into rows and columns.

* Each **column** has a name (like "Name", "Age", or "Price") and holds the same kind of data (numbers, text, dates…).
* Each **row** is one record — a person, a product, a day of sales.

| Name  | Age | City      |
|-------|-----|-----------|
| Alice | 30  | New York  |
| Bob   | 25  | San Diego |
| Cara  | 35  | Austin    |

That is essentially a DataFrame.

DataFrames make it easy to look at your data, filter or sort it, do maths on it, and clean it up. They are the bread-and-butter tool for data scientists and analysts. This guide shows how to use them in Haskell.

### Why Haskell?

* Types catch many bugs before you even run the code.
* Pipelines are easy to write and read.
* The GHC compiler optimises aggressively — Haskell code is fast.
* The syntax is more approachable than other compiled languages' dataframe libraries.

### Installing the tooling

Check the [README](https://github.com/mchav/dataframe?tab=readme-ov-file#installing) for installation instructions.

---

## Chapter 2: Getting Your Data In

Data enters a program in one of two ways: you type it in by hand, or you read it from a file.

### Entering data manually

Suppose you want to track a week of Seattle temperatures (a legitimate non-small-talk topic there). The dataset is small enough to type by hand.

```haskell
weather = D.fromNamedColumns
    [ ("Day",                      D.fromList ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday" :: T.Text])
    , ("High Temperature (C)",     D.fromList [24, 20, 22, 23, 25, 26, 26 :: Double])
    , ("Low Temperature (C)",      D.fromList [14, 13, 13, 13, 14, 15, 15 :: Double])
    ]

TIO.putStrLn $ D.toMarkdown weather
```

`fromNamedColumns` takes a list of `(name, column)` pairs. For data without column names there is `fromUnnamedColumns`, which assigns numeric names automatically.

```haskell
weatherUnnamed = D.fromUnnamedColumns
    [ D.fromList ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday" :: T.Text]
    , D.fromList [24, 20, 22, 23, 25, 26, 26 :: Double]
    , D.fromList [14, 13, 13, 13, 14, 15, 15 :: Double]
    ]

TIO.putStrLn $ D.toMarkdown weatherUnnamed
```

Numeric column names are fine for a quick sanity-check, but always give your columns descriptive names before doing real analysis.

### Reading from a file

Most data lives in files. CSV (comma-separated values) is the most common format.

```haskell
housing <- D.readCsv "../data/housing.csv"

TIO.putStrLn $ D.toMarkdown (D.take 5 housing)
```

`take n df` keeps the first `n` rows. It is the quickest way to eyeball a fresh dataset.

The type of `D.take` is `Int -> DataFrame -> DataFrame` — an integer in, a dataframe in, a dataframe out.

### Opting into stronger type safety

`F.col @Type "colName"` tells the compiler what type to expect in a column. If you get the type wrong you hear about it immediately.

```haskell
-- Runtime-checked (flexible):
D.mean (F.col @Double "High Temperature (C)") weather

-- Compile-time-checked — $(F.declareColumns …) generates typed bindings:
$(F.declareColumns weather)
D.mean high_temperature_c weather
```

The `$(F.declareColumns df)` splice inspects the dataframe at compile time and generates one typed binding per column (column names are sanitised into valid Haskell identifiers). If you try to use a column that does not exist, the program will not compile.

---

## Chapter 3: Exploring Your Data

You have data. What now? First you need to understand its shape: what columns exist, what types they hold, how complete they are.

Three functions cover most of this ground:

* `D.take` — peek at the first few rows
* `D.describeColumns` — schema: types, null counts, unique counts
* `D.summarize` — descriptive statistics

### take and takeLast

```haskell
TIO.putStrLn $ D.toMarkdown (D.take 10 housing)
```

```haskell
TIO.putStrLn $ D.toMarkdown (D.takeLast 5 housing)
```

### describeColumns

```haskell
TIO.putStrLn $ D.toMarkdown (D.describeColumns housing)
```

`describeColumns` tells you the name, type, number of non-null values, number of nulls, and number of unique values for every column. It is the first thing to run on any new dataset.

### summarize

```haskell
TIO.putStrLn $ D.toMarkdown (D.summarize housing)
```

`summarize` shows the mean, min, 25th percentile, median, 75th percentile, max, standard deviation, IQR, and skewness for every numeric column in one table.

#### Aside: type errors

Haskell's type errors are a conversation with the compiler, not a failure. Here are two common examples.

Passing a character instead of an integer to `take`:

```haskell
-- D.take '5' housing
-- error: Couldn't match expected type 'Int' with actual type 'Char'
-- Fix: D.take 5 housing
```

Passing a `Double` column expression where an `Int` is expected:

```haskell
-- D.mean (F.col @Int "longitude") housing
-- error: Couldn't match type 'Double' with 'Int' for column "longitude"
-- Fix: D.mean (F.col @Double "longitude") housing
```

Errors tell you the exact location and what went wrong. Over time you learn to read them as precise hints.

---

## Chapter 4: Data Cleaning and Preparation

Data from the real world is rarely clean. Haskell's type system makes common cleaning patterns safe and explicit.

### Handling missing data

Potentially-missing values are represented by `Maybe`. `Just x` means the value is present; `Nothing` means it is absent. This is not a special dataframe convention — it is a core Haskell type.

```haskell
messy = D.fromNamedColumns
    [ ("id",    D.fromList [Just 1, Just 2, Nothing, Nothing  :: Maybe Int])
    , ("score", D.fromList [Just 6.5, Nothing, Nothing, Just 6.5 :: Maybe Double])
    , ("rate",  D.fromList [Just 3.0, Nothing, Nothing, Just 3.0 :: Maybe Double])
    ]

TIO.putStrLn $ D.toMarkdown messy
```

#### Filtering by nulls

`filterJust col df` drops all rows where `col` is `Nothing` and unwraps the `Maybe`:

```haskell
TIO.putStrLn $ D.toMarkdown (D.filterJust "id" messy)
```

`filterAllJust df` keeps only rows where *every* column is non-null:

```haskell
TIO.putStrLn $ D.toMarkdown (D.filterAllJust messy)
```

The companions `filterNothing` and `filterAllNothing` do the opposite — they let you inspect the bad rows.

#### Imputing missing values

`impute expr default df` fills every `Nothing` in a column with a given value:

```haskell
TIO.putStrLn $ D.toMarkdown
    (D.impute (F.col @(Maybe Int) "id") 0 messy)
```

Notice the `@(Maybe Int)` type annotation — it tells the imputer what type the column holds. Passing the wrong type throws a clear runtime error:

```haskell
-- D.impute (F.col @(Maybe Double) "id") 0 messy
-- Exception: Type Mismatch — expected 'Maybe Double' but column is 'Maybe Integer'
```

#### Imputing with a statistic

`imputeWith` fills nulls with the result of an aggregation, e.g. the column mean:

```haskell
TIO.putStrLn $ D.toMarkdown $ D.take 10
    (D.imputeWith F.mean (F.col @(Maybe Double) "total_bedrooms") housing)
```

### Removing duplicates

`distinct df` keeps one copy of each unique row:

```haskell
dupData = D.fromNamedColumns
    [ ("k1", D.fromList (take 6 (cycle ["one","two"]) ++ ["two"]))
    , ("k2", D.fromList [1, 1, 2, 3, 3, 4, 4 :: Int])
    ]

TIO.putStrLn $ D.toMarkdown (D.distinct dupData)
```

### Opting into stronger type safety

After `$(F.declareColumns df)` any imputation or filter expression is checked at compile time. A typo in a column name becomes a compile error, not a runtime surprise.

```haskell
$(F.declareColumns housing)

-- Compile-time checked — 'total_bedrooms' must exist and be Maybe Double:
TIO.putStrLn $ D.toMarkdown $ D.take 10
    (D.imputeWith F.mean total_bedrooms housing)
```

---

## Chapter 5: Data Transformation

Most datasets need some reshaping before analysis. Take this hypothetical meat dataset:

```haskell
foodOptions  = ["bacon","pulled pork","bacon","pastrami","corned beef","bacon","pastrami","honey ham","nova lox" :: T.Text]
measurements = [4, 3, 12, 6, 7.5, 8, 3, 5, 6 :: Double]

meat = D.fromNamedColumns
    [ ("food",   D.fromList foodOptions)
    , ("ounces", D.fromList measurements)
    ]

TIO.putStrLn $ D.toMarkdown meat
```

### Adding derived columns

`derive name expr df` adds a new column computed from an expression.

```haskell
TIO.putStrLn $ D.toMarkdown
    (D.derive "kilograms" (F.col @Double "ounces" * 0.03) meat)
```

### Expressions and F.col

`F.col @Type "name"` creates an *expression* — a typed reference to a column that can be combined with arithmetic operators, boolean operators, or custom functions.

```
F.col @Double "ounces"   -- :: Expr Double
F.col @Text   "food"     -- :: Expr Text
```

You can compose expressions:

```haskell
roomsPerHousehold = D.derive "rooms_per_household"
    (F.col @Double "total_rooms" / F.col @Double "households")
    housing

TIO.putStrLn $ D.toMarkdown (D.take 5 roomsPerHousehold)
```

### Lifting custom functions

When the built-in arithmetic operators are not enough, `F.lift` applies any function to a column expression.

```haskell
import Data.Text (Text)

meatToAnimal :: Text -> Text
meatToAnimal "bacon"       = "pig"
meatToAnimal "pulled pork" = "pig"
meatToAnimal "pastrami"    = "cow"
meatToAnimal "corned beef" = "cow"
meatToAnimal "honey ham"   = "pig"
meatToAnimal "nova lox"    = "salmon"
meatToAnimal _             = "unknown"

TIO.putStrLn $ D.toMarkdown
    (D.derive "animal" (F.lift meatToAnimal (F.col @Text "food")) meat)
```

### Recoding values

`F.recode mapping expr` is a concise way to map one set of values to another:

```haskell
animalMapping = [("bacon","pig"),("pulled pork","pig"),("pastrami","cow"),("corned beef","cow"),("honey ham","pig"),("nova lox","salmon")]

TIO.putStrLn $ D.toMarkdown
    (D.derive "animal2" (F.recode animalMapping (F.col @Text "food")) meat)
```

### Opting into stronger type safety

After `$(F.declareColumns meat)`, column references are checked at compile time:

```haskell
$(F.declareColumns meat)

-- Using declared column bindings — compiler catches typos and type mismatches:
TIO.putStrLn $ D.toMarkdown
    (D.derive "kilograms" (ounces * 0.03) meat)
```

Accidentally using a `Text` column in arithmetic would be a compile error:

```haskell
-- D.derive "wrong" (food * 0.03) meat
-- error: No instance for Num (Expr Text) — food is Text, not a number
```

### The FrameM monad — pipelines without plumbing

Every transformation so far has required threading the dataframe through manually: `df` in, `df'` out, `df''` out of that. For multi-step pipelines that grows tedious fast. `DataFrame.Monad` provides `FrameM`, a state-monad wrapper that threads the dataframe implicitly.

```haskell
import DataFrame.Monad

$(F.declareColumnsFromCsvWithOpts (D.defaultReadOptions{D.typeSpec = D.InferFromSample 300}) "../data/housing.csv")

housing <- D.readCsv "../data/housing.csv"

pipelined = execFrameM housing $ do
    isExpensive       <- deriveM "is_expensive"        (median_house_value .>=. 500000)
    roomsPerHousehold <- deriveM "rooms_per_household" (total_rooms / households)
    meanBeds          <- inspectM (D.meanMaybe total_bedrooms)
    totalBedrooms     <- imputeM  total_bedrooms meanBeds
    filterWhereM (isExpensive .&&. roomsPerHousehold .>=. 7 .&&. totalBedrooms .>=. 200)

TIO.putStrLn $ D.toMarkdown $ D.take 5 pipelined
```

`$(F.declareColumnsFromCsvFile path)` generates compile-time column bindings by reading the CSV header at splice time — no live dataframe needs to be in scope, unlike `$(F.declareColumns df)` which requires a bound frame.

Inside the `do`-block, `<-` binds the typed `Expr` returned by each step; those expressions can be reused in later steps (e.g. `isExpensive` in the final `filterWhereM`) without any extra plumbing.

**Extracting results alongside the final frame** — `runFrameM` returns a pair `(a, DataFrame)`:

```haskell
((isExp, bedrooms), housingEnriched) = runFrameM housing $ do
    isExp    <- deriveM "is_expensive" (median_house_value .>=. 500000)
    meanBeds <- inspectM (D.meanMaybe total_bedrooms)
    bedrooms <- imputeM  total_bedrooms meanBeds
    pure (isExp, bedrooms)
```

The three exit functions:

| Function | Returns | Use when |
|---|---|---|
| `execFrameM df block` | `DataFrame` | you only need the final frame |
| `runFrameM  df block` | `(a, DataFrame)` | you need extracted values *and* the frame |
| `evalFrameM df block` | `a` | you only need the extracted values |

Available monadic operations: `deriveM`, `imputeM`, `filterWhereM`, `inspectM`, `renameM`.

---

## Chapter 6: Data Loading, Storage, and File Formats

Real workflows involve reading data from many sources and writing results back to disk. This chapter covers the file I/O functions you will use most.

### CSV files

You have already seen `D.readCsv`. Under the hood it calls `readCsvWithOpts` with sensible defaults.

```haskell
housingFull <- D.readCsv "../data/housing.csv"

TIO.putStrLn $ D.toMarkdown (D.take 3 housingFull)
```

### TSV files

Tab-separated files work exactly like CSV:

```haskell
-- housingTsv <- D.readTsv "data/housing.tsv"
-- TIO.putStrLn $ D.toMarkdown (D.take 3 housingTsv)
```

### Custom separators and options

`ReadOptions` controls the separator character, header handling, date format, and more.

```haskell
pipeOpts = D.defaultReadOptions { D.columnSeparator = '|' }
-- pipeDelimited <- D.readCsvWithOpts pipeOpts "data/data.psv"
```

The `HeaderSpec` type determines how the first row is treated:

* `UseFirstRow` (default) — the first row contains column names.
* `NoHeader` — there is no header; columns get numeric names.
* `ProvideNames ["a","b","c"]` — supply names explicitly.

```haskell
noHeaderOpts = D.defaultReadOptions { D.headerSpec = D.NoHeader }
-- D.readCsvWithOpts noHeaderOpts "data/raw.csv"

namedOpts = D.defaultReadOptions
    { D.headerSpec = D.ProvideNames ["longitude","latitude","age","rooms","bedrooms","population","households","income","value","proximity"] }
-- D.readCsvWithOpts namedOpts "data/housing_no_header.csv"
```

### Writing CSV files

`D.writeCsv path df` writes a dataframe back to disk:

```haskell
housingSubset = D.select ["longitude","latitude","median_house_value"] housingFull

D.writeCsv "/tmp/housing_subset.csv" housingSubset
```

You can write any character-separated format:

```haskell
D.writeSeparated '|' "/tmp/housing_pipe.psv" housingSubset
```

### Parquet files

Parquet is a columnar binary format common in data engineering pipelines:

```haskell
-- parquetDf <- D.readParquet "data/housing.parquet"
-- TIO.putStrLn $ D.toMarkdown (D.take 5 parquetDf)
```

For a directory of Parquet shards (e.g. from a Spark job):

```haskell
-- shardDf <- D.readParquetFiles "data/housing_shards/"
```

### Inspecting the schema after load

Always run `describeColumns` on freshly loaded data to confirm types and check for unexpected nulls:

```haskell
TIO.putStrLn $ D.toMarkdown (D.describeColumns housingFull)
```

### Opting into stronger type safety

After loading, use `F.cast` to produce a column expression with the exact type you expect. Any row whose value cannot be converted becomes `Nothing` in the result:

```haskell
-- Retype a text column to Double, converting unparseable values to Nothing:
withIncomeCast = D.derive "income_cast" (F.cast @Double "median_income") housingFull

TIO.putStrLn $ D.toMarkdown (D.take 5 withIncomeCast)
```

### Streaming large files with DataFrame.Lazy

All of the above reads the entire file into memory upfront. For datasets larger than available RAM, `DataFrame.Lazy` offers a pull-based streaming executor: operations build a logical plan tree and nothing is read from disk until `runDataFrame` is called. The optimizer pushes `filter` predicates down to the scan, so unneeded rows are discarded before any column is allocated.

The Lazy path requires an explicit schema — there is no inference. Build one with `schemaType @T`:

```haskell
-- cabal: build-depends: containers
import qualified DataFrame.Lazy as L
import DataFrame.Internal.Schema (Schema (..), schemaType)
import qualified Data.Map.Strict as M

housingSchema = Schema $ M.fromList
    [ ("longitude",          schemaType @Double)
    , ("latitude",           schemaType @Double)
    , ("housing_median_age", schemaType @Double)
    , ("total_rooms",        schemaType @Double)
    , ("total_bedrooms",     schemaType @(Maybe Double))
    , ("population",         schemaType @Double)
    , ("households",         schemaType @Double)
    , ("median_income",      schemaType @Double)
    , ("median_house_value", schemaType @Double)
    , ("ocean_proximity",    schemaType @T.Text)
    ]
```

Build and run a lazy pipeline:

```haskell
lazyQuery =
    L.scanCsv housingSchema "../data/housing.csv"
        |> L.filter (F.col @Double "median_house_value" .>=. 300000)
        |> L.select ["longitude","latitude","median_house_value","ocean_proximity"]
        |> L.take 10

lazyResult <- L.runDataFrame lazyQuery

TIO.putStrLn $ D.toMarkdown lazyResult
```

Key API:

| Function | Description |
|---|---|
| `L.scanCsv schema path` | stream a CSV file |
| `L.scanSeparated sep schema path` | stream any character-delimited file |
| `L.scanParquet schema path` | stream a Parquet file |
| `L.fromDataFrame df` | lift an eager frame into the lazy plan |
| `L.filter`, `L.select`, `L.derive` | lazy counterparts of the eager operations |
| `L.take`, `L.sortBy`, `L.groupBy`, `L.join` | same semantics, deferred execution |
| `L.runDataFrame plan` | execute the plan and materialise a `DataFrame` |

The optimizer automatically reorders `filter` before `select` and prunes columns that are not referenced downstream — columns excluded by `L.select` are never allocated at the scan level.

---

## Chapter 7: Data Cleaning and Type Coercion

Real datasets come with type mismatches, messy strings, and dates in a dozen formats. This chapter shows how to clean them up.

### Type coercion

Three functions handle column re-typing with different failure modes:

| Function | On failure | Return type |
|---|---|---|
| `F.cast @T "col"` | returns `Nothing` | `Expr (Maybe T)` |
| `F.castWithDefault v "col"` | returns `v` | `Expr T` |
| `F.castEither @T "col"` | returns `Left originalText` | `Expr (Either Text T)` |

#### Lenient cast — nullify bad values

```haskell
messyNums = D.fromNamedColumns
    [ ("raw", D.fromList ["1.5", "2.0", "bad", "3.7", "" :: T.Text]) ]

withCast = D.derive "as_double" (F.cast @Double "raw") messyNums

TIO.putStrLn $ D.toMarkdown withCast
```

#### Cast with default — fill bad values

```haskell
withDefault = D.derive "as_double" (F.castWithDefault 0.0 "raw") messyNums

TIO.putStrLn $ D.toMarkdown withDefault
```

#### castEither — audit bad rows

`castEither` returns `Right` for successes and `Left` for failures, so you can inspect every bad row before deciding what to do:

```haskell
withAudit = D.derive "audit" (F.castEither @Double "raw") messyNums

TIO.putStrLn $ D.toMarkdown withAudit
```

Haskell forces you to handle the `Left` case before you can use the values downstream — making data quality issues visible at the type level.

### Text operations

Three functions manipulate `Text` columns inside expressions.

#### splitOn

`F.splitOn delim expr` splits each string at the delimiter, producing a list:

```haskell
emails = D.fromNamedColumns
    [ ("email", D.fromList ["alice@example.com", "bob@haskell.org", "cara@data.io" :: T.Text]) ]

emailParts = D.derive "parts" (F.splitOn "@" (F.col @T.Text "email")) emails

TIO.putStrLn $ D.toMarkdown emailParts
```

#### match

`F.match pattern expr` returns `Just` the first regex match, or `Nothing` if there is none:

```haskell
domains = D.derive "domain" (F.match "[a-z]+\\.[a-z]+" (F.col @T.Text "email")) emails

TIO.putStrLn $ D.toMarkdown domains
```

#### matchAll

`F.matchAll pattern expr` returns a list of *all* matches:

```haskell
withWords = D.derive "words" (F.matchAll "[a-z]+" (F.col @T.Text "email")) emails

TIO.putStrLn $ D.toMarkdown withWords
```

### Date operations

#### parseDate

`F.parseDate fmt expr` parses a text column into a `Day`, returning `Nothing` for values that do not match the format:

```haskell
import Data.Time (Day)

events = D.fromNamedColumns
    [ ("name",       D.fromList ["release","conference","deadline" :: T.Text])
    , ("date_text",  D.fromList ["2025-03-01","2025-06-15","2025-09-30" :: T.Text])
    ]

withDates = D.derive "date" (F.parseDate @Day "%Y-%m-%d" (F.col @T.Text "date_text")) events

TIO.putStrLn $ D.toMarkdown withDates
```

#### daysBetween

`F.daysBetween expr1 expr2` computes the number of days between two `Day` expressions:

```haskell
-- This requires two Day columns; here we illustrate the pattern:
-- D.derive "days_until" (F.daysBetween today_col deadline_col) df
```

### Opting into stronger type safety

`F.castEither` returns `Either Text a` — Haskell's type system ensures you address the `Left` (failure) case before proceeding. The alternative coercion functions (`cast`, `castWithDefault`) have types that make the tradeoff explicit in the signature.

```haskell
$(F.declareColumns withAudit)

-- 'audit' is Expr (Either Text Double) — you must handle Left before using it as a number
TIO.putStrLn $ D.toMarkdown (D.take 5 withAudit)
```

---

## Chapter 8: Data Wrangling — Join, Combine, and Reshape

Real analyses often involve multiple tables. This chapter shows how to combine them.

### Vertical concatenation with <>

`(<>)` stacks two dataframes with compatible schemas. Columns missing from one side are padded with `Nothing`. The `import DataFrame.Operations.Merge ()` at the top of this file brings the `Semigroup` instance into scope.

```haskell
firstHalf  = D.take 5 housing
secondHalf = D.range (5, 10) housing

combined = firstHalf <> secondHalf

TIO.putStrLn $ D.toMarkdown combined
```

To stack a list of frames:

```haskell
chunks = map (\i -> D.range (i, i+3) housing) [0, 4, 8]

stacked = mconcat chunks

TIO.putStrLn $ D.toMarkdown stacked
```

### Horizontal concatenation with |||

`(|||)` joins two frames side by side (column-wise). Both must have the same number of rows.

```haskell
leftCols  = D.select ["longitude","latitude"] housing
rightCols = D.select ["median_house_value","ocean_proximity"] housing

sideBy = leftCols ||| rightCols

TIO.putStrLn $ D.toMarkdown (D.take 5 sideBy)
```

### SQL-style joins

Joins combine rows from two tables based on a shared key.

```haskell
customers = D.fromNamedColumns
    [ ("customer_id", D.fromList [1, 2, 3, 4 :: Int])
    , ("name",        D.fromList ["Alice","Bob","Cara","Dave" :: T.Text])
    , ("city",        D.fromList ["Seattle","Portland","Austin","Denver" :: T.Text])
    ]

orders = D.fromNamedColumns
    [ ("customer_id", D.fromList [1, 2, 2, 3, 5 :: Int])
    , ("product",     D.fromList ["laptop","keyboard","mouse","monitor","tablet" :: T.Text])
    , ("amount",      D.fromList [1200.0, 85.0, 30.0, 350.0, 600.0 :: Double])
    ]
```

#### Inner join — only matching rows

```haskell
innerResult = D.innerJoin ["customer_id"] customers orders

TIO.putStrLn $ D.toMarkdown innerResult
```

Dave (id=4) has no orders; customer 5 has no customer record. Neither appears in an inner join.

#### Left join — keep all left rows

```haskell
leftResult = D.leftJoin ["customer_id"] customers orders

TIO.putStrLn $ D.toMarkdown leftResult
```

Dave appears with `Nothing` for order columns.

#### Right join — keep all right rows

```haskell
rightResult = D.rightJoin ["customer_id"] customers orders

TIO.putStrLn $ D.toMarkdown rightResult
```

The orphan order (customer_id=5) appears with `Nothing` for customer columns.

#### Full outer join — keep everything

```haskell
outerResult = D.fullOuterJoin ["customer_id"] customers orders

TIO.putStrLn $ D.toMarkdown outerResult
```

All rows from both sides appear; unmatched rows get `Nothing` in the other side's columns.

### Reshaping

#### select and exclude

```haskell
TIO.putStrLn $ D.toMarkdown
    (D.select ["longitude","latitude","median_house_value"] (D.take 5 housing))
```

```haskell
TIO.putStrLn $ D.toMarkdown
    (D.exclude ["longitude","latitude"] (D.take 5 housing))
```

#### rename

`D.rename old new df` renames a single column; `D.renameMany pairs df` renames several at once:

```haskell
renamedHousing = D.renameMany
    [ ("median_house_value", "price")
    , ("median_income",      "income")
    ] housing

TIO.putStrLn $ D.toMarkdown (D.take 3 renamedHousing)
```

#### sortBy

```haskell
TIO.putStrLn $ D.toMarkdown
    (D.take 5 (D.sortBy D.Descending ["median_house_value"] housing))
```

#### range

`D.range (start, end) df` slices rows from index `start` (inclusive) to `end` (exclusive):

```haskell
TIO.putStrLn $ D.toMarkdown (D.range (10, 15) housing)
```

### Opting into stronger type safety

After a join the combined schema is known. Use `$(F.declareColumns …)` to get compile-time bindings for all columns of the joined result:

```haskell
$(F.declareColumns innerResult)

-- All columns of innerResult are now bound with their exact types.
-- Typos or type mismatches become compile errors, not runtime surprises.
enriched = D.derive "total_with_tax" (amount * 1.1) innerResult

TIO.putStrLn $ D.toMarkdown enriched
```

---

## Chapter 9: Plotting and Visualization

A picture is worth a thousand rows. This chapter shows two layers of plotting:

1. **Low-level Granite.Svg functions** — full control over bins, axes, and titles.
2. **High-level `D.*` wrappers** — one-liners that inspect the dataframe directly.

The `import Granite.Svg` at the top of this file brings the low-level functions into scope.

### Histograms

Histograms show the distribution of a single numeric variable.

```haskell
import qualified DataFrame.Display.Terminal.Plot as P

houseValues = D.columnAsList (F.col @Double "median_house_value") housing

TIO.putStrLn $
    histogram
        (bins 30 140000 502000)
        houseValues
        defPlot
            { legendPos   = LegendBottom
            , xFormatter  = \_ _ v -> T.pack (show (round v :: Int))
            , xNumTicks   = 8
            , yNumTicks   = 5
            , plotTitle   = "Median House Prices of California Houses ($)"
            }
```

The high-level wrapper is even shorter — it uses a default bin count of 30:

```haskell
P.plotHistogram "median_house_value" housing
```

### Scatter plots

Scatter plots reveal relationships between two numeric variables.

```haskell
incomes = D.columnAsList (F.col @Double "median_income") housing
values  = D.columnAsList (F.col @Double "median_house_value") housing

TIO.putStrLn $
    scatter
        [("income vs value", zip incomes values)]
        defPlot { plotTitle = "Income vs House Value" }
```

```haskell
P.plotScatter "median_income" "median_house_value" housing
```

### Bar charts

Bar charts are good for categorical counts.

```haskell
P.plotBars "ocean_proximity" housing
```

### Pie charts

```haskell
P.plotPie "ocean_proximity" Nothing housing
```

### Box plots

Box plots summarise the five-number summary (min, Q1, median, Q3, max) for one or more numeric columns.

```haskell
P.plotBoxPlots ["median_house_value", "median_income"] housing
```

### Line graphs

Line graphs show trends along a continuous x-axis. The second argument is a list of y-axis column names.

```haskell
ages = D.columnAsList (F.col @Double "housing_median_age") housing

TIO.putStrLn $
    lineGraph
        [("age vs value", zip ages values)]
        defPlot { plotTitle = "Housing Age vs Value" }
```

```haskell
P.plotLines "housing_median_age" ["median_house_value"] housing
```

### Correlation matrix (heatmap)

A correlation matrix shows pairwise Pearson correlations across all numeric columns.

```haskell
P.plotCorrelationMatrix housing
```

### Opting into stronger type safety

Using `F.col @Double` in `D.columnAsList` ensures only numeric columns reach the plotting functions. Passing a `Text` column to a histogram is a compile error:

```haskell
-- D.columnAsList (F.col @Double "ocean_proximity") housing
-- error: column "ocean_proximity" has type Text, not Double
```

---

## Chapter 10: Data Aggregation and Group Operations

Grouping is the backbone of most summary analyses. This chapter covers `groupBy` and the rich set of aggregation functions.

### groupBy and aggregate

`D.groupBy keys df` returns a `GroupedDataFrame`. Calling `D.aggregate exprs grouped` reduces each group to a single row.

```haskell
import DataFrame.Operators (as)

grouped = D.groupBy ["ocean_proximity"] housing

summary = D.aggregate
    [ F.count  (F.col @Double "median_house_value") `as` "count"
    , F.mean   (F.col @Double "median_house_value") `as` "mean_value"
    , F.median (F.col @Double "median_house_value") `as` "median_value"
    , F.maximum (F.col @Double "median_house_value") `as` "max_value"
    , F.minimum (F.col @Double "median_house_value") `as` "min_value"
    ] grouped

TIO.putStrLn $ D.toMarkdown summary
```

### Full aggregation function menu

| Function | Description | Return type |
|---|---|---|
| `F.count expr` | number of elements | `Expr Int` |
| `F.sum expr` | sum | `Expr a` |
| `F.sumMaybe expr` | sum ignoring Nothing | `Expr a` |
| `F.mean expr` | arithmetic mean | `Expr Double` |
| `F.meanMaybe expr` | mean ignoring Nothing | `Expr Double` |
| `F.median expr` | median | `Expr Double` |
| `F.medianMaybe expr` | median ignoring Nothing | `Expr Double` |
| `F.minimum expr` | minimum | `Expr a` |
| `F.maximum expr` | maximum | `Expr a` |
| `F.stddev expr` | standard deviation | `Expr Double` |
| `F.variance expr` | variance | `Expr Double` |
| `F.percentile n expr` | n-th percentile | `Expr Double` |
| `F.mode expr` | most frequent value | `Expr a` |
| `F.collect expr` | all values as a list | `Expr [a]` |

### Multi-key grouping

Group by multiple columns by passing a longer key list:

```haskell
$(F.declareColumns meat)

meatGrouped = D.groupBy ["food"] meat

meatSummary = D.aggregate
    [ F.count (F.col @T.Text "food") `as` "count"
    , F.sum   ounces                 `as` "total_oz"
    , F.mean  ounces                 `as` "mean_oz"
    ] meatGrouped

TIO.putStrLn $ D.toMarkdown meatSummary
```

### Value counts with frequencies

`D.frequencies expr df` returns a frequency table — row counts and percentages for each unique value:

```haskell
TIO.putStrLn $ D.toMarkdown (D.frequencies (F.col @T.Text "ocean_proximity") housing)
```

### Pearson correlation

`D.correlation col1 col2 df` computes the Pearson correlation coefficient between two columns. It returns `Maybe Double` because the result is undefined for constant columns:

```haskell
D.correlation "median_income" "median_house_value" housing
```

### Derived columns on aggregated results

Aggregate first, then derive new columns from the summary:

```haskell
totalRows = fromIntegral (D.nRows housing) :: Double

withShare = D.derive "pct_of_total"
    (F.toDouble (F.col @Int "count") / F.lit totalRows * F.lit 100.0)
    summary

TIO.putStrLn $ D.toMarkdown withShare
```

### Z-score normalisation pipeline

A full group-normalise pipeline: group → compute mean and stddev → derive z-scores.

```haskell
incomeByProx = D.aggregate
    [ F.mean   (F.col @Double "median_income") `as` "mean_income"
    , F.stddev (F.col @Double "median_income") `as` "stddev_income"
    ] (D.groupBy ["ocean_proximity"] housing)

TIO.putStrLn $ D.toMarkdown incomeByProx
```

### Opting into stronger type safety

After aggregation the result has a new schema. A new `$(F.declareColumns …)` gives you compile-time bindings for that schema:

```haskell
$(F.declareColumns summary)

-- All aggregated columns are now typed. mean_value :: Expr Double etc.
ranked = D.sortBy D.Descending [F.name mean_value] summary

TIO.putStrLn $ D.toMarkdown ranked
```

A type-level mistake on an aggregated column — say, treating `count` (an `Int`) as a `Double` — is caught immediately by the compiler rather than silently producing wrong numbers at runtime.

---

## Chapter 11: Compile-Time Schema Safety with DataFrame.Typed

### Motivation

Throughout this guide we have used `F.col @Type "colName"` to reference columns. The expressions are typed (the `@Type` annotation is checked), but the schema of the *dataframe* is not in the Haskell type. This creates a subtle foot-gun: an `Expr` built against one dataframe can be silently applied to a completely different dataframe — the compiler will accept it, but at runtime you get either a missing-column error or, worse, wrong results from a same-named column with different semantics.

`DataFrame.Typed` solves this by moving the full column schema into the type system. The schema becomes a type-level list of `DT.Column "name" Type` entries. If a column does not exist in a `TypedDataFrame`, or if you use it with the wrong type, the code will not compile.

The trade-off is more explicit upfront ceremony (a TH splice, a freeze call, and the `DataKinds` extension). When you want that guarantee for a production pipeline, it is worth it.

### Generating a schema

**At compile time from a CSV file** — `$(DT.deriveSchemaFromCsvFile "Housing" path)` reads the header and generates a type alias called `Housing`:

```haskell
import qualified DataFrame.Typed as DT

$(DT.deriveSchemaFromCsvFile "Housing" "../data/housing.csv")
```

**By hand** — equivalent to what the TH splice generates:

```haskell
type Housing = [ DT.Column "longitude"          Double
               , DT.Column "latitude"           Double
               , DT.Column "housing_median_age" Double
               , DT.Column "total_rooms"        Double
               , DT.Column "total_bedrooms"     (Maybe Double)
               , DT.Column "population"         Double
               , DT.Column "households"         Double
               , DT.Column "median_income"      Double
               , DT.Column "median_house_value" Double
               , DT.Column "ocean_proximity"    T.Text
               ]
```

### Freezing a runtime dataframe

`D.readCsv` returns a plain `DataFrame`. `DT.freezeWithError @Housing` checks that it conforms to the `Housing` schema and produces a `TypedDataFrame Housing`. Any mismatch — wrong column type, missing column — is caught here, near the top of the pipeline, rather than several steps in:

```haskell
thousing <- either (error . show) id . DT.freezeWithError @Housing <$> D.readCsv "../data/housing.csv"
```

`DT.thaw` converts back to a plain `DataFrame` when you need to pass the result to a function that does not know about typed frames.

### Typed transforms

`DT.col @"colName"` looks up the column in the schema at compile time. The type of the expression is inferred automatically — no `@Type` annotation needed. `DT.derive @"newCol"` adds the column to the schema type so subsequent steps can reference it:

```haskell
typedResult = thousing
    |> DT.derive @"rooms_per_household"    (DT.col @"total_rooms" / DT.col @"households")
    |> DT.derive @"bedrooms_per_household" (DT.col @"total_bedrooms" / DT.col @"households")

TIO.putStrLn $ D.toMarkdown (DT.thaw typedResult)
```

`DT.impute @"colName" defaultValue` fills `Nothing` values in a nullable column; like `derive`, it updates the schema type (the column becomes non-optional after imputation).

### Typed group and aggregate

```haskell
typedGrouped = thousing
    |> DT.groupBy @'["ocean_proximity"]
    |> DT.aggregate (DT.agg @"count" (DT.count (DT.col @"median_house_value")) DT.aggNil)

TIO.putStrLn $ D.toMarkdown (DT.thaw typedGrouped)
```

`DT.groupBy @'["col1","col2"]` takes a type-level list of column names (note the leading `'` for a promoted list). The aggregation spec is built with `DT.agg @"resultCol" aggregationExpr rest` and terminated with `DT.aggNil`.

### When to use each layer

| Layer | Best for |
|---|---|
| Eager string API (`F.col @T "name"`) | exploration, quick scripts, one-off analyses |
| `FrameM` | multi-step transformation pipelines where threading `df` by hand is noisy |
| `DataFrame.Lazy` | files larger than RAM; push-down filters; ETL pipelines |
| `DataFrame.Typed` | production pipelines where schema correctness must be guaranteed at compile time |

The layers compose: you can `L.fromDataFrame (DT.thaw typedResult)` to hand a typed result to the lazy executor, or use `FrameM` for the messy cleaning phase and `Typed` for the aggregation phase.
