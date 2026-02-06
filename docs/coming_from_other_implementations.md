# Coming from Other DataFrame Implementations

This guide helps users familiar with pandas, Polars, or dplyr understand how to work with our Haskell DataFrame library. We'll walk through common patterns and show how concepts translate between these tools.

## Philosophy and Key Differences

Before diving into specific examples, it's important to understand the core philosophy of our Haskell DataFrame implementation:

1. **Type Safety**: We leverage Haskell's type system to catch errors at compile time rather than runtime
2. **Explicit Operations**: Rather than overloading operators like `[]`, we provide explicit functions for each operation
3. **Functional Composition**: Operations are designed to chain together using `|>` (pipe operator) or function composition
4. **Immutability**: All operations return new DataFrames rather than modifying existing ones
5. **No Implicit Conversions**: Type conversions and transformations must be explicit

## Coming from pandas

We'll port over concepts from [10 minutes to Pandas](https://pandas.pydata.org/docs/user_guide/10min.html), showing how familiar pandas operations map to our library.

### Basic Data Structures

**pandas Series → DataFrame Column**

A pandas `Series` is an indexable (labelled) array. In our library, these map to `Column` values. However, we currently don't support row indexing, so `Column`s aren't typically manipulated directly—instead, you work with them through DataFrame operations.

**pandas DataFrame → DataFrame DataFrame**

Both use the name `DataFrame`, but the internal representation differs. Our DataFrames are essentially lists of `Vector`s with metadata for managing state and type information. This means operations are designed to work efficiently with columnar data.

### Creating Data Structures

#### Creating a Series/Column

In pandas, you might create a series with some missing values:

```python
python> s = pd.Series([1, 3, 5, np.nan, 6, 8])
python> s
0    1.0
1    3.0
2    5.0
3    NaN
4    6.0
5    8.0
dtype: float64
```

In our library, you can create a similar structure:

```haskell
dataframe> import qualified DataFrame as D
dataframe> D.fromList [1, 3, 5, read @Float "NaN", 6, 8]
[1.0,3.0,5.0,NaN,6.0,8.0]
```

**However**, this is considered an anti-pattern in Haskell. Using `NaN` mixes valid data with invalid data in an unsafe way. The idiomatic Haskell approach uses the `Maybe` type to explicitly represent missing values:

```haskell
dataframe> D.fromList [Just (1 :: Double), Just 3, Just 5, Nothing, Just 6, Just 8]
[Just 1.0, Just 3.0, Just 5.0, Nothing, Just 6.0, Just 8.0]
```

This approach is better because:
- The type system forces you to handle missing values explicitly
- You can't accidentally treat `Nothing` as a number
- Pattern matching ensures you consider all cases

#### Creating Date Ranges

pandas provides convenient date range generation:

```python
python> dates = pd.date_range("20130101", periods=6)
python> dates
DatetimeIndex(['2013-01-01', '2013-01-02', '2013-01-03', '2013-01-04',
               '2013-01-05', '2013-01-06'],
              dtype='datetime64[ns]', freq='D')
```

In Haskell, we use the `Data.Time.Calendar` module and leverage lazy list generation:

```haskell
dataframe> import Data.Time.Calendar
dataframe> dates = D.fromList $ Prelude.take 6 $ [fromGregorian 2013 01 01..]
dataframe> dates
[2013-01-01,2013-01-02,2013-01-03,2013-01-04,2013-01-05,2013-01-06]
```

Here we're using:
- `fromGregorian` to create a `Day` value
- `[fromGregorian 2013 01 01..]` to create an infinite lazy list of consecutive days
- `take 6` to extract just the first 6 days
- `D.fromList` to convert to our DataFrame column type

#### Creating a DataFrame from Random Data

pandas makes it easy to create DataFrames with random data:

```python
python> df = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list("ABCD"))
python> df
                   A         B         C         D
2013-01-01  0.469112 -0.282863 -1.509059 -1.135632
2013-01-02  1.212112 -0.173215  0.119209 -1.044236
2013-01-03 -0.861849 -2.104569 -0.494929  1.071804
2013-01-04  0.721555 -0.706771 -1.039575  0.271860
2013-01-05 -0.424972  0.567020  0.276232 -1.087401
2013-01-06 -0.673690  0.113648 -1.478427  0.524988
```

In Haskell, we need to be more explicit, but we gain type safety:

```haskell
dataframe> import qualified Data.Vector as V
dataframe> import System.Random (randomRIO)
dataframe> import Control.Monad (replicateM)
dataframe> import Data.List (foldl')
dataframe> :set -XOverloadedStrings

-- Start with a DataFrame containing just the date column
dataframe> initDf = D.fromNamedColumns [("date", dates)]

-- Generate 4 columns of 6 random numbers each
dataframe> ns <- replicateM 4 (replicateM 6 (randomRIO (-2.0, 2.0)))

-- Add each column to the DataFrame
dataframe> df = foldl' (\d (name, col) -> D.insert name col d) 
                  initDf 
                  (zip ["A","B","C","D"] ns)
dataframe> df
-----------------------------------------------------------------------------------------------------
    date    |          A          |          B           |          C           |          D         
------------|---------------------|----------------------|----------------------|--------------------
    Day     |       Double        |        Double        |        Double        |       Double       
------------|---------------------|----------------------|----------------------|--------------------
 2013-01-01 | 0.49287792598710745 | 1.2126312556288785   | -1.3553292904555625  | 1.8491213627748553 
 2013-01-02 | 0.7936547276080512  | -1.5209756494542028  | -0.5208055385837551  | 0.8895325450813525 
 2013-01-03 | 1.8883976214395153  | 1.3453541205495676   | -1.1801018894304223  | 0.20583994035730901
 2013-01-04 | -1.3262867911904324 | -0.37375298679005686 | -0.8580515357149543  | 1.4681616115128593 
 2013-01-05 | 1.9068894062167745  | 0.792553168600036    | -0.13526265076664545 | -1.6239378251651466
 2013-01-06 | -0.5541246187320041 | -1.5791034339829042  | -1.5650415391333796  | -1.7802523632196152
```

Notice how the output includes type information in the header—this is part of our library's explicit approach to types.

#### Creating a DataFrame with Mixed Types

pandas allows heterogeneous DataFrames with various column types:

```python
df2 = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    }
)

## Result
##      A          B    C  D      E    F
## 0  1.0 2013-01-02  1.0  3   test  foo
## 1  1.0 2013-01-02  1.0  3  train  foo
## 2  1.0 2013-01-02  1.0  3   test  foo
## 3  1.0 2013-01-02  1.0  3  train  foo
```

In Haskell, we achieve the same result with explicit types:

```haskell
-- Define a custom type for categorical data
-- All DataFrame types must be printable (Show) and orderable (Ord, Eq)
data Transport = Test | Train deriving (Show, Ord, Eq)

dataframe> :{
ghci| df = D.fromNamedColumns [
ghci|        ("A", D.fromList (replicate 4 1.0)),
ghci|        ("B", D.fromList (replicate 4 (fromGregorian 2013 01 02))),
ghci|        ("C", D.fromList (replicate 4 (1.0 :: Float))),
ghci|        ("D", D.fromList (replicate 4 (3 :: Int))),
ghci|        ("E", D.fromList (take 4 $ cycle [Test, Train])),
ghci|        ("F", D.fromList (replicate 4 "foo"))]
ghci|:}
dataframe> df
-------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F   
--------|------------|-------|-----|-----------|-------
 Double |    Day     | Float | Int | Transport | [Char]
--------|------------|-------|-----|-----------|-------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo
```

**Key differences from pandas:**

1. **Broadcasting**: pandas automatically broadcasts scalar values. In Haskell, you must explicitly replicate values
2. **Categorical Data**: Instead of marking strings as categorical, we define custom algebraic data types. This provides compile-time guarantees about valid values
3. **Type Annotations**: We sometimes need type annotations (like `:: Float`) to disambiguate numeric types
4. **Named Columns**: `fromNamedColumns` takes a list of `(name, column)` tuples

### Viewing Data

#### Taking the First N Rows

pandas provides `head()` to view the first few rows:

```python
python> df.head()  # Shows first 5 rows by default
```

We provide a `take` function that requires you to specify the number of rows:

```haskell
dataframe> D.take 2 df
-------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F   
--------|------------|-------|-----|-----------|-------
 Double |    Day     | Float | Int | Transport | [Char]
--------|------------|-------|-----|-----------|-------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo 
```

By default, our library prints the entire DataFrame (unlike pandas which truncates large DataFrames). Use `take` when you want to limit output.

#### Summary Statistics

pandas provides `describe()` for summary statistics:

```python
python> df.describe()
```

Our equivalent is `summarize`, which computes statistics for numeric columns:

```haskell
dataframe> D.summarize df
----------------------------------------------
 Statistic |     D     |     C     |     A    
-----------|-----------|-----------|----------
   Text    |  Double   |  Double   |  Double  
-----------|-----------|-----------|----------
 Mean      | 3.0       | 1.0       | 1.0      
 Minimum   | 3.0       | 1.0       | 1.0      
 25%       | 3.0       | 1.0       | 1.0      
 Median    | 3.0       | 1.0       | 1.0      
 75%       | 3.0       | 1.0       | 1.0      
 Max       | 3.0       | 1.0       | 1.0      
 StdDev    | 0.0       | 0.0       | 0.0      
 IQR       | 0.0       | 0.0       | 0.0      
 Skewness  | -Infinity | -Infinity | -Infinity
```

**Note**: `summarize` only operates on numeric columns. The `-Infinity` for skewness occurs when there's no variance in the data (all values are identical).

### Sorting

#### Sorting by Column Values

pandas allows sorting by index or column values:

```python
python> df.sort_values(by='E')
```

Since we don't support row indexes, we only provide column-based sorting. You specify the sort direction and column names:

```haskell
dataframe> D.sortBy [D.Asc (F.col @Transport "E")] df
-------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F   
--------|------------|-------|-----|-----------|-------
 Double |    Day     | Float | Int | Transport | [Char]
--------|------------|-------|-----|-----------|-------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo   
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo
```

You can sort by multiple columns by providing a list: `["E", "A"]` would sort first by E, then by A for tied values.

### Selection

pandas' `[]` operator is overloaded to perform many different operations depending on what you pass to it. We provide explicit functions for each operation type instead.

#### Selecting Columns

In pandas, you can select columns in several ways:

```python
python> df.loc[:, ["A", "B"]]
                   A         B
2013-01-01  0.469112 -0.282863
2013-01-02  1.212112 -0.173215
2013-01-03 -0.861849 -2.104569
2013-01-04  0.721555 -0.706771
2013-01-05 -0.424972  0.567020
2013-01-06 -0.673690  0.113648
```

We use the SQL-inspired `select` function:

```haskell
dataframe> D.select ["A"] df
-------
   A   
-------
 Double
-------
 1.0   
 1.0   
 1.0   
 1.0
```

The function name makes it immediately clear we're selecting columns, and the type signature ensures we pass valid column names.

#### Filtering Rows

pandas allows row selection by index range:

```python
python> df.loc["20130102":"20130104", ["A", "B"]]
                   A         B
2013-01-02  1.212112 -0.173215
2013-01-03 -0.861849 -2.104569
2013-01-04  0.721555 -0.706771
```

Since we don't have row indexes, we filter by actual values using predicates:

```haskell
dataframe> :{
ghci| df' |> D.filter "date" (\d -> d >= (fromGregorian 2013 01 02) 
ghci|                              && d <= (fromGregorian 2013 01 04))
ghci|     |> D.select ["A", "B"]
ghci| :}
--------------------
   A    |     B     
--------|-----------
 Double |    Day    
--------|-----------
 1.0    | 2013-01-02
 1.0    | 2013-01-02
 1.0    | 2013-01-02
```

**Key points:**

- `filter` takes a column name and a predicate function
- The predicate function receives each value from that column
- We use the pipe operator `|>` to chain operations
- This approach is more flexible than index-based selection since it works with any filtering logic

### Missing Values

In pandas, missing values are typically represented as `NaN` or `None`. In Haskell, we use the `Maybe` type, which forces explicit handling.

#### Working with Missing Data

Let's add a column with missing values:

```haskell
dataframe> df' = D.insert "G" [Just 1, Just 2, Nothing, Just 4] df
dataframe> df'
-----------------------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F    |       G      
--------|------------|-------|-----|-----------|--------|--------------
 Double |    Day     | Float | Int | Transport | [Char] | Maybe Integer
--------|------------|-------|-----|-----------|--------|--------------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | Just 1       
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 2       
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | Nothing      
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 4
```

#### Filling Missing Values

pandas provides `fillna()`:

```python
python> df.fillna(5)
```

In Haskell, we use the `impute` function:

```haskell
dataframe> D.impute (F.col @Integer "G") 5 df'
-----------------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F    |    G   
--------|------------|-------|-----|-----------|--------|--------
 Double |    Day     | Float | Int | Transport | [Char] | Integer
--------|------------|-------|-----|-----------|--------|--------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | 1      
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 2      
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | 5      
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 4
```

The `@Integer` is a type application that tells Haskell what type of `Maybe` we're working with.

Notice how the type changed from `Maybe Integer` to `Integer` since we eliminated the possibility of `Nothing`.

#### Filtering Out Missing Values

pandas provides `dropna()`:

```python
python> df.dropna()
```

We use `filterJust`:

```haskell
dataframe> df' |> D.filterJust "G"
--------------------------------------------------------------------
   A    |     B      |   C   |  D  |     E     |   F    |    G      
--------|------------|-------|-----|-----------|--------|-----------
 Double |    Day     | Float | Int | Transport | [Char] | Integer
--------|------------|-------|-----|-----------|--------|-----------
 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | 1       
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 2       
 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 4
```

---

## Coming from Polars

This section walks through [Polars' getting started guide](https://docs.pola.rs/user-guide/getting-started/), showing how Polars concepts map to our library.

### Reading and Writing CSV Files

#### Round-trip Test

Let's create a DataFrame, write it to CSV, then read it back—a good way to test serialization.

**Polars version:**

```python
import polars as pl
import datetime as dt

df = pl.DataFrame(
    {
        "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
        "birthdate": [
            dt.date(1997, 1, 10),
            dt.date(1985, 2, 15),
            dt.date(1983, 3, 22),
            dt.date(1981, 4, 30),
        ],
        "weight": [57.9, 72.5, 53.6, 83.1],  ## (kg)
        "height": [1.56, 1.77, 1.65, 1.75],  ## (m)
    }
)
df.write_csv("docs/assets/data/output.csv")
df_csv = pl.read_csv("docs/assets/data/output.csv", try_parse_dates=True)
print(df_csv)
```

**Our version:**

```haskell
import qualified DataFrame as D
import Data.Time.Calendar

main :: IO ()
main = do
    let df = D.fromNamedColumns [
            ("name", D.fromList [ "Alice Archer"
                                , "Ben Brown"
                                , "Chloe Cooper"
                                , "Daniel Donovan"])
          , ("birthdate", D.fromList [ fromGregorian 1997 01 10
                                     , fromGregorian 1985 02 15
                                     , fromGregorian 1983 03 22
                                     , fromGregorian 1981 04 30])
          , ("weight", D.fromList [57.9, 72.5, 53.6, 83.1])
          , ("height", D.fromList [1.56, 1.77, 1.65, 1.75])
          ]
    print df
    D.writeCsv "./data/output.csv" df
    df_csv <- D.readCsv "./data/output.csv"
    print df_csv
```

**Output:**

```
----------------------------------------------
      name      | birthdate  | weight | height
----------------|------------|--------|-------
     [Char]     |    Day     | Double | Double
----------------|------------|--------|-------
 Alice Archer   | 1997-01-10 | 57.9   | 1.56  
 Ben Brown      | 1985-02-15 | 72.5   | 1.77  
 Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
 Daniel Donovan | 1981-04-30 | 83.1   | 1.75

----------------------------------------------
      name      | birthdate  | weight | height
----------------|------------|--------|-------
      Text      |    Day     | Double | Double
----------------|------------|--------|-------
 Alice Archer   | 1997-01-10 | 57.9   | 1.56  
 Ben Brown      | 1985-02-15 | 72.5   | 1.77  
 Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
 Daniel Donovan | 1981-04-30 | 83.1   | 1.75  
```

**Notice**: The string column type changes from `[Char]` (Haskell's string type) to `Text` (our library's preferred string type) after the round-trip. This is because `Text` is more efficient for CSV operations.

### Expressions

Both Polars and our library support "expressions"—composable operations that describe transformations. This is a powerful pattern that separates *what* you want to compute from *how* to compute it.

#### Basic Column Expressions

**Polars version:**

```python
result = df.select(
    pl.col("name"),
    pl.col("birthdate").dt.year().alias("birth_year"),
    (pl.col("weight") / (pl.col("height") ** 2)).alias("bmi"),
)
print(result)
```

**Our version:**

```haskell
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
import qualified DataFrame as D
import qualified Data.Text as T
import qualified DataFrame.Expressions as F
import DataFrame.Operations ( (|>) )
import Data.Time.Calendar (toGregorian)

main :: IO ()
main = do
    -- ... create df_csv ...
    
    -- Helper to extract year from Day
    let year d = let (y, _, _) = toGregorian d in y
    
    print $ df_csv
          |> D.derive "birth_year" (F.lift year (F.col @Day "birthdate"))
          |> D.derive "bmi" (F.col @Double "weight" / (F.pow (F.col @Double "height") 2))
          |> D.select ["name", "birth_year", "bmi"]
```

**Output:**

```
-------------------------------------------------
      name      | birth_year |        bmi        
----------------|------------|-------------------
      Text      |  Integer   |       Double      
----------------|------------|-------------------
 Alice Archer   | 1997       | 23.791913214990135
 Ben Brown      | 1985       | 23.14149829231702 
 Chloe Cooper   | 1983       | 19.687786960514234
 Daniel Donovan | 1981       | 27.13469387755102 
```

**Reading the code top-to-bottom:**

1. `derive "birth_year"` creates a new column by extracting years from birthdates
   - `F.lift` adapts a regular Haskell function to work with columns
   - `F.col @Day "birthdate"` references the birthdate column with explicit type
2. `derive "bmi"` creates another column with the BMI formula
   - `F.pow <col> 2` squares the height
   - Division works directly on column expressions
3. `select` keeps only the columns we want

**Key differences from Polars:**

- Type annotations (`@Day`, `@Double`) make column types explicit
- `lift` explicitly converts regular functions to work on columns
- `derive` is our version of Polars' `with_columns`

#### Lifting Functions

The `lift` family of functions is central to our expression system:

- `lift`: Apply a unary function to one column
- `lift2`: Apply a binary function to two columns  

Example:

```haskell
-- Using lift for a unary function
D.derive "doubled" (F.lift (*2) weight)

-- Using lift2 for a binary function
D.derive "weight_per_height" (F.lift2 (/) weight height)
```

#### Column Expansion

Polars allows selecting multiple columns in a single expression:

```python
result = df.select(
    pl.col("name"),
    (pl.col("weight", "height") * 0.95).round(2).name.suffix("-5%"),
)
print(result)
```

We don't provide built-in column expansion, so you write multiple explicit operations:

```haskell
df_csv
    |> D.deriveMany
          [ "weight-5%" .=  weight * 0.95
          , "height-5%" .= height * 0.95
          ]
    |> D.select ["name", "weight-5%", "height-5%"]
```

**However**, you can use standard Haskell functions to reduce repetition:

```haskell
let reduce name = D.derive (name <> "-5%") ((F.col @Double name) * (F.lit 0.95))
df_csv
    |> foldl (flip reduce) ["weight", "height"]
    |> D.select ["name", "weight-5%", "height-5%"]
```

Or, if you're transforming columns in place:

```haskell
let addSuffix suffix name = D.rename name (name <> suffix)
df_csv
  |> D.applyMany ["weight", "height"] (*0.95)
  |> foldl (flip (addSuffix "-5%")) ["weight", "height"]
  |> D.select ["name", "weight-5%", "height-5%"]
```

This demonstrates a key philosophy: rather than building every possible operation into the library, we leverage Haskell's functional programming features to compose operations elegantly.

### Filtering

Filtering by predicates is straightforward in both libraries.

**Polars version:**

```python
result = df.filter(pl.col("birthdate").dt.year() < 1990)
print(result)
```

**Our version:**

```haskell
let bornBefore1990 d = let (y, _, _) = toGregorian d 
                        in y < 1990

df_csv |> D.filter "birthdate" bornBefore1990
```

**Output:**

```
----------------------------------------------
      name      | birthdate  | weight | height
----------------|------------|--------|-------
      Text      |    Day     | Double | Double
----------------|------------|--------|-------
 Ben Brown      | 1985-02-15 | 72.5   | 1.77  
 Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
 Daniel Donovan | 1981-04-30 | 83.1   | 1.75
```

The predicate function receives individual values from the named column and returns `True` to keep the row.

#### Multiple Filter Conditions

Polars allows multiple filters in one call:

```python
result = df.filter(
    pl.col("birthdate").is_between(dt.date(1982, 12, 31), dt.date(1996, 1, 1)),
    pl.col("height") > 1.7,
)
```

We chain multiple `filter` calls:

```haskell
let year d = let (y, _, _) = toGregorian d in y
    between lo hi val = val >= lo && val <= hi

df_csv
  |> D.filter "birthdate" (between 1982 1996 . year)
  |> D.filter "height" (> 1.7)
```

**Output:**

```
-----------------------------------------
   name    | birthdate  | weight | height
-----------|------------|--------|-------
   Text    |    Day     | Double | Double
-----------|------------|--------|-------
 Ben Brown | 1985-02-15 | 72.5   | 1.77
```

Each filter operates on the DataFrame sequentially, and the types ensure you can't filter on non-existent columns.

Alternatively, you can use `filterWhere` with boolean expression combinations:

```haskell
df_csv
  |> D.filterWhere ((F.col @Int "birth_year" .>= 1982) 
                    .&& (F.col @Int "birth_year" .<= 1996)
                    .&& (F.col @Double "height" .> 1.7))
```

### Grouping and Aggregation

Grouping and aggregation is where our library diverges most significantly from Polars.

#### Simple Count by Group

**Polars version:**

```python
result = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
    maintain_order=True,
).len()
print(result)
```

**Our version:**

```haskell
let decade d = let (y, _, _) = toGregorian d 
                in (y `div` 10) * 10

df_csv
    |> D.derive "decade" (F.lift decade (F.col @Day "birthdate"))
    |> D.groupBy ["decade"]
    |> D.aggregate [F.count (F.col @Day "decade") `F.as` "Count"]
```

**Output:**

```
---------------
 decade | Count
--------|------
  Int   | Int
--------|------
 1990   | 1  
 1980   | 3 
```

**Key differences:**

1. **Explicit Derivation**: We first create the decade column explicitly with `derive`
2. **Separate Selection**: We use `select` to choose columns before grouping
3. **Group Then Aggregate**: `groupBy` creates a grouped DataFrame, then `aggregate` computes statistics
4. **Named Results**: The aggregation result column must be explicitly named with `as`

This separation makes the data flow clearer: derive → select → group → aggregate.

#### Multiple Aggregations

**Polars version:**

```python
result = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
    maintain_order=True,
).agg(
    pl.len().alias("sample_size"),
    pl.col("weight").mean().round(2).alias("avg_weight"),
    pl.col("height").max().alias("tallest"),
)
```

**Our version:**

```haskell
let decade d = let (y, _, _) = toGregorian d 
                in (y `div` 10) * 10

df_csv
    |> D.derive "decade" (F.lift decade (F.col @Day "birthdate"))
    |> D.groupBy ["decade"]
    |> D.aggregate [ F.count (F.col @Day "decade") `F.as` "sample_size"
                   , F.mean (F.col @Double "weight") `F.as` "avg_weight"
                   , F.max (F.col @Double "height") `F.as` "tallest"
                   ]
```

**Output:**

```
---------------------------------------------------
 decade | sample_size |    avg_weight     | tallest
--------|-------------|-------------------|--------
  Int   |     Int     |      Double       | Double    
--------|-------------|-------------------|--------
 1990   | 1           | 57.9              | 1.56          
 1980   | 3           | 69.73333333333333 | 1.77
```

The `aggregate` function takes a list of aggregation expressions. Each expression specifies:
- What column to aggregate (`F.col @Type "name"`)
- What aggregation to perform (`mean`, `max`, `count`, etc.)
- What to name the result (`as "new_name"`)

#### Complex Aggregations

**Polars version:**

```python
result = (
    df.with_columns(
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
        pl.col("name").str.split(by=" ").list.first(),
    )
    .select(pl.all().exclude("birthdate"))
    .group_by(pl.col("decade"), maintain_order=True)
    .agg(
        pl.col("name"),
        pl.col("weight", "height").mean().round(2).name.prefix("avg_"),
    )
)
```

**Our version:**

```haskell
import qualified Data.Text as T

let decade d = let (y, _, _) = toGregorian d 
                in (y `div` 10) * 10
    firstName = head . T.split (== ' ')

df_csv
    |> D.derive "name" (F.lift firstName (F.col @T.Text "name"))
    |> D.derive "decade" (F.lift decade (F.col @Day "birthdate"))
    |> D.exclude ["birthdate"]
    |> D.groupBy ["decade"]
    |> D.aggregate [ F.mean (F.col @Double "weight") `F.as` "avg_weight"
                   , F.mean (F.col @Double "height") `F.as` "avg_height"
                   , F.collect (F.col @T.Text "name") `F.as` "names"
                   ]
```

**Output:**

```
---------------------------------------------------------------------------
decade  |    avg_weight     |     avg_height     |          names          
--------|-------------------|--------------------|-------------------------
Integer |      Double       |       Double       |          [Text]         
--------|-------------------|--------------------|-------------------------
1980    | 69.73333333333333 | 1.7233333333333334 | ["Daniel","Chloe","Ben"]
1990    | 57.9              | 1.56               | ["Alice"]
```

The `collect` aggregation gathers all values in a group into a list, which is our library's way of handling list-like aggregations.

---

## Coming from dplyr

This section walks through [dplyr's mini tutorial](https://dplyr.tidyverse.org/), showing how R's dplyr concepts map to our library.

### The Pipe Operator

Both dplyr and our library use a pipe operator for chaining operations:

- **dplyr**: `%>%` (magrittr pipe)
- **Our library**: `|>` (Haskell's forward application operator)

The concept is identical: pass the result of one operation as the first argument to the next.

### Filtering Rows

**dplyr version:**

```r
starwars %>% 
  filter(species == "Droid")
#> # A tibble: 6 × 14
#>   name   height  mass hair_color skin_color  eye_color birth_year sex   gender  
#>   <chr>   <int> <dbl> <chr>      <chr>       <chr>          <dbl> <chr> <chr>   
#> 1 C-3PO     167    75 <NA>       gold        yellow           112 none  masculi…
#> 2 R2-D2      96    32 <NA>       white, blue red               33 none  masculi…
#> ...
```

**Our version:**

```haskell
import qualified Data.Text as T

starwars 
  |> D.filterWhere (F.col @Text "species" .== "Droid")
```

**Output (truncated for readability):**

```
-----------------------------------
  name  |  height   | species | ...
--------|-----------|---------|----
  Text  | Maybe Int |  Text   | ...
--------|-----------|---------|----
 C-3PO  | Just 167  | Droid   | ...
 R2-D2  | Just 96   | Droid   | ...
 R5-D4  | Just 97   | Droid   | ...
 ...
```

**Note**: The type application `@Text` is necessary because string literals in Haskell are polymorphic. This tells the compiler we're comparing against `Text` values, not strings.

### Selecting Columns

**dplyr version:**

```r
starwars %>% 
  select(name, ends_with("color"))
#> # A tibble: 87 × 4
#>   name           hair_color skin_color  eye_color
#>   <chr>          <chr>      <chr>       <chr>    
#> 1 Luke Skywalker blond      fair        blue     
#> 2 C-3PO          <NA>       gold        yellow   
#> ...
```

**Our version (explicit):**

```haskell
starwars 
  |> D.select ["name", "hair_color", "skin_color", "eye_color"]
  |> D.take 5
```

**Our version (with predicate):**

For predicate-based selection like `ends_with()`, we use `selectBy`:

```haskell
starwars 
  |> D.selectBy [D.byName "name", D.byNameProperty (T.isSuffixOf "color")]
  |> D.take 5
```

**Output:**

```
-------------------------------------------------------------
        name        |  hair_color   | skin_color  | eye_color
--------------------|---------------|-------------|----------
        Text        |     Text      |    Text     |   Text   
--------------------|---------------|-------------|----------
 Luke Skywalker     | blond         | fair        | blue     
 C-3PO              | NA            | gold        | yellow   
 R2-D2              | NA            | white, blue | red      
 Darth Vader        | none          | white       | yellow   
 Leia Organa        | brown         | light       | brown    
```

`selectBy` takes a predicate function that receives each column name and returns `True` to keep that column.

### Creating New Columns

**dplyr version:**

```r
starwars %>% 
  mutate(bmi = mass / ((height / 100) ^ 2)) %>%
  select(name:mass, bmi)
```

**Our version:**

```haskell
starwars
  -- Remove the maybes.
  |> D.filterJust "mass"
  |> D.filterJust "height"
  |> D.derive "bmi" (F.col @Double "mass" / F.pow (F.col @Double "height" / F.lit 100) 2)
  |> D.select ["name", "height", "mass", "bmi"]
  |> D.take 5
```

We can enable non-started evaluation by one of three methods:
* In a terminal, calling the `:declareColumns` macro on the dataframe.
* In a notebook calling the function `declareColumns`
* In a script adding `$(declareColumnsFromCsv)` after the imports section.

Once we do this we can rewrite our expressions as (in this case in a notebook):

```haskell
let cleaned = starwars
  -- Remove the maybes.
  |> D.filterAllJust

F.declareColumns cleaned

cleaned
  |> D.derive "bmi" (mass / F.pow height 2 / 100)
  |> D.select ["name", "height", "mass", "bmi"]
  |> D.take 5
```

**Output:**

```
------------------------------------------------------------------------
       name        |  height   |   mass    |           bmi          
-------------------|-----------|-----------|------------------------
       Text        | Maybe Int | Maybe Int |      Maybe Double      
-------------------|-----------|-----------|------------------------
 Luke Skywalker    | Just 172  | Just 77   | Just 26.027582477014604
 C-3PO             | Just 167  | Just 75   | Just 26.89232313815483 
 R2-D2             | Just 96   | Just 32   | Just 34.72222222222222 
 Darth Vader       | Just 202  | Just 136  | Just 33.33006567983531 
 Leia Organa       | Just 150  | Just 49   | Just 21.77777777777778 
```

**What's happening:**

1. Convert string/integer columns to `Maybe Double` (handling "NA" values)
2. Filter out rows with missing height or mass
3. Create the BMI column using column expressions
4. Select the columns we want to view

This is more verbose than dplyr's `mutate`, but it's explicit about type conversions and missing data handling.

### Sorting

**dplyr version:**

```r
starwars %>% 
  arrange(desc(mass))
```

**Our version:**

```haskell
starwars 
  |> D.sortBy [D.Desc "mass"]
```

**Output (truncated):**

```
-------------------------------------------------
         name          |  height   |   mass    | ...
-----------------------|-----------|-----------|----
         Text          | Maybe Int | Maybe Int | ...
-----------------------|-----------|-----------|----
 Jabba Desilijic Tiure | Just 175  | Just 1358 | ...
 Grievous              | Just 216  | Just 159  | ...
 IG-88                 | Just 200  | Just 140  | ...
 Tarfful               | Just 234  | Just 136  | ...
 Darth Vader           | Just 202  | Just 136  | ...
```

For multi-column sorting, provide multiple column names: `D.sortBy [D.Desc mass, D.Asc height]`

### Grouping and Summarizing

**dplyr version:**

```r
starwars %>%
  group_by(species) %>%
  summarise(
    n = n(),
    mass = mean(mass, na.rm = TRUE)
  ) %>%
  filter(n > 1, mass > 50)
```

**Our version:**

```haskell
starwars 
  |> D.select ["species", "mass"]
  |> D.groupBy ["species"]
  |> D.aggregate [ F.mean mass `F.as` "mean_mass"
                 , F.count mass `F.as` "count"
                 ]
  |> D.filterWhere ((F.col @Int "count" .> 1) .&& (F.col @Double "mean_mass" .> 50))
```

**Output:**

```
-------------------------------------
 species  |     mean_mass     | count
----------|-------------------|------
   Text   |      Double       |  Int 
----------|-------------------|------
 Human    | 81.47368421052632 | 35   
 Droid    | 69.75             | 6    
 Wookiee  | 124.0             | 2    
 NA       | 81.0              | 4    
 Gungan   | 74.0              | 3    
 Zabrak   | 80.0              | 2    
 Twi'lek  | 55.0              | 2    
 Kaminoan | 88.0              | 2
```

**Key points:**

1. We use `aggregate` with a list of aggregation expressions
2. `filterWhere` allows filtering based on multiple column conditions using `.&&` (AND) and `.|` (OR)
3. Type annotations ensure we're comparing the right types

---

## Summary of Key Patterns

### Our Library's Design Philosophy

1. **Explicit Over Implicit**: Every operation is named and clear
2. **Types as Documentation**: Type signatures tell you what's possible
3. **Composition Over Configuration**: Use Haskell functions to build complex operations
4. **Functional Pipeline**: Chain operations using `|>` for readable data transformations

### Common Idioms

**Creating DataFrames:**
```haskell
df = D.fromNamedColumns [
    ("col1", D.fromList [val1, val2, ...]),
    ("col2", D.fromList [val3, val4, ...])
]
```

**Selecting and Filtering:**
```haskell
df |> D.select ["col1", "col2"]
   |> D.filter "col1" (> (10 :: Int))
```

**Creating Derived Columns:**
```haskell
df |> D.derive "new_col" (old_col * 2)
```

**Grouping and Aggregating:**
```haskell
df |> D.groupBy ["group_col"]
   |> D.aggregate [ F.mean val `F.as` "avg_val"
                  , F.count val `F.as` "n"
                  ]
```

**Handling Missing Data:**
```haskell
-- Filter out Nothing values
df |> D.filterJust "col"

-- Fill Nothing with default
df |> D.impute (F.col @Type "col") defaultVal
```

### Type Annotations

Our library often requires type annotations to disambiguate operations:

```haskell
F.col @Double "weight"  -- Specify column contains Doubles
F.lit @Int 5            -- Specify literal is an Int
filter "col" (== ("text" :: T.Text))  -- Specify string type
```

This explicitness catches errors early and serves as inline documentation.

---

## Next Steps

Now that you understand how familiar operations translate to our library:

1. **Explore the API documentation** for the full set of available functions
2. **Start with simple pipelines** and gradually add complexity
3. **Leverage Haskell's type system** to catch errors early
4. **Use functional composition** to build reusable transformation pipelines
5. **Experiment with expression building** using the `F.*` functions

Remember: while our library requires more explicitness than pandas, Polars, or dplyr, this explicitness provides safety, clarity, and composability that becomes invaluable in production code.
