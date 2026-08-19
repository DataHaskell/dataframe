# Working with Nullable Columns

Real-world datasets almost always have missing values. This guide covers every approach the library offers for representing, filtering, transforming, and filling nullable data — from loading a CSV through to building type-safe pipelines with `DataFrame.Typed`.

## How nullability is represented

A nullable column has the Haskell type `Maybe a`. Under the hood it is stored as an `OptionalColumn (Vector (Maybe a))`. You can confirm this by inspecting a column or checking the schema printed by `D.print`.

```haskell
import qualified Data.Vector as V
import qualified DataFrame as D
import qualified DataFrame.Core as DI

-- Manually build a frame with a nullable Int column
df :: D.DataFrame
df = D.fromNamedColumns
    [ ("id",    DI.fromList [1, 2, 3 :: Int])
    , ("score", DI.fromList [Just 90, Nothing, Just 75 :: Maybe Int])
    ]
```

When you load a CSV with `D.readCsv`, columns that contain blank cells are automatically inferred as `Maybe T` for the appropriate `T`.

---

## Detecting and filtering missing values

### Drop rows where a specific column is `Nothing`

```haskell
D.filterJust "score" df
-- keeps only rows where score IS NOT Nothing
-- strips the Maybe wrapper: column becomes plain Int
```

### Keep only the rows that ARE missing

```haskell
D.filterNothing "score" df
```

### Drop rows where **any** column is `Nothing`

```haskell
D.filterAllJust df
```

---

## Filling (imputing) missing values

### Fill with a constant

```haskell
import qualified DataFrame.Functions as F

-- Replace every Nothing in "score" with 0
D.impute (F.col @(Maybe Int) "score") 0 df
```

### Fill with a computed aggregate (mean, median, etc.)

```haskell
-- Replace every Nothing in "score" with the mean of the non-missing values
D.imputeWith F.mean (F.col @(Maybe Int) "score") df
```

`imputeWith` accepts any aggregate expression (`F.mean`, `F.median`, a custom fold, etc.) and computes it over the non-null rows before filling.

---

## Computing with nullable columns in expressions

The library offers three levels of null-awareness for expression operators.

### Same-type strict operators (`.==.`, `.<.`, …)

These work on non-nullable columns and return plain `Bool`.

```haskell
-- Both columns must be non-nullable
D.filterWhere (F.col @Int "id" .==. F.lit 2) df
```

### Nullable-aware operators (`.+`, `.-`, `.*`, `./`, `.==`, `.<`, …)

These accept any combination of nullable and non-nullable operands and propagate `Nothing` automatically (three-valued logic).

```haskell
import DataFrame.Operators

-- Int column + Maybe Int column → Maybe Int column
D.derive "adjusted" (F.col @Int "id" .+ F.col @(Maybe Int) "score") df

-- Comparison: Maybe Int .== Int → Maybe Bool column
D.derive "match" (F.col @(Maybe Int) "score" .== F.lit 90) df
```

The result type is determined at compile time:

| Left operand | Right operand | Result |
|---|---|---|
| `a` | `a` | `a` (arithmetic) / `Bool` (comparison) |
| `Maybe a` | `a` | `Maybe a` / `Maybe Bool` |
| `a` | `Maybe a` | `Maybe a` / `Maybe Bool` |
| `Maybe a` | `Maybe a` | `Maybe a` / `Maybe Bool` |

### `nullLift` and `nullLift2` — apply arbitrary functions

When the built-in operators don't cover your function, use `nullLift` (unary) and `nullLift2` (binary). They propagate `Nothing` automatically, matching the same null-short-circuit semantics as the arithmetic operators.

```haskell
-- Unary: negate over Maybe Int column → Maybe Int
D.derive "neg_score" (F.nullLift negate (F.col @(Maybe Int) "score")) df

-- Unary: negate over plain Int column → Int (no wrapping)
D.derive "neg_id" (F.nullLift negate (F.col @Int "id")) df

-- Binary: mixed nullable — result type follows the same table above
D.derive "sum" (F.nullLift2 (+) (F.col @Int "id") (F.col @(Maybe Int) "score")) df

-- Binary: custom function, both non-nullable
D.derive "product" (F.nullLift2 (*) (F.col @Int "id") (F.col @Int "id")) df
```

`nullLift` / `nullLift2` work for **any** function, including those returning a different type:

```haskell
import qualified Data.Text as T

-- Convert nullable Int to nullable Text
D.derive "score_text"
    (F.nullLift (T.pack . show) (F.col @(Maybe Int) "score"))
    df
-- produces a Maybe Text column
```

### `whenBothPresent` — legacy binary combinator

`whenBothPresent` predates `nullLift2` and is retained for backward compatibility. It handles the both-nullable case for operands of the same type:

```haskell
F.whenBothPresent (+) (F.col @(Maybe Int) "a") (F.col @(Maybe Int) "b")
-- equivalent to: F.nullLift2 (+) ...
```

Prefer `nullLift2` for new code.

---

## Transforming nullable columns with `apply`

`D.apply` is lenient with respect to optionality. If you pass a function `f :: a -> b` but the column holds `Maybe a`, the function is automatically `fmap`-ed over the inner values — `Nothing` rows stay `Nothing`.

```haskell
-- Column "score" is Maybe Int; negate :: Int -> Int
D.apply @Int negate "score" df
-- result: Maybe Int column with Just values negated, Nothing rows unchanged
```

This means you rarely need to write `D.apply @(Maybe Int) (fmap negate)` explicitly.

---

## Casting to and from `Maybe`

`F.cast` can promote a non-nullable column to its `Maybe` counterpart:

```haskell
-- Wrap the plain Int "id" column in Maybe
D.derive "maybe_id" (F.cast @(Maybe Int) Nothing "id") df
-- if "id" is already Maybe Int, the column is used as-is
-- if "id" is plain Int, each value is wrapped in Just
```

`F.unsafeCast` strips `Maybe` when you know (at runtime) there are no `Nothing` values:

```haskell
D.derive "bare_score" (F.coerce @Int "score") df
```

---

## The typed API (`DataFrame.Typed`)

`TypedDataFrame` tracks the schema — including whether each column is `Maybe` — in the type. The typed operators mirror the untyped ones.

```haskell
{-# LANGUAGE DataKinds, TypeApplications #-}
import qualified DataFrame.Typed as T
import qualified DataFrame.Typed.Expr as TE

type MySchema = '[ '("id", Int), '("score", Maybe Int)]

-- filterAllJust strips Maybe from all columns in the type
stripped :: T.TypedDataFrame '[ '("id", Int), '("score", Int)]
stripped = T.filterAllJust typedDf

-- Nullable-aware expression
sumExpr :: TE.TExpr MySchema (Maybe Int)
sumExpr = TE.col @"id" TE..+ TE.col @"score"   -- Int + Maybe Int → Maybe Int

-- nullLift on a typed expression
negScore :: TE.TExpr MySchema (Maybe Int)
negScore = TE.nullLift negate (TE.col @"score")
```

### `filterAllJust` removes `Maybe` from the schema type

```haskell
-- Before
df :: T.TypedDataFrame '[ '("x", Maybe Double), '("y", Int)]

-- After
T.filterAllJust df :: T.TypedDataFrame '[ '("x", Double), '("y", Int)]
```

This is tracked statically — you get a type error if you later try to treat the stripped column as `Maybe`.

### `impute` in the typed API

```haskell
-- Replace Nothing in "score" with 0; schema changes from Maybe Int → Int
T.impute @"score" 0 typedDf
    :: T.TypedDataFrame '[ '("id", Int), '("score", Int)]
```

---

## Decision guide

| Situation | Recommended approach |
|---|---|
| Drop rows with missing values | `D.filterJust` / `D.filterAllJust` |
| Fill missing values with a constant | `D.impute` |
| Fill missing values with mean/median | `D.imputeWith F.mean` / `D.imputeWith F.median` |
| Arithmetic between nullable and non-nullable columns | `.+`, `.-`, `.*`, `./` |
| Comparison involving nullable columns | `.==`, `.<`, `.<=`, `.>=`, `.>`, `./=` |
| Apply an arbitrary unary function, propagating `Nothing` | `F.nullLift` |
| Apply an arbitrary binary function, propagating `Nothing` | `F.nullLift2` |
| Apply a function to a column that may or may not be `Maybe` | `D.apply` (auto-fmaps) |
| Promote a non-nullable column to `Maybe` | `F.cast @(Maybe T)` |
| Strip `Maybe` when you know there are no `Nothing` values | `F.coerce` |
| Track nullability in the type at compile time | `DataFrame.Typed` |
