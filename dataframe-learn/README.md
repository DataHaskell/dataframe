<!-- scripths: 0.5.3.0 -->

<!--
  This README is a runnable scripths (https://github.com/DataHaskell/scripths)
  notebook. Every ```haskell block runs top-to-bottom in one shared session and
  scripths inserts each block's output beneath it as a blockquote. The
  `-- cabal: packages:` directive builds against the local working tree.
  Regenerate (from the repo root) with:

      scripths dataframe-learn/README.md -o dataframe-learn/README.md
-->

# dataframe-learn

Symbolic machine learning for [`dataframe`](https://hackage.haskell.org/package/dataframe)
where a fitted model is a dataframe expression. The API borrows from the now ubiquitous
scikit-learn `fit` + `predict` convention. `fit` returns a record containing model information.
`predict` takes that record and returns an `Expr` over your columns. The expression is a
normal dataframe expression that can be:

* applied with `derive`
* pretty-printed
* manipulated symbolically.

## Linear regression

For a linear regression `fit` returns a record (with `regCoef`/`regIntercept` for inspection)
and `predict` compiles it to an `Expr Double`.

```haskell
-- cabal: packages: .., ., ../dataframe-core, ../dataframe-parsing, ../dataframe-operations, ../dataframe-csv, ../dataframe-json, ../dataframe-parquet, ../dataframe-lazy, ../dataframe-viz, ../dataframe-expr-serializer, ../dataframe-th, ../dataframe-csv-th, ../dataframe-parquet-th, ../dataframe-huggingface
-- cabal: build-depends: dataframe, dataframe-learn, text, random
-- cabal: default-extensions: OverloadedStrings, TypeApplications, DataKinds, TypeOperators, FlexibleContexts
-- cabal: ghc-options: -w
import qualified DataFrame as D
import DataFrame.Learn

sales = D.fromNamedColumns
    [ ("x", D.fromList ([1, 2, 3, 4, 5, 6] :: [Double]))
    , ("y", D.fromList ([2 * x + 1 | x <- [1, 2, 3, 4, 5, 6]] :: [Double]))
    ]

model = fit defaultLinearConfig (D.col @Double "y") sales
putStrLn (D.prettyPrint (predict model))
```

> <!-- scripths:mime text/plain -->
> 2.0 * x + 0.9999999999999989

## Type-safe linear regression

`fit` and `predict` work on both typed and untyped dataframes. You can
have the compiler enforce that you don't hand the fit function a frame
with nullable fields or a non-Double:

```haskell
import qualified DataFrame.Typed as T
import Data.Maybe (fromJust)

salesT     = T.unsafeFreeze @'[ '("x", Double), '("y", Double) ] sales
typedModel = fit defaultLinearConfig (T.col @"y") salesT
scored     = T.derive @"prediction" (predict typedModel) salesT

putStr (unlines
    [ "typed model:  " ++ D.prettyPrint (T.unTExpr (predict typedModel))
    , "schema after: " ++ show (T.columnNames scored) ])
```

> <!-- scripths:mime text/plain -->
> typed model:  2.0 * x + 0.9999999999999989
> schema after: ["x","y","prediction"]

## Decision trees

The tree compiles to nested `if/then/else` over your columns:

```haskell
flowers = D.fromNamedColumns
    [ ("petal_length", D.fromList ([1.4, 1.3, 1.5, 1.4, 4.5, 4.7, 4.6, 4.4, 5.5, 5.8, 5.6, 5.7] :: [Double]))
    , ("petal_width",  D.fromList ([0.2, 0.2, 0.1, 0.3, 1.5, 1.4, 1.6, 1.3, 2.0, 2.1, 1.9, 2.2] :: [Double]))
    , ("species",      D.fromList ([0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2] :: [Double]))
    ]

tree = fit defaultTreeConfig (D.col @Double "species") flowers
putStrLn (D.prettyPrint (predict tree))
```

> <!-- scripths:mime text/plain -->
> if petal_length .<=. 2.95
> then 0.0
> else if petal_length .<=. 5.1
> then 1.0
> else 2.0

## Symbolic regression discovers a formula

Genetic programming searches for an expression that fits the data, and returns
it as a dataframe `Expr` plus the accuracy/complexity Pareto front:

```haskell
curve = D.fromNamedColumns
    [ ("x", D.fromList xs)
    , ("y", D.fromList [x * x + x | x <- xs])
    ]
  where xs = [-3, -2, -1, 0, 1, 2, 3, 4, 5, 6] :: [Double]

sr = fit
        defaultSRConfig { srSeed = 3, srGenerations = 50, srPopSize = 300, srUnaryOps = [] }
        (D.col @Double "y") curve
putStrLn (D.prettyPrint (srBest sr) ++ "   (mse " ++ show (srBestMSE sr) ++ ")")
```

> <!-- scripths:mime text/plain -->
> x + x * x   (mse 0.0)

## Deploy: applying an expression to a frame

Because the model is an `Expr` you can use `derive` to do inference.

```haskell
D.columnNames (D.derive "prediction" (predict model) sales)
```

> <!-- scripths:mime text/plain -->
> ["x","y","prediction"]

## A model and its preprocessing compose by substitution

Preprocessing is an expression too, so a model trained in a transformed space and
the transform that produced it compose. Composition of expressions is
substitution of one into the other. `compileThrough` performs that composition,
folding a fitted transform into a prediction so the result is a single formula
over the raw inputs. Below we standardize `x`, fit in the scaled space, then fold
the scaler back in to recover a raw-column model:

```haskell
scaler      = standardScaler ["x"] sales
scaledSales = applyTransform (scalerTransform scaler) sales
scaledModel = fit defaultLinearConfig (D.col @Double "y") scaledSales

deployed = compileThrough (scalerTransform scaler) (predict scaledModel)
putStr (unlines
    [ "trained in scaled space: " ++ D.prettyPrint (predict scaledModel)
    , "folded to raw columns:   " ++ D.prettyPrint deployed ])
```

> <!-- scripths:mime text/plain -->
> trained in scaled space: 3.4156502553198655 * x + 8.0
> folded to raw columns:   3.4156502553198655 * (x - 3.5) / 1.707825127659933 + 8.0

The folded expression is a function of the raw `x` alone, so it scores the
original frame with no preprocessing step at inference time.

```haskell
evaluate rmse deployed (D.col @Double "y") sales
```

> <!-- scripths:mime text/plain -->
> 3.6259732146947156e-16

## Splitting the data, and evaluation

```haskell
import qualified DataFrame as D

realistic = D.fromNamedColumns
    [ ("id", D.fromList [fromIntegral ((i * 7919) `mod` 97) | i <- [1 .. 40 :: Int]])
    , ("x",  D.fromList xs)
    , ("y",  D.fromList [2 * x + 1 + noise i | (i, x) <- zip [0 :: Int ..] xs])
    ]
  where
    xs      = map fromIntegral [1 .. 40 :: Int] :: [Double]
    noise i = fromIntegral ((i * 2654435761 + 12345) `mod` 1000) / 100 - 5

clean = D.select ["x", "y"] realistic
```

> <!-- scripths:mime text/plain -->

**Hold-out evaluation.** `randomSplit` (seeded, deterministic) keeps the
score honest — evaluate on rows the model never saw, and the metrics are
realistic, not the `1e-15` of an in-sample toy:

```haskell
import System.Random (mkStdGen)

(train, test) = D.randomSplit (mkStdGen 7) 0.75 clean
heldModel     = fit defaultLinearConfig (D.col @Double "y") train
putStr (unlines
    [ "held-out R^2:  " ++ show (evaluate r2   (predict heldModel) (D.col @Double "y") test)
    , "held-out RMSE: " ++ show (evaluate rmse (predict heldModel) (D.col @Double "y") test) ])
```

> <!-- scripths:mime text/plain -->
> held-out R^2:  0.9671190074242891
> held-out RMSE: 3.56674709632647

**Cross-validation.** `crossValidate` is scikit-learn's `cross_val_score`: it
fits on each training fold and scores the prediction expression on the held-out
fold. You pass a `train -> Expr` closure, so it works with any model:

```haskell
cv = crossValidate 5 0 rmse (D.col @Double "y")
         (\tr -> predict (fit defaultLinearConfig (D.col @Double "y") tr))
         clean
putStrLn ("5-fold RMSE: " ++ show (sum cv / fromIntegral (length cv)))
```

> <!-- scripths:mime text/plain -->
> 5-fold RMSE: 3.0325616706245713

`gridSearch` tunes hyperparameters the same way, over a list of configs.

## Reporting metrics

Metrics are plain functions (`rmse`, `mse`, `r2`, `accuracy`, multiclass
`precision`/`recall`/`f1`), and `classificationReport` bundles the common numbers
with a scikit-learn-style layout (per-class precision/recall/F1/support plus
macro/weighted averages):

```haskell
clf = fit defaultLogisticConfig (D.col @Double "species") flowers
putStr (show (classificationReportExpr (predict clf) (D.col @Double "species") flowers))
```

> <!-- scripths:mime text/plain -->
> class       precision   recall      f1          support     
> 0.0         1.0         1.0         1.0         4           
> 1.0         1.0         1.0         1.0         4           
> 2.0         1.0         1.0         1.0         4           
> 
> accuracy    = 1.0
> macro f1    = 1.0
> weighted f1 = 1.0

## Pipelines compose as a monoid

A fitted preprocessing step is a `Transform`, and transforms compose with `<>`.
`applyTransform` runs the whole pipeline; `compileThrough` folds it into a single
expression over the raw columns for export:

```haskell
features = ["petal_length", "petal_width"]
scalerF  = standardScaler features flowers
pca      = fit (PCAConfig (NComp 2) True) (map (D.col @Double) features) flowers
pipeline = scalerTransform scalerF <> pcaTransform pca

D.columnNames (applyTransform pipeline flowers)
```

> <!-- scripths:mime text/plain -->
> ["petal_length","petal_width","species","pc1","pc2"]

## Synthesize the feature you would have hand-engineered

`DataFrame.Synthesis` is automated feature engineering: a bottom-up enumerative
search (with observational-equivalence pruning) for a small, interpretable
expression over your columns that tracks the target. Here `y` is the interaction
`a * b`, which a linear model on the raw columns cannot capture; synthesis
discovers the term, and feeding it back as a column lifts the fit from mediocre
to exact — still a formula you can read:

```haskell
interactions = D.fromNamedColumns
    [ ("a", D.fromList as)
    , ("b", D.fromList bs)
    , ("y", D.fromList (zipWith (*) as bs))
    ]
  where
    as = [-1, -1, 1, 1, -2, 2, -2, 2] :: [Double]
    bs = [-1, 1, -1, 1, -2, -2, 2, 2] :: [Double]

rawModel = fit defaultLinearConfig (D.col @Double "y") interactions
feature  = fit defaultSynthesisConfig (D.col @Double "y") interactions
withFeat = D.derive "synth" (predict feature) interactions
fitModel =
    fit defaultLinearConfig (D.col @Double "y")
        (D.select ["synth", "y"] withFeat)

putStr (unlines
    [ "discovered feature: " ++ D.prettyPrint (predict feature)
    , "raw linear R^2:     " ++ show (evaluate r2 (predict rawModel) (D.col @Double "y") interactions)
    , "with synth feature: " ++ show (evaluate r2 (predict fitModel) (D.col @Double "y") withFeat)
    ])
```

> <!-- scripths:mime text/plain -->
> discovered feature: a * b
> raw linear R^2:     0.0
> with synth feature: 1.0

`predict feature` is the single best expression; `sfFeatures feature` is the whole
ranked, deduplicated bank, ready to `derive` as a batch of candidate columns.
