<!-- scripths: 0.5.5.0 -->

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
import DataFrame ((=:))
import DataFrame.Learn

sales = D.fromColumns
    [ "x" =: [1, 2, 3, 4, 5, 6] :: [Double]
    , "y" =: [2 * x + 1 | x <- [1, 2, 3, 4, 5, 6]] :: [Double]
    ]

model = fit defaultLinearConfig (D.col @Double "y") sales
putStrLn (D.prettyPrint (predict model))
```

> <!-- scripths:mime text/plain -->
> <interactive>:17:7: error: [GHC-83865]
>     • Couldn't match expected type: [Double]
>                   with actual type: (Data.Text.Internal.Text, D.Column)
>     • In the expression: "x" =: [1, 2, 3, ....] :: [Double]
>       In the first argument of ‘D.fromColumns’, namely
>         ‘["x" =: [1, 2, ....] :: [Double],
>           "y" =: [2 * x + 1 | x <- [1, ....]] :: [Double]]’
>       In the expression:
>         D.fromColumns
>           ["x" =: [1, 2, ....] :: [Double],
>            "y" =: [2 * x + 1 | x <- [1, ....]] :: [Double]]
> 
> <interactive>:18:7: error: [GHC-83865]
>     • Couldn't match expected type: [Double]
>                   with actual type: (Data.Text.Internal.Text, D.Column)
>     • In the expression:
>           "y" =: [2 * x + 1 | x <- [1, 2, ....]] :: [Double]
>       In the first argument of ‘D.fromColumns’, namely
>         ‘["x" =: [1, 2, ....] :: [Double],
>           "y" =: [2 * x + 1 | x <- [1, ....]] :: [Double]]’
>       In the expression:
>         D.fromColumns
>           ["x" =: [1, 2, ....] :: [Double],
>            "y" =: [2 * x + 1 | x <- [1, ....]] :: [Double]]
> 
> <interactive>:24:34: error: [GHC-88464]
>     Variable not in scope: model

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
> <interactive>:35:66: error: [GHC-88464]
>     Variable not in scope: sales :: D.DataFrame
>     Suggested fix: Perhaps use ‘salesT’ (line 35)
> 
> <interactive>:42:61: error: [GHC-88464]
>     Variable not in scope: typedModel
> 
> <interactive>:43:47: error: [GHC-88464]
>     Variable not in scope: scored :: T.TypedDataFrame cols1

## Decision trees

The tree compiles to nested `if/then/else` over your columns:

```haskell
flowers = D.fromColumns
    [ "petal_length" =: [1.4, 1.3, 1.5, 1.4, 4.5, 4.7, 4.6, 4.4, 5.5, 5.8, 5.6, 5.7] :: [Double]
    , "petal_width"  =: [0.2, 0.2, 0.1, 0.3, 1.5, 1.4, 1.6, 1.3, 2.0, 2.1, 1.9, 2.2] :: [Double]
    , "species"      =: [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2] :: [Double]
    ]

tree = fit defaultTreeConfig (D.col @Double "species") flowers
putStrLn (D.prettyPrint (predict tree))
```

> <!-- scripths:mime text/plain -->
> <interactive>:52:7: error: [GHC-83865]
>     • Couldn't match expected type: [Double]
>                   with actual type: (Data.Text.Internal.Text, D.Column)
>     • In the expression:
>           "petal_length" =: [1.4, 1.3, 1.5, ....] :: [Double]
>       In the first argument of ‘D.fromColumns’, namely
>         ‘["petal_length" =: [1.4, 1.3, ....] :: [Double],
>           "petal_width" =: [0.2, 0.2, ....] :: [Double],
>           "species" =: [0, 0, ....] :: [Double]]’
>       In the expression:
>         D.fromColumns
>           ["petal_length" =: [1.4, 1.3, ....] :: [Double],
>            "petal_width" =: [0.2, 0.2, ....] :: [Double],
>            "species" =: [0, 0, ....] :: [Double]]
> 
> <interactive>:53:7: error: [GHC-83865]
>     • Couldn't match expected type: [Double]
>                   with actual type: (Data.Text.Internal.Text, D.Column)
>     • In the expression:
>           "petal_width" =: [0.2, 0.2, 0.1, ....] :: [Double]
>       In the first argument of ‘D.fromColumns’, namely
>         ‘["petal_length" =: [1.4, 1.3, ....] :: [Double],
>           "petal_width" =: [0.2, 0.2, ....] :: [Double],
>           "species" =: [0, 0, ....] :: [Double]]’
>       In the expression:
>         D.fromColumns
>           ["petal_length" =: [1.4, 1.3, ....] :: [Double],
>            "petal_width" =: [0.2, 0.2, ....] :: [Double],
>            "species" =: [0, 0, ....] :: [Double]]
> 
> <interactive>:54:7: error: [GHC-83865]
>     • Couldn't match expected type: [Double]
>                   with actual type: (Data.Text.Internal.Text, D.Column)
>     • In the expression: "species" =: [0, 0, 0, ....] :: [Double]
>       In the first argument of ‘D.fromColumns’, namely
>         ‘["petal_length" =: [1.4, 1.3, ....] :: [Double],
>           "petal_width" =: [0.2, 0.2, ....] :: [Double],
>           "species" =: [0, 0, ....] :: [Double]]’
>       In the expression:
>         D.fromColumns
>           ["petal_length" =: [1.4, 1.3, ....] :: [Double],
>            "petal_width" =: [0.2, 0.2, ....] :: [Double],
>            "species" =: [0, 0, ....] :: [Double]]
> 
> <interactive>:60:34: error: [GHC-88464] Variable not in scope: tree

## Symbolic regression discovers a formula

Genetic programming searches for an expression that fits the data, and returns
it as a dataframe `Expr` plus the accuracy/complexity Pareto front:

```haskell
curve = D.fromColumns
    [ "x" =: xs
    , "y" =: [x * x + x | x <- xs]
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
> <interactive>:87:47: error: [GHC-88464]
>     Variable not in scope: model
>     Suggested fix: Perhaps use ‘T.mode’ (imported from DataFrame.Typed)
> 
> <interactive>:87:54: error: [GHC-88464]
>     Variable not in scope: sales :: D.DataFrame

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
> <interactive>:95:36: error: [GHC-88464]
>     Variable not in scope: sales :: D.DataFrame
> 
> <interactive>:96:55: error: [GHC-88464]
>     Variable not in scope: sales :: D.DataFrame
> 
> <interactive>:103:61: error: [GHC-88464]
>     Variable not in scope: scaledModel
>     Suggested fix:
>       Perhaps use data constructor ‘ScalerModel’ (imported from DataFrame.Learn)
> 
> <interactive>:104:52: error: [GHC-88464]
>     Variable not in scope: deployed :: D.Expr a1

The folded expression is a function of the raw `x` alone, so it scores the
original frame with no preprocessing step at inference time.

```haskell
evaluate rmse deployed (D.col @Double "y") sales
```

> <!-- scripths:mime text/plain -->
> <interactive>:112:15: error: [GHC-88464]
>     Variable not in scope: deployed :: D.Expr Double
> 
> <interactive>:112:44: error: [GHC-88464]
>     Variable not in scope: sales :: D.DataFrame

## Splitting the data, and evaluation

```haskell
import qualified DataFrame as D

realistic = D.fromColumns
    [ "id" =: [fromIntegral ((i * 7919) `mod` 97) | i <- [1 .. 40 :: Int]]
    , "x"  =: xs
    , "y"  =: [2 * x + 1 + noise i | (i, x) <- zip [0 :: Int ..] xs]
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
> <interactive>:170:59: error: [GHC-88464]
>     Variable not in scope: flowers :: D.DataFrame
> 
> <interactive>:173:49: error: [GHC-88464] Variable not in scope: clf
> 
> <interactive>:173:80: error: [GHC-88464]
>     Variable not in scope: flowers :: D.DataFrame

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
> <interactive>:182:36: error: [GHC-88464]
>     Variable not in scope: flowers :: D.DataFrame
> 
> <interactive>:183:74: error: [GHC-88464]
>     Variable not in scope: flowers :: D.DataFrame
> 
> <interactive>:188:31: error: [GHC-88464]
>     Variable not in scope: pipeline :: Transform
> 
> <interactive>:188:40: error: [GHC-88464]
>     Variable not in scope: flowers :: D.DataFrame

## Synthesize the feature you would have hand-engineered

`DataFrame.Synthesis` is automated feature engineering: a bottom-up enumerative
search (with observational-equivalence pruning) for a small, interpretable
expression over your columns that tracks the target. Here `y` is the interaction
`a * b`, which a linear model on the raw columns cannot capture; synthesis
discovers the term, and feeding it back as a column lifts the fit from mediocre
to exact — still a formula you can read:

```haskell
interactions = D.fromColumns
    [ "a" =: as
    , "b" =: bs
    , "y" =: zipWith (*) as bs
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
