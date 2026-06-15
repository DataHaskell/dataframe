<!--
  This README is a runnable scripths (https://github.com/DataHaskell/scripths)
  notebook. Every ```haskell block runs top-to-bottom in one shared session and
  scripths inserts each block's output beneath it as a blockquote. The
  `-- cabal: packages:` directive builds against the local working tree.
  Regenerate (from the repo root) with:

      scripths dataframe-learn/README.md -o dataframe-learn/README.md
-->

# dataframe-learn

Machine learning for [`dataframe`](https://hackage.haskell.org/package/dataframe)
where **a fitted model is a dataframe expression**. You `fit` a model and
`predict` hands you back an `Expr` over your columns — pretty-print it to read
the formula, apply it with `derive` to score a frame, fold preprocessing into it
with `compileThrough`. The model *is* the prediction, not an opaque blob, and the
scikit-learn-style record (coefficients, centroids, components, support) is right
there too for inspection. Because every prediction is the same kind of `Expr`,
preprocessing, prediction, and deployment all compose the same way — the
[design notes](#design-notes-the-categorical-account) at the end explain why.

## A linear model is a formula

`fit` returns a record (with `regCoef`/`regIntercept` for inspection) and
`predict` compiles it to an `Expr Double` you can read. The `D` import (the
public `DataFrame` umbrella, which also gives `D.col` for the expression DSL)
carries through the rest of the notebook; each later section adds the one model
module it needs:

```haskell
-- cabal: packages: .., ., ../dataframe-core, ../dataframe-operations, ../dataframe-parsing
-- cabal: build-depends: dataframe, dataframe-learn, text
-- cabal: default-extensions: OverloadedStrings, TypeApplications
-- cabal: ghc-options: -w
import qualified DataFrame as D
import DataFrame.LinearModel
import DataFrame.Model (fit, predict)

sales = D.fromNamedColumns
    [ ("x", D.fromList ([1, 2, 3, 4, 5, 6] :: [Double]))
    , ("y", D.fromList ([2 * x + 1 | x <- [1, 2, 3, 4, 5, 6]] :: [Double]))
    ]

model = fit defaultLinearConfig (D.col @Double "y") sales
putStrLn (D.prettyPrint (predict model))
```

> <!-- scripths:mime text/plain -->
> 2.0 * x + 0.9999999999999989

## A decision tree is a readable expression

The tree compiles to nested `if/then/else` over your columns — no special
viewer, it is just an expression:

```haskell
import DataFrame.DecisionTree (defaultTreeConfig)
import DataFrame.DecisionTree.Model ()

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
>   then 0.0
>   else if petal_length .<=. 5.1
>     then 1.0
>     else 2.0

## Symbolic regression discovers a formula

Genetic programming searches for an expression that fits the data, and returns
it as a dataframe `Expr` plus the accuracy/complexity Pareto front:

```haskell
import DataFrame.SymbolicRegression

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

## When the formula is bigger than a glance

Not every model is a one-liner. A linear model, a small tree, or a symbolic
expression you can *read*; a 40-tree gradient booster you cannot — its `predict`
is an exact sum of forty trees. Counting the characters in each printed formula
shows the gap:

```haskell
import DataFrame.Boosting

gbm = fit defaultGBConfig { gbNEstimators = 40, gbMaxDepth = 2 } (D.col @Double "y") sales
putStr (unlines
    [ "linear prediction: " ++ show (length (D.prettyPrint (predict model))) ++ " characters"
    , "GBM(40 trees):     " ++ show (length (D.prettyPrint (predict gbm)))  ++ " characters" ])
```

> <!-- scripths:mime text/plain -->
> linear prediction: 28 characters
> GBM(40 trees):     7151 characters

Even when it is too big to eyeball, the expression is still the whole story:
a self-contained, dependency-free artifact that scores a frame with `derive` —
no pickled blob, no runtime to ship. For the big ensembles the *interpretability*
comes from `gbFeatureImportances` and pretty-printing individual trees, not from
reading the summed formula.

## Deploy: applying an expression to a frame

Because the model is an `Expr`, deploying it is just `derive` — you add the
prediction as a new column with the ordinary dataframe API:

```haskell
D.columnNames (D.derive "prediction" (predict model) sales)
```

> <!-- scripths:mime text/plain -->
> ["x","y","prediction"]

## A model and its preprocessing compose by substitution

Preprocessing is an expression too, so a model trained in a transformed space and
the transform that produced it *compose* — and composition of expressions is
substitution of one into the other. `compileThrough` performs that composition,
folding a fitted transform into a prediction so the result is a single formula
over the raw inputs. Here we standardize `x`, fit in the scaled space, then fold
the scaler back in to recover a raw-column model:

```haskell
import DataFrame.Transform
import DataFrame.Metrics

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
original frame with no preprocessing step at inference time — and by the
substitution lemma it computes the same result (up to floating point) as
transforming the frame and then predicting:

```haskell
evaluate rmse deployed (D.col @Double "y") sales
```

> <!-- scripths:mime text/plain -->
> 3.6259732146947156e-16

## A realistic run: pick features, split, evaluate held-out, tune

Real frames are noisy and carry columns you must not train on. Here is a noisy
linear signal with a spurious `id` column:

```haskell
realistic = D.fromNamedColumns
    [ ("id", D.fromList [fromIntegral ((i * 7919) `mod` 97) | i <- [1 .. 40 :: Int]])
    , ("x",  D.fromList xs)
    , ("y",  D.fromList [2 * x + 1 + noise i | (i, x) <- zip [0 :: Int ..] xs])
    ]
  where
    xs      = map fromIntegral [1 .. 40 :: Int] :: [Double]
    noise i = fromIntegral ((i * 2654435761 + 12345) `mod` 1000) / 100 - 5
```

> <!-- scripths:mime text/plain -->

**Feature selection.** Supervised `fit` uses *every* non-target column as a
feature, so a naive fit drags `id` into the model. `selectFeatures` restricts to
the columns you mean (mirroring the explicit feature list the unsupervised
fitters take), which is the difference between a leaky model and a clean one:

```haskell
import DataFrame.Model (selectFeatures)

naive   = fit defaultLinearConfig (D.col @Double "y") realistic
guarded = fit defaultLinearConfig (D.col @Double "y")
              (selectFeatures ["x"] (D.col @Double "y") realistic)
putStr (unlines
    [ "all columns:           " ++ D.prettyPrint (predict naive)
    , "selectFeatures [\"x\"]:   " ++ D.prettyPrint (predict guarded) ])
```

> <!-- scripths:mime text/plain -->
> all columns:           -7.746701620642152e-3 * id + 1.9914915483217268 * x + 1.6474622984919354
> selectFeatures ["x"]:   1.9918011257035637 * x + 1.2630769230769452

**Hold-out evaluation.** `trainTestSplit` (seeded, deterministic) keeps the score
honest — evaluate on rows the model never saw, and the metrics are realistic, not
the `1e-15` of an in-sample toy:

```haskell
import DataFrame.ModelSelection

clean        = selectFeatures ["x"] (D.col @Double "y") realistic
(train, test) = trainTestSplit 0.75 7 clean
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

## Reports without hand-rolling metrics

Metrics are plain functions (`rmse`, `mse`, `r2`, `accuracy`, multiclass
`precision`/`recall`/`f1`), and `classificationReport` bundles the common numbers
with a scikit-learn-style layout (per-class precision/recall/F1/support plus
macro/weighted averages):

```haskell
import DataFrame.Metrics.Report

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
import DataFrame.PCA

features = ["petal_length", "petal_width"]
scalerF  = standardScaler features flowers
pca      = fit (PCAConfig (NComp 2) True) (map (D.col @Double) features) flowers
pipeline = scalerTransform scalerF <> pcaTransform pca

D.columnNames (applyTransform pipeline flowers)
```

> <!-- scripths:mime text/plain -->
> ["petal_length","petal_width","species","pc1","pc2"]

## Unsupervised models are inspectable too

k-means returns `cluster_centers_`-style centroids, and per-cluster distance /
assignment expressions:

```haskell
import DataFrame.KMeans

km = fit defaultKMeansConfig { kmK = 3, kmSeed = 1 } (map (D.col @Double) features) flowers
kmCenters km
```

> <!-- scripths:mime text/plain -->
> [[1.4,0.2],[5.65,2.05],[4.55,1.4500000000000002]]

## Synthesize the feature you would have hand-engineered

`DataFrame.Synthesis` is automated feature engineering: a bottom-up enumerative
search (with observational-equivalence pruning) for a small, interpretable
expression over your columns that tracks the target. Here `y` is the interaction
`a * b`, which a linear model on the raw columns cannot capture; synthesis
discovers the term, and feeding it back as a column lifts the fit from mediocre
to exact — still a formula you can read:

```haskell
import DataFrame.Synthesis

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
        (selectFeatures ["synth"] (D.col @Double "y") withFeat)

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

## What's in the box

| Task | Models |
|------|--------|
| Regression | OLS, ridge, lasso, elastic net, regression trees, gradient boosting, symbolic regression |
| Classification | logistic regression, linear SVC, RFF kernel SVM, decision trees, gradient boosting, AdaBoost |
| Dimensionality reduction | PCA, Nyström kernel PCA |
| Clustering | k-means, Gaussian mixtures, DBSCAN |
| Feature engineering | `DataFrame.Synthesis` (enumerative feature synthesis), symbolic regression |
| Evaluation | `DataFrame.Metrics` (metrics + `evaluate`), `DataFrame.Metrics.Report` (reports) |
| Pipelines & tuning | `DataFrame.Transform` (composable transforms), `DataFrame.ModelSelection` (`trainTestSplit`, `crossValidate`, `gridSearch`) |

Every model is a `Fit` instance, so there is one verb to train — `fit cfg input
df` — and every model with an honest out-of-sample prediction is a `Predict`
instance, so one verb to compile it — `predict model`. Auxiliary outputs
(`gbProbaExpr`, `logisticProbExprs`, `kmeansDistanceExprs`, `pcaTransform`, …)
keep descriptive names; transductive models like DBSCAN deliberately have no
`Predict` instance. Fits that use randomness take a `seed` in their config, so
results are reproducible across Linux, macOS, and Windows. Pure Haskell — the
only extra dependency beyond the dataframe packages is `random`.

## Design notes: the categorical account

The two verbs live in `DataFrame.Model`:

```
class Fit cfg input model | cfg input -> model where
    fit :: cfg -> input -> DataFrame -> model

class Predict model r | model -> r where
    predict :: model -> Expr r
```

They are small on purpose, because the structure they hang on lives in the
expression language, not in the classes. The framing borrows from
[*Seven Sketches in Compositionality*](https://arxiv.org/abs/1803.05316)
(Fong & Spivak) and the Para/Lens account of learners
([Fong, Johnson & Spivak, *Lenses and Learners*](https://arxiv.org/abs/1903.03671);
[Cruttwell et al., *Categorical Foundations of Gradient-Based Learning*](https://arxiv.org/abs/2103.01931)).
What follows is deliberately careful about what is load-bearing and what is only
analogy.

**The row-wise fragment is a category.** Restrict to the row-wise expression
constructors — `Col`, `Lit`, `Unary`, `Binary`, `If`. Take typed column contexts
as objects and, as an arrow `Γ → Δ`, a `Δ`-tuple of such expressions over `Γ`.
Composition is simultaneous substitution (`substituteColumns`, added to
`dataframe-core` for exactly this) and the identities are the column projections
(`Col`). This is the category of contexts (the Lawvere theory) of the column
signature. The restriction is load-bearing for *both* laws, not just composition:
`Agg` and `Over` are column-level/relational, not row-wise maps, and the raw-text
column reference inside `CastWith` is opaque to substitution — so identity-by-`Col`
fails on those constructors too. They are excluded by construction (transforms
reject `Agg`/`Over`), which is why composition and identity stay well defined.

**`predict` gives every model a uniform codomain.** `Predict model r` interprets a
fitted model as an arrow in that category: `predict model :: Expr r` runs from the
model's feature context to the one-column context `{r}`, and the dependency
`model -> r` fixes the codomain object. This is *not* a functor or a denotation in
the technical sense — there is no category of models to be functorial over. The
real, useful property is uniformity: every model's prediction lands in the *same*
expression type (`Expr Double`/`Expr a`/`Expr Int`), so `derive`, the `Transform`
monoid, and `compileThrough` all apply with no per-model glue. That the compiled
`Expr` actually agrees with the fitted record's own parameters is a tested property
(`tests/Learn/Denotation.hs`), not a typeclass law — the class only knows the
symbolic half.

**`fit` is the parametrized-morphism (Para) fragment.** `fit cfg input df` chooses a
parameter — the trained record — and `predict` is the forward map applied at it.
In the Para/Lens picture of learning a learner is a *parametrized lens* carrying a
forward map plus backward update/request maps; we inhabit only the forward (Para)
part and expose no backward maps, because this interface is batch training, not
online gradient exchange. That is a complete, self-contained sub-structure, not a
half-built one — but it does mean the Lens vocabulary is motivation here, not
something the code instantiates. (The functional dependency `cfg input -> model`
fixes the parameter *type*; `fit` is the value-level map that picks the point.)

**`Transform` is a monoid of derived-column lists.** `Transform`'s `<>` keeps the
earlier step's outputs and rewrites the later step's column references through them
by substitution; `mempty` is the empty list. These are context-*extending* maps
(`applyTransform` adds columns), so this is an ordinary algebraic monoid — a monoid
*is* a one-object category (Seven Sketches ch. 3) — not the endomorphism monoid of a
fixed object. Associativity and identity hold for the row-wise fragment **provided
output names do not collide**: the implementation merges output maps with
`Data.Map.fromList`, which keeps the last binding on a clash, so reusing a column
name across steps is the one way to break the law.

**Composition is the point.** `compileThrough t (predict m)` realizes the composite
`predict m ∘ t` (read right-to-left: first `t`, then `predict m`) by substituting
`t`'s definitions into `predict m`. By the substitution lemma it denotes the same
function as transforming the frame and then predicting — equal results up to
floating point, not syntactically identical expressions. That is exactly the
"compose by substitution" example above, and it is why a model trained in a
transformed space deploys as one formula over the raw columns.

**What deliberately has no `predict` — two different reasons.** DBSCAN is
transductive: every clustering *fit* depends on the whole training set, but what
distinguishes the models is whether the *fitted* model induces an out-of-sample
rule. k-means (nearest centroid) and GMM (max posterior) do, so they have honest
`predict` arrows; DBSCAN's density-reachability assignment has no per-row rule, so
we give it no `Predict` instance rather than a fake `Maybe` or a throwing stub.
PCA and kernel PCA are the *opposite* case: they *are* arrows, but multi-output
feature maps with no privileged label column, so their canonical interface is a
`Transform` (`pcaTransform`/`pcaExprs`), not a one-column `predict`.

**A note on classifiers.** A multiclass `predict` is a genuine arrow into the label
object, but it compiles arg-max to a nested-`If` cascade (`argMaxExpr`), quadratic
in the number of classes — so for a 5-class model `prettyPrint (predict m)` is an
If-tree, not a tidy formula. The "model is a readable formula" aesthetic is honest
for affine and tree models; for classifiers and clusterers the value is that the
arrow exists and composes, not that it is short.

**An aside.** A linear or affine model's prediction is a signal-flow graph — a
weighted sum of inputs. `affineExpr` builds the arrow in the prop of affine *maps*
(the single-valued sub-prop of Seven Sketches ch. 5's signal-flow calculus of
affine relations), and dropping zero-weight terms is diagram simplification —
deleting a zero-gain wire.

(The "instance is a functor `C → Set`" slogan from Spivak's functorial data model,
Seven Sketches ch. 3, is sometimes invoked for dataframes; a single flat table is
the degenerate case — a schema with no foreign-key morphisms — so it is an analogy
here, not a structure we use.)
