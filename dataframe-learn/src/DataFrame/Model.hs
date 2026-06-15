{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}

{- | The two verbs every model speaks. Instead of a per-model @fitX@ / @xExpr@
zoo, every estimator is an instance of these classes:

  * 'fit' trains a model from a hyperparameter config, an @input@ (the supervised
    target @Expr a@ or the unsupervised feature list @[Expr Double]@), and a frame.
  * 'predict' compiles the model's canonical prediction to an @Expr@ over the raw
    columns (regressors give @Expr Double@, classifiers @Expr a@, clusterers
    @Expr Int@). Models with no honest out-of-sample prediction (e.g. DBSCAN) simply
    have no instance — @predict@ on them is a compile error, not a fake.

Every prediction lands in the /same/ expression type, so a fitted model composes
with 'DataFrame.Operations.Transformations.derive', the 'DataFrame.Transform'
monoid, and 'DataFrame.Transform.compileThrough' with no per-model glue.

Auxiliary outputs (class probabilities, per-cluster distances, component
loadings, the @*Transform@ pipeline pieces) keep their own descriptive
functions — they are not the one canonical prediction, so they are not forced
through 'predict'.

Supervised 'fit' treats every non-target column as a feature; use
'selectFeatures' to restrict to an explicit set when the frame also carries ids,
timestamps, or a second candidate target.
-}
module DataFrame.Model (
    Fit (..),
    Predict (..),
    selectFeatures,
) where

import qualified Data.Text as T

import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Operations.Subset (select)

{- | Train a model. @cfg@ is the hyperparameter config; @input@ is the supervised
target @Expr a@ or the unsupervised feature list @[Expr Double]@. The config and
input together determine the model, so @fit cfg target df@ needs no annotation
(a classifier's label type comes from its @Expr a@ target).
-}
class Fit cfg input model | cfg input -> model where
    fit :: cfg -> input -> DataFrame -> model

{- | Compile a fitted model's canonical prediction to an expression over the raw
columns. The result type @r@ is determined by the model.
-}
class Predict model r | model -> r where
    predict :: model -> Expr r

{- | Restrict @df@ to exactly the named feature columns plus the supervised
target (when the target is a column), so a following 'fit' trains on those
features only.

Supervised 'fit' otherwise uses /every/ non-target column as a feature —
convenient on a clean frame, a leakage hazard when the frame carries ids,
timestamps, or a second candidate target. This mirrors the explicit
@[Expr Double]@ feature list the unsupervised fitters already take:

> model = fit defaultLinearConfig target (selectFeatures ["age", "income"] target df)
-}
selectFeatures :: [T.Text] -> Expr a -> DataFrame -> DataFrame
selectFeatures cols target = select (cols ++ targetCols target)
  where
    targetCols :: Expr b -> [T.Text]
    targetCols (Col n) = [n]
    targetCols _ = []
