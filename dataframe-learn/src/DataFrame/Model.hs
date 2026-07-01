{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

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

'fit' is a single, frame-polymorphic verb: it accepts an untyped 'DataFrame' or a
phantom-typed 'DataFrame.Typed.Types.TypedDataFrame', via 'ToDataFrame'. When a
model declares a schema requirement (via 'FrameReq'), that requirement is checked
at /compile time/ for a typed frame and is a no-op for an untyped one. Linear
regression, for instance, requires an all-'Double' frame ('AllDoubleFrame'), so
@fit@ on a typed frame with a non-'Double' column is a compile error; on an
untyped frame the same mistake surfaces as a fit-time error.
-}
module DataFrame.Model (
    Fit (..),
    ToDataFrame (..),
    Fitted (..),
    FitResult,
    FrameFor,
    FrameKind (..),
    CheckFrame,
    AllDouble,
    Predict (..),
    AsTExpr,
    ToTExpr (..),
) where

import Data.Kind (Constraint, Type)

import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Typed.Freeze (ToDataFrame (..), thaw)
import DataFrame.Typed.Schema (AllDouble)
import DataFrame.Typed.Types (AsTExpr, TExpr (..), ToTExpr (..), TypedDataFrame)

{- | A model trained on a typed frame, carrying the schema @cols@ as a phantom so
its 'predict' yields a typed 'TExpr'. Use 'fittedModel' to recover the bare model
record (coefficients, etc.).
-}
newtype Fitted (cols :: [Type]) model = Fitted {fittedModel :: model}

{- | The type 'fit' returns for a given frame source: the bare @model@ for an
untyped 'DataFrame', or a schema-tagged 'Fitted' for a 'TypedDataFrame'. Training
on an untyped frame is therefore unchanged.
-}
type family FitResult (f :: Type) (model :: Type) :: Type where
    FitResult DataFrame model = model
    FitResult (TypedDataFrame cols) model = Fitted cols model

{- | The frame a given training @input@ is fit against: an untyped target/feature
input pairs with a plain 'DataFrame'; a typed input ('TExpr' / @['TExpr']@) pairs
with a 'TypedDataFrame' over the /same/ schema. So the target expression and the
frame are forced to share their columns at compile time.
-}
type family FrameFor (input :: Type) :: Type where
    FrameFor (Expr a) = DataFrame
    FrameFor (TExpr cols a) = TypedDataFrame cols
    FrameFor [Expr Double] = DataFrame
    FrameFor [TExpr cols Double] = TypedDataFrame cols

{- | The schema requirement a model places on its training frame. 'AnyFrame'
imposes nothing (the default); 'AllDoubleFrame' demands every column be 'Double'.
-}
data FrameKind = AnyFrame | AllDoubleFrame

{- | Turn a model's 'FrameKind' requirement into a constraint on the actual frame
type. Untyped 'DataFrame's are never constrained (they are runtime-checked); a
typed frame must satisfy the requirement at compile time.
-}
type family CheckFrame (req :: FrameKind) (f :: Type) :: Constraint where
    CheckFrame _ DataFrame = ()
    CheckFrame 'AnyFrame _ = ()
    CheckFrame 'AllDoubleFrame (TypedDataFrame cols) = AllDouble cols

{- | Train a model. @cfg@ is the hyperparameter config; @input@ is the supervised
target @Expr a@ or the unsupervised feature list @[Expr Double]@; the frame is
any 'ToDataFrame' source (an untyped 'DataFrame' or a 'TypedDataFrame'). The
config and input together determine the model, so no annotation is needed (a
classifier's label type comes from its @Expr a@ target).

A model overrides 'FrameReq' to demand a schema shape (e.g. 'AllDoubleFrame'),
which 'fit' enforces at compile time on a typed frame; the default is 'AnyFrame'.

The frame is determined by the @input@ (via 'FrameFor'): an untyped target pairs
with a 'DataFrame' and yields the bare model; a typed target ('TExpr' over @cols@)
pairs with a @TypedDataFrame cols@ and yields a @Fitted cols model@, so 'predict'
gives a typed 'TExpr'. A non-'Double' typed column is then a compile error.
-}
class Fit cfg input where
    {- | The model this config + input trains, computed as a type family so it is
    visible in the instance head (and so the typed-lift instances below can
    forward it) without needing a result annotation at the call site.
    -}
    type ModelOf cfg input :: Type

    type FrameReq cfg input :: FrameKind
    type FrameReq cfg input = 'AnyFrame
    fit ::
        (CheckFrame (FrameReq cfg input) (FrameFor input)) =>
        cfg ->
        input ->
        FrameFor input ->
        FitResult (FrameFor input) (ModelOf cfg input)

{- | Lift any model fittable on an untyped target @Expr a@ to a typed target
@TExpr cols a@ over a @TypedDataFrame cols@, returning a schema-tagged 'Fitted'.
-}
instance (Fit cfg (Expr a)) => Fit cfg (TExpr cols a) where
    type ModelOf cfg (TExpr cols a) = ModelOf cfg (Expr a)
    type FrameReq cfg (TExpr cols a) = FrameReq cfg (Expr a)
    fit cfg (TExpr e) tdf = Fitted (fit cfg e (thaw tdf))

-- | The same lift for the unsupervised feature-list inputs.
instance (Fit cfg [Expr Double]) => Fit cfg [TExpr cols Double] where
    type ModelOf cfg [TExpr cols Double] = ModelOf cfg [Expr Double]
    type FrameReq cfg [TExpr cols Double] = FrameReq cfg [Expr Double]
    fit cfg feats tdf = Fitted (fit cfg (map unTExpr feats) (thaw tdf))

{- | Compile a fitted model's canonical prediction to an expression over the raw
columns. The 'Prediction' type tracks the model: a bare model gives @Expr r@; a
'Fitted' model (trained on a typed frame) gives @TExpr cols r@.
-}
class Predict model where
    type Prediction model :: Type
    predict :: model -> Prediction model

instance (Predict model, ToTExpr cols (Prediction model)) => Predict (Fitted cols model) where
    type Prediction (Fitted cols model) = AsTExpr cols (Prediction model)
    predict (Fitted m) = toTExpr @cols (predict m)
