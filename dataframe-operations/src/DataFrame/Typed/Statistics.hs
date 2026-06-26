{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

{- | Typed statistical reducers over a 'TypedDataFrame'.

These mirror the untyped reducers in "DataFrame.Operations.Statistics", taking
a schema-checked 'TExpr' instead of a raw @Expr@. The names (@sum@, @mean@,
@median@, …) deliberately collide with the aggregation-expression combinators
in "DataFrame.Typed.Expr", so this module is meant to be imported qualified:

@
import qualified DataFrame.Typed.Statistics as TS

avg = TS.mean (col \@\"salary\") employees
@
-}
module DataFrame.Typed.Statistics (
    mean,
    meanMaybe,
    median,
    medianMaybe,
    percentile,
    genericPercentile,
    standardDeviation,
    skewness,
    variance,
    interQuartileRange,
    sum,
    correlation,
    frequencies,
    imputeWith,
    summarize,
    describeColumns,
) where

import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import GHC.TypeLits (KnownSymbol, symbolVal)
import Prelude hiding (sum)

import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Internal.Nullable (BaseType)
import qualified DataFrame.Operations.Core as Core
import qualified DataFrame.Operations.Statistics as Stats
import DataFrame.Operations.Transformations (ImputeOp)
import DataFrame.Typed.Schema (AssertPresent, SafeLookup)
import DataFrame.Typed.Types (TExpr (..), TypedDataFrame (..))

-- | Mean of a column.
mean ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
mean (TExpr e) (TDF df) = Stats.mean e df

-- | Mean of a nullable column, ignoring 'Nothing'.
meanMaybe ::
    (Columnable a, Real a) =>
    TExpr cols (Maybe a) -> TypedDataFrame cols -> Double
meanMaybe (TExpr e) (TDF df) = Stats.meanMaybe e df

-- | Median of a column.
median ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
median (TExpr e) (TDF df) = Stats.median e df

-- | Median of a nullable column, ignoring 'Nothing'.
medianMaybe ::
    (Columnable a, Real a) =>
    TExpr cols (Maybe a) -> TypedDataFrame cols -> Double
medianMaybe (TExpr e) (TDF df) = Stats.medianMaybe e df

-- | The @n@-th percentile of a column.
percentile ::
    (Columnable a, Real a, VU.Unbox a) =>
    Int -> TExpr cols a -> TypedDataFrame cols -> Double
percentile n (TExpr e) (TDF df) = Stats.percentile n e df

-- | The @n@-th percentile of a column of any 'Ord' type.
genericPercentile ::
    (Columnable a, Ord a) =>
    Int -> TExpr cols a -> TypedDataFrame cols -> a
genericPercentile n (TExpr e) (TDF df) = Stats.genericPercentile n e df

-- | Standard deviation of a column.
standardDeviation ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
standardDeviation (TExpr e) (TDF df) = Stats.standardDeviation e df

-- | Skewness of a column.
skewness ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
skewness (TExpr e) (TDF df) = Stats.skewness e df

-- | Variance of a column.
variance ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
variance (TExpr e) (TDF df) = Stats.variance e df

-- | Inter-quartile range of a column.
interQuartileRange ::
    (Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedDataFrame cols -> Double
interQuartileRange (TExpr e) (TDF df) = Stats.interQuartileRange e df

-- | Sum of a column.
sum :: (Columnable a, Num a) => TExpr cols a -> TypedDataFrame cols -> a
sum (TExpr e) (TDF df) = Stats.sum e df

{- | Pearson's correlation coefficient between two columns, named by type
application. Both columns must exist in the schema and be numeric — these are
checked at compile time via 'SafeLookup' on each name.

@
TS.correlation \@\"height\" \@\"weight\" people
@
-}
correlation ::
    forall c1 c2 a b cols.
    ( KnownSymbol c1
    , KnownSymbol c2
    , a ~ SafeLookup c1 cols
    , b ~ SafeLookup c2 cols
    , Columnable a
    , Columnable b
    , Real a
    , Real b
    , VU.Unbox a
    , VU.Unbox b
    , AssertPresent c1 cols
    , AssertPresent c2 cols
    ) =>
    TypedDataFrame cols -> Maybe Double
correlation (TDF df) =
    Stats.correlation
        (T.pack (symbolVal (Proxy @c1)))
        (T.pack (symbolVal (Proxy @c2)))
        df

{- | Frequency table for a column. The result schema is data-dependent
(one column per distinct value), so an untyped 'D.DataFrame' is returned.
-}
frequencies ::
    (Columnable a, Ord a) => TExpr cols a -> TypedDataFrame cols -> D.DataFrame
frequencies (TExpr e) (TDF df) = Stats.frequencies e df

{- | Impute missing values in a column using a derived scalar (e.g. the mean).
Schema-preserving: the imputed column keeps its type-level @Maybe@ even though
its runtime values are now fully populated.
-}
imputeWith ::
    (ImputeOp a, Columnable (BaseType a)) =>
    (TExpr cols (BaseType a) -> TExpr cols (BaseType a)) ->
    TExpr cols a ->
    TypedDataFrame cols ->
    TypedDataFrame cols
imputeWith f (TExpr e) (TDF df) =
    TDF (Stats.imputeWith (unTExpr . f . TExpr) e df)

{- | Descriptive statistics of the numeric columns. Returns an untyped
'D.DataFrame' (the result is a fixed set of statistic rows, not the input schema).
-}
summarize :: TypedDataFrame cols -> D.DataFrame
summarize (TDF df) = Stats.summarize df

{- | Per-column summary (non-null\/null counts, unique values, type). Returns an
untyped 'D.DataFrame'.
-}
describeColumns :: TypedDataFrame cols -> D.DataFrame
describeColumns (TDF df) = Core.describeColumns df
