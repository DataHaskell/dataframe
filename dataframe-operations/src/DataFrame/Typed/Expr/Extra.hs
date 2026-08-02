{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Typed counterparts of the remaining "DataFrame.Functions" expression
combinators not already provided by "DataFrame.Typed.Expr". Each wraps the
untyped combinator 1:1, replacing @Expr@ with @'TExpr' cols@ so column
references stay schema-checked. Re-exported from "DataFrame.Typed.Expr".
-}
module DataFrame.Typed.Expr.Extra (
    div,
    mod,
    mode,
    sumMaybe,
    meanMaybe,
    variance,
    medianMaybe,
    percentile,
    stddev,
    stddevMaybe,
    zScore,
    pow,
    relu,
    min,
    max,
    reduce,
    toMaybe,
    fromMaybe,
    isJust,
    isNothing,
    fromJust,
    whenPresent,
    whenBothPresent,
    recode,
    recodeWithCondition,
    recodeWithDefault,
    firstOrNothing,
    lastOrNothing,
    splitOn,
    match,
    matchAll,
    parseDate,
    daysBetween,
    bind,
) where

import qualified Data.Text as T
import Data.Time (Day, ParseTime)
import qualified Data.Vector.Unboxed as VU
import Prelude hiding (div, max, min, mod)

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Typed.Types (TExpr (..))

-- | Integer division.
div ::
    (Integral a, Columnable a) => TExpr cols a -> TExpr cols a -> TExpr cols a
div (TExpr a) (TExpr b) = TExpr (F.div a b)

-- | Integer modulus.
mod ::
    (Integral a, Columnable a) => TExpr cols a -> TExpr cols a -> TExpr cols a
mod (TExpr a) (TExpr b) = TExpr (F.mod a b)

-- | Most frequent value (aggregation).
mode :: (Ord a, Columnable a, Eq a) => TExpr cols a -> TExpr cols a
mode (TExpr e) = TExpr (F.mode e)

-- | Sum of a nullable column, ignoring 'Nothing' (aggregation).
sumMaybe :: (Columnable a, Num a) => TExpr cols (Maybe a) -> TExpr cols a
sumMaybe (TExpr e) = TExpr (F.sumMaybe e)

-- | Mean of a nullable column, ignoring 'Nothing' (aggregation).
meanMaybe :: (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
meanMaybe (TExpr e) = TExpr (F.meanMaybe e)

-- | Variance (aggregation).
variance ::
    (Columnable a, Real a, VU.Unbox a) => TExpr cols a -> TExpr cols Double
variance (TExpr e) = TExpr (F.variance e)

-- | Median of a nullable column, ignoring 'Nothing' (aggregation).
medianMaybe ::
    (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
medianMaybe (TExpr e) = TExpr (F.medianMaybe e)

-- | The @n@-th percentile (aggregation).
percentile :: Int -> TExpr cols Double -> TExpr cols Double
percentile n (TExpr e) = TExpr (F.percentile n e)

-- | Standard deviation (aggregation).
stddev ::
    (Columnable a, Real a, VU.Unbox a) => TExpr cols a -> TExpr cols Double
stddev (TExpr e) = TExpr (F.stddev e)

-- | Standard deviation of a nullable column, ignoring 'Nothing' (aggregation).
stddevMaybe ::
    (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
stddevMaybe (TExpr e) = TExpr (F.stddevMaybe e)

-- | Z-score (value minus group mean, over standard deviation).
zScore :: TExpr cols Double -> TExpr cols Double
zScore (TExpr e) = TExpr (F.zScore e)

-- | Raise an expression to an integer power.
pow :: (Columnable a, Num a) => TExpr cols a -> Int -> TExpr cols a
pow (TExpr e) i = TExpr (F.pow e i)

-- | Rectified linear unit: @max 0@.
relu :: (Columnable a, Num a, Ord a) => TExpr cols a -> TExpr cols a
relu (TExpr e) = TExpr (F.relu e)

-- | Element-wise minimum of two expressions.
min :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols a
min (TExpr a) (TExpr b) = TExpr (F.min a b)

-- | Element-wise maximum of two expressions.
max :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols a
max (TExpr a) (TExpr b) = TExpr (F.max a b)

-- | Fold a column into a single value with a seed and step function (aggregation).
reduce ::
    (Columnable a, Columnable b) =>
    TExpr cols b -> a -> (a -> b -> a) -> TExpr cols a
reduce (TExpr e) start f = TExpr (F.reduce e start f)

-- | Wrap each value in 'Just'.
toMaybe :: (Columnable a) => TExpr cols a -> TExpr cols (Maybe a)
toMaybe (TExpr e) = TExpr (F.toMaybe e)

-- | Replace 'Nothing' with a default.
fromMaybe :: (Columnable a) => a -> TExpr cols (Maybe a) -> TExpr cols a
fromMaybe d (TExpr e) = TExpr (F.fromMaybe d e)

-- | True where the value is 'Just'.
isJust :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isJust (TExpr e) = TExpr (F.isJust e)

-- | True where the value is 'Nothing'.
isNothing :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isNothing (TExpr e) = TExpr (F.isNothing e)

-- | Unwrap a 'Just', erroring on 'Nothing'.
fromJust :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols a
fromJust (TExpr e) = TExpr (F.fromJust e)

-- | Apply a function only where the value is present.
whenPresent ::
    (Columnable a, Columnable b) =>
    (a -> b) -> TExpr cols (Maybe a) -> TExpr cols (Maybe b)
whenPresent f (TExpr e) = TExpr (F.whenPresent f e)

-- | Apply a binary function only where both values are present.
whenBothPresent ::
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) ->
    TExpr cols (Maybe a) ->
    TExpr cols (Maybe b) ->
    TExpr cols (Maybe c)
whenBothPresent f (TExpr a) (TExpr b) = TExpr (F.whenBothPresent f a b)

-- | Map values through a lookup table, yielding 'Nothing' for misses.
recode ::
    (Columnable a, Columnable b, Show a, Show b, Show (a, b)) =>
    [(a, b)] -> TExpr cols a -> TExpr cols (Maybe b)
recode mapping (TExpr e) = TExpr (F.recode mapping e)

-- | Pick the first value whose condition holds, else a fallback.
recodeWithCondition ::
    (Columnable a, Columnable b) =>
    TExpr cols b ->
    [(TExpr cols a -> TExpr cols Bool, b)] ->
    TExpr cols a ->
    TExpr cols b
recodeWithCondition (TExpr fallback) conds (TExpr e) =
    TExpr (F.recodeWithCondition fallback (map untype conds) e)
  where
    untype (p, v) = (unTExpr . p . TExpr, v)

-- | Map values through a lookup table, with a default for misses.
recodeWithDefault ::
    (Columnable a, Columnable b, Show (a, b)) =>
    b -> [(a, b)] -> TExpr cols a -> TExpr cols b
recodeWithDefault d mapping (TExpr e) = TExpr (F.recodeWithDefault d mapping e)

-- | First element of a list column, or 'Nothing'.
firstOrNothing :: (Columnable a) => TExpr cols [a] -> TExpr cols (Maybe a)
firstOrNothing (TExpr e) = TExpr (F.firstOrNothing e)

-- | Last element of a list column, or 'Nothing'.
lastOrNothing :: (Columnable a) => TExpr cols [a] -> TExpr cols (Maybe a)
lastOrNothing (TExpr e) = TExpr (F.lastOrNothing e)

-- | Split text on a delimiter.
splitOn :: T.Text -> TExpr cols T.Text -> TExpr cols [T.Text]
splitOn delim (TExpr e) = TExpr (F.splitOn delim e)

-- | First regex match, or 'Nothing'.
match :: T.Text -> TExpr cols T.Text -> TExpr cols (Maybe T.Text)
match regex (TExpr e) = TExpr (F.match regex e)

-- | All regex matches.
matchAll :: T.Text -> TExpr cols T.Text -> TExpr cols [T.Text]
matchAll regex (TExpr e) = TExpr (F.matchAll regex e)

-- | Parse text into a time value with the given format.
parseDate ::
    (ParseTime t, Columnable t) =>
    T.Text -> TExpr cols T.Text -> TExpr cols (Maybe t)
parseDate format (TExpr e) = TExpr (F.parseDate format e)

-- | Number of days between two dates.
daysBetween :: TExpr cols Day -> TExpr cols Day -> TExpr cols Int
daysBetween (TExpr a) (TExpr b) = TExpr (F.daysBetween a b)

-- | Monadic bind over a column of monadic values.
bind ::
    (Columnable a, Columnable (m a), Monad m, Columnable b, Columnable (m b)) =>
    (a -> m b) -> TExpr cols (m a) -> TExpr cols (m b)
bind f (TExpr e) = TExpr (F.bind f e)
