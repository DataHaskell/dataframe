{-# LANGUAGE FlexibleContexts #-}

{- | Typed sampling and splitting. All operations are schema-preserving: they
change which rows are present, never the columns, so every result reuses the
input schema @cols@.
-}
module DataFrame.Typed.Sampling (
    randomSplit,
    kFolds,
    selectRows,
    stratifiedSample,
    stratifiedSplit,
) where

import System.Random (RandomGen)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Operations.Subset (SplittableGen)
import qualified DataFrame.Operations.Subset as D
import DataFrame.Typed.Types (TExpr (..), TypedDataFrame (..))

-- | Split rows into two DataFrames by a fraction.
randomSplit ::
    (RandomGen g) =>
    g -> Double -> TypedDataFrame cols -> (TypedDataFrame cols, TypedDataFrame cols)
randomSplit g p (TDF df) = let (a, b) = D.randomSplit g p df in (TDF a, TDF b)

-- | Partition rows into @k@ folds.
kFolds ::
    (RandomGen g) => g -> Int -> TypedDataFrame cols -> [TypedDataFrame cols]
kFolds g k (TDF df) = map TDF (D.kFolds g k df)

{- | Select rows by index.
| This may fail if the indices are out of bounds;
| use with caution or use 'filter' to select rows by a predicate instead.
-}
selectRows :: [Int] -> TypedDataFrame cols -> TypedDataFrame cols
selectRows ixs (TDF df) = TDF (D.selectRows ixs df)

-- | Sample a fraction of rows, preserving the distribution of a strata column.
stratifiedSample ::
    (SplittableGen g, Columnable a) =>
    g -> Double -> TExpr cols a -> TypedDataFrame cols -> TypedDataFrame cols
stratifiedSample g p (TExpr e) (TDF df) = TDF (D.stratifiedSample g p e df)

-- | Split rows by a fraction, preserving the distribution of a strata column.
stratifiedSplit ::
    (SplittableGen g, Columnable a) =>
    g ->
    Double ->
    TExpr cols a ->
    TypedDataFrame cols ->
    (TypedDataFrame cols, TypedDataFrame cols)
stratifiedSplit g p (TExpr e) (TDF df) =
    let (a, b) = D.stratifiedSplit g p e df in (TDF a, TDF b)
