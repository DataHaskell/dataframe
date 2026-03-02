{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Operations (
    -- * Schema-preserving operations
    filterWhere,
    filter,
    filterBy,
    filterAllJust,
    filterJust,
    filterNothing,
    sortBy,
    take,
    takeLast,
    drop,
    dropLast,
    range,
    cube,
    distinct,
    sample,
    shuffle,

    -- * Schema-modifying operations
    derive,
    impute,
    select,
    exclude,
    rename,
    renameMany,
    insert,
    insertColumn,
    insertVector,
    cloneColumn,
    dropColumn,
    replaceColumn,

    -- * Metadata
    dimensions,
    nRows,
    nColumns,
    columnNames,

    -- * Vertical merge
    append,

    -- * Pipe operator
    (|>),
) where

import Data.Function ((&))
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as V
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import System.Random (RandomGen)
import Prelude hiding (drop, filter, take)

import qualified DataFrame.Functions as DF
import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Operations.Aggregation as DA
import qualified DataFrame.Operations.Core as D
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Permutation as D
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D

-- Semigroup instance

import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types (TExpr (..), TSortOrder (..), TypedDataFrame (..))
import qualified DataFrame.Typed.Types as T

-- | Pipe operator, re-exported for convenience.
(|>) :: a -> (a -> b) -> b
(|>) = (&)

infixl 1 |>

-------------------------------------------------------------------------------
-- Schema-preserving operations
-------------------------------------------------------------------------------

{- | Filter rows where a boolean expression evaluates to True.
The expression is validated against the schema at compile time.
-}
filterWhere :: TExpr cols Bool -> TypedDataFrame cols -> TypedDataFrame cols
filterWhere (TExpr expr) (TDF df) = TDF (D.filterWhere expr df)

-- | Filter rows by applying a predicate to a typed expression.
filter ::
    (Columnable a) =>
    TExpr cols a -> (a -> Bool) -> TypedDataFrame cols -> TypedDataFrame cols
filter (TExpr expr) pred' (TDF df) = TDF (D.filter expr pred' df)

-- | Filter rows by a predicate on a column expression (flipped argument order).
filterBy ::
    (Columnable a) =>
    (a -> Bool) -> TExpr cols a -> TypedDataFrame cols -> TypedDataFrame cols
filterBy pred' (TExpr expr) (TDF df) = TDF (D.filterBy pred' expr df)

{- | Keep only rows where ALL Optional columns have Just values.
Strips 'Maybe' from all column types in the result schema.

@
df :: TDF '[Column \"x\" (Maybe Double), Column \"y\" Int]
filterAllJust df :: TDF '[Column \"x\" Double, Column \"y\" Int]
@
-}
filterAllJust :: TypedDataFrame cols -> TypedDataFrame (StripAllMaybe cols)
filterAllJust (TDF df) = unsafeFreeze (D.filterAllJust df)

{- | Keep only rows where the named column has Just values.
Strips 'Maybe' from that column's type in the result schema.

@
filterJust \@\"x\" df
@
-}
filterJust ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame (StripMaybeAt name cols)
filterJust (TDF df) = unsafeFreeze (D.filterJust colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Keep only rows where the named column has Nothing.
Schema is preserved (column types unchanged, just fewer rows).
-}
filterNothing ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame cols
filterNothing (TDF df) = TDF (D.filterNothing colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Sort by the given typed sort orders.
Sort orders reference columns that are validated against the schema.
-}
sortBy :: [TSortOrder cols] -> TypedDataFrame cols -> TypedDataFrame cols
sortBy ords (TDF df) = TDF (D.sortBy (map toUntypedSort ords) df)
  where
    toUntypedSort :: TSortOrder cols -> D.SortOrder
    toUntypedSort (Asc (TExpr e)) = D.Asc e
    toUntypedSort (Desc (TExpr e)) = D.Desc e

-- | Take the first @n@ rows.
take :: Int -> TypedDataFrame cols -> TypedDataFrame cols
take n (TDF df) = TDF (D.take n df)

-- | Take the last @n@ rows.
takeLast :: Int -> TypedDataFrame cols -> TypedDataFrame cols
takeLast n (TDF df) = TDF (D.takeLast n df)

-- | Drop the first @n@ rows.
drop :: Int -> TypedDataFrame cols -> TypedDataFrame cols
drop n (TDF df) = TDF (D.drop n df)

-- | Drop the last @n@ rows.
dropLast :: Int -> TypedDataFrame cols -> TypedDataFrame cols
dropLast n (TDF df) = TDF (D.dropLast n df)

-- | Take rows in the given range (start, end).
range :: (Int, Int) -> TypedDataFrame cols -> TypedDataFrame cols
range r (TDF df) = TDF (D.range r df)

-- | Take a sub-cube of the DataFrame.
cube :: (Int, Int) -> TypedDataFrame cols -> TypedDataFrame cols
cube c (TDF df) = TDF (D.cube c df)

-- | Remove duplicate rows.
distinct :: TypedDataFrame cols -> TypedDataFrame cols
distinct (TDF df) = TDF (DA.distinct df)

-- | Randomly sample a fraction of rows.
sample ::
    (RandomGen g) => g -> Double -> TypedDataFrame cols -> TypedDataFrame cols
sample g frac (TDF df) = TDF (D.sample g frac df)

-- | Shuffle all rows randomly.
shuffle :: (RandomGen g) => g -> TypedDataFrame cols -> TypedDataFrame cols
shuffle g (TDF df) = TDF (D.shuffle g df)

-------------------------------------------------------------------------------
-- Schema-modifying operations
-------------------------------------------------------------------------------

{- | Derive a new column from a typed expression. The column name must NOT
already exist in the schema (enforced at compile time via 'AssertAbsent').
The expression is validated against the current schema.

@
df' = derive \@\"total\" (col \@\"price\" * col \@\"qty\") df
-- df' :: TDF (Column \"total\" Double ': originalCols)
@
-}
derive ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    TExpr cols a ->
    TypedDataFrame cols ->
    TypedDataFrame (Snoc cols (T.Column name a))
derive (TExpr expr) (TDF df) = unsafeFreeze (D.derive colName expr df)
  where
    colName = T.pack (symbolVal (Proxy @name))

impute ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    ) =>
    a ->
    TypedDataFrame cols ->
    TypedDataFrame (Impute name cols)
impute value (TDF df) =
    unsafeFreeze
        (D.derive colName (DF.fromMaybe value (DF.col @(Maybe a) colName)) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Select a subset of columns by name.
select ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names) =>
    TypedDataFrame cols -> TypedDataFrame (SubsetSchema names cols)
select (TDF df) = unsafeFreeze (D.select (symbolVals @names) df)

-- | Exclude columns by name.
exclude ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names) =>
    TypedDataFrame cols -> TypedDataFrame (ExcludeSchema names cols)
exclude (TDF df) = unsafeFreeze (D.exclude (symbolVals @names) df)

-- | Rename a column.
rename ::
    forall old new cols.
    (KnownSymbol old, KnownSymbol new) =>
    TypedDataFrame cols -> TypedDataFrame (RenameInSchema old new cols)
rename (TDF df) = unsafeFreeze (D.rename oldName newName df)
  where
    oldName = T.pack (symbolVal (Proxy @old))
    newName = T.pack (symbolVal (Proxy @new))

-- | Rename multiple columns from a type-level list of pairs.
renameMany ::
    forall (pairs :: [(Symbol, Symbol)]) cols.
    (AllKnownPairs pairs) =>
    TypedDataFrame cols -> TypedDataFrame (RenameManyInSchema pairs cols)
renameMany (TDF df) = unsafeFreeze (foldRenames (pairVals @pairs) df)
  where
    foldRenames [] df' = df'
    foldRenames ((old, new) : rest) df' = foldRenames rest (D.rename old new df')

-- | Insert a new column from a Foldable container.
insert ::
    forall name a cols t.
    ( KnownSymbol name
    , Columnable a
    , Foldable t
    , AssertAbsent name cols
    ) =>
    t a -> TypedDataFrame cols -> TypedDataFrame (T.Column name a ': cols)
insert xs (TDF df) = unsafeFreeze (D.insert colName xs df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a raw 'Column' value.
insertColumn ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    C.Column -> TypedDataFrame cols -> TypedDataFrame (T.Column name a ': cols)
insertColumn col (TDF df) = unsafeFreeze (D.insertColumn colName col df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a boxed 'Vector'.
insertVector ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    V.Vector a -> TypedDataFrame cols -> TypedDataFrame (T.Column name a ': cols)
insertVector vec (TDF df) = unsafeFreeze (D.insertVector colName vec df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Clone an existing column under a new name.
cloneColumn ::
    forall old new cols.
    ( KnownSymbol old
    , KnownSymbol new
    , AssertPresent old cols
    , AssertAbsent new cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame (T.Column new (Lookup old cols) ': cols)
cloneColumn (TDF df) = unsafeFreeze (D.cloneColumn oldName newName df)
  where
    oldName = T.pack (symbolVal (Proxy @old))
    newName = T.pack (symbolVal (Proxy @new))

-- | Drop a column by name.
dropColumn ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame (RemoveColumn name cols)
dropColumn (TDF df) = unsafeFreeze (D.exclude [colName] df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Replace an existing column with new values derived from a typed expression.
The column must already exist and the new type must match.
-}
replaceColumn ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , a ~ Lookup name cols
    , AssertPresent name cols
    ) =>
    TExpr cols a -> TypedDataFrame cols -> TypedDataFrame cols
replaceColumn (TExpr expr) (TDF df) = unsafeFreeze (D.derive colName expr df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Vertically merge two DataFrames with the same schema.
append :: TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
append (TDF a) (TDF b) = TDF (a <> b)

-------------------------------------------------------------------------------
-- Metadata (pass-through)
-------------------------------------------------------------------------------

dimensions :: TypedDataFrame cols -> (Int, Int)
dimensions (TDF df) = D.dimensions df

nRows :: TypedDataFrame cols -> Int
nRows (TDF df) = D.nRows df

nColumns :: TypedDataFrame cols -> Int
nColumns (TDF df) = D.nColumns df

columnNames :: TypedDataFrame cols -> [T.Text]
columnNames (TDF df) = D.columnNames df

-------------------------------------------------------------------------------
-- Internal helpers
-------------------------------------------------------------------------------

-- | Helper class for extracting [(Text, Text)] from type-level pairs.
class AllKnownPairs (pairs :: [(Symbol, Symbol)]) where
    pairVals :: [(T.Text, T.Text)]

instance AllKnownPairs '[] where
    pairVals = []

instance
    (KnownSymbol a, KnownSymbol b, AllKnownPairs rest) =>
    AllKnownPairs ('(a, b) ': rest)
    where
    pairVals =
        ( T.pack (symbolVal (Proxy @a))
        , T.pack (symbolVal (Proxy @b))
        )
            : pairVals @rest
