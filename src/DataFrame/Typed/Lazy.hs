{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- |
Module      : DataFrame.Typed.Lazy
Copyright   : (c) 2025
License     : MIT
Stability   : experimental

Type-safe lazy query pipelines.

This module combines the compile-time schema tracking of 'TypedDataFrame'
with the deferred execution of 'LazyDataFrame'. Queries are built as a
logical plan tree with phantom-typed schema tracking; execution is deferred
until 'run' is called.

@
{\-\# LANGUAGE DataKinds, TypeApplications, TypeOperators \#-\}
import qualified DataFrame.Typed.Lazy as TL
import DataFrame.Typed (Column)

type Schema = '[Column \"id\" Int, Column \"name\" Text, Column \"score\" Double]

main = do
    let query = TL.scanCsv \@Schema \"data.csv\"
              & TL.filter (TL.col \@\"score\" TL..>. TL.lit 0.5)
              & TL.select \@'[\"id\", \"name\"]
    df <- TL.run query   -- TypedDataFrame '[Column \"id\" Int, Column \"name\" Text]
    print df
@
-}
module DataFrame.Typed.Lazy (
    -- * Core type
    TypedLazyDataFrame,

    -- * Data sources
    scanCsv,
    scanSeparated,
    scanParquet,
    fromDataFrame,
    fromTypedDataFrame,

    -- * Schema-preserving operations
    filter,
    take,

    -- * Schema-modifying operations
    derive,
    select,

    -- * Aggregation
    groupBy,
    aggregate,

    -- * Joins
    join,

    -- * Sort
    sortBy,

    -- * Execution
    run,

    -- * Re-exports for pipeline construction
    module DataFrame.Typed.Expr,
    module DataFrame.Typed.Types,
    SortOrder (..),
) where

import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude hiding (filter, take)

import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Schema (Schema)
import DataFrame.Lazy.Internal.DataFrame (LazyDataFrame)
import qualified DataFrame.Lazy.Internal.DataFrame as L
import DataFrame.Lazy.Internal.LogicalPlan (SortOrder (..))
import DataFrame.Operations.Join (JoinType)
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types

-- | A lazy query with compile-time schema tracking.
newtype TypedLazyDataFrame (cols :: [Type]) = TLD {_unTLD :: LazyDataFrame}

instance Show (TypedLazyDataFrame cols) where
    show (TLD ldf) = "TypedLazyDataFrame { " ++ show ldf ++ " }"

-- | Scan a CSV file with a given schema.
scanCsv ::
    Schema ->
    T.Text ->
    TypedLazyDataFrame cols
scanCsv schema path = TLD (L.scanCsv schema path)

-- | Scan a character-separated file with a given schema.
scanSeparated ::
    Char ->
    Schema ->
    T.Text ->
    TypedLazyDataFrame cols
scanSeparated sep schema path = TLD (L.scanSeparated sep schema path)

-- | Scan a Parquet file, directory, or glob pattern with a given schema.
scanParquet ::
    Schema ->
    T.Text ->
    TypedLazyDataFrame cols
scanParquet schema path = TLD (L.scanParquet schema path)

-- | Lift an already-loaded eager 'TypedDataFrame' into a lazy plan.
fromDataFrame :: TypedDataFrame cols -> TypedLazyDataFrame cols
fromDataFrame (TDF df) = TLD (L.fromDataFrame df)

-- | Synonym for 'fromDataFrame'.
fromTypedDataFrame :: TypedDataFrame cols -> TypedLazyDataFrame cols
fromTypedDataFrame = fromDataFrame

-- | Keep rows that satisfy the predicate.
filter :: TExpr cols Bool -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
filter (TExpr expr) (TLD ldf) = TLD (L.filter expr ldf)

-- | Retain at most @n@ rows.
take :: Int -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
take n (TLD ldf) = TLD (L.take n ldf)

-- | Add a computed column.
derive ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (Snoc cols (Column name a))
derive (TExpr expr) (TLD ldf) =
    TLD (L.derive (T.pack (symbolVal (Proxy @name))) expr ldf)

-- | Retain only the listed columns.
select ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names, AssertAllPresent names cols) =>
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (SubsetSchema names cols)
select (TLD ldf) = TLD (L.select (DataFrame.Typed.Schema.symbolVals @names) ldf)

-- | A typed lazy grouped query.
newtype TypedLazyGrouped (keys :: [Symbol]) (cols :: [Type]) = TLG
    { _unTLG :: ([T.Text], LazyDataFrame)
    }

-- | Group by key columns.
groupBy ::
    forall (keys :: [Symbol]) cols.
    (AllKnownSymbol keys, AssertAllPresent keys cols) =>
    TypedLazyDataFrame cols ->
    TypedLazyGrouped keys cols
groupBy (TLD ldf) = TLG (DataFrame.Typed.Schema.symbolVals @keys, ldf)

-- | Aggregate a grouped lazy query.
aggregate ::
    forall keys cols aggs.
    TAgg keys cols aggs ->
    TypedLazyGrouped keys cols ->
    TypedLazyDataFrame (Append (GroupKeyColumns keys cols) (Reverse aggs))
aggregate tagg (TLG (keys, ldf)) =
    TLD (L.groupBy keys (aggToNamedExprs tagg) ldf)

-- | Join two lazy queries on a shared key column.
join ::
    JoinType ->
    T.Text ->
    T.Text ->
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame left -- TODO: compute join result schema
join jt leftKey rightKey (TLD left) (TLD right) =
    TLD (L.join jt leftKey rightKey left right)

-- | Sort the result by column name and direction.
sortBy ::
    [(T.Text, SortOrder)] ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
sortBy cols (TLD ldf) = TLD (L.sortBy cols ldf)

-- | Execute the lazy query and return a typed DataFrame.
run ::
    forall cols.
    (KnownSchema cols) =>
    TypedLazyDataFrame cols ->
    IO (TypedDataFrame cols)
run (TLD ldf) = unsafeFreeze <$> L.runDataFrame ldf

-- | Convert TAgg to untyped named expressions for the lazy groupBy.
aggToNamedExprs :: TAgg keys cols aggs -> [(T.Text, E.UExpr)]
aggToNamedExprs TAggNil = []
aggToNamedExprs (TAggCons name (TExpr expr) rest) =
    (name, E.UExpr expr) : aggToNamedExprs rest
