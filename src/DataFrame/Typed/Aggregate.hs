{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Aggregate (
    -- * Typed groupBy
    groupBy,

    -- * Typed aggregation builder (Option B)
    agg,
    aggNil,

    -- * Running aggregations
    aggregate,

    -- * Escape hatch
    aggregateUntyped,
) where

import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)

import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Internal.Expression (NamedExpr)
import qualified DataFrame.Operations.Aggregation as DA

import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types

{- | Group a typed DataFrame by one or more key columns.

@
grouped = groupBy \@'[\"department\"] employees
@
-}
groupBy ::
    forall (keys :: [Symbol]) cols.
    (AllKnownSymbol keys) =>
    TypedDataFrame cols -> TypedGrouped keys cols
groupBy (TDF df) = TGD (DA.groupBy (symbolVals @keys) df)

-- | The empty aggregation — no output columns beyond the group keys.
aggNil :: TAgg keys cols '[]
aggNil = TAggNil

{- | Add one aggregation to the builder.

Each call prepends a @Column name a@ to the result schema and records
the runtime 'NamedExpr'. The expression is validated against the
source schema @cols@ at compile time.

@
agg \@\"total_sales\" (tsum (col \@\"salary\"))
  $ agg \@\"avg_price\" (tmean (col \@\"price\"))
  $ aggNil
@
-}
agg ::
    forall name a keys cols aggs.
    ( KnownSymbol name
    , Columnable a
    ) =>
    TExpr cols a -> TAgg keys cols aggs -> TAgg keys cols (Column name a ': aggs)
agg = TAggCons colName
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Run a typed aggregation.

Result schema = grouping key columns ++ aggregated columns (in declaration order).

@
result = aggregate
    (agg \@\"total\" (tsum (col @"salary")) $ agg \@\"count\" (tcount (col @"salary") $ aggNil)
    (groupBy \@'[\"dept\"] employees)
-- result :: TDF '[Column \"dept\" Text, Column \"total\" Double, Column \"count\" Int]
@
-}
aggregate ::
    forall keys cols aggs.
    TAgg keys cols aggs ->
    TypedGrouped keys cols ->
    TypedDataFrame (Append (GroupKeyColumns keys cols) (Reverse aggs))
aggregate tagg (TGD gdf) =
    unsafeFreeze (DA.aggregate (taggToNamedExprs tagg) gdf)

-- | Escape hatch: run an untyped aggregation and return a raw 'DataFrame'.
aggregateUntyped :: [NamedExpr] -> TypedGrouped keys cols -> D.DataFrame
aggregateUntyped exprs (TGD gdf) = DA.aggregate exprs gdf
