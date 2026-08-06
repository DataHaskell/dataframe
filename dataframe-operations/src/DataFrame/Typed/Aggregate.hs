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

    -- * Naming an aggregation
    as,

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
    (AllKnownSymbol keys, AssertAllPresent keys cols) =>
    TypedDataFrame cols -> TypedGrouped keys cols
groupBy (TDF df) = TGD (DA.groupBy (symbolVals @keys) df)

{- | Build a named aggregation entry. The result column name is supplied via
@TypeApplications@; the underlying expression is validated against the
source schema at compile time.

@as@ produces a /transformer/ on the aggregation chain — entries compose
with plain @(.)@ from Prelude (or via @(|>)@ for SQL-like postfix
reading). 'aggregate' applies the composed transformer to the empty chain
internally, so no terminator is needed.

==== __Prefix form__

@
result = grouped |> aggregate
    ( as \@\"total\"  (sum   (col \@\"amount\"))
    . as \@\"orders\" (count (col \@\"order_id\"))
    . as \@\"avg\"    (mean  (col \@\"amount\"))
    )
@

==== __Postfix form (SQL-like)__

@
result = grouped |> aggregate
    ( (sum   (col \@\"amount\")   |> as \@\"total\")
    . (count (col \@\"order_id\") |> as \@\"orders\")
    . (mean  (col \@\"amount\")   |> as \@\"avg\")
    )
@

Per-entry parentheses are required in the postfix form because
@(.)@ binds tighter than @(|>)@.
-}
as ::
    forall name a keys cols aggs.
    (KnownSymbol name, Columnable a) =>
    TExpr cols a ->
    TAgg keys cols aggs ->
    TAgg keys cols ('(name, a) ': aggs)
as = TAggCons (T.pack (symbolVal (Proxy @name)))

{- | Run a typed aggregation against a grouped DataFrame.

The first argument is a chain of 'as' entries composed with @(.)@. The
empty composition (@id@) yields just the group keys. The result schema is
the group-key columns followed by the aggregation columns in declaration
order.

@
result = grouped |> aggregate
    ( as \@\"total\"  (sum (col \@\"amount\"))
    . as \@\"orders\" (count (col \@\"order_id\"))
    )
-- result :: TypedDataFrame
--     '[ '(\"region\", Text)
--      , '(\"total\", Double)
--      , '(\"orders\", Int)
--      ]
@
-}
aggregate ::
    forall keys cols aggs.
    (TAgg keys cols '[] -> TAgg keys cols aggs) ->
    TypedGrouped keys cols ->
    TypedDataFrame (Append (GroupKeyColumns keys cols) (Reverse aggs))
aggregate build (TGD gdf) =
    unsafeFreeze (DA.aggregate (taggToNamedExprs (build TAggNil)) gdf)

-- | Escape hatch: run an untyped aggregation and return a raw 'DataFrame'.
aggregateUntyped :: [NamedExpr] -> TypedGrouped keys cols -> D.DataFrame
aggregateUntyped exprs (TGD gdf) = DA.aggregate exprs gdf
