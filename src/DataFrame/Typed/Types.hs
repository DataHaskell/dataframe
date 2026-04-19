{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Types (
    -- * Core phantom-typed wrapper
    TypedDataFrame (..),

    -- * Column phantom type (no constructors)
    Column,

    -- * Typed expressions (schema-validated)
    TExpr (..),

    -- * Typed sort orders
    TSortOrder (..),

    -- * Grouped typed dataframe
    TypedGrouped (..),

    -- * Typed aggregation builder (Option B)
    TAgg (..),
    taggToNamedExprs,

    -- * Re-export These
    These (..),
) where

import Data.Kind (Type)
import Data.These (These (..))
import GHC.TypeLits (Symbol)

import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Internal.Expression (Expr, NamedExpr, UExpr (..))

{- | A phantom-typed wrapper over the untyped 'DataFrame'.

The type parameter @cols@ is a type-level list of @Column name ty@ entries
that tracks the schema at compile time. All operations delegate to the
untyped core at runtime and update the phantom type at compile time.
-}
newtype TypedDataFrame (cols :: [Type]) = TDF {unTDF :: D.DataFrame}

instance Show (TypedDataFrame cols) where
    show (TDF df) = show df

instance Eq (TypedDataFrame cols) where
    (TDF a) == (TDF b) = a == b

{- | A phantom type that pairs a type-level column name ('Symbol')
with its element type. Has no value-level constructors — used
purely at the type level to describe schemas.
-}
data Column (name :: Symbol) (a :: Type)

{- | A typed expression validated against schema @cols@, producing values of type @a@.

Unlike the untyped 'Expr a', a 'TExpr' can only be constructed through
type-safe combinators ('col', 'lit', arithmetic operations) that verify
column references exist in the schema with the correct type.

Use 'unTExpr' to extract the underlying 'Expr' for delegation to the untyped API.
-}
newtype TExpr (cols :: [Type]) a = TExpr {unTExpr :: Expr a}

-- | A typed sort order validated against schema @cols@.
data TSortOrder (cols :: [Type]) where
    Asc :: (Columnable a, Ord a) => TExpr cols a -> TSortOrder cols
    Desc :: (Columnable a, Ord a) => TExpr cols a -> TSortOrder cols

-- | A phantom-typed wrapper over 'GroupedDataFrame'.
newtype TypedGrouped (keys :: [Symbol]) (cols :: [Type])
    = TGD {unTGD :: D.GroupedDataFrame}

{- | A typed aggregation builder (Option B).

Accumulates 'NamedExpr' values at the term level while building
the result schema at the type level. Each @agg@ call prepends a
'Column' to the @aggs@ phantom list.

Usage:

@
agg \@\"total\" (F.sum salary)
  $ agg \@\"avg_age\" (F.mean age)
  $ aggNil
@
-}
data TAgg (keys :: [Symbol]) (cols :: [Type]) (aggs :: [Type]) where
    TAggNil :: TAgg keys cols '[]
    TAggCons ::
        (Columnable a) =>
        -- | column name
        T.Text ->
        -- | typed aggregation expression
        TExpr cols a ->
        -- | rest
        TAgg keys cols aggs ->
        TAgg keys cols (Column name a ': aggs)

{- | Extract the runtime 'NamedExpr' list from a 'TAgg', in
declaration order (reversed from the cons-built order).
-}
taggToNamedExprs :: TAgg keys cols aggs -> [NamedExpr]
taggToNamedExprs = reverse . go
  where
    go :: TAgg keys cols aggs -> [NamedExpr]
    go TAggNil = []
    go (TAggCons name (TExpr expr) rest) = (name, UExpr expr) : go rest
