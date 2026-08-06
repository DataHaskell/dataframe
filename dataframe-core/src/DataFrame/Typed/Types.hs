{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Types (
    -- * Core phantom-typed wrapper
    TypedDataFrame (..),

    -- * Typed expressions (schema-validated)
    TExpr (..),

    -- * Lifting untyped expressions into the typed world
    AsTExpr,
    ToTExpr (..),

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
import GHC.TypeLits (Symbol)

import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Internal.Expression (Expr, NamedExpr, UExpr (..))
import DataFrame.Internal.Types (These (..))

{- | A phantom-typed wrapper over the untyped 'DataFrame'.

The type parameter @cols@ is a type-level list of @'(name, ty)@ pairs
that tracks the schema at compile time. All operations delegate to the
untyped core at runtime and update the phantom type at compile time.
-}
newtype TypedDataFrame (cols :: [(Symbol, Type)]) = TDF {unTDF :: D.DataFrame}

instance Show (TypedDataFrame cols) where
    show (TDF df) = show df

instance Eq (TypedDataFrame cols) where
    (TDF a) == (TDF b) = a == b

{- | A typed expression validated against schema @cols@, producing values of type @a@.

Unlike the untyped 'Expr a', a 'TExpr' can only be constructed through
type-safe combinators ('col', 'lit', arithmetic operations) that verify
column references exist in the schema with the correct type.

Use 'unTExpr' to extract the underlying 'Expr' for delegation to the untyped API.
-}
newtype TExpr (cols :: [(Symbol, Type)]) a = TExpr {unTExpr :: Expr a}

-- | Shows the underlying expression; the schema phantom is type-level only.
instance (Show a) => Show (TExpr cols a) where
    showsPrec d (TExpr e) = showsPrec d e

{- | The typed counterpart of an untyped expression type for schema @cols@:
@AsTExpr cols (Expr r) = TExpr cols r@. Lets a result type follow the frame —
an @Expr@ over a plain frame becomes a @TExpr@ over a typed one.
-}
type family AsTExpr (cols :: [(Symbol, Type)]) (e :: Type) :: Type where
    AsTExpr cols (Expr r) = TExpr cols r

-- | Lift an untyped expression into its 'TExpr' for schema @cols@.
class ToTExpr (cols :: [(Symbol, Type)]) e where
    toTExpr :: e -> AsTExpr cols e

instance ToTExpr cols (Expr r) where
    toTExpr = TExpr

-- | A typed sort order validated against schema @cols@.
data TSortOrder (cols :: [(Symbol, Type)]) where
    Asc :: (Columnable a, Ord a) => TExpr cols a -> TSortOrder cols
    Desc :: (Columnable a, Ord a) => TExpr cols a -> TSortOrder cols

-- | A phantom-typed wrapper over 'GroupedDataFrame'.
newtype TypedGrouped (keys :: [Symbol]) (cols :: [(Symbol, Type)])
    = TGD {unTGD :: D.GroupedDataFrame}

{- | Internal aggregation chain. Each cons prepends a 'Column' to the
@aggs@ phantom list. End users never construct this directly — they
compose 'DataFrame.Typed.Aggregate.as' entries with @(.)@ and let
'DataFrame.Typed.Aggregate.aggregate' apply the composition to
'TAggNil'.

@
as \@\"total\"   (F.sum  salary)
  . as \@\"avg_age\" (F.mean age)
@
-}
data TAgg (keys :: [Symbol]) (cols :: [(Symbol, Type)]) (aggs :: [(Symbol, Type)]) where
    TAggNil :: TAgg keys cols '[]
    TAggCons ::
        (Columnable a) =>
        -- | column name
        T.Text ->
        -- | typed aggregation expression
        TExpr cols a ->
        -- | rest
        TAgg keys cols aggs ->
        TAgg keys cols ('(name, a) ': aggs)

{- | Extract the runtime 'NamedExpr' list from a 'TAgg', in
declaration order (reversed from the cons-built order).
-}
taggToNamedExprs :: TAgg keys cols aggs -> [NamedExpr]
taggToNamedExprs = reverse . go
  where
    go :: TAgg keys cols aggs -> [NamedExpr]
    go TAggNil = []
    go (TAggCons name (TExpr expr) rest) = (name, UExpr expr) : go rest
