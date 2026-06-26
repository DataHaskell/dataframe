{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

{- | Typed column transformations: the @apply@\/@derive@ family and
default-valued inserts, plus the horizontal merge @('|||')@. Schema changes are
tracked at the type level (e.g. 'applyColumn' rewrites a column's element type
via 'SetColumnType').
-}
module DataFrame.Typed.Apply (
    applyColumn,
    applyMany,
    applyWhere,
    applyAtIndex,
    safeApply,
    deriveWithExpr,
    insertWithDefault,
    insertVectorWithDefault,
    insertUnboxedVector,
    (|||),
) where

import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)

import DataFrame.Errors (DataFrameException)
import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Merge as D
import qualified DataFrame.Operations.Transformations as D
import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema (
    AllKnownSymbol,
    Append,
    AssertAbsent,
    AssertAllColumnsHaveType,
    AssertDisjoint,
    AssertPresent,
    SafeLookup,
    SetColumnType,
    Snoc,
    symbolVals,
 )
import DataFrame.Typed.Types (Column, TExpr (..), TypedDataFrame (..))

{- | Map a function over a column, rewriting its element type from @a@ to @b@.
The schema's entry for @name@ is updated via 'SetColumnType'.

@
df' = applyColumn \@\"age\" (show :: Int -> String) df
-- the \"age\" column is now String-typed
@
-}
applyColumn ::
    forall name a b cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , Columnable b
    , AssertPresent name cols
    ) =>
    (a -> b) ->
    TypedDataFrame cols ->
    TypedDataFrame (SetColumnType name b cols)
applyColumn f (TDF df) = unsafeFreeze (D.apply f colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Like 'applyColumn' but returns the error instead of throwing.
safeApply ::
    forall name a b cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , Columnable b
    , AssertPresent name cols
    ) =>
    (a -> b) ->
    TypedDataFrame cols ->
    Either DataFrameException (TypedDataFrame (SetColumnType name b cols))
safeApply f (TDF df) = fmap unsafeFreeze (D.safeApply f colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Apply a type-preserving function to several columns at once. Every named
column must already share the element type @a@ (enforced by
'AssertAllColumnsHaveType').
-}
applyMany ::
    forall (names :: [Symbol]) a cols.
    (AllKnownSymbol names, Columnable a, AssertAllColumnsHaveType names a cols) =>
    (a -> a) ->
    TypedDataFrame cols ->
    TypedDataFrame cols
applyMany f (TDF df) = TDF (D.applyMany f (symbolVals @names) df)

{- | Apply a function to a target column only on rows where a condition holds on
a filter column. Both columns are named by type application; the target keeps
its type.

@
applyWhere \@\"flagged\" \@\"score\" id (* 2) df
@
-}
applyWhere ::
    forall filterName targetName a b cols.
    ( KnownSymbol filterName
    , KnownSymbol targetName
    , a ~ SafeLookup filterName cols
    , b ~ SafeLookup targetName cols
    , Columnable a
    , Columnable b
    , AssertPresent filterName cols
    , AssertPresent targetName cols
    ) =>
    (a -> Bool) ->
    (b -> b) ->
    TypedDataFrame cols ->
    TypedDataFrame cols
applyWhere cond f (TDF df) = TDF (D.applyWhere cond filterName f targetName df)
  where
    filterName = T.pack (symbolVal (Proxy @filterName))
    targetName = T.pack (symbolVal (Proxy @targetName))

-- | Apply a type-preserving function to a single row of a column.
applyAtIndex ::
    forall name a cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    Int ->
    (a -> a) ->
    TypedDataFrame cols ->
    TypedDataFrame cols
applyAtIndex i f (TDF df) = TDF (D.applyAtIndex i f colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Derive a new column and also return a typed reference to it. The returned
expression lives in the extended schema, so it can feed later operations.
-}
deriveWithExpr ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    TExpr cols a ->
    TypedDataFrame cols ->
    ( TExpr (Snoc cols (Column name a)) a
    , TypedDataFrame (Snoc cols (Column name a))
    )
deriveWithExpr (TExpr expr) (TDF df) =
    let (e', df') = D.deriveWithExpr colName expr df
     in (TExpr e', unsafeFreeze df')
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a column from a 'Foldable', padding missing rows with a default.
insertWithDefault ::
    forall name a cols t.
    ( KnownSymbol name
    , Columnable a
    , Foldable t
    , AssertAbsent name cols
    ) =>
    a -> t a -> TypedDataFrame cols -> TypedDataFrame (Column name a ': cols)
insertWithDefault def xs (TDF df) =
    unsafeFreeze (D.insertWithDefault def colName xs df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a boxed 'V.Vector', padding missing rows with a default.
insertVectorWithDefault ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    a -> V.Vector a -> TypedDataFrame cols -> TypedDataFrame (Column name a ': cols)
insertVectorWithDefault def vec (TDF df) =
    unsafeFreeze (D.insertVectorWithDefault def colName vec df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert an unboxed 'VU.Vector' as a new column.
insertUnboxedVector ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , VU.Unbox a
    , AssertAbsent name cols
    ) =>
    VU.Vector a -> TypedDataFrame cols -> TypedDataFrame (Column name a ': cols)
insertUnboxedVector vec (TDF df) =
    unsafeFreeze (D.insertUnboxedVector colName vec df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Horizontal merge: place two DataFrames side by side. The schemas must be
disjoint (no shared column names), enforced by 'AssertDisjoint'; the result
schema is their concatenation.
-}
(|||) ::
    (AssertDisjoint left right) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (Append left right)
(TDF a) ||| (TDF b) = unsafeFreeze (a D.||| b)
