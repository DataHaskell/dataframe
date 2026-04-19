{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Typed.Schema (
    -- * Type families for schema manipulation
    Lookup,
    SafeLookup,
    HasName,
    RemoveColumn,
    Impute,
    SubsetSchema,
    ExcludeSchema,
    RenameInSchema,
    RenameManyInSchema,
    Append,
    Snoc,
    Reverse,
    ColumnNames,
    AssertAbsent,
    AssertPresent,
    AssertAllPresent,
    IsElem,

    -- * Maybe-stripping families
    StripAllMaybe,
    StripMaybeAt,

    -- * Join schema families
    SharedNames,
    UniqueLeft,
    InnerJoinSchema,
    LeftJoinSchema,
    RightJoinSchema,
    FullOuterJoinSchema,
    WrapMaybe,
    WrapMaybeColumns,
    CollidingColumns,

    -- * GroupBy helpers
    GroupKeyColumns,

    -- * KnownSchema class
    KnownSchema (..),

    -- * Helpers
    AllKnownSymbol (..),
) where

import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Data.These (These)
import GHC.TypeLits
import Type.Reflection (SomeTypeRep, Typeable, someTypeRep)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Typed.Types (Column)

-- | Look up the element type of a column by name.
type family Lookup (name :: Symbol) (cols :: [Type]) :: Type where
    Lookup name (Column name a ': _) = a
    Lookup name (Column _ _ ': rest) = Lookup name rest
    Lookup name '[] =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' not found in schema")

{- | Like 'Lookup', but returns a harmless fallback ('Int') instead of
'TypeError' when the column is not found.  Use together with
'AssertPresent' so the error fires exactly once.
-}
type family SafeLookup (name :: Symbol) (cols :: [Type]) :: Type where
    SafeLookup name (Column name a ': _) = a
    SafeLookup name (Column _ _ ': rest) = SafeLookup name rest
    SafeLookup name '[] = Int

-- | Unwrap a Maybe from a type after we impute values.
type family Impute (name :: Symbol) (cols :: [Type]) :: [Type] where
    Impute name (Column name (Maybe a) ': rest) = Column name a ': rest
    Impute name (Column name _ ': rest) =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' is not of kind Maybe *")
    Impute name (col ': rest) = col ': Impute name rest
    Impute name '[] = '[]

-- | Add type to the end of a list.
type family Snoc (xs :: [k]) (x :: k) :: [k] where
    Snoc '[] x = '[x]
    Snoc (y ': ys) x = y ': Snoc ys x

-- | Check whether a column name exists in a schema (type-level Bool).
type family HasName (name :: Symbol) (cols :: [Type]) :: Bool where
    HasName name (Column name _ ': _) = 'True
    HasName name (Column _ _ ': rest) = HasName name rest
    HasName name '[] = 'False

-- | Remove a column by name from a schema.
type family RemoveColumn (name :: Symbol) (cols :: [Type]) :: [Type] where
    RemoveColumn name (Column name _ ': rest) = rest
    RemoveColumn name (col ': rest) = col ': RemoveColumn name rest
    RemoveColumn name '[] = '[]

-- | Select a subset of columns by a list of names.
type family SubsetSchema (names :: [Symbol]) (cols :: [Type]) :: [Type] where
    SubsetSchema '[] cols = '[]
    SubsetSchema (n ': ns) cols = Column n (Lookup n cols) ': SubsetSchema ns cols

-- | Exclude columns by a list of names.
type family ExcludeSchema (names :: [Symbol]) (cols :: [Type]) :: [Type] where
    ExcludeSchema names '[] = '[]
    ExcludeSchema names (Column n a ': rest) =
        ExcludeSchemaHelper (IsElem n names) n a names rest

type family
    ExcludeSchemaHelper
        (found :: Bool)
        (n :: Symbol)
        (a :: Type)
        (names :: [Symbol])
        (rest :: [Type]) ::
        [Type]
    where
    ExcludeSchemaHelper 'True n a names rest = ExcludeSchema names rest
    ExcludeSchemaHelper 'False n a names rest =
        Column n a ': ExcludeSchema names rest

-- | Type-level elem for Symbols
type family IsElem (x :: Symbol) (xs :: [Symbol]) :: Bool where
    IsElem x '[] = 'False
    IsElem x (x ': _) = 'True
    IsElem x (_ ': xs) = IsElem x xs

-- | Rename a column in the schema.
type family RenameInSchema (old :: Symbol) (new :: Symbol) (cols :: [Type]) :: [Type] where
    RenameInSchema old new (Column old a ': rest) = Column new a ': rest
    RenameInSchema old new (col ': rest) = col ': RenameInSchema old new rest
    RenameInSchema old new '[] =
        TypeError
            ('Text "Cannot rename: column '" ':<>: 'Text old ':<>: 'Text "' not found")

-- | Rename multiple columns.
type family RenameManyInSchema (pairs :: [(Symbol, Symbol)]) (cols :: [Type]) :: [Type] where
    RenameManyInSchema '[] cols = cols
    RenameManyInSchema ('(old, new) ': rest) cols =
        RenameManyInSchema rest (RenameInSchema old new cols)

-- | Append two type-level lists.
type family Append (xs :: [k]) (ys :: [k]) :: [k] where
    Append '[] ys = ys
    Append (x ': xs) ys = x ': Append xs ys

-- | Reverse a type-level list.
type family Reverse (xs :: [Type]) :: [Type] where
    Reverse xs = ReverseAcc xs '[]

type family ReverseAcc (xs :: [Type]) (acc :: [Type]) :: [Type] where
    ReverseAcc '[] acc = acc
    ReverseAcc (x ': xs) acc = ReverseAcc xs (x ': acc)

-- | Extract column names as a type-level list of Symbols.
type family ColumnNames (cols :: [Type]) :: [Symbol] where
    ColumnNames '[] = '[]
    ColumnNames (Column n _ ': rest) = n ': ColumnNames rest

-- | Assert that a column name is absent from the schema (for derive/insert).
type family AssertAbsent (name :: Symbol) (cols :: [Type]) :: Constraint where
    AssertAbsent name cols = AssertAbsentHelper name (HasName name cols) cols

type family
    AssertAbsentHelper (name :: Symbol) (found :: Bool) (cols :: [Type]) ::
        Constraint
    where
    AssertAbsentHelper name 'False cols = ()
    AssertAbsentHelper name 'True cols =
        TypeError
            ( 'Text "Column '"
                ':<>: 'Text name
                ':<>: 'Text "' already exists in schema. "
                ':<>: 'Text "Use replaceColumn to overwrite."
            )

-- | Assert that a column name is present in the schema.
type family AssertPresent (name :: Symbol) (cols :: [Type]) :: Constraint where
    AssertPresent name cols = AssertPresentHelper name (HasName name cols) cols

type family
    AssertPresentHelper (name :: Symbol) (found :: Bool) (cols :: [Type]) ::
        Constraint
    where
    AssertPresentHelper name 'True cols = ()
    AssertPresentHelper name 'False cols =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' not found in schema")

-- | Assert that a column name is present in the schema.
type family AssertAllPresent (name :: [Symbol]) (cols :: [Type]) :: Constraint where
    AssertAllPresent (name ': rest) cols =
        AssertAllPresentHelper (HasName name cols) name rest cols
    AssertAllPresent '[] cols = ()

type family
    AssertAllPresentHelper
        (found :: Bool)
        (name :: Symbol)
        (rest :: [Symbol])
        (cols :: [Type]) ::
        Constraint
    where
    AssertAllPresentHelper 'True name rest cols = AssertAllPresent rest cols
    AssertAllPresentHelper 'False name rest cols =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' not found in schema")

{- | Strip 'Maybe' from all columns. Used by 'filterAllJust'.

@Column "x" (Maybe Double)@ becomes @Column "x" Double@.
@Column "y" Int@ stays @Column "y" Int@.
-}
type family StripAllMaybe (cols :: [Type]) :: [Type] where
    StripAllMaybe '[] = '[]
    StripAllMaybe (Column n (Maybe a) ': rest) = Column n a ': StripAllMaybe rest
    StripAllMaybe (Column n a ': rest) = Column n a ': StripAllMaybe rest

{- | Strip 'Maybe' from a single named column. Used by 'filterJust'.

@StripMaybeAt "x" '[Column "x" (Maybe Double), Column "y" Int]@
  = @'[Column "x" Double, Column "y" Int]@
-}
type family StripMaybeAt (name :: Symbol) (cols :: [Type]) :: [Type] where
    StripMaybeAt name (Column name (Maybe a) ': rest) = Column name a ': rest
    StripMaybeAt name (Column name a ': rest) = Column name a ': rest
    StripMaybeAt name (col ': rest) = col ': StripMaybeAt name rest
    StripMaybeAt name '[] =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' not found in schema")

-- | Extract column names that appear in both schemas.
type family SharedNames (left :: [Type]) (right :: [Type]) :: [Symbol] where
    SharedNames '[] right = '[]
    SharedNames (Column n _ ': rest) right =
        SharedNamesHelper (HasName n right) n rest right

type family
    SharedNamesHelper
        (found :: Bool)
        (n :: Symbol)
        (rest :: [Type])
        (right :: [Type]) ::
        [Symbol]
    where
    SharedNamesHelper 'True n rest right = n ': SharedNames rest right
    SharedNamesHelper 'False n rest right = SharedNames rest right

-- | Columns from @left@ whose names do NOT appear in @right@.
type family UniqueLeft (left :: [Type]) (rightNames :: [Symbol]) :: [Type] where
    UniqueLeft '[] _ = '[]
    UniqueLeft (Column n a ': rest) rn =
        UniqueLeftHelper (IsElem n rn) n a rest rn

type family
    UniqueLeftHelper
        (found :: Bool)
        (n :: Symbol)
        (a :: Type)
        (rest :: [Type])
        (rn :: [Symbol]) ::
        [Type]
    where
    UniqueLeftHelper 'True n a rest rn = UniqueLeft rest rn
    UniqueLeftHelper 'False n a rest rn = Column n a ': UniqueLeft rest rn

-- | Wrap column types in Maybe.
type family WrapMaybe (cols :: [Type]) :: [Type] where
    WrapMaybe '[] = '[]
    WrapMaybe (Column n a ': rest) = Column n (Maybe a) ': WrapMaybe rest

-- | Wrap selected columns in Maybe by name list.
type family WrapMaybeColumns (names :: [Symbol]) (cols :: [Type]) :: [Type] where
    WrapMaybeColumns names '[] = '[]
    WrapMaybeColumns names (Column n a ': rest) =
        WrapMaybeColumnsHelper (IsElem n names) n a names rest

type family
    WrapMaybeColumnsHelper
        (found :: Bool)
        (n :: Symbol)
        (a :: Type)
        (names :: [Symbol])
        (rest :: [Type]) ::
        [Type]
    where
    WrapMaybeColumnsHelper 'True n a names rest =
        Column n (Maybe a) ': WrapMaybeColumns names rest
    WrapMaybeColumnsHelper 'False n a names rest =
        Column n a ': WrapMaybeColumns names rest

-- | Columns in left whose names collide with right (excluding keys).
type family CollidingColumns (left :: [Type]) (right :: [Type]) (keys :: [Symbol]) :: [Type] where
    CollidingColumns '[] _ _ = '[]
    CollidingColumns (Column n a ': rest) right keys =
        CollidingColumnsHelper1 (IsElem n keys) n a rest right keys

type family
    CollidingColumnsHelper1
        (isKey :: Bool)
        (n :: Symbol)
        (a :: Type)
        (rest :: [Type])
        (right :: [Type])
        (keys :: [Symbol]) ::
        [Type]
    where
    CollidingColumnsHelper1 'True n a rest right keys =
        CollidingColumns rest right keys
    CollidingColumnsHelper1 'False n a rest right keys =
        CollidingColumnsHelper2 (HasName n right) n a rest right keys

type family
    CollidingColumnsHelper2
        (inRight :: Bool)
        (n :: Symbol)
        (a :: Type)
        (rest :: [Type])
        (right :: [Type])
        (keys :: [Symbol]) ::
        [Type]
    where
    CollidingColumnsHelper2 'True n a rest right keys =
        Column n (These a (Lookup n right)) ': CollidingColumns rest right keys
    CollidingColumnsHelper2 'False n a rest right keys =
        CollidingColumns rest right keys

-- | Inner join result schema.
type family InnerJoinSchema (keys :: [Symbol]) (left :: [Type]) (right :: [Type]) :: [Type] where
    InnerJoinSchema keys left right =
        Append
            (SubsetSchema keys left)
            ( Append
                (UniqueLeft left (Append keys (ColumnNames right)))
                ( Append
                    (UniqueLeft right (Append keys (ColumnNames left)))
                    (CollidingColumns left right keys)
                )
            )

-- | Left join result schema.
type family LeftJoinSchema (keys :: [Symbol]) (left :: [Type]) (right :: [Type]) :: [Type] where
    LeftJoinSchema keys left right =
        Append
            (SubsetSchema keys left)
            ( Append
                (UniqueLeft left (Append keys (ColumnNames right)))
                ( Append
                    (WrapMaybe (UniqueLeft right (Append keys (ColumnNames left))))
                    (CollidingColumns left right keys)
                )
            )

-- | Right join result schema.
type family RightJoinSchema (keys :: [Symbol]) (left :: [Type]) (right :: [Type]) :: [Type] where
    RightJoinSchema keys left right =
        Append
            (SubsetSchema keys right)
            ( Append
                (WrapMaybe (UniqueLeft left (Append keys (ColumnNames right))))
                ( Append
                    (UniqueLeft right (Append keys (ColumnNames left)))
                    (CollidingColumns left right keys)
                )
            )

-- | Full outer join result schema.
type family
    FullOuterJoinSchema (keys :: [Symbol]) (left :: [Type]) (right :: [Type]) ::
        [Type]
    where
    FullOuterJoinSchema keys left right =
        Append
            (WrapMaybe (SubsetSchema keys left))
            ( Append
                (WrapMaybe (UniqueLeft left (Append keys (ColumnNames right))))
                ( Append
                    (WrapMaybe (UniqueLeft right (Append keys (ColumnNames left))))
                    (CollidingColumns left right keys)
                )
            )

-- | Extract Column entries from a schema whose names appear in @keys@.
type family GroupKeyColumns (keys :: [Symbol]) (cols :: [Type]) :: [Type] where
    GroupKeyColumns keys '[] = '[]
    GroupKeyColumns keys (Column n a ': rest) =
        GroupKeyColumnsHelper (IsElem n keys) n a keys rest

type family
    GroupKeyColumnsHelper
        (found :: Bool)
        (n :: Symbol)
        (a :: Type)
        (keys :: [Symbol])
        (rest :: [Type]) ::
        [Type]
    where
    GroupKeyColumnsHelper 'True n a keys rest =
        Column n a ': GroupKeyColumns keys rest
    GroupKeyColumnsHelper 'False n a keys rest = GroupKeyColumns keys rest

-- | Provides runtime evidence of a schema: a list of (name, TypeRep) pairs.
class KnownSchema (cols :: [Type]) where
    schemaEvidence :: [(T.Text, SomeTypeRep)]

instance KnownSchema '[] where
    schemaEvidence = []

instance
    (KnownSymbol name, Typeable a, Columnable a, KnownSchema rest) =>
    KnownSchema (Column name a ': rest)
    where
    schemaEvidence =
        (T.pack (symbolVal (Proxy @name)), someTypeRep (Proxy @a))
            : schemaEvidence @rest

-- | A class that provides a list of 'Text' values for a type-level list of Symbols.
class AllKnownSymbol (names :: [Symbol]) where
    symbolVals :: [T.Text]

instance AllKnownSymbol '[] where
    symbolVals = []

instance (KnownSymbol n, AllKnownSymbol ns) => AllKnownSymbol (n ': ns) where
    symbolVals = T.pack (symbolVal (Proxy @n)) : symbolVals @ns
