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
    SetColumnType,
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
    AssertKeyTypesMatch,
    AssertDisjoint,
    AssertAllColumnsHaveType,
    AssertRealColumn,
    AllColumnsReal,
    AllDouble,
    IsRealType,
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
    ToMaybe,
    WrapMaybe,
    WrapMaybeColumns,
    CollidingColumns,

    -- * GroupBy helpers
    GroupKeyColumns,

    -- * KnownSchema class
    KnownSchema (..),
    schemaColumnNames,

    -- * Helpers
    AllKnownSymbol (..),
) where

import Data.Int (Int16, Int32, Int64, Int8)
import Data.Kind (Constraint, Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import GHC.TypeLits
import Type.Reflection (SomeTypeRep, Typeable, someTypeRep)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Types (These)
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

type family SetColumnType (name :: Symbol) (b :: Type) (cols :: [Type]) :: [Type] where
    SetColumnType name b (Column name _ ': rest) = Column name b ': rest
    SetColumnType name b (col ': rest) = col ': SetColumnType name b rest
    SetColumnType name b '[] =
        TypeError
            ('Text "Column '" ':<>: 'Text name ':<>: 'Text "' not found in schema")

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

{- | Assert that each join key has the same element type in both schemas,
modulo 'Maybe'-wrapping on either side (the runtime join matches a nullable
key column against a plain one). Use together with 'AssertAllPresent', which
reports keys missing from either schema; absent keys are skipped here so the
error fires exactly once.
-}
type family
    AssertKeyTypesMatch (keys :: [Symbol]) (left :: [Type]) (right :: [Type]) ::
        Constraint
    where
    AssertKeyTypesMatch '[] left right = ()
    AssertKeyTypesMatch (k ': ks) left right =
        ( KeyTypeMatchHelper k (SafeLookup k left) (SafeLookup k right)
        , AssertKeyTypesMatch ks left right
        )

type family
    KeyTypeMatchHelper (k :: Symbol) (l :: Type) (r :: Type) ::
        Constraint
    where
    KeyTypeMatchHelper k a a = ()
    KeyTypeMatchHelper k (Maybe a) a = ()
    KeyTypeMatchHelper k a (Maybe a) = ()
    KeyTypeMatchHelper k l r =
        TypeError
            ( 'Text "Join key '"
                ':<>: 'Text k
                ':<>: 'Text "' has type "
                ':<>: 'ShowType l
                ':<>: 'Text " in the left table but "
                ':<>: 'ShowType r
                ':<>: 'Text " in the right table"
            )

type family AssertDisjoint (left :: [Type]) (right :: [Type]) :: Constraint where
    AssertDisjoint left right =
        AssertDisjointHelper (SharedNames left right) left right

type family
    AssertDisjointHelper (shared :: [Symbol]) (left :: [Type]) (right :: [Type]) ::
        Constraint
    where
    AssertDisjointHelper '[] left right = ()
    AssertDisjointHelper (n ': ns) left right =
        TypeError
            ( 'Text "Cannot horizontally merge: column '"
                ':<>: 'Text n
                ':<>: 'Text "' appears in both schemas"
            )

type family
    AssertAllColumnsHaveType (names :: [Symbol]) (a :: Type) (cols :: [Type]) ::
        Constraint
    where
    AssertAllColumnsHaveType '[] a cols = ()
    AssertAllColumnsHaveType (n ': ns) a cols =
        ( SafeLookup n cols ~ a
        , AssertPresent n cols
        , AssertAllColumnsHaveType ns a cols
        )

-- | Is @a@ a real, unboxed numeric type — i.e. a valid numeric-column element?
type family IsRealType (a :: Type) :: Bool where
    IsRealType Int = 'True
    IsRealType Int8 = 'True
    IsRealType Int16 = 'True
    IsRealType Int32 = 'True
    IsRealType Int64 = 'True
    IsRealType Word = 'True
    IsRealType Word8 = 'True
    IsRealType Word16 = 'True
    IsRealType Word32 = 'True
    IsRealType Word64 = 'True
    IsRealType Double = 'True
    IsRealType Float = 'True
    IsRealType _ = 'False

{- | Emit a readable compile error when the column named @name@ is not a
real-number type, naming the calling function @fn@, the column, and the type it
actually has. Used by the numeric extractors so a wrong column type reads as a
repairable message rather than a bare @No instance for Real …@.
-}
type family AssertRealColumn (fn :: Symbol) (name :: Symbol) (a :: Type) :: Constraint where
    AssertRealColumn fn name a = AssertRealColumnGo fn name a (IsRealType a)

type family
    AssertRealColumnGo (fn :: Symbol) (name :: Symbol) (a :: Type) (isReal :: Bool) ::
        Constraint
    where
    AssertRealColumnGo fn name a 'True = ()
    AssertRealColumnGo fn name a 'False =
        TypeError
            ( 'Text fn
                ':<>: 'Text ": expected a real number column for '"
                ':<>: 'Text name
                ':<>: 'Text "' but instead you gave "
                ':<>: 'ShowType a
            )

{- | Constraint that every column in the schema is a real (numeric), unboxed
type. Lets the whole-frame matrix extractors ('toDoubleMatrix' and friends) be
total — a non-numeric or nullable column is a compile error (with the offending
column named, via 'AssertRealColumn'), not a runtime 'Left'.
-}
type family AllColumnsReal (fn :: Symbol) (cols :: [Type]) :: Constraint where
    AllColumnsReal fn '[] = ()
    AllColumnsReal fn (Column n a ': rest) =
        (AssertRealColumn fn n a, Real a, VU.Unbox a, AllColumnsReal fn rest)

-- TODO: mchavinda - we can generalist to AllX
type family AllDouble (cols :: [Type]) :: Constraint where
    AllDouble '[] = ()
    AllDouble (Column n Double ': rest) = AllDouble rest
    AllDouble (Column n a ': rest) =
        TypeError
            ( 'Text "Column '"
                ':<>: 'Text n
                ':<>: 'Text "' must be Double for this model, but is "
                ':<>: 'ShowType a
                ':$$: 'Text "Convert it (toDouble) or drop it before fitting."
            )

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

type family ToMaybe (a :: Type) :: Type where
    ToMaybe (Maybe a) = Maybe a
    ToMaybe a = Maybe a

-- | Wrap column types in Maybe; idempotent on already-optional columns.
type family WrapMaybe (cols :: [Type]) :: [Type] where
    WrapMaybe '[] = '[]
    WrapMaybe (Column n a ': rest) = Column n (ToMaybe a) ': WrapMaybe rest

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
        Column n (ToMaybe a) ': WrapMaybeColumns names rest
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

{- | The column names a schema declares, in schema order. Pass it to a reader's
options to fetch only those columns:

@
D.readCsvWithOpts
    D.defaultReadOptions{D.readColumns = Just (schemaColumnNames \@(Schema Customer))}
    "customers.csv"
@
-}
schemaColumnNames :: forall cols. (KnownSchema cols) => [T.Text]
schemaColumnNames = map fst (schemaEvidence @cols)

-- | A class that provides a list of 'Text' values for a type-level list of Symbols.
class AllKnownSymbol (names :: [Symbol]) where
    symbolVals :: [T.Text]

instance AllKnownSymbol '[] where
    symbolVals = []

instance (KnownSymbol n, AllKnownSymbol ns) => AllKnownSymbol (n ': ns) where
    symbolVals = T.pack (symbolVal (Proxy @n)) : symbolVals @ns
