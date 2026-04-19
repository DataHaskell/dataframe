{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.Internal.Schema where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T

import Data.Maybe (isJust)
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column (Columnable)
import Type.Reflection (typeRep)

-- | A runtime tag for a column’s element type.
data SchemaType where
    -- | Constructor carrying a 'Proxy' of the element type.
    SType :: (Columnable a, Read a) => P.Proxy a -> SchemaType

{- | Show the underlying element type using 'typeRep'.

==== __Examples__
>>> :set -XTypeApplications
>>> show (schemaType @Bool)
"Bool"
-}
instance Show SchemaType where
    show :: SchemaType -> String
    show (SType (_ :: P.Proxy a)) = show (typeRep @a)

{- | Two 'SchemaType's are equal iff their element types are the same.

==== __Examples__
>>> :set -XTypeApplications
>>> schemaType @Int == schemaType @Int
True

>>> schemaType @Int == schemaType @Integer
False
-}
instance Eq SchemaType where
    (==) :: SchemaType -> SchemaType -> Bool
    (==) (SType (_ :: P.Proxy a)) (SType (_ :: P.Proxy b)) =
        isJust (testEquality (typeRep @a) (typeRep @b))

{- | Construct a 'SchemaType' for the given @a@.

==== __Examples__
>>> :set -XTypeApplications
>>> schemaType @T.Text == schemaType @T.Text
True

>>> show (schemaType @Double)
"Double"
-}
schemaType :: forall a. (Columnable a, Read a) => SchemaType
schemaType = SType (P.Proxy @a)

{- | Logical schema of a 'DataFrame': a mapping from column names to their
element types ('SchemaType').

==== __Examples__
Constructing and querying a schema:

>>> import qualified Data.Map as M
>>> import qualified Data.Text as T
>>> let s = Schema (M.fromList [("country", schemaType @T.Text), ("amount", schemaType @Double)])
>>> M.lookup "amount" (elements s) == Just (schemaType @Double)
True

Extending a schema:

>>> let s' = Schema (M.insert "discount" (schemaType @Double) (elements s))
>>> M.member "discount" (elements s')
True

Equality is structural over the map contents:

>>> let a = Schema (M.fromList [("x", schemaType @Int), ("y", schemaType @Double)])
>>> let b = Schema (M.fromList [("y", schemaType @Double), ("x", schemaType @Int)])
>>> a == b
True
-}
newtype Schema = Schema
    { elements :: M.Map T.Text SchemaType
    {- ^ Mapping from /column name/ to its 'SchemaType'.

    Invariant: keys are unique column names. A missing key means the column
    is not present in the schema.
    -}
    }
    deriving (Show, Eq)

{- | Construct a 'Schema' from a list of @(columnName, schemaType)@ pairs.

==== __Example__
>>> :set -XTypeApplications
>>> import qualified Data.Text as T
>>> let s = makeSchema [("name", schemaType @T.Text), ("age", schemaType @Int)]
>>> M.member "age" (elements s)
True
-}
makeSchema :: [(T.Text, SchemaType)] -> Schema
makeSchema = Schema . M.fromList
