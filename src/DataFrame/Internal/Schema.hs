{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskellQuotes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.Internal.Schema where

import Data.Char (isUpper, toLower, toUpper)
import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T

import Data.Maybe (isJust)
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Operators (col)
import Language.Haskell.TH
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

{- | Auto-generate a runtime 'Schema' (and per-column @'Expr'@ accessors)
from a record ADT.

The splice reifies the record, applies @camelCase -> snake_case@ to each
record-selector name, and emits:

* a top-level @\<lower-first TyConName\>Schema :: 'Schema'@ binding suitable
  for passing to 'DataFrame.IO.CSV.readCsvWithSchema' /
  'DataFrame.IO.CSV.readCsvWithOpts'.
* one @\<lower-first TyConName\>\<UpperFirst FieldName\> :: 'Expr' /ty/@ binding
  per field, so you can refer to columns in expression DSL code by name
  without writing @col \@/ty/ "snake_case_name"@ at every call site.

@
data Order = Order { customerId :: Int, region :: Text, amount :: Double }

\$(deriveSchema ''Order)
-- expands to:
-- orderSchema :: Schema
-- orderSchema = makeSchema
--     [ ("customer_id", schemaType \@Int)
--     , ("region",      schemaType \@Text)
--     , ("amount",      schemaType \@Double)
--     ]
-- orderCustomerId :: Expr Int
-- orderCustomerId = col "customer_id"
-- orderRegion :: Expr Text
-- orderRegion = col "region"
-- orderAmount :: Expr Double
-- orderAmount = col "amount"

main = do
    df <- D.readCsvWithSchema orderSchema "orders.csv"
    let bigOrders = D.filterWhere (orderAmount .>. 100) df
    ...
@

The data type must have exactly one record constructor; sum types or
positional constructors fail the splice with a descriptive error. Field
types must satisfy @('Columnable' a, 'Read' a)@ — the same constraints
'schemaType' already requires.
-}
deriveSchema :: Name -> DecsQ
deriveSchema tyName = do
    info <- reify tyName
    fields <- extractRecordFields tyName info
    let entries =
            [ (camelToSnake fieldBase, fieldBase, fTy)
            | (fName, _bang, fTy) <- fields
            , let fieldBase = nameBase fName
            ]
        schemaName = mkName (lowerFirst (nameBase tyName) ++ "Schema")
        prefix = lowerFirst (nameBase tyName)
        tupleE (colName, _, fTy) =
            TupE
                [ Just (AppE (VarE 'T.pack) (LitE (StringL colName)))
                , Just (AppTypeE (VarE 'schemaType) fTy)
                ]
        schemaBody =
            AppE (VarE 'makeSchema) (ListE (map tupleE entries))
        schemaDecls =
            [ SigD schemaName (ConT ''Schema)
            , ValD (VarP schemaName) (NormalB schemaBody) []
            ]
        accessorDecls =
            concat
                [ [ SigD accName (AppT (ConT ''Expr) fTy)
                  , ValD
                        (VarP accName)
                        ( NormalB
                            ( AppE
                                (VarE 'col)
                                ( AppE
                                    (VarE 'T.pack)
                                    (LitE (StringL colName))
                                )
                            )
                        )
                        []
                  ]
                | (colName, fieldBase, fTy) <- entries
                , let accName = mkName (prefix ++ upperFirst fieldBase)
                ]
    pure (schemaDecls ++ accessorDecls)

extractRecordFields :: Name -> Info -> Q [VarBangType]
extractRecordFields _ (TyConI dec) = case dec of
    DataD _ _ _ _ [RecC _ fs] _ -> pure fs
    NewtypeD _ _ _ _ (RecC _ fs) _ -> pure fs
    DataD _ n _ _ _ _ ->
        fail $
            "deriveSchema: "
                ++ show n
                ++ " must have exactly one record constructor"
    NewtypeD _ n _ _ _ _ ->
        fail $
            "deriveSchema: " ++ show n ++ " newtype must use record syntax"
    other ->
        fail $
            "deriveSchema: unsupported declaration: " ++ show other
extractRecordFields tyName _ =
    fail $
        "deriveSchema: "
            ++ show tyName
            ++ " is not a data/newtype declaration"

-- Local @camelCase -> snake_case@: lowercase the first char, then prefix
-- @\'_\'@ before any uppercase character (lowercased). Duplicated from
-- 'DataFrame.Typed.TH.camelToSnake' to keep this module free of any
-- @DataFrame.Typed.*@ imports.
camelToSnake :: String -> String
camelToSnake [] = []
camelToSnake (c : cs) = toLower c : go cs
  where
    go [] = []
    go (x : xs)
        | isUpper x = '_' : toLower x : go xs
        | otherwise = x : go xs

lowerFirst :: String -> String
lowerFirst [] = []
lowerFirst (c : cs) = toLower c : cs

upperFirst :: String -> String
upperFirst [] = []
upperFirst (c : cs) = toUpper c : cs
