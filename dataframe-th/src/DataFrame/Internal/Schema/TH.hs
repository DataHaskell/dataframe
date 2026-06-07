{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskellQuotes #-}
{-# LANGUAGE TypeApplications #-}

{- |
Template-Haskell @deriveSchema@ splice for "DataFrame.Internal.Schema".
Kept in a separate module so the runtime schema types in
"DataFrame.Internal.Schema" do not pull in @template-haskell@.
-}
module DataFrame.Internal.Schema.TH (
    deriveSchema,
    camelToSnake,
) where

import Data.Char (isUpper, toLower, toUpper)
import qualified Data.Text as T
import Language.Haskell.TH

import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Schema (Schema, makeSchema, schemaType)
import DataFrame.Operators (col)

{- | Auto-generate a runtime 'Schema' (and per-column @'Expr'@ accessors)
from a record ADT.

The splice reifies the record, applies @camelCase -> snake_case@ to each
record-selector name, and emits:

* a top-level @\<lower-first TyConName\>Schema :: 'Schema'@ binding suitable
  for passing to 'DataFrame.IO.CSV.readCsvWithSchema'.
* one @\<lower-first TyConName\>\<UpperFirst FieldName\> :: 'Expr' /ty/@ binding
  per field, so you can refer to columns in expression DSL code by name
  without writing @col \@/ty/ "snake_case_name"@ at every call site.

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
                , Just (AppE (VarE 'schemaType) (TypeE fTy))
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
                                ( AppE
                                    (VarE 'col)
                                    (TypeE fTy)
                                )
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

{- | @camelCase -> snake_case@. Lowercases the first character then prefixes
@\'_\'@ before every uppercase character (lowercased).
-}
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
