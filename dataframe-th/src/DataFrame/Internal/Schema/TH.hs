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
    schemaTypeE,
    camelToSnake,
) where

import Data.Char (isUpper, toLower, toUpper)
import qualified Data.Proxy as P
import qualified Data.Text as T
import Language.Haskell.TH

import DataFrame.Expression.Operators (col)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Schema (Schema, SchemaType (..), makeSchema)

schemaTypeE :: Type -> Exp
schemaTypeE ty =
    ConE 'SType `AppE` SigE (ConE 'P.Proxy) (ConT ''P.Proxy `AppT` ty)

{- | Auto-generate a runtime 'Schema' (and per-column @'Expr'@ accessors)
from a record ADT. Emits @\<tyName\>Schema@ plus one accessor per field
(@camelCase -> snake_case@). Requires a single record constructor.
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
                , Just (schemaTypeE fTy)
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
