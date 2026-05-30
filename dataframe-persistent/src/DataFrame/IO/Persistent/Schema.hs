{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskellQuotes #-}

{- |
Module      : DataFrame.IO.Persistent.Schema
License     : MIT

Template Haskell that reads a SQLite table's schema __at compile time__ and
generates either a typed-schema type synonym ('declareTable') or a @persistent@
entity ('declareEntity'). Both replace the hand-written @persistLowerCase@ +
@derivePersistentDataFrame@ boilerplate.

'declareTable' emits only the schema /type/. You read into it with the generic
'DataFrame.IO.Persistent.Read.readTableTyped', so the database and table are
runtime values and the same schema can be reused across databases.

@
-- Typed schema, checked at compile time:
\$('declareTable' "chinook.db" "artists")
-- generates: type ArtistsSchema = '[Column "ArtistId" Int, Column "Name" (Maybe Text)]
-- read any database/table into it:
--   readTableTyped \@ArtistsSchema "chinook.db" "artists" :: IO (TypedDataFrame ArtistsSchema)

-- Persistent entity: full Filter DSL, write-back, no hand-written schema.
\$('declareEntity' "chinook.db" "artists")
-- then: runSqlite "chinook.db" (selectToDataFrame [ArtistsName ==. Just "Accept"] [])
@
-}
module DataFrame.IO.Persistent.Schema (
    -- * Typed schema
    declareTable,
    declareTableWith,
    declareTableFromSchema,

    -- * Persistent entity generation
    declareEntity,
    declareEntityWith,

    -- * Options + re-exports
    DeclareOptions (..),
    defaultDeclareOptions,
    ColumnInfo (..),
    HaskellType (..),
) where

import Data.List (partition)
import Data.Text (Text)
import qualified Data.Text as T
import Language.Haskell.TH

import Database.Persist.Quasi (lowerCaseSettings, parse)
import Database.Persist.Quasi.Internal (UnboundEntityDef)
import Database.Persist.TH (mkPersist, sqlSettings)

import DataFrame.IO.Persistent.Schema.Common
import DataFrame.IO.Persistent.Schema.Introspect
import DataFrame.Typed.Types (Column)

{- | Read @table@ from the SQLite database at @path@ and emit a typed schema
synonym, @type \<Table\>Schema = '[Column ...]@. Read into it with
'DataFrame.IO.Persistent.Read.readTableTyped'.
-}
declareTable :: FilePath -> String -> Q [Dec]
declareTable = declareTableWith defaultDeclareOptions

-- | 'declareTable' with custom 'DeclareOptions'.
declareTableWith :: DeclareOptions -> FilePath -> String -> Q [Dec]
declareTableWith opts path table = do
    cols <- introspectQ path table
    warnOnNumeric cols
    schemaDecl opts (T.pack table) cols

{- | Like 'declareTable' but from an explicit column list, so __no database is
opened at build time__ (use in CI / cross-compilation / sdist consumers).
-}
declareTableFromSchema :: [ColumnInfo] -> String -> Q [Dec]
declareTableFromSchema cols name =
    schemaDecl defaultDeclareOptions (T.pack name) cols

-- | Emit @type \<Table\>Schema = '[Column ...]@ for the selected columns.
schemaDecl :: DeclareOptions -> Text -> [ColumnInfo] -> Q [Dec]
schemaDecl opts table cols0 = do
    let cols = selectColumns opts cols0
    failOnEmpty table cols
    pure [TySynD (mkName (schemaTypeNameOf opts table)) [] (schemaType opts cols)]

failOnEmpty :: Text -> [ColumnInfo] -> Q ()
failOnEmpty table [] =
    fail ("declareTable: no columns selected for table " <> show (T.unpack table))
failOnEmpty _ _ = pure ()

schemaType :: DeclareOptions -> [ColumnInfo] -> Type
schemaType opts cols =
    buildSchemaType
        [(ciName c, thType (resolvedType opts c), isNullable opts c) | c <- cols]

-- | Fold @(name, type, nullable)@ triples into a promoted @'[Column name ty]@.
buildSchemaType :: [(Text, Type, Bool)] -> Type
buildSchemaType = foldr cons PromotedNilT
  where
    cons col acc = PromotedConsT `AppT` columnEntry col `AppT` acc

columnEntry :: (Text, Type, Bool) -> Type
columnEntry (name, ty, nullable) =
    ConT ''Column
        `AppT` LitT (StrTyLit (T.unpack name))
        `AppT` wrapMaybe nullable ty

wrapMaybe :: Bool -> Type -> Type
wrapMaybe True ty = ConT ''Maybe `AppT` ty
wrapMaybe False ty = ty

{- | Read @table@ at compile time and generate a @persistent@ entity for it
(via 'mkPersist'), giving the full @persistent@ typed-'Filter' DSL with no
hand-written schema. Load it with 'DataFrame.IO.Persistent.Read.selectToDataFrame'.
-}
declareEntity :: FilePath -> String -> Q [Dec]
declareEntity = declareEntityWith defaultDeclareOptions

-- | 'declareEntity' with custom 'DeclareOptions'.
declareEntityWith :: DeclareOptions -> FilePath -> String -> Q [Dec]
declareEntityWith opts path table = do
    cols <- introspectQ path table
    let tableT = T.pack table
        ent = T.pack (entityNameOf opts tableT)
        dsl = renderEntityDsl opts ent tableT (selectColumns opts cols)
    parseEntityDefs dsl >>= mkPersist sqlSettings

parseEntityDefs :: Text -> Q [UnboundEntityDef]
parseEntityDefs dsl = case snd (parse lowerCaseSettings [(Nothing, dsl)]) of
    Left errs -> fail ("declareEntity: could not build entity definition:\n" <> show errs)
    Right defs -> pure defs

{- | Render the @persistLowerCase@ DSL text for an entity. An integer primary key
becomes the @Id@ field; every other column is a field bound to its DB name.
-}
renderEntityDsl :: DeclareOptions -> Text -> Text -> [ColumnInfo] -> Text
renderEntityDsl opts ent table cols =
    T.unlines ((ent <> " sql=" <> table) : idLines <> map (renderField opts) fields)
  where
    (pks, others) = partition (isIntPk opts) cols
    -- Rename persistent's default (ToBackendKey-enabled) key column rather than
    -- declaring a natural key, so generated entities keep fromSqlKey/ToBackendKey.
    (idLines, fields) = case pks of
        [pk] -> (["    Id sql=" <> ciName pk], others)
        _ -> ([], cols)

isIntPk :: DeclareOptions -> ColumnInfo -> Bool
isIntPk opts c = ciPk c && resolvedType opts c == HTInt

renderField :: DeclareOptions -> ColumnInfo -> Text
renderField opts c =
    "    "
        <> fieldIdentifier (ciName c)
        <> " "
        <> persistType (resolvedType opts c)
        <> " sql="
        <> ciName c
        <> (if isNullable opts c then " Maybe" else "")
