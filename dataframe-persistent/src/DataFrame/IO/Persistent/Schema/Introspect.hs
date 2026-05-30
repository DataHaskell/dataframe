{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : DataFrame.IO.Persistent.Schema.Introspect
License     : MIT

Compile-time-safe SQLite schema introspection. Everything here is plain 'IO'
(callable from a Template Haskell @runIO@ in "DataFrame.IO.Persistent.Schema")
plus a pure SQLite-affinity → 'HaskellType' mapping. No Template Haskell and no
DataFrame dependencies live here, so the logic can be unit-tested directly.
-}
module DataFrame.IO.Persistent.Schema.Introspect (
    -- * Column metadata
    ColumnInfo (..),
    HaskellType (..),

    -- * Introspection (IO)
    introspectTable,
    introspectTableConn,
    introspectTableNames,
    introspectTableNamesConn,

    -- * Type inference
    inferType,
    haskellTypeName,

    -- * Raw-query helper (shared with the runtime reader)
    runRawRows,
    quoteIdent,
) where

import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.Resource (MonadResource)
import Data.Conduit (runConduit, (.|))
import qualified Data.Conduit.List as CL
import Data.List (sortOn)
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist (PersistValue (..))
import Database.Persist.Sql (SqlBackend, rawQuery)
import Database.Persist.SqlBackend (getRDBMS)
import Database.Persist.Sqlite (runSqlite)

-- | A single column as reported by @PRAGMA table_info@.
data ColumnInfo = ColumnInfo
    { ciName :: Text
    -- ^ Raw column name as stored in SQLite (e.g. @"ArtistId"@).
    , ciDeclType :: Text
    -- ^ Declared type string (e.g. @"NVARCHAR(120)"@); may be empty.
    , ciNotNull :: Bool
    -- ^ Whether the column carries a @NOT NULL@ constraint.
    , ciPk :: Bool
    -- ^ Whether the column participates in the primary key.
    }
    deriving (Eq, Show)

{- | The closed set of Haskell element types we map SQLite columns onto. Every
constructor corresponds to a type that is both 'Database.Persist.PersistField'
and 'DataFrame.Internal.Column.Columnable'.
-}
data HaskellType
    = HTInt
    | HTDouble
    | HTText
    | HTBool
    | HTByteString
    | HTDay
    | HTUTCTime
    | HTTimeOfDay
    deriving (Eq, Show)

{- | Map a declared SQLite type to a 'HaskellType' using SQLite's own
(case-insensitive, substring) type-affinity rules, with a few semantic
refinements for dates/booleans. Unknown or empty declarations fall back the
same way SQLite does (BLOB affinity for empty, numeric otherwise).
-}
inferType :: Text -> HaskellType
inferType declared
    | "INT" `inUpper` declared = HTInt
    | anyIn ["CHAR", "CLOB", "TEXT"] declared = HTText
    | anyIn ["BLOB", "BYTEA"] declared || T.null (T.strip declared) = HTByteString
    | anyIn ["REAL", "FLOA", "DOUB"] declared = HTDouble
    | "BOOL" `inUpper` declared = HTBool
    | anyIn ["DATETIME", "TIMESTAMP"] declared = HTUTCTime
    | "DATE" `inUpper` declared = HTDay
    | "TIME" `inUpper` declared = HTTimeOfDay
    | otherwise = HTDouble

-- | A display name for a 'HaskellType' (the element type, without nullability).
haskellTypeName :: HaskellType -> Text
haskellTypeName HTInt = "Int"
haskellTypeName HTDouble = "Double"
haskellTypeName HTText = "Text"
haskellTypeName HTBool = "Bool"
haskellTypeName HTByteString = "ByteString"
haskellTypeName HTDay = "Day"
haskellTypeName HTUTCTime = "UTCTime"
haskellTypeName HTTimeOfDay = "TimeOfDay"

inUpper :: Text -> Text -> Bool
inUpper needle hay = needle `T.isInfixOf` T.toUpper hay

anyIn :: [Text] -> Text -> Bool
anyIn needles hay = any (`inUpper` hay) needles

{- | Read the columns of @table@ from the SQLite database at @path@, ordered by
their @PRAGMA table_info@ @cid@ (i.e. declaration order).
-}
introspectTable :: FilePath -> String -> IO [ColumnInfo]
introspectTable path table =
    runSqlite (T.pack path) (introspectTableConn (T.pack table))

{- | 'introspectTable' over an existing connection. Backend-agnostic: SQLite uses
@PRAGMA table_info@; every other backend (PostgreSQL, MySQL, etc.) uses the
standard @information_schema.columns@.
-}
introspectTableConn ::
    (MonadResource m) => Text -> ReaderT SqlBackend m [ColumnInfo]
introspectTableConn table = do
    rdbms <- getRDBMS
    if rdbms == "sqlite" then sqlitePragmaColumns table else infoSchemaColumns table

sqlitePragmaColumns ::
    (MonadResource m) => Text -> ReaderT SqlBackend m [ColumnInfo]
sqlitePragmaColumns table = do
    rows <- runRawRows ("PRAGMA table_info(" <> quoteIdent table <> ")") []
    pure (map snd (sortOn fst (map parsePragmaRow rows)))

infoSchemaColumns ::
    (MonadResource m) => Text -> ReaderT SqlBackend m [ColumnInfo]
infoSchemaColumns table = do
    rows <- runRawRows infoSchemaColumnQuery [PersistText table]
    pks <- runRawRows infoSchemaPkQuery [PersistText table]
    let pkNames = [pvText n | (n : _) <- pks]
    pure (map (parseInfoSchemaRow pkNames) rows)

infoSchemaColumnQuery :: Text
infoSchemaColumnQuery =
    "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
    \WHERE table_name = ? AND table_schema NOT IN ('pg_catalog', 'information_schema') \
    \ORDER BY ordinal_position"

infoSchemaPkQuery :: Text
infoSchemaPkQuery =
    "SELECT kcu.column_name FROM information_schema.table_constraints tc \
    \JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name \
    \WHERE tc.table_name = ? AND tc.constraint_type = 'PRIMARY KEY'"

-- | List the user tables (excluding SQLite internal tables) in @path@.
introspectTableNames :: FilePath -> IO [Text]
introspectTableNames path = runSqlite (T.pack path) introspectTableNamesConn

-- | 'introspectTableNames' over an existing connection (backend-agnostic).
introspectTableNamesConn :: (MonadResource m) => ReaderT SqlBackend m [Text]
introspectTableNamesConn = do
    rdbms <- getRDBMS
    rows <-
        runRawRows
            (if rdbms == "sqlite" then sqliteTableQuery else infoSchemaTableQuery)
            []
    pure [pvText n | (n : _) <- rows]

sqliteTableQuery :: Text
sqliteTableQuery =
    "SELECT name FROM sqlite_master WHERE type = 'table' \
    \AND name NOT LIKE 'sqlite_%' ORDER BY name"

infoSchemaTableQuery :: Text
infoSchemaTableQuery =
    "SELECT table_name FROM information_schema.tables \
    \WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
    \AND table_type = 'BASE TABLE' ORDER BY table_name"

-- | Run a raw SQL query and collect every row's column values.
runRawRows ::
    (MonadResource m) =>
    Text -> [PersistValue] -> ReaderT SqlBackend m [[PersistValue]]
runRawRows sql params = runConduit (rawQuery sql params .| CL.consume)

parsePragmaRow :: [PersistValue] -> (Int, ColumnInfo)
parsePragmaRow [cid, name, ctype, notnull, _dflt, pk] =
    ( pvInt cid
    , ColumnInfo (pvText name) (pvText ctype) (pvInt notnull == 1) (pvInt pk > 0)
    )
parsePragmaRow other =
    error ("introspectTable: unexpected PRAGMA table_info row: " <> show other)

{- | Parse an @information_schema.columns@ row @(name, data_type, is_nullable)@,
marking 'ciPk' from the table's primary-key column names.
-}
parseInfoSchemaRow :: [Text] -> [PersistValue] -> ColumnInfo
parseInfoSchemaRow pkNames [name, dtype, nullable] =
    ColumnInfo nm (pvText dtype) (pvText nullable == "NO") (nm `elem` pkNames)
  where
    nm = pvText name
parseInfoSchemaRow _ other =
    error ("introspectTable: unexpected information_schema row: " <> show other)

pvText :: PersistValue -> Text
pvText (PersistText t) = t
pvText PersistNull = ""
pvText other = T.pack (show other)

pvInt :: PersistValue -> Int
pvInt (PersistInt64 i) = fromIntegral i
pvInt _ = 0

-- | Double-quote a SQLite identifier, escaping embedded quotes.
quoteIdent :: Text -> Text
quoteIdent t = "\"" <> T.replace "\"" "\"\"" t <> "\""
