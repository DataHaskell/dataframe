{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

{- |
Module      : DataFrame.IO.Persistent.Read
License     : MIT

The runtime front door for loading SQLite into a 'DataFrame'. Mirrors the
ergonomics of @readCsv@: point at a file and a table and get a 'DataFrame', with
column types inferred from the SQLite schema. No Template Haskell, no entity
boilerplate, and it works directly in GHCi.

For compile-time typed schemas use "DataFrame.IO.Persistent.Schema"; for the
@persistent@ entity path use "DataFrame.IO.Persistent".
-}
module DataFrame.IO.Persistent.Read (
    -- * Reading a whole table (types from the schema)
    readTable,
    readTableWith,
    readTableConn,
    readTableConnWith,

    -- * Reading an arbitrary query (types sniffed from values)
    readSql,
    readSqlWith,

    -- * Reading straight into a typed schema (schema as a type argument)
    readTableTyped,
    readTableTypedConn,
    readSqlTyped,

    -- * Loading @persistent@ entities generically (no instance required)
    selectToDataFrame,

    -- * Discovery
    listTables,
    listTablesConn,
    describeTable,
    describeTableConn,

    -- * Filtering pushed down to SQLite
    ReadQuery (..),
    allRows,
    where_,
    limit,
    orderBy,

    -- * Column builders (used by generated readers)
    ColumnReader,
    buildColumn,
    buildNullableColumn,
    inferColumn,
    freezeOrThrow,

    -- * Re-exports
    ColumnInfo (..),
) where

import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.Resource (MonadResource)
import Data.List (transpose)
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist
import Database.Persist.Sql (SqlBackend)
import Database.Persist.Sqlite (runSqlite)

import DataFrame.IO.Persistent.Read.Columns
import DataFrame.IO.Persistent.Read.Sqlite (runNamedQuery)
import DataFrame.IO.Persistent.Schema.Introspect
import DataFrame.Internal.Column (fromList)
import DataFrame.Internal.DataFrame (DataFrame, fromNamedColumns)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

{- | A SQLite-side filter pushed into the generated @SELECT@. Build with
'allRows' and the '&'-friendly combinators 'where_', 'limit', 'orderBy'.
-}
data ReadQuery = ReadQuery
    { rqWhere :: Maybe Text
    -- ^ A @WHERE@ body (without the @WHERE@ keyword); may contain @?@ placeholders.
    , rqParams :: [PersistValue]
    -- ^ Values bound to the placeholders in 'rqWhere'.
    , rqOrderBy :: Maybe Text
    -- ^ An @ORDER BY@ body (without the keyword).
    , rqLimit :: Maybe Int
    , rqOffset :: Maybe Int
    }
    deriving (Eq, Show)

-- | The unfiltered query: every row, every column.
allRows :: ReadQuery
allRows = ReadQuery Nothing [] Nothing Nothing Nothing

-- | Add a @WHERE@ clause (with bound parameters) to a 'ReadQuery'.
where_ :: Text -> [PersistValue] -> ReadQuery -> ReadQuery
where_ clause params q = q{rqWhere = Just clause, rqParams = params}

-- | Limit the number of rows fetched.
limit :: Int -> ReadQuery -> ReadQuery
limit n q = q{rqLimit = Just n}

-- | Add an @ORDER BY@ clause.
orderBy :: Text -> ReadQuery -> ReadQuery
orderBy clause q = q{rqOrderBy = Just clause}

{- | Read an entire table into a 'DataFrame', inferring column types (and
nullability) from the SQLite schema. The @pd.read_sql_table@ analogue.
-}
readTable :: FilePath -> Text -> IO DataFrame
readTable path table = readTableWith path table allRows

-- | 'readTable' with a SQLite-side filter (e.g. for tables larger than memory).
readTableWith :: FilePath -> Text -> ReadQuery -> IO DataFrame
readTableWith path table q = do
    readers <- columnReadersFor path table
    runSqlite (T.pack path) (readColumnsConn table readers q)

{- | 'readTable' over an existing @persistent@ connection (any 'SqlBackend',
so it shares a transaction with @fromPersistent@ / @insert@).
-}
readTableConn :: (MonadResource m) => Text -> ReaderT SqlBackend m DataFrame
readTableConn table = readTableConnWith table allRows

-- | 'readTableConn' with a SQLite-side filter.
readTableConnWith ::
    (MonadResource m) => Text -> ReadQuery -> ReaderT SqlBackend m DataFrame
readTableConnWith table q = do
    cols <- introspectTableConn table
    readColumnsConn table (map columnInfoReader cols) q

columnReadersFor :: FilePath -> Text -> IO [(Text, ColumnReader)]
columnReadersFor path table =
    map columnInfoReader <$> introspectTable path (T.unpack table)

columnInfoReader :: ColumnInfo -> (Text, ColumnReader)
columnInfoReader ci =
    (ciName ci, columnReaderFor (inferType (ciDeclType ci)) (not (ciNotNull ci)))

{- | Run an arbitrary SQL query into a 'DataFrame'. Result columns keep their
query names (aliases included); types are inferred from the returned values.
-}
readSql :: FilePath -> Text -> IO DataFrame
readSql path sql = readSqlWith path sql []

-- | 'readSql' with bound query parameters (injection-safe).
readSqlWith :: FilePath -> Text -> [PersistValue] -> IO DataFrame
readSqlWith path sql params = do
    (names, rows) <- runNamedQuery (T.pack path) sql params
    pure (assembleInferred names rows)

{- | Read a whole table straight into a typed 'TypedDataFrame', validating the
schema at the boundary. The schema is a /type argument/ while the database and
table are ordinary values, so the same table name in two different databases is
just two calls with the same @\@Schema@, no generated names to collide:

@
a <- readTableTyped \@ArtistsSchema "db1.sqlite" "artists"
b <- readTableTyped \@ArtistsSchema "db2.sqlite" "artists"
-- ... then join a and b
@
-}
readTableTyped ::
    forall cols. (KnownSchema cols) => FilePath -> Text -> IO (TypedDataFrame cols)
readTableTyped path table = readTable path table >>= freezeOrThrow

-- | 'readTableTyped' over an existing connection (any backend).
readTableTypedConn ::
    forall cols m.
    (KnownSchema cols, MonadResource m) =>
    Text -> ReaderT SqlBackend m (TypedDataFrame cols)
readTableTypedConn table = readTableConn table >>= freezeOrThrow

-- | Read an arbitrary query (e.g. a join) straight into a typed 'TypedDataFrame'.
readSqlTyped ::
    forall cols. (KnownSchema cols) => FilePath -> Text -> IO (TypedDataFrame cols)
readSqlTyped path sql = readSql path sql >>= freezeOrThrow

-- | List the user tables in a SQLite database.
listTables :: FilePath -> IO [Text]
listTables = introspectTableNames

{- | 'listTables' over an existing connection (works on any backend, e.g. a
PostgreSQL 'SqlBackend' opened with @withPostgresqlConn@).
-}
listTablesConn :: (MonadResource m) => ReaderT SqlBackend m [Text]
listTablesConn = introspectTableNamesConn

{- | A @describeColumns@-style summary of a table's schema (one row per column),
read from the schema without scanning the data.
-}
describeTable :: FilePath -> Text -> IO DataFrame
describeTable path table = schemaFrame <$> introspectTable path (T.unpack table)

-- | 'describeTable' over an existing connection (any backend).
describeTableConn :: (MonadResource m) => Text -> ReaderT SqlBackend m DataFrame
describeTableConn table = schemaFrame <$> introspectTableConn table

schemaFrame :: [ColumnInfo] -> DataFrame
schemaFrame cols =
    fromNamedColumns
        [ ("Column Name", fromList (map ciName cols))
        , ("Type", fromList (map columnHaskellType cols))
        , ("SQLite Type", fromList (map ciDeclType cols))
        , ("Nullable", fromList (map (not . ciNotNull) cols))
        , ("Primary Key", fromList (map ciPk cols))
        ]

columnHaskellType :: ColumnInfo -> Text
columnHaskellType c =
    let base = haskellTypeName (inferType (ciDeclType c))
     in if ciNotNull c then base else "Maybe " <> base

{- | Load any @persistent@ entity into a 'DataFrame' without deriving an
instance: filters give you the typed @persistent@ DSL, the rows are decoded
generically. Powers the @declareEntity@ workflow.
-}
selectToDataFrame ::
    forall r m.
    ( MonadIO m
    , PersistEntity r
    , PersistEntityBackend r ~ SqlBackend
    ) =>
    [Filter r] -> [SelectOpt r] -> ReaderT SqlBackend m DataFrame
selectToDataFrame filters opts =
    entitiesToDataFrame <$> selectList filters opts

entitiesToDataFrame :: forall r. (PersistEntity r) => [Entity r] -> DataFrame
entitiesToDataFrame ents =
    assembleInferred ("id" : entityFieldNames r) (map entityRow ents)

-- @keyToValues@ decodes any key (single or backend) without a @ToBackendKey@
-- constraint; single-column keys (the common case) yield the @id@ column.
entityRow :: (PersistEntity r) => Entity r -> [PersistValue]
entityRow (Entity k v) = keyToValues k ++ toPersistFields v

entityFieldNames :: forall r -> (PersistEntity r) => [Text]
entityFieldNames r =
    map
        (unFieldNameHS . fieldHaskell)
        (getEntityFields (entityDef (Nothing :: Maybe r)))

{- | The streaming engine behind every table reader: build the @SELECT@, fetch
rows, transpose into columns, and apply each column's reader.
-}
readColumnsConn ::
    (MonadResource m) =>
    Text -> [(Text, ColumnReader)] -> ReadQuery -> ReaderT SqlBackend m DataFrame
readColumnsConn table cols q = do
    rows <- runRawRows (selectSql table (map fst cols) q) (rqParams q)
    pure (assembleColumns cols rows)

assembleColumns :: [(Text, ColumnReader)] -> [[PersistValue]] -> DataFrame
assembleColumns cols rows =
    fromNamedColumns (zipWith apply cols (cells (length cols) rows))
  where
    apply (name, rdr) pvs = (name, rdr pvs)

assembleInferred :: [Text] -> [[PersistValue]] -> DataFrame
assembleInferred names rows =
    fromNamedColumns (zip names (map inferColumn (cells (length names) rows)))

{- | Transpose rows into per-column value lists, padding to @n@ empty columns
when there are no rows (so column structure survives an empty result).
-}
cells :: Int -> [[PersistValue]] -> [[PersistValue]]
cells n [] = replicate n []
cells _ rows = transpose rows

selectSql :: Text -> [Text] -> ReadQuery -> Text
selectSql table cols q =
    T.unwords (filter (not . T.null) clauses)
  where
    colList = if null cols then "*" else T.intercalate ", " (map quoteIdent cols)
    clauses =
        [ "SELECT " <> colList
        , "FROM " <> quoteIdent table
        , maybe "" ("WHERE " <>) (rqWhere q)
        , maybe "" ("ORDER BY " <>) (rqOrderBy q)
        , maybe "" (("LIMIT " <>) . tshow) (rqLimit q)
        , maybe "" (("OFFSET " <>) . tshow) (rqOffset q)
        ]

tshow :: (Show a) => a -> Text
tshow = T.pack . show
