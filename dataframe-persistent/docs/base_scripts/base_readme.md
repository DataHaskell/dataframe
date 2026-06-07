<!--
  This file is the runnable scripths source for dataframe-persistent's README.
  Every ```haskell block executes in order in one shared session against
  ./data/chinook.db. Regenerate the rendered README with:

      scripths docs/base_scripts/base_readme.md -o README.md

  Run it from THIS directory (dataframe-persistent/) so ./data/... resolves and
  the local package is picked up. Every ```haskell block runs, including the
  compile-time $(declareTable ...) / $(declareEntity ...) splices.
-->

# dataframe-persistent

Load SQLite into a [`dataframe`](https://hackage.haskell.org/package/dataframe). The schema is read
for you: at runtime when you're exploring, or at compile time when you want type safety. You don't
write a `persistent` entity, a `persistLowerCase` block, or any instances.

> **This README is a runnable [scripths](https://github.com/DataHaskell/scripths) notebook.**
> Every Haskell block runs top-to-bottom in one shared session against
> [`./data/chinook.db`](./data/chinook.db). Reproduce every output below with
> `scripths docs/base_scripts/base_readme.md -o README.md` run from `dataframe-persistent/`.

## Three ways in

| Tier | You write | You get |
|------|-----------|---------|
| **Runtime** | `readTable db "artists"` | a `DataFrame`, types inferred from the schema |
| **Typed** | `$(declareTable db "artists")` + `readTableTyped @Schema` | a compile-time schema type; columns checked by `col "Name"` |
| **Persistent** | `$(declareEntity db "artists")` | a full `persistent` entity: typed `Filter` DSL, write-back |

## Tier 0: runtime reads

This is the quickest way in, similar to pandas' `read_sql_table` / `read_sql`. You point at a file
and a table.

```haskell
-- cabal: build-depends: dataframe, dataframe-persistent, text
-- cabal: default-extensions: OverloadedStrings, TemplateHaskell, DataKinds
-- cabal: default-extensions: TypeApplications, TypeOperators, FlexibleContexts
import qualified DataFrame as D
import DataFrame.IO.Persistent.Read
import Data.Function ((&))
```

What tables are in this database?

```haskell
listTables "./data/chinook.db"
```

`describeTable` shows a table's columns and their inferred types before you load it (it returns a
`describeColumns`-style `DataFrame`):

```haskell
D.toMarkdown' <$> describeTable "./data/chinook.db" "artists"
```

Load the whole table. The column types (and nullability) come from the schema:

```haskell
D.toMarkdown' . D.take 5 <$> readTable "./data/chinook.db" "artists"
```

Tables larger than memory? Filter them in the database. Raw SQL works as you'd expect:

```haskell
D.toMarkdown' <$> readSql "./data/chinook.db" "SELECT * FROM artists WHERE Name LIKE 'A%' LIMIT 5"
```

Or push a `WHERE` / `LIMIT` down to a named table with a `ReadQuery` value:

```haskell
D.toMarkdown' <$> readTableWith "./data/chinook.db" "artists" (allRows & limit 3)
```

## Tier 1: typed schema

`declareTable` reads the schema at compile time and emits just the schema type. You read into it
with `readTableTyped`, where the schema is a type argument and the database and table are ordinary
values. Column references go through `col "Name"`, checked against the schema, so a typo or a wrong
type is a compile error. Nothing is keyed on a generated function name.

```haskell
import qualified DataFrame.Typed as DT
import DataFrame.Typed ((.==.))
import DataFrame.IO.Persistent.Schema (declareTable)
```

The splice brings one thing into scope, the type
`type ArtistsSchema = '[Column "ArtistId" Int, Column "Name" (Maybe Text)]`:

```haskell
$(declareTable "./data/chinook.db" "artists")
```

`readTableTyped @ArtistsSchema` reads any database/table into a `TypedDataFrame ArtistsSchema` (it
validates the schema as it reads). You can bind your own reader:

```haskell
artists = readTableTyped @ArtistsSchema "./data/chinook.db" "artists"
```

`thaw` drops a typed frame back to an untyped `DataFrame`:

```haskell
D.toMarkdown' . D.take 5 . DT.thaw <$> artists
```

Column access is checked against the schema. `col "Name"` only compiles because `"Name"` is a
column of `ArtistsSchema` (its element type is `Maybe Text`):

```haskell
DT.columnAsList "Name" . DT.take 3 <$> artists
```

A filter on a column that doesn't exist (or has the wrong type) is a compile error rather than a
runtime surprise:

```haskell
D.toMarkdown' . DT.thaw . DT.filterWhere (DT.col "Name" .==. DT.lit (Just "Accept")) <$> artists
```

Because the database is a value, reading the same table from two sources to join them is just two
calls with the same `@ArtistsSchema`:

```text
a <- readTableTyped @ArtistsSchema "europe.sqlite" "artists"
b <- readTableTyped @ArtistsSchema "us.sqlite"     "artists"
-- DT.thaw a / DT.thaw b, then DataFrame.innerJoin on "ArtistId", etc.
```

(`readSqlTyped @cols db "SELECT ... JOIN ..."` does the same for an arbitrary query. The Postgres
section below reads this same `ArtistsSchema` from a different backend.)

## Tier 2: generate a `persistent` entity

If you want the full `persistent` experience (the typed `Filter` DSL, relations, write-back),
`declareEntity` builds the entity from the live schema, so you skip the `persistLowerCase` block. It
needs the usual `persistent` extensions and unqualified `Text` / `Int64` in scope:

```haskell
-- cabal: build-depends: persistent, persistent-sqlite
-- cabal: default-extensions: GADTs, TypeFamilies, FlexibleInstances, MultiParamTypeClasses
-- cabal: default-extensions: StandaloneDeriving, DerivingStrategies, GeneralizedNewtypeDeriving
-- cabal: default-extensions: UndecidableInstances
import Data.Int (Int64)
import Data.Text (Text)
import Database.Persist (Filter, SelectOpt (..), (<-.), (==.))
import Database.Persist.Sqlite (runSqlite)
import DataFrame.IO.Persistent.Schema (declareEntity)
```

This generates the `Albums` entity (`Id`, `title`, `artistId`), bound to the real
`albums` / `AlbumId` / `Title` / `ArtistId` SQLite names, plus its `EntityField` constructors:

```haskell
$(declareEntity "./data/chinook.db" "albums")
```

Now load it with `selectToDataFrame`. It's a generic loader: it works for any `persistent` entity,
so the one `declareEntity` splice is all the boilerplate.

```text
selectToDataFrame
    :: (MonadIO m, PersistEntity r, PersistEntityBackend r ~ SqlBackend)
    => [Filter r]      -- persistent's typed WHERE DSL, e.g. [AlbumsArtistId ==. 1]
    -> [SelectOpt r]   -- ordering / paging,        e.g. [Asc AlbumsTitle, LimitTo 10]
    -> ReaderT SqlBackend m DataFrame
```

It runs persistent's `selectList filters opts` and turns the `[Entity Albums]` into a `DataFrame`:
the entity key becomes an `id` column and each field becomes a column (named by its
Haskell field, e.g. `title`, `artistId`), with element types decoded from the stored values. The
`[Filter Albums]` and `[SelectOpt Albums]` you pass are persistent's own, checked against the
entity, so a filter for the wrong field won't compile.

Find AC/DC's albums (`ArtistId` 1):

```haskell
D.toMarkdown' <$> runSqlite "./data/chinook.db" (selectToDataFrame [AlbumsArtistId ==. 1] [])
```

You can use both arguments together: filter and order/page in one query. Here are the albums by
AC/DC or Alanis Morissette (`ArtistId` 1 or 4), sorted by title, capped at 5. (`<-.` is persistent's
"field in list"; an empty filter list loads everything.)

```haskell
D.toMarkdown' <$> runSqlite "./data/chinook.db"
    (selectToDataFrame [AlbumsArtistId <-. [1, 4]] [Asc AlbumsTitle, LimitTo 5])
```

## Hand-off between dataframe and persistent

The `...Conn` readers run on a `ReaderT SqlBackend m`, so a raw read, an entity read, and ordinary
`persistent` queries all compose in one `runSqlite` transaction (this works on Postgres/MySQL too).
Here both reads share one connection and return each frame's `(rows, columns)`:

```haskell
runSqlite "./data/chinook.db" $ do
    artists <- readTableConn "artists"                       -- raw → DataFrame
    albums  <- selectToDataFrame ([] :: [Filter Albums]) []  -- entity → DataFrame
    pure (D.dimensions artists, D.dimensions albums)
```

## PostgreSQL (and other backends)

Everything except the file-path helpers and the compile-time splices is backend-agnostic: the
`...Conn` readers and `selectToDataFrame` run on any `persistent` `SqlBackend`. Introspection picks
`PRAGMA` for SQLite and `information_schema` for PostgreSQL/MySQL. The library doesn't depend on
`persistent-postgresql`; you bring the connection.

These examples run against a real PostgreSQL with the same `artists` / `albums` data, and the
notebook starts it itself. The block below uses GHCi's `:!` shell escape to spin up a throwaway
cluster on port 54329 and load the two tables from the SQLite fixture. It needs `postgres` and
`sqlite3` on `PATH` (e.g. `brew install postgresql@16`). A matching teardown runs at the end of the
section.

```haskell
:! bash scripts/pg-setup.sh
```

Open a connection (you supply `persistent-postgresql`; `runPg` is the usual `runSqlConn` wrapper):

```haskell
-- cabal: build-depends: persistent, persistent-postgresql, transformers, resourcet, monad-logger
import Control.Monad.Trans.Reader (ReaderT)
import Control.Monad.Trans.Resource (ResourceT, runResourceT)
import Control.Monad.Logger (NoLoggingT, runNoLoggingT)
import Database.Persist.Sql (SqlBackend)
import Database.Persist.Postgresql (runSqlConn, withPostgresqlConn)

runPg :: ReaderT SqlBackend (ResourceT (NoLoggingT IO)) a -> IO a
runPg act =
    runNoLoggingT (runResourceT (withPostgresqlConn "host=localhost port=54329 dbname=chinook user=postgres" (runSqlConn act)))
```

Discovery and reads use the same functions as SQLite, just `...Conn` wrapped in `runPg`:

```haskell
runPg listTablesConn
```

```haskell
D.toMarkdown' <$> runPg (describeTableConn "artists")
```

```haskell
D.toMarkdown' . D.take 5 <$> runPg (readTableConn "artists")
```

The typed reader is backend-agnostic too. The same `ArtistsSchema` generated from the SQLite file in
Tier 1 validates this PostgreSQL read, so it's one schema type across two databases:

```haskell
D.toMarkdown' . D.take 3 . DT.thaw <$> runPg (readTableTypedConn @ArtistsSchema "artists")
```

The `Albums` entity generated from SQLite at compile time is a plain `persistent` entity, so the
same `declareEntity` splice and `selectToDataFrame` run unchanged against PostgreSQL:

```haskell
D.toMarkdown' <$> runPg (selectToDataFrame [AlbumsArtistId <-. [1, 4]] [Asc AlbumsTitle, LimitTo 5])
```

Tear the throwaway cluster back down (again via `:!`):

```haskell
:! bash scripts/pg-teardown.sh
```

## How types are inferred

SQLite type affinities map to Haskell element types as below. `NOT NULL` columns are non-null,
everything else becomes `Maybe`. You can override a column with `typeOverride` in `DeclareOptions`
(`declareTableWith` / `declareEntityWith`).

| SQLite declared type | Haskell type |
|----------------------|--------------|
| `INTEGER`, `INT...` | `Int` |
| `REAL`, `FLOAT`, `DOUBLE`, `NUMERIC`, `DECIMAL` | `Double` |
| `TEXT`, `VARCHAR`, `CHAR`, `CLOB` | `Text` |
| `BOOLEAN` | `Bool` |
| `BLOB` / no declared type | `ByteString` |
| `DATE` | `Day` |
| `DATETIME`, `TIMESTAMP` | `UTCTime` |
| `TIME` | `TimeOfDay` |

## Install

```
build-depends: dataframe, dataframe-persistent, persistent-sqlite
```

The classic path still works and is unchanged: hand-write a `persistent` entity with
`share`/`persistLowerCase` and `derivePersistentDataFrame`, then load with `fromPersistent`.
