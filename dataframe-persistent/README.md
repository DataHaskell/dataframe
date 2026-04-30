# dataframe-persistent

Persistent database integration for the Haskell DataFrame library.

## Overview

This package provides seamless integration between the `dataframe` library and the `persistent` database library, allowing you to:

- Load database entities directly into DataFrames
- Perform DataFrame operations on database data  
- Save DataFrame results back to the database
- Work with type-safe database entities

## Installation

Add to your `package.yaml`:

```yaml
dependencies:
- dataframe >= 1 && < 2
- dataframe-persistent ^>= 0.1
- persistent >= 2.14
- persistent-sqlite >= 2.13  # or your preferred backend
```

Or to your `.cabal` file:

```cabal
build-depends:
  dataframe >= 1 && < 2,
  dataframe-persistent ^>= 0.1,
  persistent >= 2.14,
  persistent-sqlite >= 2.13
```

## Quick Start

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

import Control.Monad.IO.Class (liftIO)
import Database.Persist
import Database.Persist.Sqlite
import Database.Persist.TH
import qualified DataFrame as DF
import qualified DataFrame.Functions as F
import DataFrame.IO.Persistent
import DataFrame.IO.Persistent.TH
import qualified Data.Vector as V

import DataFrame.Functions ((.<))

-- Define your entities
share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
TestUser
    name Text
    age Int
    active Bool
    deriving Show Eq
|]

-- Derive DataFrame instances
$(derivePersistentDataFrame ''TestUser)

-- Example usage
main :: IO ()
main = runSqlite "example.db" $ do
    -- Run migrations
    runMigration migrateAll
    
    -- Insert some test data
    _ <- insert $ TestUser "Alice" 25 True
    _ <- insert $ TestUser "Bob" 30 False
    _ <- insert $ TestUser "Charlie" 35 True
    
    -- Load from database
    allUsersDF <- fromPersistent @TestUser []
    liftIO $ putStrLn $ "Loaded " ++ show (nRows allUsersDF) ++ " users"
    
    -- Load with filters
    activeUsersDF <- fromPersistent @TestUser [TestUserActive ==. True]
    liftIO $ putStrLn $ "Active users: " ++ show (nRows activeUsersDF)
    
    -- Process with DataFrame operations
    -- Expressions are automaticaly generated.
    let youngUsers = DF.filterWhere (test_user_age .< 30) allUsersDF
        ages = DF.columnAsList test_user_age youngUsers
    liftIO $ putStrLn $ "Young user ages: " ++ show ages
    
    -- Custom configuration
    let config = defaultPersistentConfig 
                    { pcIdColumnName = "user_id"
                    , pcIncludeId = True
                    }
    customDF <- fromPersistentWith @TestUser config []
    liftIO $ putStrLn $ "Columns with custom config: " ++ show (DF.columnNames customDF)
```

## Features

- **Type-safe conversions** between Persistent entities and DataFrames
- **Template Haskell support** for automatic instance generation
- **Configurable loading** with batch size and column selection
- **Column name cleaning** - removes table prefixes automatically (e.g., `test_user_name` → `name`)
- **Automatically generate typed expressions** - creates expressions in snake case prefixed by table name (e.g `test_user_name`).
- **Type preservation** - maintains proper types for Text, Int, Bool, Day, etc.
- **Empty DataFrame support** - preserves column structure even with no data
- **Support for all Persistent backends** (SQLite, PostgreSQL, MySQL, etc.)

## Configuration Options

```haskell
data PersistentConfig = PersistentConfig
    { pcBatchSize :: Int        -- Number of records to fetch at once (default: 10000)
    , pcIncludeId :: Bool       -- Whether to include entity ID as column (default: True)
    , pcIdColumnName :: Text    -- Name for the ID column (default: "id")
    }
```

## Advanced Usage

### Custom Field Extraction

You can also extract fields from individual entities:

```haskell
let user = TestUser "Alice" 25 True
    columns = persistFieldsToColumns user
-- Result: [("name", SomeColumn ["Alice"]), ("age", SomeColumn [25]), ("active", SomeColumn [True])]
```

### Working with Vector Data

```haskell
-- Extract specific column data
let names = DF.columnAsList test_user_name df
    ages = DF.columnAsList test_user_age df
    activeFlags = DF.columnAsList test_user_active df
```

## Examples

For comprehensive examples and test cases, see:
- [`tests/PersistentTests.hs`](tests/PersistentTests.hs) - Full test suite with examples
- [`../docs/persistent_integration.md`](../docs/persistent_integration.md) - Detailed integration guide

## Status

This package is **actively maintained** and tested. Current test coverage includes:
- ✅ Entity loading with and without filters
- ✅ Custom configuration options
- ✅ DataFrame operations on Persistent data
- ✅ Empty result set handling
- ✅ Field extraction utilities
- ✅ Multi-table relationships

## Documentation

For detailed documentation, see:
- [Main dataframe documentation](https://github.com/mchav/dataframe)
- [Persistent integration guide](../docs/persistent_integration.md)

## License

GPL-3.0-or-later (same as the main dataframe package)