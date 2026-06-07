# DataFrame Persistent Integration

This document describes the integration between the DataFrame library and Haskell's Persistent database library.

## Overview

The DataFrame library provides integration with the Persistent database library through the `dataframe-persistent` package. This allows you to:

- Load database query results directly into DataFrames
- Perform DataFrame operations on database data
- Save DataFrame results back to the database
- Work with type-safe database entities

## Installation

Add the following dependency to your project:

```yaml
dependencies:
  - dataframe-persistent >= 0.1.0.0
```

Or in a cabal file:

```haskell
build-depends:
  dataframe >= 0.3.2.0,
  dataframe-persistent >= 0.1.0.0,
  persistent >= 2.14,
  persistent-sqlite >= 2.13  -- or your preferred backend
```

## Basic Usage

### 1. Define Your Entities

```haskell
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

import Database.Persist.TH
import DataFrame.IO.Persistent.TH

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
TestUser
    name Text
    age Int
    active Bool
    deriving Show Eq
|]

-- Derive DataFrame instances
$(derivePersistentDataFrame ''TestUser)
```

### 2. Load Data from Database

```haskell
import Database.Persist.Sqlite
import DataFrame as DF
import DataFrame.Functions as F

loadUsers :: IO ()
loadUsers = runSqlite "test.db" $ do
    -- Load all users
    allUsersDF <- fromPersistent @TestUser []
    
    -- Load with filters
    activeUsersDF <- fromPersistent @TestUser [TestUserActive ==. True]
    
    -- Custom configuration
    let config = defaultPersistentConfig 
            { pcIdColumnName = "user_id"
            , pcIncludeId = True
            }
    customDF <- fromPersistentWith @TestUser config []
    
    liftIO $ print allUsersDF
```

If the dataframe already has a primary key you can refer to it by:

```
Id sql=<name> Int64
```

### 3. Perform DataFrame Operations

Once loaded, you can use all standard DataFrame operations:

```haskell
analyzeUsers :: IO ()
analyzeUsers = runSqlite "test.db" $ do
    df <- fromPersistent @TestUser []
    
    liftIO $ do
        -- Filter
        let youngUsers = DF.filter @Int "age" (< 30) df
        
        -- Sort
        let sorted = DF.sortBy [DF.Asc "age"] df
        
        -- Derive columns
        let withAgeGroup = DF.derive "age_group"
                (F.ifThenElse (F.col Int "age" `F.lt` F.lit 30)
                    (F.lit @Text "young")
                    (F.lit @Text "adult"))
                df
        
        -- Get column values
        let ages = DF.columnAsList (F.col Int "age") sorted
        print ages  -- [25, 28, 30, 35]
```

### 4. Save DataFrame to Database

```haskell
saveToDatabase :: DataFrame -> IO ()
saveToDatabase df = runSqlite "test.db" $ do
    keys <- toPersistent @TestUser df
    liftIO $ putStrLn $ "Inserted " ++ show (length keys) ++ " records"
```

## Configuration Options

The `PersistentConfig` type allows you to customize the behavior:

```haskell
data PersistentConfig = PersistentConfig
    { pcBatchSize :: Int          -- Number of records to fetch at once (default: 10000)
    , pcIncludeId :: Bool         -- Include entity ID as column (default: True)
    , pcIdColumnName :: Text      -- Name for ID column (default: "id")
    }
```

## Advanced Features

### Custom Entity Mapping

For more control over the conversion process, you can implement the type classes manually:

```haskell
instance EntityToDataFrame MyEntity where
    entityColumnNames _ = ["id", "field1", "field2"]
    entityToColumnData (Entity key val) = 
        [ ("id", SomeColumn $ V.singleton $ fromSqlKey key)
        , ("field1", SomeColumn $ V.singleton $ myEntityField1 val)
        , ("field2", SomeColumn $ V.singleton $ myEntityField2 val)
        ]

instance DataFrameToEntity MyEntity where
    rowToEntity idx df = -- Implementation
```

### Working with Relationships

When working with related entities, load them separately and use DataFrame join operations:

```haskell
joinExample :: IO ()
joinExample = runSqlite "mydb.sqlite" $ do
    usersDF <- fromPersistent @User []
    ordersDF <- fromPersistent @Order []
    
    liftIO $ do
        -- Join users with their orders
        let joined = DF.innerJoin "id" "user_id" usersDF ordersDF
        print joined
```

## Performance Considerations

1. **Batch Size**: Adjust `pcBatchSize` based on your memory constraints and query size
2. **Filtering**: Apply database filters when possible rather than loading all data
3. **Lazy Loading**: For very large datasets, consider implementing streaming support

## Limitations

1. The current implementation loads all data into memory
2. Complex persistent fields may require custom conversion logic
3. Streaming/lazy evaluation is not yet supported

## Examples

See the `dataframe-persistent/tests/PersistentTests.hs` file for comprehensive examples and test cases demonstrating all features of the Persistent integration.

### Key Points from the Implementation

1. **Column Names**: Column names are automatically cleaned to remove table prefixes (e.g., `test_user_name` becomes `name`)
2. **Type Application**: Use TypeApplications syntax for cleaner code: `fromPersistent @TestUser []`
3. **Vector Operations**: Use `V.toList $ DF.columnAsVector @Type "column"` to extract column values
4. **Empty DataFrames**: Empty result sets still preserve the column structure
