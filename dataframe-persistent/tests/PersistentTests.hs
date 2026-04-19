{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module PersistentTests where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger (NoLoggingT)
import Control.Monad.Trans.Reader (ReaderT)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time
import qualified Data.Vector as V
import qualified DataFrame as DF
import qualified DataFrame.Functions as F
import DataFrame.IO.Persistent
import DataFrame.IO.Persistent.TH
import Database.Persist
import Database.Persist.Sqlite
import Database.Persist.TH
import System.IO (hClose)
import System.IO.Temp (withSystemTempFile)
import Test.HUnit
import UnliftIO.Resource (ResourceT)

import DataFrame.Functions ((.<.))

-- Define test entities
share
    [mkPersist sqlSettings, mkMigrate "migrateAll"]
    [persistLowerCase|
TestUser
    name Text
    age Int
    active Bool
    deriving Show Eq

TestOrder  
    userId TestUserId
    amount Double
    status Text
    orderDate Day
    deriving Show Eq
|]

share
    [mkPersist sqlSettings]
    [persistLowerCase|
Artist sql=artists
    Id sql=ArtistId Int64
    name Text sql=Name
    deriving Show Eq
|]

-- Derive DataFrame instances
$(derivePersistentDataFrame ''TestUser)
$(derivePersistentDataFrame ''TestOrder)
$(derivePersistentDataFrame ''Artist)

-- Test fixture data
testUsers :: [TestUser]
testUsers =
    [ TestUser "Alice" 25 True
    , TestUser "Bob" 30 False
    , TestUser "Charlie" 35 True
    , TestUser "Diana" 28 True
    ]

-- Helper to run tests with temp database
withTestDb :: ReaderT SqlBackend (NoLoggingT (ResourceT IO)) a -> IO a
withTestDb action = withSystemTempFile "test.db" $ \dbPath handle -> do
    -- Close the handle as we'll let sqlite manage it
    hClose handle
    runSqlite (T.pack dbPath) $ do
        runMigration migrateAll
        action

-- Test loading entities to DataFrame
testFromPersistent :: Test
testFromPersistent = TestCase $ withTestDb $ do
    -- Insert test data
    userIds <- mapM insert testUsers

    -- Load all users
    df <- fromPersistent @TestUser []
    liftIO $ do
        -- Check row count
        assertEqual "Row count" 4 (nRows df)

        -- Check columns exist
        let cols = DF.columnNames df
        print cols
        assertBool "Has id column" ("id" `elem` cols)
        assertBool "Has name column" ("name" `elem` cols)
        assertBool "Has age column" ("age" `elem` cols)
        assertBool "Has active column" ("active" `elem` cols)

        -- Check data values
        let names = DF.columnAsList test_user_name df

        assertEqual "Names match" ["Alice", "Bob", "Charlie", "Diana"] names

testChinookFromPersistent :: Test
testChinookFromPersistent = TestCase $ do
    let dbPath = "./data/chinook.db"
    runSqlite (T.pack dbPath) $ do
        -- No migration necessary since the table already exists.
        df <- fromPersistent @Artist []
        liftIO $ do
            assertEqual "Row count" 275 (nRows df)
            let cols = DF.columnNames df
            print cols
            assertBool "Has id column" ("id" `elem` cols)
            assertBool "Has name column" ("name" `elem` cols)
            let names = DF.columnAsList artist_name (DF.take 5 df)

            assertEqual
                "Names match"
                ["AC/DC", "Accept", "Aerosmith", "Alanis Morissette", "Alice In Chains"]
                names

-- Test loading with filters
testFromPersistentWithFilters :: Test
testFromPersistentWithFilters = TestCase $ withTestDb $ do
    -- Insert test data
    mapM_ insert testUsers

    -- Load active users only
    df <- fromPersistent @TestUser [TestUserActive ==. True]
    liftIO $ do
        assertEqual "Active users count" 3 (nRows df)

        -- Check all loaded users are active
        let activeFlags = DF.columnAsList test_user_active df
        assertBool "All active" (and activeFlags)

-- Test custom configuration
testCustomConfig :: Test
testCustomConfig = TestCase $ withTestDb $ do
    -- Insert test data
    mapM_ insert testUsers

    -- Load with custom config
    let config =
            defaultPersistentConfig
                { pcIdColumnName = "user_id"
                , pcBatchSize = 2
                }
    df <- fromPersistentWith @TestUser config []
    liftIO $ do
        -- Check custom ID column name
        let cols = DF.columnNames df
        print cols
        assertBool "Has custom id column" ("user_id" `elem` cols)
        assertBool "No default id column" ("id" `notElem` cols)

-- Test without ID column
testWithoutIdColumn :: Test
testWithoutIdColumn = TestCase $ withTestDb $ do
    -- Insert test data
    mapM_ insert testUsers

    -- Load without ID column
    let config = defaultPersistentConfig{pcIncludeId = False}
    df <- fromPersistentWith @TestUser config []
    liftIO $ do
        -- Check no ID column
        let cols = DF.columnNames df
        assertBool "No id column" ("id" `notElem` cols)
        assertEqual "Column count" 3 (length cols)

-- Test DataFrame operations on Persistent data
testDataFrameOperations :: Test
testDataFrameOperations = TestCase $ withTestDb $ do
    -- Insert test data
    mapM_ insert testUsers

    -- Load and perform operations
    df <- fromPersistent @TestUser []
    liftIO $ do
        -- Filter operation
        let youngUsers = DF.filter test_user_age (< 30) df
        assertEqual "Young users count" 2 (nRows youngUsers)

        -- Sort operation
        let sorted = DF.sortBy [DF.Asc (F.col @Int "age")] df
        let ages = DF.columnAsList test_user_age sorted
        assertEqual "Ages sorted" [25, 28, 30, 35] ages

        -- Derive column
        let withAgeGroup =
                DF.derive @Text
                    "age_group"
                    (F.ifThenElse (test_user_age .<. 30) "young" "adult")
                    df
        assertEqual "Has age_group column" 5 (length $ DF.columnNames withAgeGroup)

-- Test with relationships
testWithRelationships :: Test
testWithRelationships = TestCase $ withTestDb $ do
    -- Insert users and orders
    userId1 <- insert $ TestUser "Alice" 25 True
    userId2 <- insert $ TestUser "Bob" 30 False

    let today = fromGregorian 2024 3 20
    orderId1 <- insert $ TestOrder userId1 100.50 "completed" today
    orderId2 <- insert $ TestOrder userId1 50.25 "pending" (addDays 1 today)
    orderId3 <- insert $ TestOrder userId2 200.00 "completed" (addDays 2 today)

    -- Load orders
    ordersDF <- fromPersistent @TestOrder []
    liftIO $ assertEqual "Order count" 3 (nRows ordersDF)

    -- Load users
    usersDF <- fromPersistent @TestUser []

    -- TODO: Add join operations when available
    liftIO $ assertEqual "User count" 2 (nRows usersDF)

-- Test empty result set
testEmptyResultSet :: Test
testEmptyResultSet = TestCase $ withTestDb $ do
    -- Don't insert any data
    df <- fromPersistent @TestUser []
    liftIO $ do
        assertEqual "Empty DataFrame" 0 (nRows df)
        -- Should still have column names
        let cols = DF.columnNames df
        assertBool "Has columns" (not (null cols))

-- Test persistFieldsToColumns function
testPersistFieldsToColumns :: Test
testPersistFieldsToColumns = TestCase $ do
    let testUser = TestUser "Alice" 30 True
        columns = persistFieldsToColumns testUser

    -- Check that we got the expected number of columns (3 fields)
    assertEqual "Column count" 3 (length columns)

    -- Check column names
    let columnNames = map fst columns
    assertBool "Has name column" ("name" `elem` columnNames)
    assertBool "Has age column" ("age" `elem` columnNames)
    assertBool "Has active column" ("active" `elem` columnNames)

    -- Check that each column has exactly one value (since we're testing a single record)
    let nameCol = lookup "name" columns
        ageCol = lookup "age" columns
        activeCol = lookup "active" columns

    case nameCol of
        Just (SomeColumn vec) -> assertEqual "Name vector length" 1 (V.length vec)
        Nothing -> assertFailure "Name column not found"

    case ageCol of
        Just (SomeColumn vec) -> assertEqual "Age vector length" 1 (V.length vec)
        Nothing -> assertFailure "Age column not found"

    case activeCol of
        Just (SomeColumn vec) -> assertEqual "Active vector length" 1 (V.length vec)
        Nothing -> assertFailure "Active column not found"

-- All tests
persistentTests :: Test
persistentTests =
    TestList
        [ TestLabel "Load entities to DataFrame" testFromPersistent
        , TestLabel "Load with filters" testFromPersistentWithFilters
        , TestLabel "Custom configuration" testCustomConfig
        , TestLabel "Without ID column" testWithoutIdColumn
        , TestLabel "DataFrame operations" testDataFrameOperations
        , TestLabel "With relationships" testWithRelationships
        , TestLabel "Empty result set" testEmptyResultSet
        , TestLabel "Test persistFieldsToColumns" testPersistFieldsToColumns
        , TestLabel "Chinook: Reads first 10 rows" testChinookFromPersistent
        ]
