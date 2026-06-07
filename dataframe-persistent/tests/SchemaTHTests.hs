{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
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
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
End-to-end tests for the compile-time SQLite schema splices and the runtime
readers, exercised against the committed @data/chinook.db@ fixture.
-}
module SchemaTHTests (schemaThTests) where

import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Data.Int (Int64)
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Database.Persist (Filter, (==.))
import Database.Persist.Sqlite (runSqlite)
import Test.HUnit

import qualified DataFrame as DF
import DataFrame.IO.Persistent (nRows)
import DataFrame.IO.Persistent.Read
import DataFrame.IO.Persistent.Schema
import qualified DataFrame.Typed as DT

-- | Typed reader + schema for the @artists@ table (read at compile time).
$(declareTable "./data/chinook.db" "artists")

-- | A @persistent@ entity for the @albums@ table (generated from the live DB).
$(declareEntity "./data/chinook.db" "albums")

chinook :: T.Text
chinook = "./data/chinook.db"

-- Tier 0: runtime readers --------------------------------------------------

testReadTable :: Test
testReadTable = TestCase $ do
    df <- readTable "./data/chinook.db" "artists"
    assertEqual "row count" 275 (nRows df)
    let cols = DF.columnNames df
    assertBool "has raw ArtistId" ("ArtistId" `elem` cols)
    assertBool "has raw Name" ("Name" `elem` cols)

testReadTableWith :: Test
testReadTableWith = TestCase $ do
    df <- readTableWith "./data/chinook.db" "artists" (allRows & limit 5)
    assertEqual "limit pushed down" 5 (nRows df)

testReadSql :: Test
testReadSql = TestCase $ do
    df <- readSql "./data/chinook.db" "SELECT Name FROM artists LIMIT 5"
    assertEqual "raw query rows" 5 (nRows df)

testListTables :: Test
testListTables = TestCase $ do
    ts <- listTables "./data/chinook.db"
    assertBool "has artists" ("artists" `elem` ts)
    assertBool "has albums" ("albums" `elem` ts)

testDescribeTable :: Test
testDescribeTable = TestCase $ do
    d <- describeTable "./data/chinook.db" "artists"
    assertEqual
        "describe-style columns"
        ["Column Name", "Type", "SQLite Type", "Nullable", "Primary Key"]
        (DF.columnNames d)
    assertEqual "one row per column" 2 (nRows d)

-- Tier 1: typed splice -----------------------------------------------------

testDeclareTableTyped :: Test
testDeclareTableTyped = TestCase $ do
    -- declareTable emits only the ArtistsSchema type; read into it generically:
    tdf <- readTableTyped @ArtistsSchema "./data/chinook.db" "artists"
    assertEqual "typed row count" 275 (nRows (DT.thaw tdf))
    -- @col \@"Name"@ here is checked against the generated ArtistsSchema:
    let names = DT.columnAsList "Name" tdf
    assertEqual "first artist" (Just (Just "AC/DC")) (listToMaybe names)

-- Tier 2: persistent entity generation -------------------------------------

testDeclareEntity :: Test
testDeclareEntity = TestCase $ runSqlite chinook $ do
    df <- selectToDataFrame ([] :: [Filter Albums]) []
    liftIO $ assertEqual "all albums" 347 (nRows df)

testDeclareEntityFilter :: Test
testDeclareEntityFilter = TestCase $ runSqlite chinook $ do
    df <- selectToDataFrame [AlbumsArtistId ==. 1] []
    liftIO $ assertEqual "AC/DC albums" 2 (nRows df)

-- Hand-off: raw read and entity read share one connection ------------------

testHandOff :: Test
testHandOff = TestCase $ runSqlite chinook $ do
    artists <- readTableConn "artists"
    albums <- selectToDataFrame ([] :: [Filter Albums]) []
    liftIO $ do
        assertEqual "artists via raw conn" 275 (nRows artists)
        assertEqual "albums via entity" 347 (nRows albums)

schemaThTests :: Test
schemaThTests =
    TestList
        [ TestLabel "readTable" testReadTable
        , TestLabel "readTableWith limit" testReadTableWith
        , TestLabel "readSql" testReadSql
        , TestLabel "listTables" testListTables
        , TestLabel "describeTable" testDescribeTable
        , TestLabel "declareTable (typed)" testDeclareTableTyped
        , TestLabel "declareEntity" testDeclareEntity
        , TestLabel "declareEntity filter" testDeclareEntityFilter
        , TestLabel "dataframe/persistent hand-off" testHandOff
        ]
