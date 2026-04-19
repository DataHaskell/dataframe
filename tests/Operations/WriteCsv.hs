{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.WriteCsv where

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame (..), toCsv, toSeparated)
import Test.HUnit

-- Basic test: Int and Text columns produce correct CSV
toCsvBasic :: Test
toCsvBasic = TestLabel "toCsv_basic" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("name", DI.fromList @T.Text ["Alice", "Bob", "Charlie"])
                , ("age", DI.fromList @Int [30, 25, 35])
                ]
        expected = "name,age\nAlice,30\nBob,25\nCharlie,35\n"
    assertEqual "basic toCsv" expected (toCsv df)

-- Empty DataFrame produces empty text
toCsvEmpty :: Test
toCsvEmpty =
    TestLabel "toCsv_empty" $
        TestCase $
            assertEqual "empty toCsv" T.empty (toCsv D.empty)

-- toSeparated with tab produces tab-delimited output
toSeparatedTab :: Test
toSeparatedTab = TestLabel "toSeparated_tab" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList @Int [1, 2])
                , ("y", DI.fromList @Int [3, 4])
                ]
        expected = "x\ty\n1\t3\n2\t4\n"
    assertEqual "tab separated" expected (toSeparated '\t' df)

-- Double values render correctly
toCsvDouble :: Test
toCsvDouble = TestLabel "toCsv_double" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("value", DI.fromList @Double [1.5, 2.0, 2.5])
                ]
        expected = "value\n1.5\n2.0\n2.5\n"
    assertEqual "double toCsv" expected (toCsv df)

-- Single column DataFrame
toCsvSingleColumn :: Test
toCsvSingleColumn = TestLabel "toCsv_single_column" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("id", DI.fromList @Int [10, 20, 30])
                ]
        expected = "id\n10\n20\n30\n"
    assertEqual "single column toCsv" expected (toCsv df)

-- Round trip: toCsv then readCsv preserves data
toCsvRoundTrip :: Test
toCsvRoundTrip = TestLabel "toCsv_roundTrip" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList @Int [1, 2, 3])
                , ("b", DI.fromList @T.Text ["hello", "world", "test"])
                ]
    let csvText = toCsv df
    let tmpPath = "/tmp/dataframe_test_toCsv_roundtrip.csv"
    TIO.writeFile tmpPath csvText
    df' <- D.readCsv tmpPath
    assertEqual
        "round trip dimensions"
        (dataframeDimensions df)
        (dataframeDimensions df')
    assertEqual "round trip data" df df'

tests :: [Test]
tests =
    [ toCsvBasic
    , toCsvEmpty
    , toSeparatedTab
    , toCsvDouble
    , toCsvSingleColumn
    , toCsvRoundTrip
    ]
