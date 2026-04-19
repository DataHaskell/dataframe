{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module IO.CSV where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified DataFrame as D
import DataFrame.IO.CSV (fromCsv, fromCsvBytes)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame (..), toCsv)
import Test.HUnit (
    Test (TestCase, TestLabel),
    assertEqual,
    assertFailure,
 )

-- | Happy path: parse a simple CSV string with Int and Text columns.
fromCsvHappyPath :: Test
fromCsvHappyPath = TestLabel "fromCsv_happy_path" $ TestCase $ do
    result <- fromCsv "name,age\nAlice,30\nBob,25\nCharlie,35\n"
    case result of
        Left err -> assertFailure $ "Unexpected Left: " ++ err
        Right df -> do
            assertEqual "rows" 3 (D.nRows df)
            assertEqual "columns" 2 (D.nColumns df)

-- | Empty input should return a Left.
fromCsvEmpty :: Test
fromCsvEmpty = TestLabel "fromCsv_empty" $ TestCase $ do
    result <- fromCsv ""
    case result of
        Left _ -> return ()
        Right _ -> assertFailure "Expected Left for empty input"

-- | fromCsvBytes happy path.
fromCsvBytesHappyPath :: Test
fromCsvBytesHappyPath = TestLabel "fromCsvBytes_happy_path" $ TestCase $ do
    let bs = BL.fromStrict (TE.encodeUtf8 "x,y\n1,2\n3,4\n")
    df <- fromCsvBytes bs
    assertEqual "rows" 2 (D.nRows df)
    assertEqual "columns" 2 (D.nColumns df)

-- | Round trip: toCsv then fromCsv preserves data.
fromCsvRoundTrip :: Test
fromCsvRoundTrip = TestLabel "fromCsv_roundTrip" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList @Int [1, 2, 3])
                , ("b", DI.fromList @T.Text ["hello", "world", "test"])
                ]
    let csvString = T.unpack (toCsv df)
    result <- fromCsv csvString
    case result of
        Left err -> assertFailure $ "Unexpected Left: " ++ err
        Right df' -> do
            assertEqual
                "round trip dimensions"
                (dataframeDimensions df)
                (dataframeDimensions df')
            assertEqual "round trip data" df df'

-- | Round trip via fromCsvBytes.
fromCsvBytesRoundTrip :: Test
fromCsvBytesRoundTrip = TestLabel "fromCsvBytes_roundTrip" $ TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList @Int [10, 20])
                , ("y", DI.fromList @Double [1.5, 2.5])
                ]
    let bs = BL.fromStrict (TE.encodeUtf8 (toCsv df))
    df' <- fromCsvBytes bs
    assertEqual
        "round trip dimensions"
        (dataframeDimensions df)
        (dataframeDimensions df')

-- | Single column CSV.
fromCsvSingleColumn :: Test
fromCsvSingleColumn = TestLabel "fromCsv_single_column" $ TestCase $ do
    result <- fromCsv "id\n10\n20\n30\n"
    case result of
        Left err -> assertFailure $ "Unexpected Left: " ++ err
        Right df -> do
            assertEqual "rows" 3 (D.nRows df)
            assertEqual "columns" 1 (D.nColumns df)

tests :: [Test]
tests =
    [ fromCsvHappyPath
    , fromCsvEmpty
    , fromCsvBytesHappyPath
    , fromCsvRoundTrip
    , fromCsvBytesRoundTrip
    , fromCsvSingleColumn
    ]
