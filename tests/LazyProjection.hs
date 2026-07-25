{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | A lazy scan reads only the columns its 'Schema' names, and rejects
self-contradictory 'ReadOptions' before opening the file.
-}
module LazyProjection (tests) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Control.Exception (SomeException, evaluate, try)
import Data.List (isInfixOf)
import qualified DataFrame as D
import qualified DataFrame.IO.CSV as Csv
import qualified DataFrame.IO.CSV.Fast as Fast
import DataFrame.Internal.DataFrame (forceDataFrame)
import qualified DataFrame.Lazy as L
import DataFrame.Schema (Schema (..), schemaType)
import System.Directory (removeFile)
import System.IO.Temp (emptySystemTempFile)
import Test.HUnit

-- | Four columns; every schema below names at most two of them.
withCsv :: Char -> (FilePath -> IO a) -> IO a
withCsv sep k = do
    csvPath <- emptySystemTempFile "lazy_projection_.csv"
    TIO.writeFile csvPath (T.unlines (map (T.intercalate (T.singleton sep)) rows))
    r <- k csvPath
    removeFile csvPath
    pure r
  where
    rows =
        [ ["id", "name", "surname", "dob"]
        , ["1", "Ada", "Lovelace", "1815"]
        , ["2", "Alan", "Turing", "1912"]
        ]

twoCols :: Schema
twoCols =
    Schema $ M.fromList [("id", schemaType @Int), ("dob", schemaType @Int)]

{- | The scan keeps only the schema's columns — and 'dob' is not a prefix of
the header, so a positional scheme could not express this.
-}
scanProjects :: Test
scanProjects = TestLabel "scan_projects" $ TestCase $ withCsv ',' $ \csvPath -> do
    df <- L.runDataFrame (L.scanCsv twoCols (T.pack csvPath))
    assertEqual "dims" (2, 2) (D.dimensions df)
    assertEqual "columns" ["dob", "id"] (D.columnNames df)

-- | Same schema, same projection, through the SIMD reader.
scanProjectsWithFastReader :: Test
scanProjectsWithFastReader =
    TestLabel "scan_projects_fast_reader" $ TestCase $ withCsv ',' $ \csvPath -> do
        df <-
            L.runDataFrame
                (L.scanCsvWith Fast.fastReadCsvWithOpts twoCols (T.pack csvPath))
        assertEqual "dims" (2, 2) (D.dimensions df)
        assertEqual "columns" ["dob", "id"] (D.columnNames df)

-- | Both readers must agree on what a scan produces.
readersAgree :: Test
readersAgree = TestLabel "scan_readers_agree" $ TestCase $ withCsv ',' $ \csvPath -> do
    slow <- L.runDataFrame (L.scanCsv twoCols (T.pack csvPath))
    fast <-
        L.runDataFrame (L.scanCsvWith Fast.fastReadCsvWithOpts twoCols (T.pack csvPath))
    assertEqual "same frame" slow fast

{- | 'scanSeparated' used to drop its separator on the floor: the reader was
built from the schema alone and always split on commas.
-}
scanHonoursSeparator :: Test
scanHonoursSeparator =
    TestLabel "scan_honours_separator" $ TestCase $ withCsv ';' $ \csvPath -> do
        -- With the separator ignored, the whole line is one field and the
        -- schema's columns would not be found at all.
        df <- L.runDataFrame (L.scanSeparated ';' twoCols (T.pack csvPath))
        assertEqual "dims" (2, 2) (D.dimensions df)
        assertEqual "columns" ["dob", "id"] (D.columnNames df)

-- | A schema naming a column the file lacks is an error, not a short frame.
scanMissingColumn :: Test
scanMissingColumn = TestLabel "scan_missing_column" $ TestCase $ withCsv ',' $ \csvPath -> do
    let bogus = Schema $ M.fromList [("nope", schemaType @Int)]
    r <-
        try
            (L.runDataFrame (L.scanCsv bogus (T.pack csvPath)) >>= evaluate . forceDataFrame)
    case r of
        Left (e :: SomeException) ->
            assertBool
                ("expected Column not found, got " <> show e)
                ("Column not found" `isInfixOf` show e)
        Right _ -> assertFailure "expected a missing-column error"

-- | Contradictory options are rejected before the file is opened.
invalidOptionsCase :: String -> Csv.ReadOptions -> String -> Test
invalidOptionsCase label opts needle = TestLabel label $ TestCase $ do
    let expectFail which act = do
            r <- try (act >>= evaluate . forceDataFrame)
            case r of
                Left (e :: SomeException) ->
                    assertBool
                        ( label
                            <> ": "
                            <> which
                            <> " should mention "
                            <> show needle
                            <> ", got "
                            <> show e
                        )
                        (needle `isInfixOf` show e)
                Right _ -> assertFailure (label <> ": " <> which <> " should have failed")
    -- "no-such-file" proves the failure precedes any read.
    expectFail "pure reader" (Csv.readSeparated opts "no-such-file.csv")
    expectFail "fast reader" (Fast.fastReadCsvWithOpts opts "no-such-file.csv")

invalidOptions :: [Test]
invalidOptions =
    [ invalidOptionsCase
        "reject_empty_selection"
        Csv.defaultReadOptions{Csv.readColumns = Just []}
        "empty selection"
    , invalidOptionsCase
        "reject_duplicate_selection"
        Csv.defaultReadOptions{Csv.readColumns = Just ["a", "b", "a"]}
        "more than once"
    , invalidOptionsCase
        "reject_negative_row_cap"
        Csv.defaultReadOptions{Csv.numRowsToRead = Just (-1)}
        "numRowsToRead is negative"
    , invalidOptionsCase
        "reject_nameless_providenames"
        Csv.defaultReadOptions{Csv.headerSpec = Csv.ProvideNames []}
        "ProvideNames with no names"
    , invalidOptionsCase
        "reject_quote_separator"
        Csv.defaultReadOptions{Csv.columnSeparator = '"'}
        "cannot delimit fields"
    ]

{- | The SIMD reader's low-level entry takes its separator as a scanner byte,
so a 'columnSeparator' saying something else would be silently ignored.
Reject it instead.
-}
rejectSeparatorMismatch :: Test
rejectSeparatorMismatch = TestLabel "reject_separator_mismatch" $ TestCase $ do
    let opts = Csv.defaultReadOptions{Csv.columnSeparator = ';'}
        htab = 0x09
    r <- try (Fast.readSeparated htab opts "no-such-file.csv")
    case r of
        Left (e :: SomeException) ->
            assertBool
                ("expected a separator complaint, got " <> show e)
                ("columnSeparator" `isInfixOf` show e)
        Right (_ :: D.DataFrame) -> assertFailure "expected a separator mismatch error"

tests :: [Test]
tests =
    [ scanProjects
    , scanProjectsWithFastReader
    , readersAgree
    , scanHonoursSeparator
    , scanMissingColumn
    , rejectSeparatorMismatch
    ]
        <> invalidOptions
