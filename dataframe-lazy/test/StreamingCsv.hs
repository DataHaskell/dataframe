{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main (main) where

import Control.Exception (SomeException, bracket, try)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.List (isInfixOf)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified DataFrame.IO.CSV as Csv
import DataFrame.Internal.Column (Column (..))
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Lazy as L
import DataFrame.Schema (Schema (..), schemaType)
import System.Directory (removeFile)
import qualified System.Exit as Exit
import System.IO.Temp (emptySystemTempFile)
import Test.HUnit

withRawCsv :: BS.ByteString -> (FilePath -> IO a) -> IO a
withRawCsv bytes =
    bracket
        ( do
            path <- emptySystemTempFile "lazy_streaming_.csv"
            BS.writeFile path bytes
            pure path
        )
        removeFile

{- | Regression for the @hIsEOF >> hGetSome@ interaction: the handle's 8 KiB
buffer used to turn this 16 MiB input into more than two thousand parser and
temporary-file calls. A path-only reader remains source-compatible and should
now see one complete 64 MiB-or-EOF window.
-}
streamingPathReaderFillsWindow :: Test
streamingPathReaderFillsWindow =
    TestLabel "streaming_path_reader_fills_window" $ TestCase $ do
        let row = "1," <> BS.replicate 4094 0x78 <> "\n"
            rowCount = 4096
            input = "id,payload\n" <> BS.concat (replicate rowCount row)
            schema = Schema $ M.fromList [("id", schemaType @Int)]
        withRawCsv input $ \path -> do
            calls <- newIORef (0 :: Int)
            let countingReader opts windowPath = do
                    modifyIORef' calls (+ 1)
                    Csv.readSeparated opts windowPath
            df <-
                L.runDataFrame
                    (L.scanCsvStreamingWith countingReader schema (T.pack path))
            assertEqual "all rows decoded" (rowCount, 1) (D.dataframeDimensions df)
            readIORef calls >>= assertEqual "one parser window" 1

{- | A record newline exactly at the 64 MiB read boundary must not be lost or
duplicated. The byte reader records only small observations, avoiding the cost
of parsing the deliberately large payload.
-}
streamingWindowBoundaryIsLossless :: Test
streamingWindowBoundaryIsLossless =
    TestLabel "streaming_window_boundary_is_lossless" $ TestCase $ do
        let windowBytes = 64 * 1024 * 1024
            firstRecord = "1," <> BS.replicate (windowBytes - 3) 0x78 <> "\n"
            input = BS.concat ["id,payload\n", firstRecord, "2,y\n"]
            schema = Schema $ M.fromList [("id", schemaType @Int)]
        withRawCsv input $ \path -> do
            observations <- newIORef []
            let observingReader _ bytes = do
                    let (header, withNewline) = BS.break (== 0x0A) bytes
                        body = BS.drop 1 withNewline
                        observation =
                            ( C8.unpack header
                            , BS.length body
                            , BS.unpack (BS.take 2 body)
                            , if BS.null body then Nothing else Just (BS.last body)
                            )
                    modifyIORef' observations (observation :)
                    pure D.empty
            _ <-
                L.runDataFrame
                    (L.scanCsvStreamingBytesWith observingReader schema (T.pack path))
            actual <- reverse <$> readIORef observations
            assertEqual
                "two complete records, each with the original header"
                [ ("id,payload", windowBytes - 1, [0x31, 0x2C], Just 0x78)
                , ("id,payload", 3, [0x32, 0x2C], Just 0x79)
                ]
                actual

{- | An LF inside a quoted field is data, not a record boundary. If the quoted
record crosses a 64 MiB read boundary it must still reach the reader in one
complete window.
-}
streamingQuotedNewlineSpansWindow :: Test
streamingQuotedNewlineSpansWindow =
    TestLabel "streaming_quoted_newline_spans_window" $ TestCase $ do
        let windowBytes = 64 * 1024 * 1024
            firstChunk = "1,\"" <> BS.replicate (windowBytes - 4) 0x78 <> "\n"
            input = BS.concat ["id,payload\n", firstChunk, "\"\n"]
            schema = Schema $ M.fromList [("id", schemaType @Int)]
        withRawCsv input $ \path -> do
            bodyLengths <- newIORef []
            let observingReader _ bytes = do
                    let body = BS.drop 1 (snd (BS.break (== 0x0A) bytes))
                    modifyIORef' bodyLengths (BS.length body :)
                    pure D.empty
            _ <-
                L.runDataFrame
                    (L.scanCsvStreamingBytesWith observingReader schema (T.pack path))
            actual <- reverse <$> readIORef bodyLengths
            assertEqual
                "quoted record stays in one parser window"
                [windowBytes + 1]
                actual

{- | The default in-memory streaming path strips a file BOM and agrees with an
eager schema-projected read, including a final record without a newline.
-}
streamingDefaultMatchesEager :: Test
streamingDefaultMatchesEager =
    TestLabel "streaming_default_matches_eager" $ TestCase $ do
        let input = "\xEF\xBB\xBFid,name,value\n1,Ada,3.5\n2,Alan,4.5"
            schema =
                Schema $
                    M.fromList
                        [ ("id", schemaType @Int)
                        , ("name", schemaType @T.Text)
                        , ("value", schemaType @Double)
                        ]
        withRawCsv input $ \path -> do
            let opts = Csv.schemaReadOptions schema
            eager <- Csv.readSeparated opts path
            streamed <- L.runDataFrame (L.scanCsvStreaming schema (T.pack path))
            assertEqual "streaming bytes reader matches eager reader" eager streamed

{- | A parser failure happens on the producer thread; it must cross the queue
and fail the consumer rather than leaving 'runDataFrame' blocked forever.
-}
streamingReaderFailurePropagates :: Test
streamingReaderFailurePropagates =
    TestLabel "streaming_reader_failure_propagates" $ TestCase $ do
        let input = "id\n1\n"
            schema = Schema $ M.fromList [("id", schemaType @Int)]
            failingReader _ _ = ioError (userError "streaming-reader-marker")
        withRawCsv input $ \path -> do
            result <-
                try
                    ( L.runDataFrame
                        (L.scanCsvStreamingBytesWith failingReader schema (T.pack path))
                    )
            case result of
                Left (err :: SomeException) ->
                    assertBool
                        ("unexpected exception: " <> show err)
                        ("streaming-reader-marker" `isInfixOf` show err)
                Right (_ :: D.DataFrame) -> assertFailure "expected reader failure"

-- | Schema-declared text stays in the CSV reader's packed representation.
streamingKeepsTextPacked :: Test
streamingKeepsTextPacked =
    TestLabel "streaming_keeps_text_packed" $ TestCase $ do
        let input =
                "station,value\n"
                    <> "Alpha,1\nBeta,2\nAlpha,3\nBeta,4\n"
                    <> "Alpha,5\nBeta,6\nAlpha,7\nBeta,8\n"
            schema = Schema $ M.fromList [("station", schemaType @T.Text)]
        withRawCsv input $ \path -> do
            df <-
                L.runDataFrame
                    (L.scanCsvStreaming schema (T.pack path))
            case D.unsafeGetColumn "station" df of
                PackedText{} -> pure ()
                _ -> assertFailure "expected the CSV text column to stay packed"

tests :: Test
tests =
    TestList
        [ streamingPathReaderFillsWindow
        , streamingWindowBoundaryIsLossless
        , streamingQuotedNewlineSpansWindow
        , streamingDefaultMatchesEager
        , streamingReaderFailurePropagates
        , streamingKeepsTextPacked
        ]

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else pure ()
