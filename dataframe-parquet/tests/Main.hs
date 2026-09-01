{-# LANGUAGE OverloadedStrings #-}

-- | Tests for the writer-buffer logic in "DataFrame.IO.Utils.RandomAccess".
module Main where

import Control.Exception (SomeException, catch, evaluate)
import qualified Data.ByteString as BS
import Data.List (sortOn)
import qualified System.Exit as Exit
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.HUnit

import Control.Monad (unless)
import Data.Int (Int32, Int64)
import Data.Maybe (fromJust)
import qualified Data.Text as T
import DataFrame.IO.Parquet (readParquet, readParquetFiles)
import DataFrame.IO.Parquet.Writer (
    ParquetWriteOptions (..),
    defaultParquetWriteOptions,
    writeParquet,
    writeParquetWithOptions,
 )
import DataFrame.IO.Utils.RandomAccess
import DataFrame.Internal.Column (columnTypeString, fromList)
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    fromNamedColumns,
    getColumn,
 )
import System.Directory (listDirectory)

directWrites :: Test
directWrites = TestCase $ do
    buffer <- mallocBuffer 1
    writeWord8 buffer 0xaa
    writeWord32LE buffer 0x78563412
    writeWord64LE buffer 0x0807060504030201
    writeFloatLE buffer 1
    writeDoubleLE buffer 1
    writeByteString buffer (BS.pack [0xfe, 0xff])
    residency <- bufferResidency buffer
    bytes <- bufferToByteString buffer
    assertEqual "direct write residency" 27 residency
    assertEqual
        "direct write bytes"
        ( BS.pack
            [ 0xaa
            , 0x12
            , 0x34
            , 0x56
            , 0x78
            , 0x01
            , 0x02
            , 0x03
            , 0x04
            , 0x05
            , 0x06
            , 0x07
            , 0x08
            , 0x00
            , 0x00
            , 0x80
            , 0x3f
            , 0x00
            , 0x00
            , 0x00
            , 0x00
            , 0x00
            , 0x00
            , 0xf0
            , 0x3f
            , 0xfe
            , 0xff
            ]
        )
        bytes

directBufferFlush :: Test
directBufferFlush = TestCase $ do
    source <- mallocBuffer 0
    destination <- mallocBuffer 0
    writeByteString destination (BS.pack [1, 2])
    writeByteString source (BS.pack [3, 4, 5])
    flushBufferToBuffer source destination
    sourceResidency <- bufferResidency source
    destinationBytes <- bufferToByteString destination
    assertEqual "source cleared" 0 sourceResidency
    assertEqual "destination appended" (BS.pack [1, 2, 3, 4, 5]) destinationBytes
    flushBufferToBuffer destination destination
    selfFlushedBytes <- bufferToByteString destination
    assertEqual "self flush is a no-op" destinationBytes selfFlushedBytes
    resetPosition destination
    destinationResidency <- bufferResidency destination
    assertEqual "reset position" 0 destinationResidency

directFileFlush :: Test
directFileFlush = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "out.bin"
            payload = BS.pack (take 300000 (cycle [0 .. 255]))
        buffer <- mallocBuffer 1
        writeByteString buffer payload
        withWritableBinaryFile outPath $ \output ->
            flushBufferToFile output buffer
        residency <- bufferResidency buffer
        contents <- BS.readFile outPath
        assertEqual "source cleared after file flush" 0 residency
        assertEqual "large payload round-trips" payload contents

writerRoundTrip :: String -> FilePath -> Test
writerRoundTrip label path = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        df <- readParquet path
        let out = dir </> "out.parquet"
        writeParquet out df
        df' <- readParquet out
        assertEqual label df df'

writerRoundTripTiny :: String -> FilePath -> Test
writerRoundTripTiny label path = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        df <- readParquet path
        let out = dir </> "out.parquet"
        writeParquetWithOptions tinyWriteOpts out df
        df' <- readParquet out
        assertEqual label df df'

writerRoundTripLargeText :: Test
writerRoundTripLargeText = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        let payload = T.replicate 4096 "abcdefgh"
            df = fromNamedColumns [("text", fromList [payload, "short"])]
            firstOut = dir </> "large-text-1.parquet"
            secondOut = dir </> "large-text-2.parquet"
        writeParquetWithOptions tinyWriteOpts firstOut df
        firstRoundTrip <- readParquet firstOut
        writeParquetWithOptions tinyWriteOpts secondOut firstRoundTrip
        secondRoundTrip <- readParquet secondOut
        assertEqual "large text first round-trip" df firstRoundTrip
        assertEqual "large text second round-trip" df secondRoundTrip

{- | Sharded writes: @maxRowsPerFile@ splits the frame across a glob pattern,
and reading the shards back reproduces the original frame.
-}
writerRoundTripSharded :: String -> FilePath -> Int -> Int -> Test
writerRoundTripSharded label path rowsPerFile expectedShards = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        df <- readParquet path
        let pattern_ = dir </> "shards" </> "part-*.parquet"
        writeParquetWithOptions
            defaultParquetWriteOptions{maxRowsPerFile = Just rowsPerFile}
            pattern_
            df
        shards <- listDirectory (dir </> "shards")
        assertEqual (label <> ": shard count") expectedShards (Prelude.length shards)
        assertEqual
            (label <> ": shard names")
            ["part-" <> pad i <> ".parquet" | i <- [0 .. expectedShards - 1]]
            (sortOn id shards)
        df' <- readParquetFiles pattern_
        assertEqual (label <> ": shards round-trip") df df'
  where
    pad i = let s = show i in replicate (5 - Prelude.length s) '0' <> s

-- | A path without a @*@ placeholder is rejected when sharding is requested.
shardedWriteRequiresPattern :: Test
shardedWriteRequiresPattern = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        df <- readParquet "tests/data/mtcars.parquet"
        threw <-
            ( False
                <$ writeParquetWithOptions
                    defaultParquetWriteOptions{maxRowsPerFile = Just 4}
                    (dir </> "out.parquet")
                    df
            )
                `catch` (\e -> True <$ evaluate (Prelude.length (show (e :: SomeException))))
        unless threw (assertFailure "expected an error for a path without '*'")

{- | Parquet has one 64-bit integer type, so @Int@, @Int64@ and @Integer@
columns all land in the file as @INT64@. The writer stamps the original
Haskell type in the footer so the reader can put it back; without that, a
CSV-inferred @Int@ column silently widens to @Int64@ on a round trip.
-}
writerRoundTripNativeIntTypes :: Test
writerRoundTripNativeIntTypes = TestCase $
    withSystemTempDirectory "dfpq-writer" $ \dir -> do
        let df =
                fromNamedColumns
                    [ ("int", fromList [1 :: Int, 2, 3])
                    , ("int64", fromList [1 :: Int64, 2, 3])
                    , ("int32", fromList [1 :: Int32, 2, 3])
                    , ("integer", fromList [1 :: Integer, 2, 3])
                    , ("nullableInt", fromList [Just (1 :: Int), Nothing, Just 3])
                    , ("nullableInt64", fromList [Just (1 :: Int64), Nothing, Just 3])
                    ]
            out = dir </> "int-types.parquet"
        writeParquet out df
        df' <- readParquet out
        assertEqual
            "native int types: column types"
            (columnTypes df)
            (columnTypes df')
        assertEqual "native int types: frame" df df'

columnTypes :: DataFrame -> [(String, String)]
columnTypes df =
    [ (T.unpack name, columnTypeString (fromJust (getColumn name df)))
    | name <- columnNames df
    ]

tinyWriteOpts :: ParquetWriteOptions
tinyWriteOpts =
    defaultParquetWriteOptions
        { pageSize = 64
        , rowGroupSize = 512
        , batchRows = 4
        , subBatchRows = 3
        }

tests :: Test
tests =
    TestList
        [ TestLabel "direct buffer writes" directWrites
        , TestLabel "direct buffer-to-buffer flush" directBufferFlush
        , TestLabel "direct buffer-to-file flush" directFileFlush
        , TestLabel
            "writer roundtrip: alltypes_plain"
            (writerRoundTrip "alltypes_plain" "tests/data/alltypes_plain.parquet")
        , TestLabel
            "writer roundtrip: alltypes_plain.snappy"
            ( writerRoundTrip
                "alltypes_plain.snappy"
                "tests/data/alltypes_plain.snappy.parquet"
            )
        , TestLabel
            "writer roundtrip: alltypes_dictionary"
            (writerRoundTrip "alltypes_dictionary" "tests/data/alltypes_dictionary.parquet")
        , TestLabel
            "writer roundtrip: alltypes_tiny_pages"
            (writerRoundTrip "alltypes_tiny_pages" "tests/data/alltypes_tiny_pages.parquet")
        , TestLabel
            "writer roundtrip: transactions"
            (writerRoundTrip "transactions" "tests/data/transactions.parquet")
        , TestLabel
            "writer roundtrip: mtcars"
            (writerRoundTrip "mtcars" "tests/data/mtcars.parquet")
        , TestLabel
            "writer roundtrip: int32_decimal"
            (writerRoundTrip "int32_decimal" "tests/data/int32_decimal.parquet")
        , TestLabel
            "writer roundtrip: int64_decimal"
            (writerRoundTrip "int64_decimal" "tests/data/int64_decimal.parquet")
        , TestLabel
            "writer roundtrip: sharded mtcars"
            (writerRoundTripSharded "sharded mtcars" "tests/data/mtcars.parquet" 10 4)
        , TestLabel
            "writer roundtrip: sharded exact multiple"
            (writerRoundTripSharded "sharded exact" "tests/data/mtcars.parquet" 32 1)
        , TestLabel
            "sharded write requires a '*' pattern"
            shardedWriteRequiresPattern
        , TestLabel
            "writer roundtrip: Int/Integer keep their Haskell type"
            writerRoundTripNativeIntTypes
        , TestLabel
            "writer roundtrip: alltypes_plain multi-page"
            ( writerRoundTripTiny
                "alltypes_plain multi-page"
                "tests/data/alltypes_plain.parquet"
            )
        , TestLabel "writer roundtrip: large text" writerRoundTripLargeText
        ]

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
