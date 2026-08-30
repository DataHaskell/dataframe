-- | Tests for the writer-buffer logic in "DataFrame.IO.Utils.RandomAccess".
module Main where

import qualified Data.ByteString as BS
import qualified System.Exit as Exit
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import Test.HUnit

import DataFrame.IO.Parquet (readParquet)
import DataFrame.IO.Parquet.Writer (
    ParquetWriteOptions (..),
    defaultParquetWriteOptions,
    writeParquet,
    writeParquetWithOptions,
 )
import DataFrame.IO.Utils.RandomAccess

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
            "writer roundtrip: alltypes_plain multi-page"
            ( writerRoundTripTiny
                "alltypes_plain multi-page"
                "tests/data/alltypes_plain.parquet"
            )
        ]

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
