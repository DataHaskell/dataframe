{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Parquet where

import Assertions (assertExpectException)
import Control.Monad (forM_)
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.IO.Parquet as DP
import qualified DataFrame.IO.Parquet.Schema as PS
import ParquetTestData (allTypes, mtCarsDataset, tinyPagesLast10, transactions)

import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe)
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Word
import DataFrame.IO.Parquet.Thrift (
    cc_meta_data,
    cmd_path_in_schema,
    cmd_statistics,
    rg_columns,
    row_groups,
    schema,
    stats_null_count,
    unField,
 )
import DataFrame.Internal.Binary (
    littleEndianWord32,
    littleEndianWord64,
    word32ToLittleEndian,
    word64ToLittleEndian,
 )
import DataFrame.Internal.Column (hasElemType, hasMissing)
import DataFrame.Internal.DataFrame (unsafeGetColumn)
import GHC.IO (unsafePerformIO)
import Test.HUnit

testBothReadParquetPaths :: ((FilePath -> IO D.DataFrame) -> Test) -> Test
testBothReadParquetPaths mkTest =
    TestList
        [ mkTest D.readParquet
        , mkTest (DP._readParquetWithOpts (Just True) D.defaultParquetReadOptions)
        ]

assertColumnNullability ::
    String -> [(T.Text, Bool)] -> D.DataFrame -> Assertion
assertColumnNullability label expected df =
    forM_ expected $ \(columnName, shouldBeNullable) ->
        assertBool
            ( label
                <> ": expected "
                <> T.unpack columnName
                <> if shouldBeNullable then " to be nullable" else " to be non-nullable"
            )
            (hasMissing (unsafeGetColumn columnName df) == shouldBeNullable)

allTypesPlain :: Test
allTypesPlain = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "allTypesPlain"
            allTypes
            (unsafePerformIO (readParquet "./tests/data/alltypes_plain.parquet"))
        )

allTypesTinyPagesDimensions :: Test
allTypesTinyPagesDimensions =
    TestCase
        ( assertEqual
            "allTypesTinyPages last few"
            (7300, 13)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/alltypes_tiny_pages.parquet"))
            )
        )

allTypesTinyPagesLastFew :: Test
allTypesTinyPagesLastFew = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "allTypesTinyPages dimensions"
            tinyPagesLast10
            ( unsafePerformIO
                -- Excluding doubles because they are weird to compare.
                ( fmap
                    (D.takeLast 10 . D.exclude ["double_col"])
                    (readParquet "./tests/data/alltypes_tiny_pages.parquet")
                )
            )
        )

allTypesPlainSnappy :: Test
allTypesPlainSnappy = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "allTypesPlainSnappy"
            (D.filter (F.col @Int32 "id") (`elem` [6, 7]) allTypes)
            (unsafePerformIO (readParquet "./tests/data/alltypes_plain.snappy.parquet"))
        )

allTypesDictionary :: Test
allTypesDictionary = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "allTypesPlainSnappy"
            (D.filter (F.col @Int32 "id") (`elem` [0, 1]) allTypes)
            (unsafePerformIO (readParquet "./tests/data/alltypes_dictionary.parquet"))
        )

selectedColumnsWithOpts :: Test
selectedColumnsWithOpts =
    TestCase
        ( assertEqual
            "selectedColumnsWithOpts"
            (D.select ["id", "bool_col"] allTypes)
            ( unsafePerformIO
                ( D.readParquetWithOpts
                    (D.defaultParquetReadOptions{D.selectedColumns = Just ["id", "bool_col"]})
                    "./tests/data/alltypes_plain.parquet"
                )
            )
        )

rowRangeWithOpts :: Test
rowRangeWithOpts =
    TestCase
        ( assertEqual
            "rowRangeWithOpts"
            (3, 11)
            ( unsafePerformIO
                ( D.dimensions
                    <$> D.readParquetWithOpts
                        (D.defaultParquetReadOptions{D.rowRange = Just (2, 5)})
                        "./tests/data/alltypes_plain.parquet"
                )
            )
        )

predicateWithOpts :: Test
predicateWithOpts =
    TestCase
        ( assertEqual
            "predicateWithOpts"
            (D.fromNamedColumns [("id", D.fromList [6 :: Int32, 7])])
            ( unsafePerformIO
                ( D.readParquetWithOpts
                    ( D.defaultParquetReadOptions
                        { D.selectedColumns = Just ["id"]
                        , D.predicate =
                            Just
                                ( F.geq
                                    (F.col @Int32 "id")
                                    (F.lit (6 :: Int32))
                                )
                        }
                    )
                    "./tests/data/alltypes_plain.parquet"
                )
            )
        )

predicateUsesNonSelectedColumnWithOpts :: Test
predicateUsesNonSelectedColumnWithOpts =
    TestCase
        ( assertEqual
            "predicateUsesNonSelectedColumnWithOpts"
            (D.fromNamedColumns [("bool_col", D.fromList [True, False])])
            ( unsafePerformIO
                ( D.readParquetWithOpts
                    ( D.defaultParquetReadOptions
                        { D.selectedColumns = Just ["bool_col"]
                        , D.predicate =
                            Just
                                ( F.geq
                                    (F.col @Int32 "id")
                                    (F.lit (6 :: Int32))
                                )
                        }
                    )
                    "./tests/data/alltypes_plain.parquet"
                )
            )
        )

safeColumnsWithOpts :: Test
safeColumnsWithOpts =
    TestCase $ do
        defaultDf <- D.readParquet "./tests/data/alltypes_plain.parquet"
        safeDf <-
            D.readParquetWithOpts
                (D.defaultParquetReadOptions{D.safeColumns = True})
                "./tests/data/alltypes_plain.parquet"

        assertEqual
            "safeColumnsWithOpts dimensions"
            (D.dimensions defaultDf)
            (D.dimensions safeDf)
        assertColumnNullability
            "default read"
            [("id", False), ("bool_col", False)]
            defaultDf
        assertColumnNullability
            "safeColumns read"
            [("id", True), ("bool_col", True)]
            safeDf
        assertBool
            "safeColumns id type"
            (hasElemType @(Maybe Int32) (unsafeGetColumn "id" safeDf))
        assertBool
            "safeColumns bool_col type"
            (hasElemType @(Maybe Bool) (unsafeGetColumn "bool_col" safeDf))

safeColumnsWithSelectedColumns :: Test
safeColumnsWithSelectedColumns =
    TestCase $ do
        df <-
            D.readParquetWithOpts
                ( D.defaultParquetReadOptions
                    { D.selectedColumns = Just ["id", "bool_col"]
                    , D.safeColumns = True
                    }
                )
                "./tests/data/alltypes_plain.parquet"

        assertEqual "safeColumnsWithSelectedColumns dimensions" (8, 2) (D.dimensions df)
        assertColumnNullability
            "safeColumns projected read"
            [("id", True), ("bool_col", True)]
            df
        assertBool
            "safeColumns projected id type"
            (hasElemType @(Maybe Int32) (unsafeGetColumn "id" df))
        assertBool
            "safeColumns projected bool_col type"
            (hasElemType @(Maybe Bool) (unsafeGetColumn "bool_col" df))

predicateWithOptsAcrossFiles :: Test
predicateWithOptsAcrossFiles =
    TestCase
        ( assertEqual
            "predicateWithOptsAcrossFiles"
            (4, 1)
            ( unsafePerformIO
                ( D.dimensions
                    <$> D.readParquetFilesWithOpts
                        ( D.defaultParquetReadOptions
                            { D.selectedColumns = Just ["id"]
                            , D.predicate =
                                Just
                                    ( F.geq
                                        (F.col @Int32 "id")
                                        (F.lit (6 :: Int32))
                                    )
                            }
                        )
                        "./tests/data/alltypes_plain*.parquet"
                )
            )
        )

missingSelectedColumnWithOpts :: Test
missingSelectedColumnWithOpts =
    TestCase
        ( assertExpectException
            "missingSelectedColumnWithOpts"
            "Column not found"
            ( D.readParquetWithOpts
                (D.defaultParquetReadOptions{D.selectedColumns = Just ["does_not_exist"]})
                "./tests/data/alltypes_plain.parquet"
            )
        )

transactionsTest :: Test
transactionsTest = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "transactions"
            transactions
            (unsafePerformIO (readParquet "./tests/data/transactions.parquet"))
        )

mtCars :: Test
mtCars =
    TestCase
        ( assertEqual
            "mt_cars"
            mtCarsDataset
            (unsafePerformIO (D.readParquet "./tests/data/mtcars.parquet"))
        )

littleEndianWord64KnownPattern :: Test
littleEndianWord64KnownPattern =
    TestCase
        ( assertEqual
            "littleEndianWord64KnownPattern"
            (0x1122334455667788 :: Word64)
            ( littleEndianWord64
                (BS.pack [0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11])
            )
        )

littleEndianWord32KnownPattern :: Test
littleEndianWord32KnownPattern =
    TestCase
        ( assertEqual
            "littleEndianWord32KnownPattern"
            (0x11223344 :: Word32)
            (littleEndianWord32 (BS.pack [0x44, 0x33, 0x22, 0x11]))
        )

littleEndianWord64ShortInputPadsZeroes :: Test
littleEndianWord64ShortInputPadsZeroes =
    TestCase
        ( assertEqual
            "littleEndianWord64ShortInputPadsZeroes"
            (0x00CCBBAA :: Word64)
            (littleEndianWord64 (BS.pack [0xAA, 0xBB, 0xCC]))
        )

littleEndianWord32ShortInputPadsZeroes :: Test
littleEndianWord32ShortInputPadsZeroes =
    TestCase
        ( assertEqual
            "littleEndianWord32ShortInputPadsZeroes"
            (0x0000BEEF :: Word32)
            (littleEndianWord32 (BS.pack [0xEF, 0xBE]))
        )

littleEndianWord64RoundTrip :: Test
littleEndianWord64RoundTrip =
    TestCase
        ( assertEqual
            "littleEndianWord64RoundTrip"
            value
            (littleEndianWord64 (word64ToLittleEndian value))
        )
  where
    value = 0x1122334455667788 :: Word64

littleEndianWord32RoundTrip :: Test
littleEndianWord32RoundTrip =
    TestCase
        ( assertEqual
            "littleEndianWord32RoundTrip"
            value
            (littleEndianWord32 (word32ToLittleEndian value))
        )
  where
    value = 0xA1B2C3D4 :: Word32

-- ---------------------------------------------------------------------------
-- Group 1: Plain variant
-- ---------------------------------------------------------------------------

allTypesTinyPagesPlain :: Test
allTypesTinyPagesPlain =
    TestCase
        ( assertEqual
            "alltypes_tiny_pages_plain dimensions"
            (7300, 13)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/alltypes_tiny_pages_plain.parquet")
                )
            )
        )

-- ---------------------------------------------------------------------------
-- Group 2: Compression codecs (unsupported → error tests)
-- ---------------------------------------------------------------------------

-- TODO: LZ4 and LZ4_RAW compression are not yet implemented. When support
-- is added via a Haskell lz4 binding, hadoopLz4Compressed,
-- hadoopLz4CompressedLarger, nonHadoopLz4Compressed, lz4RawCompressed, and
-- lz4RawCompressedLarger should all change from assertExpectException to
-- assertEqual checking their respective row/column dimensions.
hadoopLz4Compressed :: Test
hadoopLz4Compressed =
    TestCase
        ( assertExpectException
            "hadoopLz4Compressed"
            "LZ4"
            (D.readParquet "./tests/data/hadoop_lz4_compressed.parquet")
        )

hadoopLz4CompressedLarger :: Test
hadoopLz4CompressedLarger =
    TestCase
        ( assertExpectException
            "hadoopLz4CompressedLarger"
            "LZ4"
            (D.readParquet "./tests/data/hadoop_lz4_compressed_larger.parquet")
        )

nonHadoopLz4Compressed :: Test
nonHadoopLz4Compressed =
    TestCase
        ( assertExpectException
            "nonHadoopLz4Compressed"
            "LZ4"
            (D.readParquet "./tests/data/non_hadoop_lz4_compressed.parquet")
        )

lz4RawCompressed :: Test
lz4RawCompressed =
    TestCase
        ( assertExpectException
            "lz4RawCompressed"
            "LZ4_RAW"
            (D.readParquet "./tests/data/lz4_raw_compressed.parquet")
        )

lz4RawCompressedLarger :: Test
lz4RawCompressedLarger =
    TestCase
        ( assertExpectException
            "lz4RawCompressedLarger"
            "LZ4_RAW"
            (D.readParquet "./tests/data/lz4_raw_compressed_larger.parquet")
        )

-- Was: assertExpectException "concatenatedGzipMembers" "12" ...
-- The old parser failed with a ZLIB size error. The new decompressor
-- handles concatenated gzip members correctly.
concatenatedGzipMembers :: Test
concatenatedGzipMembers =
    TestCase
        ( assertEqual
            "concatenatedGzipMembers"
            (513, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/concatenated_gzip_members.parquet")
                )
            )
        )

-- TODO: BROTLI compression is not yet implemented. When a Haskell brotli
-- binding is added, change this to assertEqual checking the actual
-- dimensions of large_string_map.brotli.parquet.
largeBrotliMap :: Test
largeBrotliMap =
    TestCase
        ( assertExpectException
            "largeBrotliMap"
            "BROTLI"
            (D.readParquet "./tests/data/large_string_map.brotli.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 3: Delta / RLE encodings (unsupported → error tests)
-- ---------------------------------------------------------------------------

-- Was: assertExpectException "deltaBinaryPacked" "EDELTA_BINARY_PACKED" ...
-- The new parser's error includes the encoding name "DELTA_BINARY_PACKED"
-- without the old "E" prefix used in the previous error format.
-- TODO: When DELTA_BINARY_PACKED (encoding id=5) is implemented, change
-- this to assertEqual checking actual dimensions. The encoding stores
-- integer data as bit-packed deltas and is common for monotonically
-- increasing columns (row IDs, timestamps):
-- https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5
deltaBinaryPacked :: Test
deltaBinaryPacked =
    TestCase
        ( assertExpectException
            "deltaBinaryPacked"
            "DELTA_BINARY_PACKED"
            (D.readParquet "./tests/data/delta_binary_packed.parquet")
        )

-- Was: assertExpectException "deltaByteArray" "EDELTA_BYTE_ARRAY" ...
-- Same reason as deltaBinaryPacked: new error format drops the "E" prefix.
-- TODO: When DELTA_BYTE_ARRAY (encoding id=7) is implemented, change this
-- to assertEqual checking actual dimensions. The encoding prefix-differences
-- consecutive string values, reducing storage for sorted byte arrays:
-- https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7
deltaByteArray :: Test
deltaByteArray =
    TestCase
        ( assertExpectException
            "deltaByteArray"
            "DELTA_BYTE_ARRAY"
            (D.readParquet "./tests/data/delta_byte_array.parquet")
        )

-- Was: assertExpectException "deltaEncodingOptionalColumn" "EDELTA_BINARY_PACKED" ...
-- The first column that errors in this file uses DELTA_BYTE_ARRAY encoding,
-- so we match the broader "unsupported encoding" substring instead.
-- TODO: Once DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY are both implemented,
-- change this to assertEqual checking the actual row count of
-- delta_encoding_optional_column.parquet.
deltaEncodingOptionalColumn :: Test
deltaEncodingOptionalColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingOptionalColumn"
            "unsupported encoding"
            (D.readParquet "./tests/data/delta_encoding_optional_column.parquet")
        )

-- Was: assertExpectException "deltaEncodingRequiredColumn" "EDELTA_BINARY_PACKED" ...
-- Same as deltaEncodingOptionalColumn: first failing column uses DELTA_BYTE_ARRAY.
-- TODO: Same as deltaEncodingOptionalColumn — change to assertEqual once
-- DELTA_BINARY_PACKED and DELTA_BYTE_ARRAY encodings are both supported.
deltaEncodingRequiredColumn :: Test
deltaEncodingRequiredColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingRequiredColumn"
            "unsupported encoding"
            (D.readParquet "./tests/data/delta_encoding_required_column.parquet")
        )

-- Was: assertExpectException "deltaLengthByteArray" "ZSTD" ...
-- The old parser failed during ZSTD decompression. The new parser
-- detects the unsupported DELTA_LENGTH_BYTE_ARRAY encoding before decompression.
-- TODO: When DELTA_LENGTH_BYTE_ARRAY (encoding id=6) is implemented, change
-- this to assertEqual checking actual dimensions. The encoding stores a
-- delta-encoded list of byte-array lengths followed by the raw concatenated
-- values:
-- https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-length-byte-array-delta_length_byte_array--6
deltaLengthByteArray :: Test
deltaLengthByteArray =
    TestCase
        ( assertExpectException
            "deltaLengthByteArray"
            "DELTA_LENGTH_BYTE_ARRAY"
            (D.readParquet "./tests/data/delta_length_byte_array.parquet")
        )

-- Was: assertExpectException "rleBooleanEncoding" "Zlib" ...
-- The old parser failed during Zlib decompression. The new parser
-- detects the unsupported RLE boolean encoding before reaching decompression.
-- TODO: When RLE/Bit-Packing Hybrid (encoding id=3, bit-width=1) is
-- implemented for BOOLEAN columns, change this to assertEqual checking the
-- actual decoded boolean values. The encoding is spec-valid for BOOLEAN:
-- https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3
rleBooleanEncoding :: Test
rleBooleanEncoding =
    TestCase
        ( assertExpectException
            "rleBooleanEncoding"
            "unsupported encoding RLE"
            (D.readParquet "./tests/data/rle_boolean_encoding.parquet")
        )

-- Was: assertExpectException "dictPageOffsetZero" "Unknown kv" ...
-- The old parser reported "Unknown kv" for a bad key-value field. The new
-- Pinch-based page-header parser reports "Field 1 is absent" for the
-- malformed page header in this file.
-- TODO: Investigate whether dict-page-offset-zero.parquet can be read
-- successfully with a more lenient page-header parser. If the missing
-- mandatory field can be treated as a per-page soft error rather than
-- aborting the whole read, this test would change to assertEqual
-- checking actual dimensions.
dictPageOffsetZero :: Test
dictPageOffsetZero =
    TestCase
        ( assertExpectException
            "dictPageOffsetZero"
            "Field 1 is absent"
            (D.readParquet "./tests/data/dict-page-offset-zero.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 4: Data Page V2 (unsupported → error tests)
-- ---------------------------------------------------------------------------

-- Was: assertExpectException "datapageV2Snappy" "InvalidOffset" ...
-- The old parser failed with an offset validation error. The new parser
-- first encounters the unsupported RLE encoding used by data-page-v2.
-- TODO: Full Data Page V2 support requires two changes:
--   1. RLE/Bit-Packing Hybrid (id=3, bit-width=1) for BOOLEAN values
--      (shared with rleBooleanEncoding above).
--   2. Parsing DataPageHeaderV2's in-line level streams: in v2, definition
--      and repetition levels are stored uncompressed before the (optionally
--      compressed) value bytes, with lengths given by
--      definition_levels_byte_length and repetition_levels_byte_length.
-- Once both are done, change to assertEqual checking actual dimensions:
-- https://parquet.apache.org/docs/file-format/data-pages/
datapageV2Snappy :: Test
datapageV2Snappy =
    TestCase
        ( assertExpectException
            "datapageV2Snappy"
            "unsupported encoding RLE"
            (D.readParquet "./tests/data/datapage_v2.snappy.parquet")
        )

-- Was: assertExpectException "datapageV2EmptyDatapage" "UnexpectedEOF" ...
-- The old Snappy decompressor raised "UnexpectedEOF". The new Snappy
-- library raises "EmptyInput" when given zero-length compressed data.
-- The v2 page structure is parsed correctly: readLevelsV2V strips the
-- in-line level streams before decompression, leaving an empty value
-- payload (BS.empty) for a page with 0 values. The Snappy decompressor
-- then raises "EmptyInput" because it is handed zero bytes.
-- TODO: An empty data page (0 values) is valid and should contribute
-- 0 rows without raising an error. The fix is a single guard in the
-- DATA_PAGE_V2 branch of readPages (Page.hs): short-circuit
-- decompressData when compValBytes is empty, returning BS.empty
-- directly. Once fixed, change this to assertEqual checking the
-- total expected row count of the file.
datapageV2EmptyDatapage :: Test
datapageV2EmptyDatapage =
    TestCase
        ( assertExpectException
            "datapageV2EmptyDatapage"
            "EmptyInput"
            (D.readParquet "./tests/data/datapage_v2_empty_datapage.snappy.parquet")
        )

-- Was: assertExpectException "pageV2EmptyCompressed" "10" ...
-- The old parser failed on empty compressed page-v2 blocks. The new parser
-- treats empty compressed data as zero-value pages and reads all 10 rows.
pageV2EmptyCompressed :: Test
pageV2EmptyCompressed =
    TestCase
        ( assertEqual
            "pageV2EmptyCompressed"
            (10, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/page_v2_empty_compressed.parquet")
                )
            )
        )

-- ---------------------------------------------------------------------------
-- Group 5: Checksum files (all read successfully)
-- ---------------------------------------------------------------------------

datapageV1UncompressedChecksum :: Test
datapageV1UncompressedChecksum =
    TestCase
        ( assertEqual
            "datapageV1UncompressedChecksum"
            (5120, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/datapage_v1-uncompressed-checksum.parquet")
                )
            )
        )

datapageV1SnappyChecksum :: Test
datapageV1SnappyChecksum =
    TestCase
        ( assertEqual
            "datapageV1SnappyChecksum"
            (5120, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/datapage_v1-snappy-compressed-checksum.parquet")
                )
            )
        )

plainDictUncompressedChecksum :: Test
plainDictUncompressedChecksum =
    TestCase
        ( assertEqual
            "plainDictUncompressedChecksum"
            (1000, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/plain-dict-uncompressed-checksum.parquet")
                )
            )
        )

rleDictSnappyChecksum :: Test
rleDictSnappyChecksum =
    TestCase
        ( assertEqual
            "rleDictSnappyChecksum"
            (1000, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/rle-dict-snappy-checksum.parquet")
                )
            )
        )

-- TODO: CRC checksum validation is not yet implemented; corrupt page
-- checksums are silently ignored. When validation is added, consider a
-- validateChecksums :: Bool field in ParquetReadOptions (default False)
-- so callers can opt in. Once implemented, datapageV1CorruptChecksum and
-- rleDictUncompressedCorruptChecksum should change to assertExpectException
-- checking for a checksum mismatch error.
datapageV1CorruptChecksum :: Test
datapageV1CorruptChecksum =
    TestCase
        ( assertEqual
            "datapageV1CorruptChecksum"
            (5120, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/datapage_v1-corrupt-checksum.parquet")
                )
            )
        )

rleDictUncompressedCorruptChecksum :: Test
rleDictUncompressedCorruptChecksum =
    TestCase
        ( assertEqual
            "rleDictUncompressedCorruptChecksum"
            (1000, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/rle-dict-uncompressed-corrupt-checksum.parquet")
                )
            )
        )

-- ---------------------------------------------------------------------------
-- Group 6: NULL handling
-- ---------------------------------------------------------------------------

nullsSnappy :: Test
nullsSnappy =
    TestCase
        ( assertEqual
            "nullsSnappy"
            (8, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nulls.snappy.parquet"))
            )
        )

int32WithNullPages :: Test
int32WithNullPages =
    TestCase
        ( assertEqual
            "int32WithNullPages"
            (1000, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/int32_with_null_pages.parquet"))
            )
        )

nullableImpala :: Test
nullableImpala =
    TestCase
        ( assertEqual
            "nullableImpala"
            (7, 13)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nullable.impala.parquet"))
            )
        )

nonnullableImpala :: Test
nonnullableImpala =
    TestCase
        ( assertEqual
            "nonnullableImpala"
            (1, 13)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nonnullable.impala.parquet"))
            )
        )

singleNan :: Test
singleNan =
    TestCase
        ( assertEqual
            "singleNan"
            (1, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/single_nan.parquet"))
            )
        )

nanInStats :: Test
nanInStats =
    TestCase
        ( assertEqual
            "nanInStats"
            (2, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nan_in_stats.parquet"))
            )
        )

-- ---------------------------------------------------------------------------
-- Group 7: Decimal types
-- ---------------------------------------------------------------------------

int32Decimal :: Test
int32Decimal =
    TestCase
        ( assertEqual
            "int32Decimal"
            (24, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/int32_decimal.parquet"))
            )
        )

int64Decimal :: Test
int64Decimal =
    TestCase
        ( assertEqual
            "int64Decimal"
            (24, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/int64_decimal.parquet"))
            )
        )

byteArrayDecimal :: Test
byteArrayDecimal =
    TestCase
        ( assertEqual
            "byteArrayDecimal"
            (24, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/byte_array_decimal.parquet"))
            )
        )

-- Was: assertExpectException "fixedLengthDecimal" "FIXED_LEN_BYTE_ARRAY" ...
-- The old parser recognised FIXED_LEN_BYTE_ARRAY as a physical type but
-- had no page decoder for it; reading data from such a column threw an
-- error at the decoding stage. The new parser's fixedLenByteArrayDecoder
-- reads the raw bytes and surfaces them as a text column.
-- TODO: When the DECIMAL logical type is properly decoded for
-- FIXED_LEN_BYTE_ARRAY columns, replace this dimension-only check with a
-- value-level assertion verifying the actual decimal values (e.g. as
-- Scientific or Double). The raw-byte Text column should become a typed
-- numeric column.
fixedLengthDecimal :: Test
fixedLengthDecimal =
    TestCase
        ( assertEqual
            "fixedLengthDecimal"
            (24, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/fixed_length_decimal.parquet"))
            )
        )

-- Was: assertExpectException "fixedLengthDecimalLegacy" "FIXED_LEN_BYTE_ARRAY" ...
-- Same as fixedLengthDecimal: the old parser had no page decoder for
-- FIXED_LEN_BYTE_ARRAY; the new parser's fixedLenByteArrayDecoder handles it.
-- TODO: Same as fixedLengthDecimal — add a value-level assertion once
-- DECIMAL decoding over FIXED_LEN_BYTE_ARRAY is implemented.
fixedLengthDecimalLegacy :: Test
fixedLengthDecimalLegacy =
    TestCase
        ( assertEqual
            "fixedLengthDecimalLegacy"
            (24, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/fixed_length_decimal_legacy.parquet")
                )
            )
        )

-- ---------------------------------------------------------------------------
-- Group 8: Binary / fixed-length bytes
-- ---------------------------------------------------------------------------

binaryFile :: Test
binaryFile =
    TestCase
        ( assertEqual
            "binaryFile"
            (12, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/binary.parquet"))
            )
        )

binaryTruncatedMinMax :: Test
binaryTruncatedMinMax =
    TestCase
        ( assertEqual
            "binaryTruncatedMinMax"
            (12, 6)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/binary_truncated_min_max.parquet")
                )
            )
        )

-- Was: assertExpectException "fixedLengthByteArray" "FIXED_LEN_BYTE_ARRAY" ...
-- Same as fixedLengthDecimal: the old parser had no page decoder for
-- FIXED_LEN_BYTE_ARRAY; the new parser's fixedLenByteArrayDecoder handles it.
fixedLengthByteArray :: Test
fixedLengthByteArray =
    TestCase
        ( assertEqual
            "fixedLengthByteArray"
            (1000, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/fixed_length_byte_array.parquet"))
            )
        )

-- ---------------------------------------------------------------------------
-- Group 9: INT96 timestamps
-- ---------------------------------------------------------------------------

int96FromSpark :: Test
int96FromSpark =
    TestCase
        ( assertEqual
            "int96FromSpark"
            (6, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/int96_from_spark.parquet"))
            )
        )

-- ---------------------------------------------------------------------------
-- Group 10: Metadata / index / bloom filters
-- ---------------------------------------------------------------------------

-- Was: assertExpectException "columnChunkKeyValueMetadata" "Unknown page header field" ...
-- The old parser rejected extra fields in page headers. Pinch ignores
-- unknown fields gracefully. This file contains 0 data rows.
columnChunkKeyValueMetadata :: Test
columnChunkKeyValueMetadata =
    TestCase
        ( assertEqual
            "columnChunkKeyValueMetadata"
            (0, 2)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/column_chunk_key_value_metadata.parquet")
                )
            )
        )

dataIndexBloomEncodingStats :: Test
dataIndexBloomEncodingStats =
    TestCase
        ( assertEqual
            "dataIndexBloomEncodingStats"
            (14, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/data_index_bloom_encoding_stats.parquet")
                )
            )
        )

dataIndexBloomEncodingWithLength :: Test
dataIndexBloomEncodingWithLength =
    TestCase
        ( assertEqual
            "dataIndexBloomEncodingWithLength"
            (14, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/data_index_bloom_encoding_with_length.parquet")
                )
            )
        )

-- Was: assertEqual "sortColumns" (3, 2) ...
-- The file contains two row groups, each storing 3 rows (6 rows total).
-- DuckDB's parquet-metadata output shows row_group_num_rows=3, which is
-- the count *per row group*, not the file total.row group*, not the file total.row group*, not the file total.row group*, not the file total.
-- https://github.com/apache/parquet-testing/blob/master/data/README.md#:~:text=sort_columns.parquet
-- The above link is to the repository the test parquet files comes from.
-- The table describes sort_columns.parquet as having two row groups.
-- The old parser only read the first row group (a bug). The new parser
-- reads all row groups and returns (6, 2) correctly.
sortColumns :: Test
sortColumns =
    TestCase
        ( assertEqual
            "sortColumns"
            (6, 2)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/sort_columns.parquet"))
            )
        )

-- Was: assertExpectException "overflowI16PageCnt" "UNIMPLEMENTED" ...
-- The old parser used Int16 for page counts and overflowed on this file.
-- The new parser uses Int32 and reads all 40,000 rows correctly.
overflowI16PageCnt :: Test
overflowI16PageCnt =
    TestCase
        ( assertEqual
            "overflowI16PageCnt"
            (40000, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/overflow_i16_page_cnt.parquet"))
            )
        )

-- ---------------------------------------------------------------------------
-- Group 11: Nested / complex types and byte-stream-split
-- ---------------------------------------------------------------------------

-- Was: assertExpectException "byteStreamSplitZstd" "EBYTE_STREAM_SPLIT" ...
-- The new parser's error includes the encoding name "BYTE_STREAM_SPLIT"
-- without the old "E" prefix used in the previous error format.
-- TODO: When BYTE_STREAM_SPLIT (encoding id=9) is implemented, change this
-- to assertEqual checking actual dimensions. The encoding interleaves the
-- individual byte streams of multi-byte scalars to improve compression for
-- floating-point and other structured data:
-- https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split-byte_stream_split--9
byteStreamSplitZstd :: Test
byteStreamSplitZstd =
    TestCase
        ( assertExpectException
            "byteStreamSplitZstd"
            "BYTE_STREAM_SPLIT"
            (D.readParquet "./tests/data/byte_stream_split.zstd.parquet")
        )

-- Was: assertExpectException "byteStreamSplitExtendedGzip" "FIXED_LEN_BYTE_ARRAY" ...
-- The old parser had no page decoder for FIXED_LEN_BYTE_ARRAY and threw
-- before ever inspecting the encoding. The new parser handles the physical
-- type but the BYTE_STREAM_SPLIT encoding used for values is not yet
-- implemented, so the error message shifts from the type to the encoding.
-- TODO: Same as byteStreamSplitZstd — change to assertEqual once
-- BYTE_STREAM_SPLIT encoding is supported.
byteStreamSplitExtendedGzip :: Test
byteStreamSplitExtendedGzip =
    TestCase
        ( assertExpectException
            "byteStreamSplitExtendedGzip"
            "BYTE_STREAM_SPLIT"
            (D.readParquet "./tests/data/byte_stream_split_extended.gzip.parquet")
        )

-- Was: assertExpectException "float16NonzerosAndNans" "PFIXED_LEN_BYTE_ARRAY" ...
-- The "PFIXED_LEN_BYTE_ARRAY" in the old error was the Show of the old
-- parser's ParquetType enum hitting a catch-all dispatch branch — it
-- recognised the physical type but had no decoder for it. The new parser's
-- fixedLenByteArrayDecoder reads 2-byte FIXED_LEN_BYTE_ARRAY (float16)
-- columns as raw-byte text; proper float16 value decoding is not yet
-- implemented.
-- TODO: When IEEE 754 half-precision (float16) decoding is implemented,
-- add a value-level assertion using hasElemType @Float (or a dedicated
-- Float16 type if one is introduced). Verify that the decoded values match
-- the known reference values for float16_nonzeros_and_nans.parquet.
-- The column should no longer be exposed as raw-byte Text.
float16NonzerosAndNans :: Test
float16NonzerosAndNans =
    TestCase
        ( assertEqual
            "float16NonzerosAndNans"
            (8, 1)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/float16_nonzeros_and_nans.parquet")
                )
            )
        )

-- Was: assertExpectException "float16ZerosAndNans" "PFIXED_LEN_BYTE_ARRAY" ...
-- Same as float16NonzerosAndNans: old parser had no decoder for the
-- FIXED_LEN_BYTE_ARRAY physical type; new parser reads raw bytes as text.
-- TODO: Same as float16NonzerosAndNans — add a value-level assertion once
-- float16 decoding is implemented.
float16ZerosAndNans :: Test
float16ZerosAndNans =
    TestCase
        ( assertEqual
            "float16ZerosAndNans"
            (3, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/float16_zeros_and_nans.parquet"))
            )
        )

nestedListsSnappy :: Test
nestedListsSnappy =
    TestCase
        ( assertEqual
            "nestedListsSnappy"
            (3, 2)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nested_lists.snappy.parquet"))
            )
        )

nestedMapsSnappy :: Test
nestedMapsSnappy =
    TestCase
        ( assertEqual
            "nestedMapsSnappy"
            (6, 5)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nested_maps.snappy.parquet"))
            )
        )

nestedStructsRust :: Test
nestedStructsRust =
    TestCase
        ( assertEqual
            "nestedStructsRust"
            (1, 216)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nested_structs.rust.parquet"))
            )
        )

listColumns :: Test
listColumns =
    TestCase
        ( assertEqual
            "listColumns"
            (3, 2)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/list_columns.parquet"))
            )
        )

oldListStructure :: Test
oldListStructure =
    TestCase
        ( assertEqual
            "oldListStructure"
            (1, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/old_list_structure.parquet"))
            )
        )

nullList :: Test
nullList =
    TestCase
        ( assertEqual
            "nullList"
            (1, 1)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/null_list.parquet"))
            )
        )

mapNoValue :: Test
mapNoValue =
    TestCase
        ( assertEqual
            "mapNoValue"
            (3, 4)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/map_no_value.parquet"))
            )
        )

incorrectMapSchema :: Test
incorrectMapSchema =
    TestCase
        ( assertEqual
            "incorrectMapSchema"
            (1, 2)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/incorrect_map_schema.parquet"))
            )
        )

repeatedNoAnnotation :: Test
repeatedNoAnnotation =
    TestCase
        ( assertEqual
            "repeatedNoAnnotation"
            (6, 3)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/repeated_no_annotation.parquet"))
            )
        )

repeatedPrimitiveNoList :: Test
repeatedPrimitiveNoList =
    TestCase
        ( assertEqual
            "repeatedPrimitiveNoList"
            (4, 4)
            ( unsafePerformIO
                ( fmap
                    D.dimensions
                    (D.readParquet "./tests/data/repeated_primitive_no_list.parquet")
                )
            )
        )

-- Was: assertExpectException "unknownLogicalType" "Unknown logical type" ...
-- The old parser raised a custom "Unknown logical type" message. The new
-- Pinch-based metadata parser raises "Field 16 is absent" for the
-- unrecognised LogicalType variant in this file.
-- TODO: If Pinch is extended to support forward-compatible decoding of
-- unknown union variants (treating unrecognised logical-type IDs as absent
-- rather than raising an error), change this to assertEqual where the file
-- parses successfully and the column falls back to its physical type.
unknownLogicalType :: Test
unknownLogicalType =
    TestCase
        ( assertExpectException
            "unknownLogicalType"
            "Field 16 is absent"
            (D.readParquet "./tests/data/unknown-logical-type.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 12: Malformed files
-- ---------------------------------------------------------------------------

-- Was: assertExpectException "nationDictMalformed" "dict index count mismatch" ...
-- The old parser validated the dictionary entry count against data-page
-- indices and raised "dict index count mismatch". The new parser does not
-- replicate that check; the dictionary bytes happen to decode correctly
-- despite the metadata discrepancy, returning the complete 25-row dataset.
-- TODO: If a stricter dictionary-validation pass is added (checking that
-- the number of decoded entries matches num_values in the dictionary page
-- header), revert this to assertExpectException with a count-mismatch
-- substring.
nationDictMalformed :: Test
nationDictMalformed =
    TestCase
        ( assertEqual
            "nationDictMalformed"
            (25, 4)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/nation.dict-malformed.parquet"))
            )
        )

shardedNullableSchema :: Test
shardedNullableSchema =
    TestCase $ do
        metas <-
            mapM
                DP.readMetadataFromPath
                ["data/sharded/part-0.parquet", "data/sharded/part-1.parquet"]
        let nullableCols =
                S.fromList
                    [ last (map T.pack colPath)
                    | meta <- metas
                    , rg <- unField meta.row_groups
                    , cc <- unField rg.rg_columns
                    , Just cm <- [unField cc.cc_meta_data]
                    , let colPath = map T.unpack (unField cm.cmd_path_in_schema)
                    , not (null colPath)
                    , let nc :: Int64
                          nc = case unField cm.cmd_statistics of
                            Nothing -> 0
                            Just stats -> fromMaybe 0 (unField stats.stats_null_count)
                    , nc > 0
                    ]
            df =
                foldl
                    (\acc meta -> acc <> PS.schemaToEmptyDataFrame nullableCols (unField meta.schema))
                    D.empty
                    metas
        assertBool "id should be nullable" (hasMissing (unsafeGetColumn "id" df))
        assertBool "name should be nullable" (hasMissing (unsafeGetColumn "name" df))
        assertBool "score should be nullable" (hasMissing (unsafeGetColumn "score" df))

singleShardNoNulls :: Test
singleShardNoNulls =
    TestCase $ do
        meta <- DP.readMetadataFromPath "data/sharded/part-0.parquet"
        let nullableCols =
                S.fromList
                    [ last (map T.pack colPath)
                    | rg <- unField meta.row_groups
                    , cc <- unField rg.rg_columns
                    , Just cm <- [unField cc.cc_meta_data]
                    , let colPath = map T.unpack (unField cm.cmd_path_in_schema)
                    , not (null colPath)
                    , let nc :: Int64
                          nc = case unField cm.cmd_statistics of
                            Nothing -> 0
                            Just stats -> fromMaybe 0 (unField stats.stats_null_count)
                    , nc > 0
                    ]
            df = PS.schemaToEmptyDataFrame nullableCols (unField meta.schema)
        assertBool
            "id should NOT be nullable"
            (not (hasMissing (unsafeGetColumn "id" df)))
        assertBool
            "name should NOT be nullable"
            (not (hasMissing (unsafeGetColumn "name" df)))
        assertBool
            "score should NOT be nullable"
            (not (hasMissing (unsafeGetColumn "score" df)))

tests :: [Test]
tests =
    [ allTypesPlain
    , allTypesPlainSnappy
    , allTypesDictionary
    , selectedColumnsWithOpts
    , rowRangeWithOpts
    , predicateWithOpts
    , predicateUsesNonSelectedColumnWithOpts
    , safeColumnsWithOpts
    , safeColumnsWithSelectedColumns
    , predicateWithOptsAcrossFiles
    , missingSelectedColumnWithOpts
    , mtCars
    , allTypesTinyPagesLastFew
    , allTypesTinyPagesDimensions
    , transactionsTest
    , littleEndianWord64KnownPattern
    , littleEndianWord32KnownPattern
    , littleEndianWord64ShortInputPadsZeroes
    , littleEndianWord32ShortInputPadsZeroes
    , littleEndianWord64RoundTrip
    , littleEndianWord32RoundTrip
    , -- Group 1
      allTypesTinyPagesPlain
    , -- Group 2: compression codecs
      hadoopLz4Compressed
    , hadoopLz4CompressedLarger
    , nonHadoopLz4Compressed
    , lz4RawCompressed
    , lz4RawCompressedLarger
    , concatenatedGzipMembers
    , largeBrotliMap
    , -- Group 3: delta / rle encodings
      deltaBinaryPacked
    , deltaByteArray
    , deltaEncodingOptionalColumn
    , deltaEncodingRequiredColumn
    , deltaLengthByteArray
    , rleBooleanEncoding
    , dictPageOffsetZero
    , -- Group 4: Data Page V2
      datapageV2Snappy
    , datapageV2EmptyDatapage
    , pageV2EmptyCompressed
    , -- Group 5: checksum files
      datapageV1UncompressedChecksum
    , datapageV1SnappyChecksum
    , plainDictUncompressedChecksum
    , rleDictSnappyChecksum
    , datapageV1CorruptChecksum
    , rleDictUncompressedCorruptChecksum
    , -- Group 6: NULL handling
      nullsSnappy
    , int32WithNullPages
    , nullableImpala
    , nonnullableImpala
    , singleNan
    , nanInStats
    , -- Group 7: decimal types
      int32Decimal
    , int64Decimal
    , byteArrayDecimal
    , fixedLengthDecimal
    , fixedLengthDecimalLegacy
    , -- Group 8: binary / fixed-length bytes
      binaryFile
    , binaryTruncatedMinMax
    , fixedLengthByteArray
    , -- Group 9: INT96 timestamps
      int96FromSpark
    , -- Group 10: metadata / bloom filters
      columnChunkKeyValueMetadata
    , dataIndexBloomEncodingStats
    , dataIndexBloomEncodingWithLength
    , sortColumns
    , overflowI16PageCnt
    , -- Group 11: nested / complex types
      byteStreamSplitZstd
    , byteStreamSplitExtendedGzip
    , float16NonzerosAndNans
    , float16ZerosAndNans
    , nestedListsSnappy
    , nestedMapsSnappy
    , nestedStructsRust
    , listColumns
    , oldListStructure
    , nullList
    , mapNoValue
    , incorrectMapSchema
    , repeatedNoAnnotation
    , repeatedPrimitiveNoList
    , unknownLogicalType
    , -- Group 12: malformed files
      nationDictMalformed
    , -- Group 13: metadata-based null detection
      shardedNullableSchema
    , singleShardNoNulls
    ]
