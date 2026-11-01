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
import Data.Time (UTCTime (UTCTime), fromGregorian, picosecondsToDiffTime)
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
            (D.filterBy (`elem` [6, 7]) (F.int32 "id") allTypes)
            (unsafePerformIO (readParquet "./tests/data/alltypes_plain.snappy.parquet"))
        )

allTypesDictionary :: Test
allTypesDictionary = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "allTypesPlainSnappy"
            (D.filterBy (`elem` [0, 1]) (F.int32 "id") allTypes)
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
            (D.fromColumns [("id", D.fromList [6 :: Int32, 7])])
            ( unsafePerformIO
                ( D.readParquetWithOpts
                    ( D.defaultParquetReadOptions
                        { D.selectedColumns = Just ["id"]
                        , D.predicate =
                            Just
                                ( F.geq
                                    (F.int32 "id")
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
            (D.fromColumns [("bool_col", D.fromList [True, False])])
            ( unsafePerformIO
                ( D.readParquetWithOpts
                    ( D.defaultParquetReadOptions
                        { D.selectedColumns = Just ["bool_col"]
                        , D.predicate =
                            Just
                                ( F.geq
                                    (F.int32 "id")
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
                                        (F.int32 "id")
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

-- TODO: Once LZ4/LZ4_RAW are implemented, the lz4 tests become assertEqual.
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

-- The decompressor now handles concatenated gzip members.
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

-- TODO: BROTLI is not implemented; change this to assertEqual once it is.
largeBrotliMap :: Test
largeBrotliMap =
    TestCase
        ( assertExpectException
            "largeBrotliMap"
            "BROTLI"
            (D.readParquet "./tests/data/large_string_map.brotli.parquet")
        )

-- TODO: Once DELTA_BINARY_PACKED is implemented, assert actual dimensions.
deltaBinaryPacked :: Test
deltaBinaryPacked =
    TestCase
        ( assertExpectException
            "deltaBinaryPacked"
            "DELTA_BINARY_PACKED"
            (D.readParquet "./tests/data/delta_binary_packed.parquet")
        )

-- TODO: Once DELTA_BYTE_ARRAY is implemented, assert actual dimensions.
deltaByteArray :: Test
deltaByteArray =
    TestCase
        ( assertExpectException
            "deltaByteArray"
            "DELTA_BYTE_ARRAY"
            (D.readParquet "./tests/data/delta_byte_array.parquet")
        )

-- TODO: Once both DELTA encodings are implemented, assert the row count.
deltaEncodingOptionalColumn :: Test
deltaEncodingOptionalColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingOptionalColumn"
            "unsupported encoding"
            (D.readParquet "./tests/data/delta_encoding_optional_column.parquet")
        )

-- TODO: Once both DELTA encodings are implemented, change to assertEqual.
deltaEncodingRequiredColumn :: Test
deltaEncodingRequiredColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingRequiredColumn"
            "unsupported encoding"
            (D.readParquet "./tests/data/delta_encoding_required_column.parquet")
        )

-- TODO: Once DELTA_LENGTH_BYTE_ARRAY is implemented, assert dimensions.
deltaLengthByteArray :: Test
deltaLengthByteArray =
    TestCase
        ( assertExpectException
            "deltaLengthByteArray"
            "DELTA_LENGTH_BYTE_ARRAY"
            (D.readParquet "./tests/data/delta_length_byte_array.parquet")
        )

-- TODO: Once RLE boolean encoding is implemented, assert the decoded values.
rleBooleanEncoding :: Test
rleBooleanEncoding =
    TestCase
        ( assertExpectException
            "rleBooleanEncoding"
            "unsupported encoding RLE"
            (D.readParquet "./tests/data/rle_boolean_encoding.parquet")
        )

-- TODO: Check whether a lenient page-header parser can read this file.
dictPageOffsetZero :: Test
dictPageOffsetZero =
    TestCase
        ( assertExpectException
            "dictPageOffsetZero"
            "Field 1 is absent"
            (D.readParquet "./tests/data/dict-page-offset-zero.parquet")
        )

-- TODO: Data Page V2 needs RLE booleans and in-line level stream parsing.
datapageV2Snappy :: Test
datapageV2Snappy =
    TestCase
        ( assertExpectException
            "datapageV2Snappy"
            "unsupported encoding RLE"
            (D.readParquet "./tests/data/datapage_v2.snappy.parquet")
        )

-- TODO: Guard empty V2 data pages in readPages so they yield 0 rows.
datapageV2EmptyDatapage :: Test
datapageV2EmptyDatapage =
    TestCase
        ( assertExpectException
            "datapageV2EmptyDatapage"
            "EmptyInput"
            (D.readParquet "./tests/data/datapage_v2_empty_datapage.snappy.parquet")
        )

-- Empty compressed page-v2 data is treated as zero-value pages.
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

-- TODO: CRC checksum validation is not implemented; corrupt ones are ignored.
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

nullableIntDecimal :: Test
nullableIntDecimal = testBothReadParquetPaths $ \readParquet ->
    TestCase
        ( assertEqual
            "nullableIntDecimal"
            ( D.fromColumns
                [ ("int32_dec", D.fromList [Just (1 :: Double), Nothing, Just 2.5, Just 0.75])
                , ("int64_dec", D.fromList [Nothing, Just (1234.5 :: Double), Nothing, Just (-5)])
                ]
            )
            (unsafePerformIO (readParquet "./tests/data/nullable_int_decimal.parquet"))
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

-- TODO: Once DECIMAL is decoded for FIXED_LEN_BYTE_ARRAY, assert values.
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

-- TODO: Add a value-level assertion once DECIMAL decoding is implemented.
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

-- FIXED_LEN_BYTE_ARRAY is read by fixedLenByteArrayDecoder as raw text.
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

-- Unknown page header fields are ignored; this file has 0 data rows.
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

-- Two row groups of 3 rows each: all row groups are read, giving (6, 2).
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

-- The parser uses Int32 page counts and reads all 40,000 rows.
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

-- TODO: Once BYTE_STREAM_SPLIT is implemented, assert actual dimensions.
byteStreamSplitZstd :: Test
byteStreamSplitZstd =
    TestCase
        ( assertExpectException
            "byteStreamSplitZstd"
            "BYTE_STREAM_SPLIT"
            (D.readParquet "./tests/data/byte_stream_split.zstd.parquet")
        )

-- TODO: Change to assertEqual once BYTE_STREAM_SPLIT is supported.
byteStreamSplitExtendedGzip :: Test
byteStreamSplitExtendedGzip =
    TestCase
        ( assertExpectException
            "byteStreamSplitExtendedGzip"
            "BYTE_STREAM_SPLIT"
            (D.readParquet "./tests/data/byte_stream_split_extended.gzip.parquet")
        )

-- TODO: Once float16 decoding is implemented, assert the decoded values.
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

-- TODO: Add a value-level assertion once float16 decoding is implemented.
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

-- TODO: If Pinch decodes unknown union variants, assert a successful parse.
unknownLogicalType :: Test
unknownLogicalType =
    TestCase
        ( assertExpectException
            "unknownLogicalType"
            "Field 16 is absent"
            (D.readParquet "./tests/data/unknown-logical-type.parquet")
        )

-- TODO: If stricter dictionary validation is added, expect a count mismatch.
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

-- | Nanosecond timestamps must decode to the correct UTCTime.
timestampNanos :: Test
timestampNanos = testBothReadParquetPaths $ \readParquet ->
    TestCase $
        assertEqual
            "TIMESTAMP(NANOS) decodes to correct UTCTime"
            ( D.fromColumns
                [
                    ( "ts"
                    , D.fromList
                        [ UTCTime (fromGregorian 2020 1 1) 0
                        , UTCTime (fromGregorian 2021 6 15) (picosecondsToDiffTime 45045123456789000)
                        , UTCTime (fromGregorian 1999 12 31) (picosecondsToDiffTime 86399999999999000)
                        ]
                    )
                ]
            )
            (unsafePerformIO (readParquet "./tests/data/timestamp_nanos.parquet"))

tests :: [Test]
tests =
    [ timestampNanos
    , allTypesPlain
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
    , nullableIntDecimal
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
