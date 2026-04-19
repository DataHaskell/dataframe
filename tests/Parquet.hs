{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Parquet where

import Assertions (assertExpectException)
import Control.Monad (forM_)
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.IO.Parquet as DP
import ParquetTestData (allTypes, mtCarsDataset, tinyPagesLast10, transactions)

import qualified Data.ByteString as BS
import Data.Int
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Word
import DataFrame.IO.Parquet.Thrift (
    columnMetaData,
    columnPathInSchema,
    columnStatistics,
    rowGroupColumns,
    rowGroups,
    schema,
 )
import DataFrame.IO.Parquet.Types (columnNullCount)
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

concatenatedGzipMembers :: Test
concatenatedGzipMembers =
    TestCase
        ( assertExpectException
            "concatenatedGzipMembers"
            "12"
            (D.readParquet "./tests/data/concatenated_gzip_members.parquet")
        )

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

deltaBinaryPacked :: Test
deltaBinaryPacked =
    TestCase
        ( assertExpectException
            "deltaBinaryPacked"
            "EDELTA_BINARY_PACKED"
            (D.readParquet "./tests/data/delta_binary_packed.parquet")
        )

deltaByteArray :: Test
deltaByteArray =
    TestCase
        ( assertExpectException
            "deltaByteArray"
            "EDELTA_BYTE_ARRAY"
            (D.readParquet "./tests/data/delta_byte_array.parquet")
        )

deltaEncodingOptionalColumn :: Test
deltaEncodingOptionalColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingOptionalColumn"
            "EDELTA_BINARY_PACKED"
            (D.readParquet "./tests/data/delta_encoding_optional_column.parquet")
        )

deltaEncodingRequiredColumn :: Test
deltaEncodingRequiredColumn =
    TestCase
        ( assertExpectException
            "deltaEncodingRequiredColumn"
            "EDELTA_BINARY_PACKED"
            (D.readParquet "./tests/data/delta_encoding_required_column.parquet")
        )

deltaLengthByteArray :: Test
deltaLengthByteArray =
    TestCase
        ( assertExpectException
            "deltaLengthByteArray"
            "ZSTD"
            (D.readParquet "./tests/data/delta_length_byte_array.parquet")
        )

rleBooleanEncoding :: Test
rleBooleanEncoding =
    TestCase
        ( assertExpectException
            "rleBooleanEncoding"
            "Zlib"
            (D.readParquet "./tests/data/rle_boolean_encoding.parquet")
        )

dictPageOffsetZero :: Test
dictPageOffsetZero =
    TestCase
        ( assertExpectException
            "dictPageOffsetZero"
            "Unknown kv"
            (D.readParquet "./tests/data/dict-page-offset-zero.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 4: Data Page V2 (unsupported → error tests)
-- ---------------------------------------------------------------------------

datapageV2Snappy :: Test
datapageV2Snappy =
    TestCase
        ( assertExpectException
            "datapageV2Snappy"
            "InvalidOffset"
            (D.readParquet "./tests/data/datapage_v2.snappy.parquet")
        )

datapageV2EmptyDatapage :: Test
datapageV2EmptyDatapage =
    TestCase
        ( assertExpectException
            "datapageV2EmptyDatapage"
            "UnexpectedEOF"
            (D.readParquet "./tests/data/datapage_v2_empty_datapage.snappy.parquet")
        )

pageV2EmptyCompressed :: Test
pageV2EmptyCompressed =
    TestCase
        ( assertExpectException
            "pageV2EmptyCompressed"
            "10"
            (D.readParquet "./tests/data/page_v2_empty_compressed.parquet")
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

fixedLengthDecimal :: Test
fixedLengthDecimal =
    TestCase
        ( assertExpectException
            "fixedLengthDecimal"
            "FIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/fixed_length_decimal.parquet")
        )

fixedLengthDecimalLegacy :: Test
fixedLengthDecimalLegacy =
    TestCase
        ( assertExpectException
            "fixedLengthDecimalLegacy"
            "FIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/fixed_length_decimal_legacy.parquet")
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

fixedLengthByteArray :: Test
fixedLengthByteArray =
    TestCase
        ( assertExpectException
            "fixedLengthByteArray"
            "FIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/fixed_length_byte_array.parquet")
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

columnChunkKeyValueMetadata :: Test
columnChunkKeyValueMetadata =
    TestCase
        ( assertExpectException
            "columnChunkKeyValueMetadata"
            "Unknown page header field"
            (D.readParquet "./tests/data/column_chunk_key_value_metadata.parquet")
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

sortColumns :: Test
sortColumns =
    TestCase
        ( assertEqual
            "sortColumns"
            (3, 2)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/sort_columns.parquet"))
            )
        )

overflowI16PageCnt :: Test
overflowI16PageCnt =
    TestCase
        ( assertExpectException
            "overflowI16PageCnt"
            "UNIMPLEMENTED"
            (D.readParquet "./tests/data/overflow_i16_page_cnt.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 11: Nested / complex types and byte-stream-split
-- ---------------------------------------------------------------------------

byteStreamSplitZstd :: Test
byteStreamSplitZstd =
    TestCase
        ( assertExpectException
            "byteStreamSplitZstd"
            "EBYTE_STREAM_SPLIT"
            (D.readParquet "./tests/data/byte_stream_split.zstd.parquet")
        )

byteStreamSplitExtendedGzip :: Test
byteStreamSplitExtendedGzip =
    TestCase
        ( assertExpectException
            "byteStreamSplitExtendedGzip"
            "FIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/byte_stream_split_extended.gzip.parquet")
        )

float16NonzerosAndNans :: Test
float16NonzerosAndNans =
    TestCase
        ( assertExpectException
            "float16NonzerosAndNans"
            "PFIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/float16_nonzeros_and_nans.parquet")
        )

float16ZerosAndNans :: Test
float16ZerosAndNans =
    TestCase
        ( assertExpectException
            "float16ZerosAndNans"
            "PFIXED_LEN_BYTE_ARRAY"
            (D.readParquet "./tests/data/float16_zeros_and_nans.parquet")
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

unknownLogicalType :: Test
unknownLogicalType =
    TestCase
        ( assertExpectException
            "unknownLogicalType"
            "Unknown logical type"
            (D.readParquet "./tests/data/unknown-logical-type.parquet")
        )

-- ---------------------------------------------------------------------------
-- Group 12: Malformed files
-- ---------------------------------------------------------------------------

nationDictMalformed :: Test
nationDictMalformed =
    TestCase
        ( assertExpectException
            "nationDictMalformed"
            "dict index count mismatch"
            (D.readParquet "./tests/data/nation.dict-malformed.parquet")
        )

shardedNullableSchema :: Test
shardedNullableSchema =
    TestCase $ do
        metas <-
            mapM
                (fmap fst . DP.readMetadataFromPath)
                ["data/sharded/part-0.parquet", "data/sharded/part-1.parquet"]
        let nullableCols =
                S.fromList
                    [ last (map T.pack colPath)
                    | meta <- metas
                    , rg <- rowGroups meta
                    , cc <- rowGroupColumns rg
                    , let cm = columnMetaData cc
                          colPath = columnPathInSchema cm
                    , not (null colPath)
                    , columnNullCount (columnStatistics cm) > 0
                    ]
            df =
                foldl
                    (\acc meta -> acc <> F.schemaToEmptyDataFrame nullableCols (schema meta))
                    D.empty
                    metas
        assertBool "id should be nullable" (hasMissing (unsafeGetColumn "id" df))
        assertBool "name should be nullable" (hasMissing (unsafeGetColumn "name" df))
        assertBool "score should be nullable" (hasMissing (unsafeGetColumn "score" df))

singleShardNoNulls :: Test
singleShardNoNulls =
    TestCase $ do
        (meta, _) <- DP.readMetadataFromPath "data/sharded/part-0.parquet"
        let nullableCols =
                S.fromList
                    [ last (map T.pack colPath)
                    | rg <- rowGroups meta
                    , cc <- rowGroupColumns rg
                    , let cm = columnMetaData cc
                          colPath = columnPathInSchema cm
                    , not (null colPath)
                    , columnNullCount (columnStatistics cm) > 0
                    ]
            df = F.schemaToEmptyDataFrame nullableCols (schema meta)
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
