module DataFrame.IO.Parquet.Types where

import qualified Data.ByteString as BS
import Data.Int
import qualified Data.Text as T
import Data.Time

data ParquetType
    = PBOOLEAN
    | PINT32
    | PINT64
    | PINT96
    | PFLOAT
    | PDOUBLE
    | PBYTE_ARRAY
    | PFIXED_LEN_BYTE_ARRAY
    | PARQUET_TYPE_UNKNOWN
    deriving (Show, Eq)

parquetTypeFromInt :: Int32 -> ParquetType
parquetTypeFromInt 0 = PBOOLEAN
parquetTypeFromInt 1 = PINT32
parquetTypeFromInt 2 = PINT64
parquetTypeFromInt 3 = PINT96
parquetTypeFromInt 4 = PFLOAT
parquetTypeFromInt 5 = PDOUBLE
parquetTypeFromInt 6 = PBYTE_ARRAY
parquetTypeFromInt 7 = PFIXED_LEN_BYTE_ARRAY
parquetTypeFromInt _ = PARQUET_TYPE_UNKNOWN

data PageType
    = DATA_PAGE
    | INDEX_PAGE
    | DICTIONARY_PAGE
    | DATA_PAGE_V2
    | PAGE_TYPE_UNKNOWN
    deriving (Show, Eq)

pageTypeFromInt :: Int32 -> PageType
pageTypeFromInt 0 = DATA_PAGE
pageTypeFromInt 1 = INDEX_PAGE
pageTypeFromInt 2 = DICTIONARY_PAGE
pageTypeFromInt 3 = DATA_PAGE_V2
pageTypeFromInt _ = PAGE_TYPE_UNKNOWN

data ParquetEncoding
    = EPLAIN
    | EPLAIN_DICTIONARY
    | ERLE
    | EBIT_PACKED
    | EDELTA_BINARY_PACKED
    | EDELTA_LENGTH_BYTE_ARRAY
    | EDELTA_BYTE_ARRAY
    | ERLE_DICTIONARY
    | EBYTE_STREAM_SPLIT
    | PARQUET_ENCODING_UNKNOWN
    deriving (Show, Eq)

parquetEncodingFromInt :: Int32 -> ParquetEncoding
parquetEncodingFromInt 0 = EPLAIN
parquetEncodingFromInt 2 = EPLAIN_DICTIONARY
parquetEncodingFromInt 3 = ERLE
parquetEncodingFromInt 4 = EBIT_PACKED
parquetEncodingFromInt 5 = EDELTA_BINARY_PACKED
parquetEncodingFromInt 6 = EDELTA_LENGTH_BYTE_ARRAY
parquetEncodingFromInt 7 = EDELTA_BYTE_ARRAY
parquetEncodingFromInt 8 = ERLE_DICTIONARY
parquetEncodingFromInt 9 = EBYTE_STREAM_SPLIT
parquetEncodingFromInt _ = PARQUET_ENCODING_UNKNOWN

data CompressionCodec
    = UNCOMPRESSED
    | SNAPPY
    | GZIP
    | LZO
    | BROTLI
    | LZ4
    | ZSTD
    | LZ4_RAW
    | COMPRESSION_CODEC_UNKNOWN
    deriving (Show, Eq)

data PageEncodingStats = PageEncodingStats
    { pageEncodingPageType :: PageType
    , pageEncoding :: ParquetEncoding
    , pagesWithEncoding :: Int32
    }
    deriving (Show, Eq)

emptyPageEncodingStats :: PageEncodingStats
emptyPageEncodingStats = PageEncodingStats PAGE_TYPE_UNKNOWN PARQUET_ENCODING_UNKNOWN 0

data SizeStatistics = SizeStatisics
    { unencodedByteArrayDataTypes :: Int64
    , repetitionLevelHistogram :: [Int64]
    , definitionLevelHistogram :: [Int64]
    }
    deriving (Show, Eq)

emptySizeStatistics :: SizeStatistics
emptySizeStatistics = SizeStatisics 0 [] []

data BoundingBox = BoundingBox
    { xmin :: Double
    , xmax :: Double
    , ymin :: Double
    , ymax :: Double
    , zmin :: Double
    , zmax :: Double
    , mmin :: Double
    , mmax :: Double
    }
    deriving (Show, Eq)

emptyBoundingBox :: BoundingBox
emptyBoundingBox = BoundingBox 0 0 0 0 0 0 0 0

data GeospatialStatistics = GeospatialStatistics
    { bbox :: BoundingBox
    , geospatialTypes :: [Int32]
    }
    deriving (Show, Eq)

emptyGeospatialStatistics :: GeospatialStatistics
emptyGeospatialStatistics = GeospatialStatistics emptyBoundingBox []

data ColumnStatistics = ColumnStatistics
    { columnMin :: BS.ByteString
    , columnMax :: BS.ByteString
    , columnNullCount :: Int64
    , columnDistictCount :: Int64
    , columnMinValue :: BS.ByteString
    , columnMaxValue :: BS.ByteString
    , isColumnMaxValueExact :: Bool
    , isColumnMinValueExact :: Bool
    }
    deriving (Show, Eq)

emptyColumnStatistics :: ColumnStatistics
emptyColumnStatistics = ColumnStatistics BS.empty BS.empty 0 0 BS.empty BS.empty False False

data ColumnCryptoMetadata
    = COLUMN_CRYPTO_METADATA_UNKNOWN
    | ENCRYPTION_WITH_FOOTER_KEY
    | EncryptionWithColumnKey
        { columnCryptPathInSchema :: [String]
        , columnKeyMetadata :: BS.ByteString
        }
    deriving (Show, Eq)

data SortingColumn = SortingColumn
    { columnIndex :: Int32
    , columnOrderDescending :: Bool
    , nullFirst :: Bool
    }
    deriving (Show, Eq)

emptySortingColumn :: SortingColumn
emptySortingColumn = SortingColumn 0 False False

data ColumnOrder
    = TYPE_ORDER
    | COLUMN_ORDER_UNKNOWN
    deriving (Show, Eq)

data EncryptionAlgorithm
    = ENCRYPTION_ALGORITHM_UNKNOWN
    | AesGcmV1
        { aadPrefix :: BS.ByteString
        , aadFileUnique :: BS.ByteString
        , supplyAadPrefix :: Bool
        }
    | AesGcmCtrV1
        { aadPrefix :: BS.ByteString
        , aadFileUnique :: BS.ByteString
        , supplyAadPrefix :: Bool
        }
    deriving (Show, Eq)

data DictVals
    = DBool [Bool]
    | DInt32 [Int32]
    | DInt64 [Int64]
    | DInt96 [UTCTime]
    | DFloat [Float]
    | DDouble [Double]
    | DText [T.Text]
    deriving (Show, Eq)

data Page = Page
    { pageHeader :: PageHeader
    , pageBytes :: BS.ByteString
    }
    deriving (Show, Eq)

data PageHeader = PageHeader
    { pageHeaderPageType :: PageType
    , uncompressedPageSize :: Int32
    , compressedPageSize :: Int32
    , pageHeaderCrcChecksum :: Int32
    , pageTypeHeader :: PageTypeHeader
    }
    deriving (Show, Eq)

emptyPageHeader = PageHeader PAGE_TYPE_UNKNOWN 0 0 0 PAGE_TYPE_HEADER_UNKNOWN

data PageTypeHeader
    = DataPageHeader
        { dataPageHeaderNumValues :: Int32
        , dataPageHeaderEncoding :: ParquetEncoding
        , definitionLevelEncoding :: ParquetEncoding
        , repetitionLevelEncoding :: ParquetEncoding
        , dataPageHeaderStatistics :: ColumnStatistics
        }
    | DataPageHeaderV2
        { dataPageHeaderV2NumValues :: Int32
        , dataPageHeaderV2NumNulls :: Int32
        , dataPageHeaderV2NumRows :: Int32
        , dataPageHeaderV2Encoding :: ParquetEncoding
        , definitionLevelByteLength :: Int32
        , repetitionLevelByteLength :: Int32
        , dataPageHeaderV2IsCompressed :: Bool
        , dataPageHeaderV2Statistics :: ColumnStatistics
        }
    | DictionaryPageHeader
        { dictionaryPageHeaderNumValues :: Int32
        , dictionaryPageHeaderEncoding :: ParquetEncoding
        , dictionaryPageIsSorted :: Bool
        }
    | INDEX_PAGE_HEADER
    | PAGE_TYPE_HEADER_UNKNOWN
    deriving (Show, Eq)

emptyDictionaryPageHeader = DictionaryPageHeader 0 PARQUET_ENCODING_UNKNOWN False
emptyDataPageHeader =
    DataPageHeader
        0
        PARQUET_ENCODING_UNKNOWN
        PARQUET_ENCODING_UNKNOWN
        PARQUET_ENCODING_UNKNOWN
        emptyColumnStatistics
emptyDataPageHeaderV2 =
    DataPageHeaderV2
        0
        0
        0
        PARQUET_ENCODING_UNKNOWN
        0
        0 {- default for v2 is compressed -}
        True
        emptyColumnStatistics

data RepetitionType = REQUIRED | OPTIONAL | REPEATED | UNKNOWN_REPETITION_TYPE
    deriving (Eq, Show)

data LogicalType
    = STRING_TYPE
    | MAP_TYPE
    | LIST_TYPE
    | ENUM_TYPE
    | DECIMAL_TYPE
    | DATE_TYPE
    | DecimalType {decimalTypePrecision :: Int32, decimalTypeScale :: Int32}
    | TimeType {isAdjustedToUTC :: Bool, unit :: TimeUnit}
    | -- This should probably have a different, more constrained TimeUnit type.
      TimestampType {isAdjustedToUTC :: Bool, unit :: TimeUnit}
    | IntType {bitWidth :: Int8, intIsSigned :: Bool}
    | LOGICAL_TYPE_UNKNOWN
    | JSON_TYPE
    | BSON_TYPE
    | UUID_TYPE
    | FLOAT16_TYPE
    | VariantType {specificationVersion :: Int8}
    | GeometryType {crs :: T.Text}
    | GeographyType {crs :: T.Text, algorithm :: EdgeInterpolationAlgorithm}
    deriving (Eq, Show)

data TimeUnit
    = MILLISECONDS
    | MICROSECONDS
    | NANOSECONDS
    | TIME_UNIT_UNKNOWN
    deriving (Eq, Show)

data EdgeInterpolationAlgorithm
    = SPHERICAL
    | VINCENTY
    | THOMAS
    | ANDOYER
    | KARNEY
    deriving (Eq, Show)

repetitionTypeFromInt :: Int32 -> RepetitionType
repetitionTypeFromInt 0 = REQUIRED
repetitionTypeFromInt 1 = OPTIONAL
repetitionTypeFromInt 2 = REPEATED
repetitionTypeFromInt _ = UNKNOWN_REPETITION_TYPE

compressionCodecFromInt :: Int32 -> CompressionCodec
compressionCodecFromInt 0 = UNCOMPRESSED
compressionCodecFromInt 1 = SNAPPY
compressionCodecFromInt 2 = GZIP
compressionCodecFromInt 3 = LZO
compressionCodecFromInt 4 = BROTLI
compressionCodecFromInt 5 = LZ4
compressionCodecFromInt 6 = ZSTD
compressionCodecFromInt 7 = LZ4_RAW
compressionCodecFromInt _ = COMPRESSION_CODEC_UNKNOWN
