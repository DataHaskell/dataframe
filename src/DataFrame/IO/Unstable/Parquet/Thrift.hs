{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.IO.Unstable.Parquet.Thrift where
import Data.Int (Int32, Int64, Int8, Int16)
import Data.Text (Text)
import Data.ByteString (ByteString)
import GHC.Generics (Generic)
import Pinch (Field, Enumeration, Pinchable (..))
import qualified Pinch

-- Primitive Parquet Types
-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L32
data ThriftType = BOOLEAN (Enumeration 0)
                | INT32 (Enumeration 1)
                | INT64 (Enumeration 2)
                | INT96 (Enumeration 3)
                | FLOAT (Enumeration 4)
                | DOUBLE (Enumeration 5)
                | BYTE_ARRAY (Enumeration 6)
                | PFIXED_LEN_BYTE_ARRAY (Enumeration 7)
                deriving (Eq, Show, Generic)

instance Pinchable ThriftType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L183
data FieldRepetitionType = REQUIRED (Enumeration 0)
                         | OPTIONAL (Enumeration 1)
                         | REPEATED (Enumeration 2)
                         deriving (Eq, Show, Generic)

instance Pinchable FieldRepetitionType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L203
data Encoding = PLAIN (Enumeration 0)
              | PLAIN_DICTIONARY (Enumeration 2)
              | RLE (Enumeration 3)
              | BIT_PACKED (Enumeration 4)
              | DELTA_BINARY_PACKED (Enumeration 5)
              | DELTA_LENGTH_BYTE_ARRAY (Enumeration 6)
              | DELTA_BYTE_ARRAY (Enumeration 7)
              | RLE_DICTIONARY (Enumeration 8)
              | BYTE_STREAM_SPLIT (Enumeration 9)
              deriving (Eq, Show, Generic)

instance Pinchable Encoding

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L244
data CompressionCodec = UNCOMPRESSED (Enumeration 0)
                      | SNAPPY (Enumeration 1)
                      | GZIP (Enumeration 2)
                      | LZO (Enumeration 3)
                      | BROTLI (Enumeration 4)
                      | LZ4 (Enumeration 5)
                      | ZSTD (Enumeration 6)
                      | LZ4_RAW (Enumeration 7)
                      deriving (Eq, Show, Generic)

instance Pinchable CompressionCodec

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L261
data PageType = DATA_PAGE (Enumeration 0)
              | INDEX_PAGE (Enumeration 1)
              | DICTIONARY_PAGE (Enumeration 2)
              | DATA_PAGE_V2 (Enumeration 3)
              deriving (Eq, Show, Generic)

instance Pinchable PageType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L271
data BoundaryOrder = UNORDERED (Enumeration 0)
                   | ASCENDING (Enumeration 1)
                   | DESCENDING (Enumeration 2)
                   deriving (Eq, Show, Generic)

instance Pinchable BoundaryOrder

-- Logical type annotations
-- Empty structs can't use deriving Generic with Pinch, so we use a unit-like workaround.
-- We represent empty structs as a newtype over () with a manual Pinchable instance.

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L283
-- struct StringType {}
data StringType = StringType deriving (Eq, Show)
instance Pinchable StringType where
  type Tag StringType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure StringType

data UUIDType = UUIDType deriving (Eq, Show)
instance Pinchable UUIDType where
  type Tag UUIDType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure UUIDType

data MapType = MapType deriving (Eq, Show)
instance Pinchable MapType where
  type Tag MapType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure MapType

data ListType = ListType deriving (Eq, Show)
instance Pinchable ListType where
  type Tag ListType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure ListType

data EnumType = EnumType deriving (Eq, Show)
instance Pinchable EnumType where
  type Tag EnumType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure EnumType

data DateType = DateType deriving (Eq, Show)
instance Pinchable DateType where
  type Tag DateType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure DateType

data Float16Type = Float16Type deriving (Eq, Show)
instance Pinchable Float16Type where
  type Tag Float16Type = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure Float16Type

data NullType = NullType deriving (Eq, Show)
instance Pinchable NullType where
  type Tag NullType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure NullType

data JsonType = JsonType deriving (Eq, Show)
instance Pinchable JsonType where
  type Tag JsonType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure JsonType

data BsonType = BsonType deriving (Eq, Show)
instance Pinchable BsonType where
  type Tag BsonType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure BsonType

data VariantType = VariantType deriving (Eq, Show)
instance Pinchable VariantType where
  type Tag VariantType = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure VariantType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L290
data TimeUnit = MILLIS (Field 1 MilliSeconds)
              | MICROS (Field 2 MicroSeconds)
              | NANOS (Field 3 NanoSeconds)
              deriving (Eq, Show, Generic)

instance Pinchable TimeUnit

data MilliSeconds = MilliSeconds deriving (Eq, Show)
instance Pinchable MilliSeconds where
  type Tag MilliSeconds = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure MilliSeconds

data MicroSeconds = MicroSeconds deriving (Eq, Show)
instance Pinchable MicroSeconds where
  type Tag MicroSeconds = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure MicroSeconds

data NanoSeconds = NanoSeconds deriving (Eq, Show)
instance Pinchable NanoSeconds where
  type Tag NanoSeconds = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure NanoSeconds

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L317
data DecimalType
  = DecimalType
  { decimal_scale     :: Field 1 Int32
  , decimal_precision :: Field 2 Int32
  } deriving (Eq, Show, Generic)

instance Pinchable DecimalType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L328
data IntType
  = IntType
  { int_bitWidth :: Field 1 Int8
  , int_isSigned :: Field 2 Bool
  } deriving (Eq, Show, Generic)

instance Pinchable IntType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L338
data TimeType
  = TimeType
  { time_isAdjustedToUTC :: Field 1 Bool
  , time_unit            :: Field 2 TimeUnit
  } deriving (Eq, Show, Generic)

instance Pinchable TimeType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L349
data TimestampType
  = TimestampType
  { timestamp_isAdjustedToUTC :: Field 1 Bool
  , timestamp_unit            :: Field 2 TimeUnit
  } deriving (Eq, Show, Generic)

instance Pinchable TimestampType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L360
-- union LogicalType
data LogicalType = LT_STRING    (Field 1 StringType)
                 | LT_MAP       (Field 2 MapType)
                 | LT_LIST      (Field 3 ListType)
                 | LT_ENUM      (Field 4 EnumType)
                 | LT_DECIMAL   (Field 5 DecimalType)
                 | LT_DATE      (Field 6 DateType)
                 | LT_TIME      (Field 7 TimeType)
                 | LT_TIMESTAMP (Field 8 TimestampType)
                 | LT_INTEGER   (Field 10 IntType)
                 | LT_NULL      (Field 11 NullType)
                 | LT_JSON      (Field 12 JsonType)
                 | LT_BSON      (Field 13 BsonType)
                 | LT_UUID      (Field 14 UUIDType)
                 | LT_FLOAT16   (Field 15 Float16Type)
                 | LT_VARIANT   (Field 16 VariantType)
                 deriving (Eq, Show, Generic)

instance Pinchable LogicalType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L270
data ConvertedType = UTF8 (Enumeration 0)
                   | MAP (Enumeration 1)
                   | MAP_KEY_VALUE (Enumeration 2)
                   | LIST (Enumeration 3)
                   | ENUM (Enumeration 4)
                   | DECIMAL (Enumeration 5)
                   | DATE (Enumeration 6)
                   | TIME_MILLIS (Enumeration 7)
                   | TIME_MICROS (Enumeration 8)
                   | TIMESTAMP_MILLIS (Enumeration 9)
                   | TIMESTAMP_MICROS (Enumeration 10)
                   | UINT_8 (Enumeration 11)
                   | UINT_16 (Enumeration 12)
                   | UINT_32 (Enumeration 13)
                   | UINT_64 (Enumeration 14)
                   | INT_8 (Enumeration 15)
                   | INT_16 (Enumeration 16)
                   | INT_32 (Enumeration 17)
                   | INT_64 (Enumeration 18)
                   | JSON (Enumeration 19)
                   | BSON (Enumeration 20)
                   | INTERVAL (Enumeration 21)
                   deriving (Eq, Show, Generic)

instance Pinchable ConvertedType

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L505
data SchemaElement
  = SchemaElement
  { schematype      :: Field 1 (Maybe Int8) -- called just type in parquet.thrift
  , type_length     :: Field 2 (Maybe Int32)
  , repetition_type :: Field 3 (Maybe FieldRepetitionType)
  , name            :: Field 4 Text
  , num_children    :: Field 5 (Maybe Int32)
  , converted_type  :: Field 6 (Maybe ConvertedType)
  , scale           :: Field 7 (Maybe Int32)
  , precision       :: Field 8 (Maybe Int32)
  , field_id        :: Field 9 (Maybe Int32)
  , logicalType     :: Field 10 (Maybe LogicalType)
  } deriving (Eq, Show, Generic)

instance Pinchable SchemaElement

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L560
data Statistics
  = Statistics
  { stats_max           :: Field 1 (Maybe ByteString)
  , stats_min           :: Field 2 (Maybe ByteString)
  , stats_null_count    :: Field 3 (Maybe Int64)
  , stats_distinct_count :: Field 4 (Maybe Int64)
  , stats_max_value     :: Field 5 (Maybe ByteString)
  , stats_min_value     :: Field 6 (Maybe ByteString)
  , stats_is_max_value_exact :: Field 7 (Maybe Bool)
  , stats_is_min_value_exact :: Field 8 (Maybe Bool)
  } deriving (Eq, Show, Generic)

instance Pinchable Statistics

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L600
data PageEncodingStats
  = PageEncodingStats
  { pes_page_type :: Field 1 PageType
  , pes_encoding  :: Field 2 Encoding
  , pes_count     :: Field 3 Int32
  } deriving (Eq, Show, Generic)

instance Pinchable PageEncodingStats

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L614
data ColumnMetaData
  = ColumnMetaData
  { cmd_type                    :: Field 1 ThriftType
  , cmd_encodings               :: Field 2 [Encoding]
  , cmd_path_in_schema          :: Field 3 [Text]
  , cmd_codec                   :: Field 4 CompressionCodec
  , cmd_num_values              :: Field 5 Int64
  , cmd_total_uncompressed_size :: Field 6 Int64
  , cmd_total_compressed_size   :: Field 7 Int64
  , cmd_key_value_metadata      :: Field 8 (Maybe [KeyValue])
  , cmd_data_page_offset        :: Field 9 Int64
  , cmd_index_page_offset       :: Field 10 (Maybe Int64)
  , cmd_dictionary_page_offset  :: Field 11 (Maybe Int64)
  , cmd_statistics              :: Field 12 (Maybe Statistics)
  , cmd_encoding_stats          :: Field 13 (Maybe [PageEncodingStats])
  , cmd_bloom_filter_offset     :: Field 14 (Maybe Int64)
  , cmd_bloom_filter_length     :: Field 15 (Maybe Int32)
  } deriving (Eq, Show, Generic)

instance Pinchable ColumnMetaData

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L875
data EncryptionWithFooterKey = EncryptionWithFooterKey deriving (Eq, Show)
instance Pinchable EncryptionWithFooterKey where
  type Tag EncryptionWithFooterKey = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure EncryptionWithFooterKey

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L883
data EncryptionWithColumnKey
  = EncryptionWithColumnKey
  { ewck_path_in_schema :: Field 1 [Text]
  , ewck_key_metadata   :: Field 2 (Maybe ByteString)
  } deriving (Eq, Show, Generic)

instance Pinchable EncryptionWithColumnKey

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L893
-- union ColumnCryptoMetaData
data ColumnCryptoMetaData
  = CCM_ENCRYPTION_WITH_FOOTER_KEY (Field 1 EncryptionWithFooterKey)
  | CCM_ENCRYPTION_WITH_COLUMN_KEY (Field 2 EncryptionWithColumnKey)
  deriving (Eq, Show, Generic)

instance Pinchable ColumnCryptoMetaData

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L899
data ColumnChunk
  = ColumnChunk
  { cc_file_path              :: Field 1 (Maybe Text)
  , cc_file_offset            :: Field 2 Int64
  , cc_meta_data              :: Field 3 (Maybe ColumnMetaData)
  , cc_offset_index_offset    :: Field 4 (Maybe Int64)
  , cc_offset_index_length    :: Field 5 (Maybe Int32)
  , cc_column_index_offset    :: Field 6 (Maybe Int64)
  , cc_column_index_length    :: Field 7 (Maybe Int32)
  , cc_crypto_metadata        :: Field 8 (Maybe ColumnCryptoMetaData)
  , cc_encrypted_column_metadata :: Field 9 (Maybe ByteString)
  } deriving (Eq, Show, Generic)

instance Pinchable ColumnChunk

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L940
data SortingColumn
  = SortingColumn
  { sc_column_idx  :: Field 1 Int32
  , sc_descending  :: Field 2 Bool
  , sc_nulls_first :: Field 3 Bool
  } deriving (Eq, Show, Generic)

instance Pinchable SortingColumn

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L958
data RowGroup
  = RowGroup
  { rg_columns              :: Field 1 [ColumnChunk]
  , rg_total_byte_size      :: Field 2 Int64
  , rg_num_rows             :: Field 3 Int64
  , rg_sorting_columns      :: Field 4 (Maybe [SortingColumn])
  , rg_file_offset          :: Field 5 (Maybe Int64)
  , rg_total_compressed_size :: Field 6 (Maybe Int64)
  , rg_ordinal              :: Field 7 (Maybe Int16)
  } deriving (Eq, Show, Generic)

instance Pinchable RowGroup

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L980
data KeyValue
  = KeyValue
  { kv_key   :: Field 1 Text
  , kv_value :: Field 2 (Maybe Text)
  } deriving (Eq, Show, Generic)

instance Pinchable KeyValue

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L990
-- union ColumnOrder
data ColumnOrder
  = TYPE_ORDER (Field 1 TypeDefinedOrder)
  deriving (Eq, Show, Generic)

instance Pinchable ColumnOrder

-- Empty struct for TYPE_ORDER
data TypeDefinedOrder = TypeDefinedOrder deriving (Eq, Show)
instance Pinchable TypeDefinedOrder where
  type Tag TypeDefinedOrder = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure TypeDefinedOrder

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1094
data AesGcmV1
  = AesGcmV1
  { aes_gcm_v1_aad_prefix      :: Field 1 (Maybe ByteString)
  , aes_gcm_v1_aad_file_unique :: Field 2 (Maybe ByteString)
  , aes_gcm_v1_supply_aad_prefix :: Field 3 (Maybe Bool)
  } deriving (Eq, Show, Generic)

instance Pinchable AesGcmV1

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1107
data AesGcmCtrV1
  = AesGcmCtrV1
  { aes_gcm_ctr_v1_aad_prefix        :: Field 1 (Maybe ByteString)
  , aes_gcm_ctr_v1_aad_file_unique   :: Field 2 (Maybe ByteString)
  , aes_gcm_ctr_v1_supply_aad_prefix :: Field 3 (Maybe Bool)
  } deriving (Eq, Show, Generic)

instance Pinchable AesGcmCtrV1

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1118
-- union EncryptionAlgorithm
data EncryptionAlgorithm
  = AES_GCM_V1     (Field 1 AesGcmV1)
  | AES_GCM_CTR_V1 (Field 2 AesGcmCtrV1)
  deriving (Eq, Show, Generic)

instance Pinchable EncryptionAlgorithm

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1001
data PageLocation
  = PageLocation
  { pl_offset            :: Field 1 Int64
  , pl_compressed_page_size :: Field 2 Int32
  , pl_first_row_index   :: Field 3 Int64
  } deriving (Eq, Show, Generic)

instance Pinchable PageLocation

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1017
data OffsetIndex
  = OffsetIndex
  { oi_page_locations             :: Field 1 [PageLocation]
  , oi_unencoded_byte_array_data_bytes :: Field 2 (Maybe [Int64])
  } deriving (Eq, Show, Generic)

instance Pinchable OffsetIndex

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1033
data ColumnIndex
  = ColumnIndex
  { ci_null_pages        :: Field 1 [Bool]
  , ci_min_values        :: Field 2 [ByteString]
  , ci_max_values        :: Field 3 [ByteString]
  , ci_boundary_order    :: Field 4 BoundaryOrder
  , ci_null_counts       :: Field 5 (Maybe [Int64])
  , ci_repetition_level_histograms :: Field 6 (Maybe [Int64])
  , ci_definition_level_histograms :: Field 7 (Maybe [Int64])
  } deriving (Eq, Show, Generic)

instance Pinchable ColumnIndex

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1248
data DataPageHeader
  = DataPageHeader
  { dph_num_values       :: Field 1 Int32
  , dph_encoding         :: Field 2 Encoding
  , dph_definition_level_encoding :: Field 3 Encoding
  , dph_repetition_level_encoding :: Field 4 Encoding
  , dph_statistics       :: Field 5 (Maybe Statistics)
  } deriving (Eq, Show, Generic)

instance Pinchable DataPageHeader

data IndexPageHeader = IndexPageHeader deriving (Eq, Show)
instance Pinchable IndexPageHeader where
  type Tag IndexPageHeader = Pinch.TStruct
  pinch _ = Pinch.struct []
  unpinch _ = pure IndexPageHeader

data DictionaryPageHeader
  = DictionaryPageHeader
  { diph_num_values  :: Field 1 Int32
  , diph_encoding    :: Field 2 Encoding
  , diph_is_sorted   :: Field 3 (Maybe Bool)
  } deriving (Eq, Show, Generic)

instance Pinchable DictionaryPageHeader

data DataPageHeaderV2
  = DataPageHeaderV2
  { dph2_num_values             :: Field 1 Int32
  , dph2_num_nulls              :: Field 2 Int32
  , dph2_num_rows               :: Field 3 Int32
  , dph2_encoding               :: Field 4 Encoding
  , dph2_definition_levels_byte_length :: Field 5 Int32
  , dph2_repetition_levels_byte_length :: Field 6 Int32
  , dph2_is_compressed          :: Field 7 (Maybe Bool)
  , dph2_statistics             :: Field 8 (Maybe Statistics)
  } deriving (Eq, Show, Generic)

instance Pinchable DataPageHeaderV2

data PageHeader
  = PageHeader
  { ph_type                    :: Field 1 PageType
  , ph_uncompressed_page_size  :: Field 2 Int32
  , ph_compressed_page_size    :: Field 3 Int32
  , ph_crc                     :: Field 4 (Maybe Int32)
  , ph_data_page_header        :: Field 5 (Maybe DataPageHeader)
  , ph_index_page_header       :: Field 6 (Maybe IndexPageHeader)
  , ph_dictionary_page_header  :: Field 7 (Maybe DictionaryPageHeader)
  , ph_data_page_header_v2     :: Field 8 (Maybe DataPageHeaderV2)
  } deriving (Eq, Show, Generic)

instance Pinchable PageHeader

-- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L1277
data FileMetadata
  = FileMetadata
  { version                    :: Field 1 Int32
  , schema                     :: Field 2 [SchemaElement]
  , num_rows                   :: Field 3 Int64
  , row_groups                 :: Field 4 [RowGroup]
  , key_value_metadata         :: Field 5 (Maybe [KeyValue])
  , created_by                 :: Field 6 (Maybe Text)
  , column_orders              :: Field 7 (Maybe [ColumnOrder])
  , encryption_algorithm       :: Field 8 (Maybe EncryptionAlgorithm)
  , footer_signing_key_metadata :: Field 9 (Maybe ByteString)
  } deriving (Eq, Show, Generic)

instance Pinchable FileMetadata
