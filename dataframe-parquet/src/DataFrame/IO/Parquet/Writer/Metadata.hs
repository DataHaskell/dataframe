{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Writer.Metadata (
    mkSchemaElem,
    rootSchemaElement,
    mkDataPageHeader,
    mkColumnChunk,
    mkRowGroup,
    writeFooter,
    magic,
) where

import qualified Data.ByteString as BS
import Data.Int (Int64)
import qualified Data.Text as T
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Utils.RandomAccess (
    WritableBinaryHandle,
    flushBufferToFile,
    mallocBuffer,
    writeByteString,
    writeWord32LE,
 )
import Pinch (enum, putField)
import qualified Pinch

mkDataPageHeader :: Int -> Int -> Int -> PageHeader
mkDataPageHeader rows uncompressedSize compressedSize =
    PageHeader
        { ph_type = putField (DATA_PAGE enum)
        , ph_uncompressed_page_size = putField (fromIntegral uncompressedSize)
        , ph_compressed_page_size = putField (fromIntegral compressedSize)
        , ph_crc = putField Nothing
        , ph_data_page_header = putField (Just dph)
        , ph_index_page_header = putField Nothing
        , ph_dictionary_page_header = putField Nothing
        , ph_data_page_header_v2 = putField Nothing
        }
  where
    dph =
        DataPageHeader
            { dph_num_values = putField (fromIntegral rows)
            , dph_encoding = putField (PLAIN enum)
            , dph_definition_level_encoding = putField (RLE enum)
            , dph_repetition_level_encoding = putField (RLE enum)
            , dph_statistics = putField Nothing
            }

mkSchemaElem ::
    T.Text ->
    ThriftType ->
    Bool ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    SchemaElement
mkSchemaElem elementName elementType nullable converted logical =
    SchemaElement
        { schematype = putField (Just elementType)
        , type_length = putField Nothing
        , repetition_type =
            putField (Just (if nullable then OPTIONAL enum else REQUIRED enum))
        , name = putField elementName
        , num_children = putField Nothing
        , converted_type = putField converted
        , scale = putField Nothing
        , precision = putField Nothing
        , field_id = putField Nothing
        , logicalType = putField logical
        }

rootSchemaElement :: Int -> SchemaElement
rootSchemaElement count =
    SchemaElement
        { schematype = putField Nothing
        , type_length = putField Nothing
        , repetition_type = putField Nothing
        , name = putField "schema"
        , num_children = putField (Just (fromIntegral count))
        , converted_type = putField Nothing
        , scale = putField Nothing
        , precision = putField Nothing
        , field_id = putField Nothing
        , logicalType = putField Nothing
        }

mkColumnChunk ::
    CompressionCodec ->
    ThriftType ->
    T.Text ->
    Int64 ->
    Int ->
    Int64 ->
    Int ->
    ColumnChunk
mkColumnChunk codec columnType columnName offset compressedSize uncompressedSize rgRows =
    ColumnChunk
        { cc_file_path = putField Nothing
        , cc_file_offset = putField offset
        , cc_meta_data = putField (Just metadata)
        , cc_offset_index_offset = putField Nothing
        , cc_offset_index_length = putField Nothing
        , cc_column_index_offset = putField Nothing
        , cc_column_index_length = putField Nothing
        , cc_crypto_metadata = putField Nothing
        , cc_encrypted_column_metadata = putField Nothing
        }
  where
    metadata =
        ColumnMetaData
            { cmd_type = putField columnType
            , cmd_encodings = putField [PLAIN enum, RLE enum]
            , cmd_path_in_schema = putField [columnName]
            , cmd_codec = putField codec
            , cmd_num_values = putField (fromIntegral rgRows)
            , cmd_total_uncompressed_size = putField uncompressedSize
            , cmd_total_compressed_size = putField (fromIntegral compressedSize)
            , cmd_key_value_metadata = putField Nothing
            , cmd_data_page_offset = putField offset
            , cmd_index_page_offset = putField Nothing
            , cmd_dictionary_page_offset = putField Nothing
            , cmd_statistics = putField Nothing
            , cmd_encoding_stats = putField Nothing
            , cmd_bloom_filter_offset = putField Nothing
            , cmd_bloom_filter_length = putField Nothing
            }

mkRowGroup :: [ColumnChunk] -> Int64 -> Int64 -> Int -> RowGroup
mkRowGroup chunks totalCompressed totalUncompressed rgRows =
    RowGroup
        { rg_columns = putField chunks
        , rg_total_byte_size = putField totalUncompressed
        , rg_num_rows = putField (fromIntegral rgRows)
        , rg_sorting_columns = putField Nothing
        , rg_file_offset = putField Nothing
        , rg_total_compressed_size = putField (Just totalCompressed)
        , rg_ordinal = putField Nothing
        }

writeFooter ::
    WritableBinaryHandle ->
    [SchemaElement] ->
    Int ->
    [RowGroup] ->
    [(T.Text, T.Text)] ->
    IO ()
writeFooter output schemaElements numRows rowGroupMetadata keyValues = do
    let metadata =
            FileMetadata
                { version = putField 1
                , schema = putField schemaElements
                , num_rows = putField (fromIntegral numRows)
                , row_groups = putField rowGroupMetadata
                , key_value_metadata =
                    putField $
                        if null keyValues
                            then Nothing
                            else
                                Just
                                    [ KeyValue (putField k) (putField (Just v))
                                    | (k, v) <- keyValues
                                    ]
                , created_by = putField (Just "dataframe-parquet")
                , column_orders = putField Nothing
                , encryption_algorithm = putField Nothing
                , footer_signing_key_metadata = putField Nothing
                }
        footer = Pinch.encode Pinch.compactProtocol metadata
    buffer <- mallocBuffer (BS.length footer + 8)
    writeByteString buffer footer
    writeWord32LE buffer (fromIntegral (BS.length footer))
    writeByteString buffer magic
    flushBufferToFile output buffer

magic :: BS.ByteString
magic = "PAR1"
