{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Writer.Metadata (
    mkSchemaElem,
    rootSchemaElement,
    mkDataPageHeader,
    magic,
) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import DataFrame.IO.Parquet.Thrift
import Pinch (enum, putField)

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
    ParquetWriteOptions ->
    Int64 ->
    Int ->
    Int64 ->
    Int ->
    ColumnChunkState ->
    ColumnChunk
mkColumnChunk opts offset compressedSize uncompressedSize rgRows cs =
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
            { cmd_type = putField (ckEncoder cs).encType
            , cmd_encodings = putField [PLAIN enum, RLE enum]
            , cmd_path_in_schema = putField [ckName cs]
            , cmd_codec = putField opts.compressionCodec
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

writeFooter :: WriterState -> Int -> IO ()
writeFooter writerState numRows = do
    rowGroupMetadata <- reverse <$> readIORef writerState.rowGroupMetadataRef
    let schemaElements =
            rootSchemaElement (VB.length writerState.columnChunks) : VB.toList (VB.map schema writerState.columnChunks)
        metadata =
            FileMetadata
                { version = putField 1
                , schema = putField schemaElements
                , num_rows = putField (fromIntegral numRows)
                , row_groups = putField rowGroupMetadata
                , key_value_metadata = putField Nothing
                , created_by = putField (Just "dataframe-parquet")
                , column_orders = putField Nothing
                , encryption_algorithm = putField Nothing
                , footer_signing_key_metadata = putField Nothing
                }
        footer = Pinch.encode Pinch.compactProtocol metadata
    writeByteStringToWritableHandle writerState.outputFileHandle footer
    writeWord32LEToHandle writerState.outputFileHandle (fromIntegral (BS.length footer))
