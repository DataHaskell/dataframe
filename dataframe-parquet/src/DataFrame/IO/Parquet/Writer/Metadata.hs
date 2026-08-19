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

mkSchemaElem :: T.Text -> ThriftType -> Bool -> Maybe ConvertedType -> Maybe LogicalType -> SchemaElement
mkSchemaElem elementName elementType nullable converted logical =
    SchemaElement
        { schematype = putField (Just elementType)
        , type_length = putField Nothing
        , repetition_type = putField (Just (if nullable then OPTIONAL enum else REQUIRED enum))
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

magic :: BS.ByteString
magic = "PAR1"
