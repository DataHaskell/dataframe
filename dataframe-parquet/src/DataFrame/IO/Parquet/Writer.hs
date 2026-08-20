{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Writer (
    writeParquet,
    writeParquetWithOptions,
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
) where

import Control.Monad (when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Maybe (fromJust)
import qualified Data.Vector as VB
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Writer.ColumnChunkWriter (
    ColumnChunkState (..),
    bufferedSize,
    finalizePage,
    initColumnState,
    runColumnChunkWriter,
    writeRowAndMaybeFinalize,
 )
import DataFrame.IO.Parquet.Writer.Encoder (Encoder (..))
import DataFrame.IO.Parquet.Writer.Metadata (magic, rootSchemaElement)
import DataFrame.IO.Parquet.Writer.Options (
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
 )
import DataFrame.IO.Utils.RandomAccess (
    WritableBinaryHandle,
    bufferResidency,
    flushBufferToFile,
    mallocBuffer,
    onBuffer,
    putByteString,
    putWord32LE,
    withWritableBinaryFile,
    writeByteStringToFile,
 )
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    dataframeDimensions,
    getColumn,
 )
import Pinch (enum, putField)
import qualified Pinch

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions opts path df = do
    when (opts.strategy == TwoPass) $
        error "writeParquet: TwoPass strategy is not yet implemented"
    let (nRows, _) = dataframeDimensions df
        names = columnNames df
    cols <-
        VB.fromList
            <$> mapM (\n -> initColumnState opts n (fromJust (getColumn n df))) names
    withWritableBinaryFile path $ \out -> do
        writeByteStringToFile out magic
        fileOff <- newIORef 4
        rowGroupsRef <- newIORef [] -- Row Group Metadata
        rgRowsRef <- newIORef 0
        let st = WriterState out cols fileOff rowGroupsRef rgRowsRef
            interval = max 1 opts.batchRows
            loop row
                | row >= nRows = pure ()
                | otherwise = do
                    -- go row by row and append to a pgee
                    -- When a page is full (frome the page size writer option) flush it to its ColumnChunk
                    -- When all the columnChunks combined match or exceed the row group size option
                    -- flush all the columnchunks to file one by one
                    VB.forM_ cols (writeRowAndMaybeFinalize opts row)
                    modifyIORef' rgRowsRef (+ 1)
                    when ((row + 1) `mod` interval == 0) $ do
                        size <- bufferedSize cols
                        when (size >= opts.rowGroupSize) (finalizeRowGroup opts st)
                    loop (row + 1)
        loop 0
        finalizeRowGroup opts st
        writeFooter st nRows

data WriterState = WriterState
    { wsOut :: !WritableBinaryHandle
    , wsCols :: !(VB.Vector ColumnChunkState)
    , wsFileOffset :: !(IORef Int64)
    , wsRowGroups :: !(IORef [RowGroup])
    , wsRgRows :: !(IORef Int)
    }

finalizeRowGroup :: ParquetWriteOptions -> WriterState -> IO ()
finalizeRowGroup opts st = do
    rgRows <- readIORef st.wsRgRows
    when (rgRows > 0) $ do
        VB.mapM_ (runColumnChunkWriter (finalizePage opts.compressionCodec)) st.wsCols
        (chunksRev, total) <-
            VB.foldM'
                ( \(acc, totalSize) cs -> do
                    offset <- readIORef st.wsFileOffset
                    size <- bufferResidency (ckBuffer cs)
                    uncompressed <- readIORef (ckUncompressed cs)
                    flushBufferToFile st.wsOut (ckBuffer cs)
                    writeIORef st.wsFileOffset (offset + fromIntegral size)
                    writeIORef (ckUncompressed cs) 0
                    let chunk = mkColumnChunk opts offset size uncompressed rgRows cs
                    pure (chunk : acc, totalSize + fromIntegral size)
                )
                ([], 0 :: Int64)
                st.wsCols
        modifyIORef' st.wsRowGroups (mkRowGroup (reverse chunksRev) total rgRows :)
        writeIORef st.wsRgRows 0

mkColumnChunk ::
    ParquetWriteOptions ->
    Int64 ->
    Int ->
    Int64 ->
    Int ->
    ColumnChunkState ->
    ColumnChunk
mkColumnChunk opts offset size uncompressed rgRows cs =
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
            , cmd_total_uncompressed_size = putField uncompressed
            , cmd_total_compressed_size = putField (fromIntegral size)
            , cmd_key_value_metadata = putField Nothing
            , cmd_data_page_offset = putField offset
            , cmd_index_page_offset = putField Nothing
            , cmd_dictionary_page_offset = putField Nothing
            , cmd_statistics = putField Nothing
            , cmd_encoding_stats = putField Nothing
            , cmd_bloom_filter_offset = putField Nothing
            , cmd_bloom_filter_length = putField Nothing
            }

mkRowGroup :: [ColumnChunk] -> Int64 -> Int -> RowGroup
mkRowGroup chunks total rgRows =
    RowGroup
        { rg_columns = putField chunks
        , rg_total_byte_size = putField total
        , rg_num_rows = putField (fromIntegral rgRows)
        , rg_sorting_columns = putField Nothing
        , rg_file_offset = putField Nothing
        , rg_total_compressed_size = putField (Just total)
        , rg_ordinal = putField Nothing
        }

writeFooter :: WriterState -> Int -> IO ()
writeFooter st nRows = do
    rowGroups <- reverse <$> readIORef st.wsRowGroups
    let schemaElements =
            rootSchemaElement (VB.length st.wsCols) : VB.toList (VB.map ckSchema st.wsCols)
        metadata =
            FileMetadata
                { version = putField 1
                , schema = putField schemaElements
                , num_rows = putField (fromIntegral nRows)
                , row_groups = putField rowGroups
                , key_value_metadata = putField Nothing
                , created_by = putField (Just "dataframe-parquet")
                , column_orders = putField Nothing
                , encryption_algorithm = putField Nothing
                , footer_signing_key_metadata = putField Nothing
                }
        footer = Pinch.encode Pinch.compactProtocol metadata
    buffer <- mallocBuffer (BS.length footer + 8)
    onBuffer buffer $ do
        putByteString footer
        putWord32LE (fromIntegral (BS.length footer))
        putByteString magic
    flushBufferToFile st.wsOut buffer
