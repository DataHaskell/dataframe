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
import Data.Int (Int64)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
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

--A Parquet file is a series of row groups followed by the file metadata (which contains the schema and the
-- metadata for all the rowgroups, which, in turn, contain the metadata for each column chunk). Inside each
-- rowgroup is a series of column chunks. Column chunks consist of a series of pages. Pages are the PageHeader
-- followed by RLE encoded definition levels (if they exist), RLE encoded repetition levels (if they exist),
-- and finally the encoded and then compressed data. I forgot about magic bytes. Those are there too.
--
-- For a parquet file to acheive efficient compression we tend to desirc row groupts of a specific size and
-- for each of our column chunks to have pages that are of a specific size. So we must expose these fiddle
-- factors to the user so they can tune the writer to have the behavior they want. (there are subtleties to
-- this that are discussed further below)
--
-- We'll set the default Page size to 1 MiB and the default rowGroupSize to 128MiB (of course users will be
-- able to adjust these numbers through write options). We need to hold the entire RowGroup in memory as 
-- we build is as the ColumnChunks need to be contiguous when written to disk. So we need to hold
-- buffers for each individual columnChunk as we go row by row and build them; the columnChunks cannot be
-- interleaved. 

-- Since dataframe is columnar to begin with, we could, in theory, go golumn by column by estimating the size
-- of a certain slice of a column, but I don't yet see a good way of doing this given we must run the gamut
-- of encodings and compressions applied to each of those ColumnChunks (and those compression libraries have
-- their own multifarious strategies for various kinds of data)
-- 
-- Each row group has to be a certain size, but each column in a  row group must contain the same number
-- of rows, even though each column may very well fit the same number of rows in very different amounts
-- of space. So how do we ensure that we both hit our page size target, our record size target, and have
-- the same number of rows in each column?
--
-- We must consider the page size and row group sizes to be best effort. They could be slightly above
-- or below the target. The characteristics of the parquet file will depend on both the write options and
-- the specific data being encoded. Arrow-rs runs batches of rows through the writer, flushing when
-- they see that a page/rowgroup has met or exceeded its limit. 
--
-- So a row group is flushed specifically only on batch boundaries and we get the same number of rows in
-- every row group except the last which will be smaller than the rest. They also use sub batching. so
-- as to not overshoot page size egregiously if the user sets a large batch size. Note:
-- arrow-rs had an issue where some columns had really large values.
-- See https://github.com/apache/arrow-rs/issues/10061.
--
-- We may need to implement batching and sub batching eventually but I'm too lazy to do it right now.
--
-- If larger row groups are required (up to a gigabyte in size if not more), we should provide users who
-- need to minimize memory usage an alternate two pass strategy where we first write to temporary files (one
-- per columnChunk) until the temporary files have grown to the size of what a rowgroup should actually be
-- and pipe the temporary files into the output. Essentially our rowgroup buffer is on disk instead of in
-- memory. This is slower but should use less memory. In cases where there is extra RAM available but the
-- user chooses the two pass strategy anyway, the temp files will tend to be held in the OS Page Cache (RAM)
-- anyway.
--
-- Niceties like statistics and bloom filters and so on have not yet been implemented. We may need some 
-- extra machinery to keep track of row ranges so we can use them with the dataframe to generate our
-- statistics.
--
-- We haven't yet implemented all the encodings and compressions possible. The writer should first 
-- be brought to parity with the reader, and then we should implement encodings and compressions in
-- both together so neither lags behind the other.
--
-- We also don't yet support a way to have different compressions/endodings per page, and I imagine
-- we would use some kind of heuristid to select these things, if we should want such a thing at all
--
-- Repetition levels and Definitions levlels above 1 are also not yet supported, but that may come hand
-- in hand with bigger work where we work out the best way to support arbitraritly nested rows in
-- dataframe in a general way (as opposed to what we have today)

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions opts path df = do
    when (opts.strategy == TwoPass) $
        error "writeParquet: TwoPass strategy is not yet implemented"
    let (nRows, _) = dataframeDimensions df
        names = columnNames df
    cols <- VB.fromList <$> mapM (\n -> initColumnState opts n (fromJust (getColumn n df))) names
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
                (\(acc, totalSize) cs -> do
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

mkColumnChunk :: ParquetWriteOptions -> Int64 -> Int -> Int64 -> Int -> ColumnChunkState -> ColumnChunk
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
    let schemaElements = rootSchemaElement (VB.length st.wsCols) : VB.toList (VB.map ckSchema st.wsCols)
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
