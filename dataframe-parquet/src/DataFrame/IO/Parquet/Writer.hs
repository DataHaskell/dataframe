{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}

module DataFrame.IO.Parquet.Writer (writeParquet, writeParquetWithOptions, defaultParquetWriteOptions) where

import qualified Data.Vector as Vector
import Data.Vector (Vector)

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

data ParquetWriteOptions = ParquetWriteOptions
    { pageSize :: Int
    , rowGroupSize :: Int
    , batchSize :: Int
    , preferredCompressionCodec :: CompressionCodec
    , rowGroupBuffer :: RowGroupBuffer
    , 
    }
    deriving (Eq, Show)

defaultParquetWriteOptions :: ParquetWriteOptions
defaultParquetWriteOptions = undefined

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
-- First we must consider the page size and row group sizes to be best effort. They could be slightly above
-- or below the target. The characteristics of the parquet file will depend on both the write options and
-- the specific data being encoded. Second, we should run batches of rows through the writer, flushing when
-- we see that a page has met or exceeded its limit, and when a row group has done the same. So a row group
-- is flushed specifically only on batch boundaries and we get the same number of rows in every row group
-- except the last which will be smaller than the rest.
--
-- But we should also not overshoot page size egregiously if the user sets a large batch size, so we can
-- batch the batches (sub-batch) and make it configurable so that page sizes can be tuned if needed. Note:
-- arrow-rs had something similar but they ran into issues where some columns had really large values.
-- See https://github.com/apache/arrow-rs/issues/10061. We may need to implement this eventually, but
-- I'm too lazy to do it right now.
--
-- If larger row groups are required (up to a gigabyte in size if not more), we should provide users who
-- need to minimize memory usage an alternate two pass strategy where we first write to temporary files (one
-- per columnChunk) until the temporary files have grown to the size of what a rowgroup should actually be
-- and pipe the temporary files into the output. Essentially our rowgroup buffer is on disk instead of in
-- memory. This is slower but should use less memory. In cases where there is extra RAM available but the
-- user chooses the two pass strategy anyway, the temp files will tend to be held in the OS Page Cache (RAM)
-- anyway.
--
-- refer to DataFrame.IO.Utils.RandomAccess for the buffer implementation.

-- I tested write speeds by doing (on Apple Silicon)
-- `dd if=/dev/zero of=test bs={$n}k oflag=direct conv=fdatasync
-- Results:
--
-- ```
--    | block size | data (GiB) |  time (s) | GiB/s |
--    |------------|------------|-----------|-------|
--    | 4k         |       4.00 |     2.371 |  1.69 |
--    | 8k         |       4.00 |     1.486 |  2.69 |
--    | 16k        |       4.00 |     1.045 |  3.83 |
--    | 32k        |       4.00 |     0.740 |  5.40 |
--    | 64k        |       4.00 |     0.675 |  5.92 |
--    | 128k       |       4.00 |     0.669 |  5.98 |
--    | 256k       |       4.00 |     0.664 |  6.03 |
--    | 512k       |       4.00 |     0.670 |  5.97 |
--    | 1024k      |       4.00 |     0.664 |  6.02 |
--    | 4096k      |       4.00 |     0.668 |  5.99 |
-- ```
-- We see that our raw data-writing throughput caps out at 64k and declines slightly above that. So we
-- need to split our buffer into 64k chunks and flush each to dis k(of course, this
-- may very well vary from machine to machine, regardless, 64KiB is probably good enough).
--


-- We need writers for each level of the Parquet file
  
-- A RowGroupWriter that flushes into the file
data RowGroupEnv = RowGroupEnv
  { rowGroupBuffer :: !(Vector ColumnChunkWriter)
  , pageWriter :: PageWriter 
  }

type RowGroupWriter b a = ReaderIO (RowGroupEnv b) a

-- The RowGroupWriter has in its env a Vector of ColumnChunkWriters
-- (which is essentially the row group buffer)
type ColumnChunkWriter = ReaderIO

-- And the RowGroupWriter also needs a PageWriter
type PageWriter = ReaderIO

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions options filepath dataframe = do
  let 
      initialState = WriterStateRecord 0 options.rowGroupSize emptyColumnChunkState  
      (_, (pages, metadata)) = runState initialState
        $ foldChunks (chunkDataFrame options.rowGroupSize dataframe) generateRowGroup
      schema = generateSchema dataframe
  writeFile filepath (pages <> buildMetadata metadata)

-- I don't see how this scales to multiple reader/writer threads so we may have
-- to change this later

data WriterStateRecord = WriterStateRecord
  { offset :: Int64
  , chunkSize :: Int64
  , columnChunkState :: ColumnChunkStateRecord
  }

data ColumnChunkStateRecord = ColumnChunkStateRecord
  { thriftType :: ThriftType
  , encodings :: Set Encoding
  , codec :: CompressionCodec
  , total_uncompressed_size :: Int64
  , total_compressed_size :: Int64
  , data_page_offset :: Maybe Int64
  , dictionary_page_offset :: Maybe Int64
  }
initColumnChunkState :: Column -> ColumnChunkStateRecord
initColumnChunkState = undefined

emptyColumnChunkState :: ColumnChunkStateRecord
emptyColumnChunkState = undefined

-- type WriterState a = State WriterStateRecord a
-- 
-- generateRowGroup :: DataFrame -> WriterState (Builder, RowGroup)
-- generateRowGroup dataframe = do
--   (builder, columns) <- foldChunks columns generateColumnChunk
--   --TODO big memory if big columns. Use vectors instead
--   let total_byte_size = sum . map (total_uncompressed_size . cc_meta_data) $ columns
--       num_rows = fst . dataframeDimensions $ dataframe
--       total_uncompressed_size = Just $ sum. map (total_uncompressed_size . cc_meta_data) $ columns
--       rowGroup = undefined --TODO
--   return (builder, rowGroup)
-- 
-- 
-- generateColumnChunk :: Column -> WriterState (Builder, ColumnChunk)
-- generateColumnChunk column = do
--   writerState <- get
--   put writerState{columnChunkState = initColumnChunkState column}
--   (builder, _) <- foldChunks (chunkColumn writerState.chunkSize column) generatePage
--   let file_path = Nothing
--       file_offset = 0
--       meta_data = undefined
--       offset_index_offset = Nothing
--       offset_index_length = Nothing
--       column_index_offset = Nothing
--       column_index_length = Nothing
--       crypto_metadata     = Nothing
--       encrypted_column_metadata = Nothing
--       columnChunk = undefined -- TODO
--   return (builder, columnChunk)
-- 
-- 
-- generatePage :: Column -> WriterState (Builder, PageHeader) -- PageHeader is also already in the builder
-- generatePage = undefined
-- 
-- generateSchema :: DataFrame -> [SchemaElement]
-- generateSchema = undefined
-- 
-- data Metadata = Metadata 
--   { schema :: [SchemaElement]
--   , rowGroups :: [RowGroup]
--   }
-- 
-- buildMetadata :: Metadata -> Builder
-- buildMetadata = undefined
-- 
-- chunkDataFrame :: Int -> DataFrame -> [DataFrame]
-- chunkDataFrame = undefined
-- 
-- chunkColumn :: Int -> Column -> [Column]
-- chunkColumn = undefined
-- 
-- -- TODO IF it becomes a problem, allocate an array ahead of time for storing the metadata
-- foldChunks :: [chunk] -> (chunk -> WriterState (Builder, metadata)) -> WriterState (Builder, Sequence metadata)
-- foldChunks chunks process = foldl' f (mempty, mempty) chunks
--   where
--     f (builder, metadata) chunk = let (nextBuilder, nextMetadata) = process chunk
--                                    in (builder <> nextBuilder, metadata <> nextMetadata)
--   state = State
--   

