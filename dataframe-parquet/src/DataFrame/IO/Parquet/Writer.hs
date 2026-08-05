{-# LANGUAGE TupleSections #-}
{-# LANGUAGE LambdaCase #-}

module DataFrame.IO.Parquet.Writer (writeParquet, writeParquetWithOptions, defaultParquetWriteOptions) where

import DataFrame.Core (DataFrame)

data ParquetWriteOptions = ParquetWriteOptions
    { rowGroupSize :: Int64
    }
    deriving (Eq, Show)

defaultParquetWriteOptions :: ParquetWriteOptions
defaultParquetWriteOptions = undefined

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

type WriterState a = State WriterStateRecord a

generateRowGroup :: DataFrame -> WriterState (Builder, RowGroup)
generateRowGroup dataframe = do
  (builder, columns) <- foldChunks columns generateColumnChunk
  --TODO big memory if big columns. Use vectors instead
  let total_byte_size = sum . map (total_uncompressed_size . cc_meta_data) $ columns
      num_rows = fst . dataframeDimensions $ dataframe
      total_uncompressed_size = Just $ sum. map (total_uncompressed_size . cc_meta_data) $ columns
      rowGroup = undefined --TODO
  return (builder, rowGroup)


generateColumnChunk :: Column -> WriterState (Builder, ColumnChunk)
generateColumnChunk column = do
  writerState <- get
  put writerState{columnChunkState = initColumnChunkState column}
  (builder, _) <- foldChunks (chunkColumn writerState.chunkSize column) generatePage
  let file_path = Nothing
      file_offset = 0
      meta_data = undefined
      offset_index_offset = Nothing
      offset_index_length = Nothing
      column_index_offset = Nothing
      column_index_length = Nothing
      crypto_metadata     = Nothing
      encrypted_column_metadata = Nothing
      columnChunk = undefined -- TODO
  return (builder, columnChunk)


generatePage :: Column -> WriterState (Builder, PageHeader) -- PageHeader is also already in the builder
generatePage = undefined

generateSchema :: DataFrame -> [SchemaElement]
generateSchema = undefined

data Metadata = Metadata 
  { schema :: [SchemaElement]
  , rowGroups :: [RowGroup]
  }

buildMetadata :: Metadata -> Builder
buildMetadata = undefined

chunkDataFrame :: Int -> DataFrame -> [DataFrame]
chunkDataFrame = undefined

chunkColumn :: Int -> Column -> [Column]
chunkColumn = undefined

-- TODO IF it becomes a problem, allocate an array ahead of time for storing the metadata
foldChunks :: [chunk] -> (chunk -> WriterState (Builder, metadata)) -> WriterState (Builder, Sequence metadata)
foldChunks chunks process = foldl' f (mempty, mempty) chunks
  where
    f (builder, metadata) chunk = let (nextBuilder, nextMetadata) = process chunk
                                   in (builder <> nextBuilder, metadata <> nextMetadata)

second :: (b -> c) -> (a, b) -> (a, c)
second f (a, b) = (a, f b)

newtype State s a = State { runState :: s -> (s, a) }

class Monad m => MonadState s m where 
  get :: m s
  get = state $ \s -> (s, s)
  put :: s -> m ()
  put s = state $ \_ -> (s, ())
  state :: (s -> (s, a)) -> m a

instance Functor (State s) where
  fmap f (State run) = State $ second f . run

instance Applicative (State s) where
  pure a = State $ (,a)
  (State r1) <*> (State r2) = State (\s ->
    let (s', f) = r1 s
        (s'', x) = r2 s'
     in (s'', f x)
  )

instance Monad (State s) where
  return = pure
  (State run) >>= f = State $ \s ->
    let (s', a) = run s
     in runState (f a) s'

instance MonadState s (State s) where
  state = State
  

--TODO ColumnIndices and OffsetIndices
-- TODO ColumnOrders
