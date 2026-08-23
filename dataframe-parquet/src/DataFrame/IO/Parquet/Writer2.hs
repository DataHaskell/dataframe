{-# LANGUAGE OverlodedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module Dataframe.IO.Parquet.Writer where

import qualified Data.Vector as VB
import System.IO (openBinaryTempFile)

data Buffer where
  MemoryBuffer :: IORef (MutableByteArray RealWorld)
               -> IORef Int
               -> Buffer
  FileBuffer   :: Handle
               -> FilePath
               -> IORef Int
               -> Buffer

bufferResidency :: Buffer -> IO Int
bufferResidency (FileBuffer _ _ n) = readIORef n
bufferResidency (MemoryBuffer _ n) = readIORef n

mallocBuffer :: WriterStrategy -> Int -> Buffer
mallocBuffer TwoPass _ = error "The Two Pass Strategy for the Parquet Writer has not yet been implemented"
mallocBuffer InMemory capacity = do
  | capacity < 1 = ioError $ userError "mallocBuffer: capacity must be greater than 0"
  | otherwise = do
      array <- newPinnedByteArray capacity
      MemoryBuffer <$> newIORef array <*> newIORef 0

-- We're using pinned ByteArrays so we must
-- not use the grow function brovided by Data.Primitive
-- instead we must alloocate a new pinned ByteArray.
-- We might have been worried about heap fragmentation
-- because a single pinned object in a 4KB GHC block can
-- keep the whole plock alive but oyr buffers will tend to
-- be much larger than that.
-- But the memory usage will temporarily spike to 2.5x the size of
-- the buffer, but it should be fine since the current writer is single threaded
-- and grows *should* be rare.
-- If it becomes an issue we should start tracking an array of pointers
-- to buffers intsead of replacing them wholesale so grwoing a buffer
-- is just a matter of adding a new buffer to the array (which we can
-- pre-allocate to three elements to begin with and grow it only on the
-- off chance that a buffer required more than three grows).
ensureCapacity :: Buffer -> Int -> IO Buffer
ensureCapacity buffer@(FileBuffer _ _ _) = pure buffer
ensureCapacity buffer@(MemoryBuffer arrayRef positionRef) needed = do
  array <- readIORef arrayRef
  maxSize <- getSizeOfMutableByteArray array
  if needed <= maxSize
    then pure buffer
    else do
      position <- readIORef positionRef
      grownArray <- newPinnedByteArray (needed + (needed `div` 2))
      copyMutableByteArray grown 0 array 0 position
      writeIORef arrayRef grownArray
      pure buffer

filledSize :: Buffer -> IO Int
filledSize (FileBuffer _ _ positionRef) = readIORef positionRef
filledSize (MemoryBuffer _ positionRef) = readIORef positionRef

flushBufferToBuffer :: Buffer -> Buffer -> IO ()
flushBufferToBuffer (FileBuffer hSource _ sourcePositionRef) (FileBuffer hSink _ sinkPositionRef) = undefined
flushBufferToBuffer (FileBuffer hSource _ sourcePositionRef) (MemoryBuffer sinkRef sinkPositionRef) = undefined
flushBufferToBuffer (MemoryBuffer sourceRef sourcePositionRef) (FileBuffer hSink _ sinkPositionRef) = undefined
flushBufferToBuffer sourceBuffer@(MemoryBuffer sourceRef sourcePositionRef) sinkBuffer@(MemoryBuffer sinkRef sinkPositionRef) = do
  source <- readIORef sourceRef
  sourcePosition <- readIORef sourcePositionRef
  sink <- readIORef sinkRef
  sinkPosition <- readIORef sinkPositionRef
  ensureCapacity sinkBuffer (sinkPosition + sourcePosition)
  copyMutableByteArray
    sink
    sinkPosition
    source
    0 -- offset
    sourcePosition -- number of bytes
  modifyIORef' sinkPositionRef (+ sourcePosition)
  writeIORef sourcePositionRef 0

flushBufferToWritableHandle :: Buffer -> WritableBinaryHandle -> IO ()
flushBufferToWritableHandle (FileBuffer _ _ _) _ = undefined
flushBufferToWritableHandle (MemoryBuffer arrayRef positionRef) wh = do
  array <- readIORef arrayRef
  position <- readIORef positionRef
  withMutableByteArrayContents array $ \ptr ->
    flushPtrToWritableHandle wh ptr position
  writeIORef positionRef 0

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
-- So when writing to a file to minimize syscall overhead while
-- trying not to create dirty pages in the kernel page cache, we'll
-- be flushing in 256 KiB chunks.
flushPtrToWritableHandle :: WritableBinaryHandle -> Ptr Word8 -> Int -> IO ()
flushPtrToWritableHandle (WritableBinaryHandle h) ptr size = loop 0
  where
      chunkSize = 262144
      loop offset
        | offset >= size = pure ()
        | otherwise = do
            let n = min chunkSize (size - offset)
            hPutBuf h (ptr `plusPtr` offset) n
            loop $ offset + n

writeWord32LEToHandle :: WritableBinaryHandle -> Word32 -> IO ()
writeWord32LEToHandle (WritableBinaryHandle h) word = writeWord32LE (FileBuffer h (Proxy :: FilePath)) word

writeWord32LE :: Buffer -> Word32 -> IO ()
writeWord32LE buffer word =
  let byte0 = fromIntegral w :: Word8
      byte1 = fromIntegral (w `shiftR` 8) :: Word8
      byte2 = fromIntegral (w `shiftR` 16) :: Word8
      byte3 = fromIntegral (w `shiftR` 24) :: Word8
   in case buffer of
        -- `alloca` temporarily allocates a small bytearray
        -- internally. This is slower for lots of writes than 
        -- a dedicated buffer. TODO: We should probably use a dedicated
        -- pinned 4 byte ByteArray scratch buffer
        (FileBuffer h _ positionRef) = allocaBytes 4 $ \ptr -> do
          pokeByteOff ptr 0 byte0
          pokeByteOff ptr 1 byte1
          pokeByteOff ptr 2 byte2
          pokeByteOff ptr 3 byte3
          hPutBuf h ptr 4
          modifyIORef' positionRef (+4)
        (MemoryBuffer arrayRef positionRef) = do
          array <- readIORef arrayRef
          position <- readIORef positionRef
          writeByteArray position byte0
          writeByteArray (position + 1) byte1
          writeByteArray (position + 2) byte2
          writeByteArray (position + 3) byte3
          modifyIORef' positionRef (+4)
    
writeWord64LE :: Buffer -> Word64 -> IO ()
writeWord64LE buffer word =
  let byte0 = fromIntegral w :: Word8
      byte1 = fromIntegral (w `shiftR` 8) :: Word8
      byte2 = fromIntegral (w `shiftR` 16) :: Word8
      byte3 = fromIntegral (w `shiftR` 24) :: Word8
      byte4 = fromIntegral (w `shiftR` 32) :: Word8
      byte5 = fromIntegral (w `shiftR` 40) :: Word8
      byte6 = fromIntegral (w `shiftR` 48) :: Word8
      byte7 = fromIntegral (w `shiftR` 56) :: Word8
   in case buffer of
        -- TODO: We should probably use a dedicated pinned 8 byte ByteArray scratch buffer
        (FileBuffer h _ positionRef) = allocaBytes 8 $ \ptr -> do
          pokeByteOff ptr 0 byte0
          pokeByteOff ptr 1 byte1
          pokeByteOff ptr 2 byte2
          pokeByteOff ptr 3 byte3
          pokeByteOff ptr 4 byte3
          pokeByteOff ptr 5 byte3
          pokeByteOff ptr 6 byte3
          pokeByteOff ptr 7 byte3
          hPutBuf h ptr 8
          modifyIORef' positionRef (+8)
        (MemoryBuffer arrayRef positionRef) = do
          array <- readIORef arrayRef
          position <- readIORef positionRef
          writeByteArray position byte0
          writeByteArray (position + 1) byte1
          writeByteArray (position + 2) byte2
          writeByteArray (position + 3) byte3
          writeByteArray (position + 4) byte4
          writeByteArray (position + 5) byte5
          writeByteArray (position + 6) byte6
          writeByteArray (position + 7) byte7
          modifyIORef' positionRef (+8)

writeFloatLE :: Buffer -> Float -> IO ()
writeFloatLE buffer = writeWord32LE buffer . castFloatToWord32

writeDoubleLE :: Buffer -> Double -> IO ()
writeDoubleLE buffer = writeWord64LE buffer . castDoubleToWord64

writeByteString :: Buffer -> ByteString -> IO ()
writeByteString (FileBuffer _ _ _) _ = undefined
writeByteString (MemoryBuffer arrayRef positionRef) bs = do
  (fptr, fptrLen) <- toForeignPtr0 bs
  array <- readIORef arrayRef
  position <- readIORef positionRef
  unsafeWithForeignPtr fptr $ \sourcePtr -> do
    withMutableByteArrayContents array $ \destinationPtr ->
      copyBytes destinationPtr sourcePtr (position + fptrLen)
  modifyIORef' positionRef (+ fptrLen)

writeByteStringToWritableHandle :: Buffer -> ByteString -> IO ()
writeByteStringToWritableHandle buffer bs = do
    (fptr, fptrLen) <- toForeignPtr0 bs
    unsafeWithForeignPtr fptr $ \ptr ->
      flushPtrToWritableHandle
        buffer
        ptr
        fptrLen

data ParquetWriterState = ParquetWriterState
  { outputFileHandle     :: !WritableBinaryHandle
  , columnChunks         :: !(VB.Vector ColumnChunkState)
  , currentFileOffsetRef :: !(IORef Int64)
  , pageState            :: !PageState
  , rowGroupMetadataRef  :: !(IORef [RowGroup])
  , rowNumberRef         :: !(IORef Int)
  }

data ColumnChunkState = ColumnChunkState
  { columnName             :: !T.Text
  , nullable               :: !Bool
  , schema                 :: !SchemaElement
  , encoder                :: Encoder
  , buffer                 :: !Buffer
  , uncompressedBufferSize :: !(IORef Int64)
  }

data PageState = PageState
  { pageBuffer            :: !Buffer
  , definitionLevels      :: !DefLevels
  , currentRowCount       :: !(IORef Int)
  }

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions options path df = do
  let (maxRows, _) = dataframeDimensions df
      names = columnNames df
      magic = 0x31524150 :: Word32 -- "PAR1 in littleendian order
  columnChunks_ <- VB.fromList <$> mapM (\name -> initColumnState options name (fromJust (getColumn n df))) names
  withWritableBinaryFile path $ \output -> do
    writeWord32LEToHandle output magic
    currentFileOffsetRef_ <- newIORef 4
    rowGroupMetadataRef_ <- newIORef []
    rowNumberRef_ <- newIORef 0
    pageState_ <- initPageState options.pageSize
    let writerState = ParquetWriterState output columnChunks_ currentFileOffsetRef_ pageState_ rowGroupMetadataRef_ rowNumberRef_
        interval = max 1 options.batchRows
        loop :: Int -> IO ()
        loop rowNum
          | row >= maxRows = pure ()
          | otherwise = do
              VB.forM_ columnChunks_ (writeRow options pageState_ rowNum)
              modifyIORef' rowNumberRef_ (+1)
              when ((row + 1) `mod` interval == 0) $ do
                size <- VB.foldM (\n columnChunkState -> filledSize columnChunkState.buffer >>= pure . (+n)) 0 columnChunks 
                when (size >= options.rowGroupSize) (flushRowGroup options writerState)
              loop $ row + 1
    loop 0
    flushRowGroup options writerState
    writeFooter writerState maxRows
    writeWord32ToHandle output magic

writeRow :: ParquetWriteOptions -> PageState -> Int -> ColumnChunkState -> IO ()
writeRow options pageState rowNum columnChunkState = do
  notNull <- columnChunkState.encoder.writeValue pageState.pageBuffer rowNum
  when (columnChunkState.nullable)
    (pushDef pageState.definitionLevels (if notNull then 1 else 0))
  modifyIORef' pageState.currentRowCount (+1)
  pageBufferResidency <- bufferResidency pageState.pageBuffer
  defLevelsResidency <- bufferResidency pageState.definitionLevels.dlBuf
  when
    (pageBufferResidency + defLevelsResidency >= options.pageSize)
    (flushPage options pageState columnChunkState)
    
flushPage :: ParquetWriteOptions -> PageState -> ColumnChunkState -> IO ()
flushPage options columnChunkState pageState = do
  numPageRows <- readIORef pageState.currentRowCount
  when (numPageRows > 0) flush_
  where
    flush_ = do
      columnChunkState.encoder.finishValues pageState.pageBuffer
      when (columnChunkState.nullable) $ do
        flushDef pageState.definitionLevels
        defLevelsSize <- bufferResidency pageState.definitionLevels.dlBuf
        writeWord32LE columnChunkState.buffer (fromIntegral defLevelsSize)
        flushBufferToBuffer pageState.definitionLevels.dlBuf columnChunkState.buffer
        uncompressedPageSize <- bufferResidency pageState.pageBuffer
        compressedByteString <- case options.compressionCodec of
          UNCOMPRESSED -> pure (uncompressedSize, pageState.pageBuffer)
          SNAPPY _ -> do
            pageArray <- readIORef pageState.pageBuffer.arrayRef
            pageArrayResidency <- bufferResidency pageState.pageBuffer
            pageValuesByteString <-
              create pageArrayResidency $ \dst ->
                withMutableByteArrayContents array $ \src ->
                  copyBytes dst (castPtr src) pageArrayResidency
            pure $ Snappy.compress pageValuesByteString
        numPageRows <- readIORef pageState.currentRowCount
        let headerBytes = Pinch.encode
                            Pinch.compactProtocol
                            (mkDataPageHeader numPageRows uncompressedPageSize (BS.length compressedByteString))
        writeByteString columnChunkState.buffer headerBytes
        writeByteString columnChunkState.buffer compressedByteString
        writeIORef pageState.pageBuffer.positionRef 0

flushRowGroup :: ParquetWriteOptions -> ParquetWriterState -> IO ()
flushRowGroup options writerState = do
  rowNumber <- readIORef writerState.rowNumberRef
  when (rowNumber > 0) flush_
  where
    flush_ = do
      rowNumber <- readIORef writerState.rowNumberRef
      VB.forM_
        writerState.columnChunks
        (flushPage options writerState.pageState)
      (reversedColumnChunks, totalCompressed, totatUncompressed) <-
        VB.foldM'
          (\(rowGroups, totalCompressedSize, totalUncompressedSize) columnChunkState -> do
            offset <- readIORef writerState.currentFileOffsetRef
            compressedSize <- bufferResidency columnChunkState.buffer
            uncompressedSize <- readIORef columnChunkState.uncompressedBufferSize
            flushBufferToWritableHandle columnChunkState.buffer writerState.outputFileHandle
            modifyIORef' writerState.currentFileOffsetRef (+ (fromIntegral compressedSize))
            writeIORef (columnChunkState.uncompressedBufferSize) 0
            let columnChunk = mkColumnChunk options offset compressedSize uncompressedSize rowNumber columnChunkState
            pure ( columnChunk : acc
                 , totalCompressedSize + fromIntegral compressedSize
                 , totalUncompressedSize + fromIntegral uncompressedSize
                 )
          )
          ([], 0 :: Int64, 0 :: Int64)
          writerState.columnChunks
      modifyIORef' writerState.rowGroupMetadataRef (mkRowGroup (reverse reversedColumnChunks) totalCompressed totalUncompressed rowNumber :)
      writeIORef writerState.rowNumberRef 0

initColumnChunkState :: ParquetWriteOptions -> T.Text -> Column -> IO ColumnChunkState
initColumnChunkState options columnName_ column = do
  encoder_ <- buildEncoder column
  let nullable_ = hasMissing column
      schema_ =
        mkSchemaElement
          name 
          encoder.encType
          nullable
          encoder.convertedType
          encoder.logicalType
bufferSize = max 1 options.pageSize
  -- ColumnChunk Buffers start at page size and grow to their
  -- actual size over the course of building out the first row
  -- group. 
  -- Each column chunk in a row group must have the same number
  -- of rows, but each column chunk is liable to fit the same
  -- number of rows in varying amounts of data depending on the
  -- encoding and the compression characteristics of the data. 
  -- So the optimal buffer size of each column chunk is liable
  -- to vary
  -- As a result while one specific column chunk in a row group 
  -- is likely to hit the page limit, the others are liable to be
  -- much smaller than the limit.
  buffer_ <- mallocBuffer bufferSize
  uncompressedBufferSize_ <- newIORef 0
  pure
    ColumnChunkState
      { columnName = columnName_
      , nullable = nullable_
      , schema = schemaElement_
      , encoder = encoder_
      , buffer = buffer_
      , uncompressedBufferSize = uncompressedBufferSize_
      }

initPageState :: Int -> IO PageState
initPageState bufferSize = do
  pageBuffer_ <- mallocBuffer bufferSize
  definitionLevels_ <- newDefLevels
  currentRowCount <- newIORef 0
  pure
    PageState
      { pageBuffer = pageBuffer_
      , definitionLevels = definitionLevels_
      , currentRowCount = currentRowCount_
      }

