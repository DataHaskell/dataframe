{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Writer (
    writeParquet,
    writeParquetWithOptions,
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
    nativeTypeKeyPrefix,
    nativeTypeKeyValues,
) where

import Control.Monad (forM_, unless, when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Maybe (fromJust)
import Data.Primitive.ByteArray (getSizeofMutableByteArray)
import qualified Data.Text as T
import qualified Data.Vector as VB
import DataFrame.IO.Parquet.Thrift hiding (schema)
import DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    flushDef,
    newDefLevels,
    pushDef,
 )
import DataFrame.IO.Parquet.Writer.Encoder (Encoder (..), buildEncoder)
import DataFrame.IO.Parquet.Writer.Metadata (
    magic,
    mkColumnChunk,
    mkDataPageHeader,
    mkRowGroup,
    mkSchemaElem,
    rootSchemaElement,
    writeFooter,
 )
import DataFrame.IO.Parquet.Writer.Options (
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
 )
import DataFrame.IO.Utils.RandomAccess (
    MemoryBuffer (..),
    WritableBinaryHandle,
    atomicallyWriteFile,
    bufferResidency,
    bufferToByteString,
    ensureCapacity,
    flushBufferToBuffer,
    flushBufferToFile,
    mallocBuffer,
    resetPosition,
    withWritableBinaryFile,
    writeByteString,
    writeByteStringToFile,
    writeWord32LE,
 )
import DataFrame.Internal.Column (Column, columnTypeString, hasMissing)
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    dataframeDimensions,
    getColumn,
 )
import qualified Pinch
import qualified Snappy
import System.Directory (createDirectoryIfMissing)
import System.FilePath (takeDirectory)
import Text.Printf (printf)

data ParquetWriterState = ParquetWriterState
    { outputFileHandle :: !WritableBinaryHandle
    , columnChunks :: !(VB.Vector ColumnChunkState)
    , currentFileOffsetRef :: !(IORef Int64)
    , scratchBuffer :: !MemoryBuffer
    , rowGroupMetadataRef :: !(IORef [RowGroup])
    , rowNumberRef :: !(IORef Int)
    }

data ColumnChunkState = ColumnChunkState
    { columnName :: !T.Text
    , nullable :: !Bool
    , schema :: !SchemaElement
    , encoder :: !Encoder
    , buffer :: !MemoryBuffer
    , uncompressedBufferSize :: !(IORef Int64)
    , pageState :: !PageState
    }

data PageState = PageState
    { pageBuffer :: !MemoryBuffer
    , definitionLevels :: !DefLevels
    , currentRowCount :: !(IORef Int)
    }

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions options path df = do
    when (options.strategy == TwoPass) $
        error
            "The Two Pass Strategy for the Parquet Writer has not yet been implemented"
    case options.compressionCodec of
        UNCOMPRESSED _ -> pure ()
        SNAPPY _ -> pure ()
        other -> error ("writeParquet: unsupported codec " <> show other)
    let (totalRows, _) = dataframeDimensions df
    case options.maxRowsPerFile of
        Nothing -> do
            when (isShardPattern path) $
                error
                    ( "writeParquet: path "
                        <> show path
                        <> " contains a '*' placeholder but maxRowsPerFile is not set"
                    )
            writeShard options path df 0 totalRows
        Just rowsPerFile -> do
            when (rowsPerFile <= 0) $
                error "writeParquet: maxRowsPerFile must be positive"
            unless (isShardPattern path) $
                error
                    ( "writeParquet: maxRowsPerFile requires a path with a '*' placeholder, got "
                        <> show path
                    )
            let starts = case [0, rowsPerFile .. totalRows - 1] of
                    [] -> [0] -- empty frame still produces one (empty) shard
                    ss -> ss
            forM_ (zip [0 ..] starts) $ \(shardIndex, start) -> do
                let shardPath = shardPathFor path shardIndex
                createDirectoryIfMissing True (takeDirectory shardPath)
                writeShard options shardPath df start (min totalRows (start + rowsPerFile))

isShardPattern :: FilePath -> Bool
isShardPattern = elem '*'

-- | Replace every @*@ in the pattern with a zero-padded shard index.
shardPathFor :: FilePath -> Int -> FilePath
shardPathFor pattern_ shardIndex =
    concatMap (\c -> if c == '*' then printf "%05d" shardIndex else [c]) pattern_

-- | Write rows @[startRow, endRow)@ of the frame to a single Parquet file.
writeShard ::
    ParquetWriteOptions -> FilePath -> DataFrame -> Int -> Int -> IO ()
writeShard options path_ df startRow endRow = do
    let names = columnNames df
        shardRows = max 0 (endRow - startRow)
    columnChunks_ <-
        VB.fromList
            <$> mapM
                ( \columnName_ ->
                    initColumnChunkState
                        options
                        columnName_
                        (fromJust (getColumn columnName_ df))
                )
                names
    scratchBuffer_ <- mallocBuffer (max 1 options.pageSize)
    atomicallyWriteFile path_ $ \path -> withWritableBinaryFile path $ \output -> do
        writeByteStringToFile output magic
        currentFileOffsetRef_ <- newIORef 4
        rowGroupMetadataRef_ <- newIORef []
        rowNumberRef_ <- newIORef 0
        let writerState =
                ParquetWriterState
                    output
                    columnChunks_
                    currentFileOffsetRef_
                    scratchBuffer_
                    rowGroupMetadataRef_
                    rowNumberRef_
            interval = max 1 options.batchRows
            subBatch = max 1 options.subBatchRows
            writeBatch :: Int -> Int -> IO ()
            writeBatch rowNum batchEnd
                | rowNum >= batchEnd = pure ()
                | otherwise = do
                    let count = min subBatch (batchEnd - rowNum)
                    VB.forM_ columnChunks_ (writeRows options scratchBuffer_ rowNum count)
                    modifyIORef' rowNumberRef_ (+ count)
                    writeBatch (rowNum + count) batchEnd
            loop :: Int -> IO ()
            loop rowNum
                | rowNum >= endRow = pure ()
                | otherwise = do
                    let batchEnd = rowNum + min interval (endRow - rowNum)
                    writeBatch rowNum batchEnd
                    size <- bufferedSize columnChunks_
                    when (size >= options.rowGroupSize) (flushRowGroup options writerState)
                    loop batchEnd
        loop startRow
        flushRowGroup options writerState
        rowGroupMetadata <- reverse <$> readIORef rowGroupMetadataRef_
        let schemaElements =
                rootSchemaElement (VB.length columnChunks_)
                    : VB.toList (VB.map schema columnChunks_)
        writeFooter
            output
            schemaElements
            shardRows
            rowGroupMetadata
            (nativeTypeKeyValues names df)

nativeTypeKeyPrefix :: T.Text
nativeTypeKeyPrefix = "dataframe.type."

-- | The type stamp for every column of @df@, as footer key-value pairs.
nativeTypeKeyValues :: [T.Text] -> DataFrame -> [(T.Text, T.Text)]
nativeTypeKeyValues names df =
    [ (nativeTypeKeyPrefix <> name, T.pack (columnTypeString col))
    | name <- names
    , Just col <- [getColumn name df]
    ]

writeRows ::
    ParquetWriteOptions -> MemoryBuffer -> Int -> Int -> ColumnChunkState -> IO ()
writeRows options scratch firstRow count ccs = do
    let page = ccs.pageState
        buf = page.pageBuffer
        encode = ccs.encoder.encodeValue
        dl = page.definitionLevels
        end = firstRow + count

    pos0 <- readIORef buf.positionRef
    let margin = options.pageSize
    arr0 <- ensureCapacity buf (pos0 + max margin (count * 64))
    size0 <- getSizeofMutableByteArray arr0

    let go !size !pos !row
            | row >= end = writeIORef buf.positionRef pos
            | pos + margin > size = do
                -- Rare: buffer nearly full, grow it
                writeIORef buf.positionRef pos
                arr' <- ensureCapacity buf (pos + max margin ((end - row) * 64))
                size' <- getSizeofMutableByteArray arr'
                go size' pos row
            | otherwise = do
                (pos', notNull) <- encode buf pos row
                when ccs.nullable $
                    pushDef dl (if notNull then 1 else 0)
                go size pos' (row + 1)

    go size0 pos0 firstRow

    -- Batch bookkeeping: once per sub-batch instead of per value
    modifyIORef' page.currentRowCount (+ count)
    flushDef dl
    pageRes <- bufferResidency buf
    defRes <- bufferResidency dl.dlBuf
    when
        (pageRes + defRes >= options.pageSize)
        (flushPage options scratch ccs)

flushPage :: ParquetWriteOptions -> MemoryBuffer -> ColumnChunkState -> IO ()
flushPage options scratch columnChunkState = do
    let page = columnChunkState.pageState
    numPageRows <- readIORef page.currentRowCount
    when (numPageRows > 0) $ do
        pos <- readIORef page.pageBuffer.positionRef
        pos' <- columnChunkState.encoder.finishValues page.pageBuffer pos
        writeIORef page.pageBuffer.positionRef pos'
        body <- assemblePageBody scratch columnChunkState
        writeDataPage options.compressionCodec numPageRows body columnChunkState
        resetPosition page.pageBuffer
        resetPosition page.definitionLevels.dlBuf
        resetPosition scratch
        writeIORef page.currentRowCount 0

assemblePageBody :: MemoryBuffer -> ColumnChunkState -> IO MemoryBuffer
assemblePageBody scratch columnChunkState
    | not columnChunkState.nullable = pure columnChunkState.pageState.pageBuffer
    | otherwise = do
        let page = columnChunkState.pageState
        flushDef page.definitionLevels
        resetPosition scratch
        defLevelsSize <- bufferResidency page.definitionLevels.dlBuf
        writeWord32LE scratch (fromIntegral defLevelsSize)
        flushBufferToBuffer page.definitionLevels.dlBuf scratch
        flushBufferToBuffer page.pageBuffer scratch
        pure scratch

writeDataPage ::
    CompressionCodec -> Int -> MemoryBuffer -> ColumnChunkState -> IO ()
writeDataPage codec numPageRows body columnChunkState = do
    uncompressedPageSize <- bufferResidency body
    compressedBody <- case codec of
        UNCOMPRESSED _ -> pure Nothing
        SNAPPY _ -> Just . Snappy.compress <$> bufferToByteString body
        other -> error ("writeParquet: unsupported codec " <> show other)
    let compressedPageSize = maybe uncompressedPageSize BS.length compressedBody
        headerBytes =
            Pinch.encode
                Pinch.compactProtocol
                (mkDataPageHeader numPageRows uncompressedPageSize compressedPageSize)
    writeByteString columnChunkState.buffer headerBytes
    case compressedBody of
        Nothing -> flushBufferToBuffer body columnChunkState.buffer
        Just bytes -> writeByteString columnChunkState.buffer bytes
    modifyIORef'
        columnChunkState.uncompressedBufferSize
        (+ fromIntegral (BS.length headerBytes + uncompressedPageSize))

flushRowGroup :: ParquetWriteOptions -> ParquetWriterState -> IO ()
flushRowGroup options writerState = do
    rowNumber <- readIORef writerState.rowNumberRef
    when (rowNumber > 0) $ do
        VB.forM_
            writerState.columnChunks
            (flushPage options writerState.scratchBuffer)
        (reversedColumnChunks, totalCompressed, totalUncompressed) <-
            VB.foldM'
                ( \(acc, totalCompressedSize, totalUncompressedSize) columnChunkState -> do
                    offset <- readIORef writerState.currentFileOffsetRef
                    compressedSize <- bufferResidency columnChunkState.buffer
                    uncompressedSize <- readIORef columnChunkState.uncompressedBufferSize
                    flushBufferToFile writerState.outputFileHandle columnChunkState.buffer
                    writeIORef
                        writerState.currentFileOffsetRef
                        (offset + fromIntegral compressedSize)
                    writeIORef columnChunkState.uncompressedBufferSize 0
                    let columnChunk =
                            mkColumnChunk
                                options.compressionCodec
                                columnChunkState.encoder.encType
                                columnChunkState.columnName
                                offset
                                compressedSize
                                uncompressedSize
                                rowNumber
                    pure
                        ( columnChunk : acc
                        , totalCompressedSize + fromIntegral compressedSize
                        , totalUncompressedSize + uncompressedSize
                        )
                )
                ([], 0 :: Int64, 0 :: Int64)
                writerState.columnChunks
        modifyIORef'
            writerState.rowGroupMetadataRef
            ( mkRowGroup
                (reverse reversedColumnChunks)
                totalCompressed
                totalUncompressed
                rowNumber
                :
            )
        writeIORef writerState.rowNumberRef 0

bufferedSize :: VB.Vector ColumnChunkState -> IO Int
bufferedSize =
    VB.foldM'
        ( \total columnChunkState -> do
            chunkSize <- bufferResidency columnChunkState.buffer
            valuesSize <- bufferResidency columnChunkState.pageState.pageBuffer
            defLevelsSize <-
                bufferResidency columnChunkState.pageState.definitionLevels.dlBuf
            pure (total + chunkSize + valuesSize + defLevelsSize)
        )
        0

initColumnChunkState ::
    ParquetWriteOptions -> T.Text -> Column -> IO ColumnChunkState
initColumnChunkState options columnName_ column = do
    encoder_ <- buildEncoder column
    let nullable_ = hasMissing column
        schema_ =
            mkSchemaElem
                columnName_
                encoder_.encType
                nullable_
                encoder_.convertedType
                encoder_.logicalType
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
    pageState_ <- initPageState bufferSize
    pure
        ColumnChunkState
            { columnName = columnName_
            , nullable = nullable_
            , schema = schema_
            , encoder = encoder_
            , buffer = buffer_
            , uncompressedBufferSize = uncompressedBufferSize_
            , pageState = pageState_
            }

initPageState :: Int -> IO PageState
initPageState bufferSize = do
    pageBuffer_ <- mallocBuffer bufferSize
    definitionLevels_ <- newDefLevels
    currentRowCount_ <- newIORef 0
    pure
        PageState
            { pageBuffer = pageBuffer_
            , definitionLevels = definitionLevels_
            , currentRowCount = currentRowCount_
            }
