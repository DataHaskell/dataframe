{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.IO.Parquet.Writer.ColumnChunkWriter (
    ColumnChunkWriter (..),
    ColumnChunkState (..),
    page,
    askColumnChunk,
    initColumnState,
    writeRow,
    maybeFinalizePage,
    finalizePage,
    bufferedSize,
) where

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (..))
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import qualified Data.Text as T
import qualified Data.Vector as VB
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Writer.DefLevels (DefLevels (..))
import DataFrame.IO.Parquet.Writer.Encoder (Encoder (..), buildEncoder)
import DataFrame.IO.Parquet.Writer.Metadata (mkDataPageHeader, mkSchemaElem)
import DataFrame.IO.Parquet.Writer.Options (ParquetWriteOptions (..))
import DataFrame.IO.Parquet.Writer.PageWriter (
    PageState (..),
    PageWriter (..),
    assemblePageBody,
    bumpRows,
    newPageState,
    pageRows,
    recordDef,
    resetPage,
 )
import DataFrame.IO.Utils.RandomAccess (
    HasBuffer (..),
    MemoryBuffer,
    ReaderIO (runReaderIO),
    Sink (..),
    bufferResidency,
    bufferToByteString,
    copyBuffer,
    mallocBuffer,
    putByteString,
 )
import DataFrame.Internal.Column (Column, hasMissing)
import qualified Pinch
import qualified Snappy

data ColumnChunkState = ColumnChunkState
    { ckName :: !T.Text
    , ckNullable :: !Bool
    , ckSchema :: !SchemaElement
    , ckEncoder :: !Encoder
    , ckBuffer :: !MemoryBuffer
    , ckUncompressed :: !(IORef Int64)
    , ckPage :: !PageState
    }

newtype ColumnChunkWriter a = ColumnChunkWriter {runColumnChunkWriter :: ColumnChunkState -> IO a}

instance Functor ColumnChunkWriter where
    fmap f (ColumnChunkWriter g) = ColumnChunkWriter (fmap f . g)

instance Applicative ColumnChunkWriter where
    pure x = ColumnChunkWriter (const (pure x))
    ColumnChunkWriter f <*> ColumnChunkWriter g = ColumnChunkWriter (\s -> f s <*> g s)

instance Monad ColumnChunkWriter where
    ColumnChunkWriter m >>= k = ColumnChunkWriter (\s -> m s >>= \a -> runColumnChunkWriter (k a) s)

instance MonadIO ColumnChunkWriter where
    liftIO io = ColumnChunkWriter (const io)

instance HasBuffer ColumnChunkWriter where
    type Buffer ColumnChunkWriter = MemoryBuffer
    askBuffer = ColumnChunkWriter (pure . ckBuffer)
    residency = ColumnChunkWriter (bufferResidency . ckBuffer)
    writeBytes bytes = ColumnChunkWriter (\cs -> runReaderIO (writeBytes bytes) (ckBuffer cs))
    flushTo sink = ColumnChunkWriter (\cs -> runReaderIO (flushTo sink) (ckBuffer cs))

page :: PageWriter a -> ColumnChunkWriter a
page (PageWriter f) = ColumnChunkWriter (f . ckPage)

askColumnChunk :: ColumnChunkWriter ColumnChunkState
askColumnChunk = ColumnChunkWriter pure

initColumnState :: ParquetWriteOptions -> T.Text -> Column -> IO ColumnChunkState
initColumnState opts name col = do
    encoder <- buildEncoder col
    let nullable = hasMissing col
        schemaElem = mkSchemaElem name encoder.encType nullable encoder.encConverted encoder.encLogical
        cap = max 1 opts.pageSize
    chunk <- mallocBuffer cap
    uncompressed <- newIORef 0
    pageState <- newPageState cap nullable
    pure
        ColumnChunkState
            { ckName = name
            , ckNullable = nullable
            , ckSchema = schemaElem
            , ckEncoder = encoder
            , ckBuffer = chunk
            , ckUncompressed = uncompressed
            , ckPage = pageState
            }

writeRow :: Int -> ColumnChunkWriter ()
writeRow row = do
    st <- askColumnChunk
    present <- page (encWriteValue (ckEncoder st) row)
    page $ do
        when (ckNullable st) (recordDef present)
        bumpRows

maybeFinalizePage :: ParquetWriteOptions -> ColumnChunkWriter ()
maybeFinalizePage opts = do
    size <- page residency
    when (size >= opts.pageSize) (finalizePage opts.compressionCodec)

finalizePage :: CompressionCodec -> ColumnChunkWriter ()
finalizePage codec = do
    st <- askColumnChunk
    rows <- page pageRows
    when (rows > 0) $ do
        page (encFinishValues (ckEncoder st))
        body <- page (assemblePageBody (ckNullable st))
        writeDataPage codec rows body
        page resetPage

writeDataPage :: CompressionCodec -> Int -> MemoryBuffer -> ColumnChunkWriter ()
writeDataPage codec rows body = do
    uncompressedSize <- liftIO (bufferResidency body)
    (compressedSize, emit) <- case codec of
        UNCOMPRESSED _ -> pure (uncompressedSize, copyBuffer body)
        SNAPPY _ -> do
            compressed <- liftIO (Snappy.compress <$> bufferToByteString body)
            pure (BS.length compressed, putByteString compressed)
        other -> error ("writeParquet: unsupported codec " <> show other)
    let headerBytes = Pinch.encode Pinch.compactProtocol (mkDataPageHeader rows uncompressedSize compressedSize)
    putByteString headerBytes
    emit
    bumpUncompressed (fromIntegral (BS.length headerBytes + uncompressedSize))

bumpUncompressed :: Int64 -> ColumnChunkWriter ()
bumpUncompressed n = ColumnChunkWriter (\st -> modifyIORef' (ckUncompressed st) (+ n))

bufferedSize :: VB.Vector ColumnChunkState -> IO Int
bufferedSize =
    VB.foldM'
        (\total st -> do
            chunk <- bufferResidency (ckBuffer st)
            values <- bufferResidency (psValues (ckPage st))
            defs <- bufferResidency (ckPage st).psDefs.dlBuf
            pure (total + chunk + values + defs)
        )
        0
