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
    writeRowAndMaybeFinalize,
    maybeFinalizePage,
    finalizePage,
    bufferedSize,
) where

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (..))
import qualified Data.ByteString as BS
import Data.IORef (IORef, modifyIORef', newIORef)
import Data.Int (Int64)
import qualified Data.Text as T
import qualified Data.Vector as VB
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Writer.DefLevels (DefLevels (..), pushDef)
import DataFrame.IO.Parquet.Writer.Encoder (Encoder (..), buildEncoder)
import DataFrame.IO.Parquet.Writer.Metadata (mkDataPageHeader, mkSchemaElem)
import DataFrame.IO.Parquet.Writer.Options (ParquetWriteOptions (..))
import DataFrame.IO.Parquet.Writer.PageWriter (
    PageState (..),
    PageWriter (..),
    assemblePageBody,
    newPageState,
    pageRows,
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
    writeBytes bytes = ColumnChunkWriter (runReaderIO (writeBytes bytes) . ckBuffer)
    flushTo sink = ColumnChunkWriter (runReaderIO (flushTo sink) . ckBuffer)

page :: PageWriter a -> ColumnChunkWriter a
page (PageWriter f) = ColumnChunkWriter (f . ckPage)

askColumnChunk :: ColumnChunkWriter ColumnChunkState
askColumnChunk = ColumnChunkWriter pure

initColumnState ::
    ParquetWriteOptions -> T.Text -> Column -> IO ColumnChunkState
initColumnState opts name col = do
    encoder <- buildEncoder col
    let nullable = hasMissing col
        schemaElem =
            mkSchemaElem
                name
                encoder.encType
                nullable
                encoder.encConverted
                encoder.encLogical
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
writeRow row = ColumnChunkWriter (writeRowIO row)

writeRowIO :: Int -> ColumnChunkState -> IO ()
writeRowIO row st = do
    let pageState = ckPage st
    present <- encWriteValue (ckEncoder st) pageState.psValues row
    when (ckNullable st) (pushDef pageState.psDefs (if present then 1 else 0))
    modifyIORef' pageState.psRows (+ 1)
{-# INLINE writeRowIO #-}

writeRowAndMaybeFinalize ::
    ParquetWriteOptions -> Int -> ColumnChunkState -> IO ()
writeRowAndMaybeFinalize opts row st = do
    writeRowIO row st
    size <- bufferResidency st.ckPage.psValues
    when
        (size >= opts.pageSize)
        (runColumnChunkWriter (finalizePage opts.compressionCodec) st)
{-# INLINE writeRowAndMaybeFinalize #-}

maybeFinalizePage :: ParquetWriteOptions -> ColumnChunkWriter ()
maybeFinalizePage opts = do
    size <- page residency
    when (size >= opts.pageSize) (finalizePage opts.compressionCodec)

finalizePage :: CompressionCodec -> ColumnChunkWriter ()
finalizePage codec = do
    st <- askColumnChunk
    rows <- page pageRows
    when (rows > 0) $ do
        liftIO (encFinishValues (ckEncoder st) st.ckPage.psValues)
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
    let headerBytes =
            Pinch.encode
                Pinch.compactProtocol
                (mkDataPageHeader rows uncompressedSize compressedSize)
    putByteString headerBytes
    emit
    bumpUncompressed (fromIntegral (BS.length headerBytes + uncompressedSize))

bumpUncompressed :: Int64 -> ColumnChunkWriter ()
bumpUncompressed n = ColumnChunkWriter (\st -> modifyIORef' (ckUncompressed st) (+ n))

bufferedSize :: VB.Vector ColumnChunkState -> IO Int
bufferedSize =
    VB.foldM'
        ( \total st -> do
            chunk <- bufferResidency (ckBuffer st)
            values <- bufferResidency (psValues (ckPage st))
            defs <- bufferResidency (ckPage st).psDefs.dlBuf
            pure (total + chunk + values + defs)
        )
        0
