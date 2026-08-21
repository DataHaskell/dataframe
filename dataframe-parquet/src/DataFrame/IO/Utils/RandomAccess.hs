{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.IO.Utils.RandomAccess (
    uncurry3,
    Range (..),
    RandomAccess (..),
    ReaderIO (runReaderIO),
    LocalFile,
    MMappedFile,
    unsafeToByteString,
    WritableBinaryHandle,
    openWritableBinaryFile,
    withWritableBinaryFile,
    HasBuffer (..),
    Sink (..),
    MemoryBuffer (..),
    mallocBuffer,
    BufferHandle,
    withFileBuffer,
    appendByteString,
    appendGeneratedBytes,
    appendTextArraySlice,
    appendByteStringHandle,
    writeWord8,
    writeWord32LE,
    writeWord64LE,
    writeFloatLE,
    writeDoubleLE,
    bufferResidency,
    bufferToByteString,
    copyBufferInto,
    resetPosition,
    flushBufferToFile,
    writeByteStringToFile,
    MemoryWriter,
    onBuffer,
    putWord8,
    putWord32LE,
    putWord64LE,
    putFloatLE,
    putDoubleLE,
    putByteString,
    putGenerated,
    copyBuffer,
) where

import Control.Exception (bracket)
import Control.Monad (foldM)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Primitive (RealWorld)
import Control.Monad.ST (stToIO)
import Data.Bits (shiftR)
import qualified Data.ByteString as BS
import Data.ByteString.Internal (ByteString (PS), create)
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Foldable as Foldable
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Primitive.ByteArray (
    MutableByteArray,
    copyMutableByteArray,
    getSizeofMutableByteArray,
    mutableByteArrayContents,
    newPinnedByteArray,
    withMutableByteArrayContents,
    writeByteArray,
 )
import qualified Data.Text.Array as TA
import qualified Data.Vector.Storable as VS
import Data.Word (Word32, Word64, Word8)
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable,
    fGet,
    fSeek,
    readLastBytes,
 )
import Foreign (castForeignPtr, castPtr, copyBytes, plusPtr)
import GHC.Float (castDoubleToWord64, castFloatToWord32)
import System.IO (
    BufferMode (NoBuffering),
    Handle,
    IOMode (ReadWriteMode, WriteMode),
    SeekMode (AbsoluteSeek),
    hClose,
    hGetBuf,
    hPutBuf,
    hSeek,
    hSetBinaryMode,
    hSetBuffering,
    openBinaryFile,
 )

uncurry3 :: (a -> b -> c -> d) -> (a, b, c) -> d
uncurry3 f (a, b, c) = f a b c

data Range = Range {offset :: !Integer, length :: !Int} deriving (Eq, Show)

class (Monad m) => RandomAccess m where
    readBytes :: Range -> m ByteString
    readRanges :: [Range] -> m [ByteString]
    readRanges = mapM readBytes
    readSuffix :: Int -> m ByteString

newtype ReaderIO r a = ReaderIO {runReaderIO :: r -> IO a}

instance Functor (ReaderIO r) where
    fmap f (ReaderIO run) = ReaderIO $ fmap f . run

instance Applicative (ReaderIO r) where
    pure a = ReaderIO $ \_ -> pure a
    (ReaderIO fg) <*> (ReaderIO fa) = ReaderIO $ \r -> do
        a <- fa r
        g <- fg r
        pure (g a)

instance Monad (ReaderIO r) where
    return = pure
    (ReaderIO ma) >>= f = ReaderIO $ \r -> do
        a <- ma r
        runReaderIO (f a) r

instance MonadIO (ReaderIO r) where
    liftIO io = ReaderIO $ const io

type LocalFile = ReaderIO FileBufferedOrSeekable

instance RandomAccess LocalFile where
    readBytes (Range offset' length') = ReaderIO $ \handle -> do
        fSeek handle AbsoluteSeek offset'
        fGet handle length'
    readSuffix n = ReaderIO (readLastBytes $ fromIntegral n)

type MMappedFile = ReaderIO (VS.Vector Word8)

-- The instance exists but we don't have the means to mmap the file currently
instance RandomAccess MMappedFile where
    readBytes (Range offset' length') =
        ReaderIO $
            pure . unsafeToByteString . VS.slice (fromInteger offset') length'
    readSuffix n =
        ReaderIO $ \v ->
            let len = VS.length v
                n' = min n len
                start = len - n'
             in pure . unsafeToByteString $ VS.slice start n' v

unsafeToByteString :: VS.Vector Word8 -> ByteString
unsafeToByteString v = PS (castForeignPtr ptr) offset' len
  where
    (ptr, offset', len) = VS.unsafeToForeignPtr v

-- Writer Buffer -----------------------------------------------------------------

-- Refer to DataFrame.IO.Parquet.Writer for a justification of what we're doing here
-- There's some overlap here with what's going on in Seeking.hs, so, if this bothers
-- us, eventually someone will have to come back and reconcile the writer buffer
-- approach with the reader oriented patterns in Seeking.hs.
--
-- We're using MutableByteArrays here for convenience and because we don't need
-- the more powerful abstractions vector provides (which uses ByteArrays internally)
--
-- since we want to use hPutBuf, we're going to need a Ptr, which means are ByteArrya
-- must be pinned. Now growing pinned arrays can be problematic, but in the vast majority
-- of cases we shouldn't be growing more than once, if that. See the docs for
-- Data.Primitive.ByteArray.byteArrayContents.

newtype WritableBinaryHandle = WritableBinaryHandle {unHandle :: Handle}

openWritableBinaryFile :: FilePath -> IO WritableBinaryHandle
openWritableBinaryFile filepath = do
    h <- openBinaryFile filepath WriteMode
    hSetBinaryMode h True
    hSetBuffering h NoBuffering
    pure . WritableBinaryHandle $ h

withWritableBinaryFile :: FilePath -> (WritableBinaryHandle -> IO a) -> IO a
withWritableBinaryFile filepath =
    bracket
        (openWritableBinaryFile filepath)
        (hClose . unHandle)

class (Monad m) => HasBuffer m where
    type Buffer m
    askBuffer :: m (Buffer m)
    residency :: m Int -- number of bytes currently in the buffer
    writeBytes :: (Foldable f) => f Word8 -> m ()
    flushTo :: Sink -> m ()

data MemoryBuffer = MemoryBuffer
    { arrayRef :: !(IORef (MutableByteArray RealWorld))
    , positionRef :: !(IORef Int)
    }

mallocBuffer :: Int -> IO MemoryBuffer
mallocBuffer capacity
    | capacity < 0 = ioError $ userError "mallocBuffer: negative capacity"
    | otherwise = do
        array <- newPinnedByteArray capacity
        MemoryBuffer <$> newIORef array <*> newIORef 0

data Sink = MemorySink MemoryBuffer | FileSink WritableBinaryHandle

instance HasBuffer (ReaderIO MemoryBuffer) where
    type Buffer (ReaderIO MemoryBuffer) = MemoryBuffer

    askBuffer = ReaderIO pure

    residency = ReaderIO $ \buffer -> readIORef buffer.positionRef

    writeBytes bytes = ReaderIO $ \buffer -> do
        position <- readIORef buffer.positionRef
        array <- ensureCapacity buffer (position + Foldable.length bytes)
        newPosition <-
            foldM (\i byte -> writeByteArray array i byte >> pure (i + 1)) position bytes
        writeIORef buffer.positionRef newPosition

    flushTo (MemorySink destination) = ReaderIO $ \source ->
        if destination.arrayRef == source.arrayRef
            then pure ()
            else do
                sourceArray <- readIORef source.arrayRef
                sourcePosition <- readIORef source.positionRef
                destinationPosition <- readIORef destination.positionRef
                let newDestinationPosition = destinationPosition + sourcePosition
                destinationArray <- ensureCapacity destination newDestinationPosition
                copyMutableByteArray
                    destinationArray
                    destinationPosition
                    sourceArray
                    0 -- offset
                    sourcePosition -- number of bytes
                writeIORef destination.positionRef newDestinationPosition
                writeIORef source.positionRef 0

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
    flushTo (FileSink (WritableBinaryHandle h)) = ReaderIO $ \buffer -> do
        array <- readIORef buffer.arrayRef
        position <- readIORef buffer.positionRef
        withMutableByteArrayContents array $ \ptr -> do
            let chunkSize = 262144
                go offset
                    | offset >= position = pure ()
                    | otherwise = do
                        let n = min chunkSize (position - offset)
                        hPutBuf h (ptr `plusPtr` offset) n
                        go (offset + n)
            go 0
        writeIORef buffer.positionRef 0

-- We're using pinned ByteArrays so we must
-- not use the grow fuynction brovided by primitive
-- instead we must alloocatie a new pinned byteArray.
-- We might have been worried about heap fragmentation
-- becasue a single pinned object in a 4KB GHC block can
-- keep the whole plock alive but oyr buffers will tend to
-- be much larger than that.
-- But the memory useage will temporarily spike to 2.5x the size of
-- the buffer, but it should be fine since the current writer is single threaded
-- and grows *should* be rare (we also allocate a little extra space to
-- begin with).
-- If it becomes an issue we should start tracking an array of pointers
-- to buffers intsead of replacing them wholesale so grwoing a buffer
-- is just a matter of adding a new buffer to the array (which we can
-- pre-allocate to three elements to begin with and grow it only on the
-- off chance that a buffer required more than three grows). The extra
-- ceremony of handling writes and flushes can be encapsulated well in
-- HasBuffer instances.
ensureCapacity :: MemoryBuffer -> Int -> IO (MutableByteArray RealWorld)
ensureCapacity buffer needed = do
    array <- readIORef buffer.arrayRef
    maxSize <- getSizeofMutableByteArray array
    if needed <= maxSize
        then pure array
        else do
            position <- readIORef buffer.positionRef
            grown <- newPinnedByteArray (needed + (needed `div` 2))
            copyMutableByteArray grown 0 array 0 position
            writeIORef buffer.arrayRef grown
            pure grown

appendByteString :: MemoryBuffer -> ByteString -> IO ()
appendByteString buffer bs =
    BU.unsafeUseAsCStringLen bs $ \(source, len) -> do
        position <- readIORef buffer.positionRef
        array <- ensureCapacity buffer (position + len)
        withMutableByteArrayContents array $ \dst ->
            copyBytes (dst `plusPtr` position) (castPtr source) len
        writeIORef buffer.positionRef (position + len)

writeWord8 :: MemoryBuffer -> Word8 -> IO ()
writeWord8 buffer b = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 1)
    writeByteArray array position b
    writeIORef buffer.positionRef (position + 1)

writeWord32LE :: MemoryBuffer -> Word32 -> IO ()
writeWord32LE buffer w = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 4)
    writeByteArray array position (fromIntegral w :: Word8)
    writeByteArray array (position + 1) (fromIntegral (w `shiftR` 8) :: Word8)
    writeByteArray array (position + 2) (fromIntegral (w `shiftR` 16) :: Word8)
    writeByteArray array (position + 3) (fromIntegral (w `shiftR` 24) :: Word8)
    writeIORef buffer.positionRef (position + 4)

writeWord64LE :: MemoryBuffer -> Word64 -> IO ()
writeWord64LE buffer w = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 8)
    writeByteArray array position (fromIntegral w :: Word8)
    writeByteArray array (position + 1) (fromIntegral (w `shiftR` 8) :: Word8)
    writeByteArray array (position + 2) (fromIntegral (w `shiftR` 16) :: Word8)
    writeByteArray array (position + 3) (fromIntegral (w `shiftR` 24) :: Word8)
    writeByteArray array (position + 4) (fromIntegral (w `shiftR` 32) :: Word8)
    writeByteArray array (position + 5) (fromIntegral (w `shiftR` 40) :: Word8)
    writeByteArray array (position + 6) (fromIntegral (w `shiftR` 48) :: Word8)
    writeByteArray array (position + 7) (fromIntegral (w `shiftR` 56) :: Word8)
    writeIORef buffer.positionRef (position + 8)

writeFloatLE :: MemoryBuffer -> Float -> IO ()
writeFloatLE buffer = writeWord32LE buffer . castFloatToWord32

writeDoubleLE :: MemoryBuffer -> Double -> IO ()
writeDoubleLE buffer = writeWord64LE buffer . castDoubleToWord64

copyBufferInto :: MemoryBuffer -> MemoryBuffer -> IO ()
copyBufferInto destination source = do
    sourceArray <- readIORef source.arrayRef
    sourcePosition <- readIORef source.positionRef
    destinationPosition <- readIORef destination.positionRef
    destinationArray <-
        ensureCapacity destination (destinationPosition + sourcePosition)
    copyMutableByteArray
        destinationArray
        destinationPosition
        sourceArray
        0
        sourcePosition
    writeIORef destination.positionRef (destinationPosition + sourcePosition)

bufferToByteString :: MemoryBuffer -> IO ByteString
bufferToByteString buffer = do
    array <- readIORef buffer.arrayRef
    position <- readIORef buffer.positionRef
    create position $ \dst ->
        withMutableByteArrayContents array $ \src ->
            copyBytes dst (castPtr src) position

bufferResidency :: MemoryBuffer -> IO Int
bufferResidency buffer = readIORef buffer.positionRef

resetPosition :: MemoryBuffer -> IO ()
resetPosition buffer = writeIORef buffer.positionRef 0

appendGeneratedBytes :: MemoryBuffer -> Int -> (Int -> Word8) -> IO ()
appendGeneratedBytes buffer count at
    | count < 0 = ioError $ userError "appendGeneratedBytes: negative length"
    | otherwise = do
        position <- readIORef buffer.positionRef
        array <- ensureCapacity buffer (position + count)
        let go i
                | i >= count = pure ()
                | otherwise = writeByteArray array (position + i) (at i) >> go (i + 1)
        go 0
        writeIORef buffer.positionRef (position + count)

appendTextArraySlice :: MemoryBuffer -> TA.Array -> Int -> Int -> IO ()
appendTextArraySlice buffer source offset count
    | count < 0 = ioError $ userError "appendTextArraySlice: negative length"
    | otherwise = do
        position <- readIORef buffer.positionRef
        array <- ensureCapacity buffer (position + count)
        withMutableByteArrayContents array $ \destination ->
            stToIO (TA.copyToPointer source offset (destination `plusPtr` position) count)
        writeIORef buffer.positionRef (position + count)
{-# INLINE appendTextArraySlice #-}

flushBufferToFile :: WritableBinaryHandle -> MemoryBuffer -> IO ()
flushBufferToFile handle = runReaderIO (flushTo (FileSink handle))

writeByteStringToFile :: WritableBinaryHandle -> ByteString -> IO ()
writeByteStringToFile handle bs = do
    buffer <- mallocBuffer (max 1 (BS.length bs))
    appendByteString buffer bs
    flushBufferToFile handle buffer

type MemoryWriter m = (HasBuffer m, Buffer m ~ MemoryBuffer, MonadIO m)

onBuffer :: (MonadIO m) => MemoryBuffer -> ReaderIO MemoryBuffer a -> m a
onBuffer buffer action = liftIO (runReaderIO action buffer)

putWord8 :: (MemoryWriter m) => Word8 -> m ()
putWord8 value = askBuffer >>= \buffer -> liftIO (writeWord8 buffer value)

putWord32LE :: (MemoryWriter m) => Word32 -> m ()
putWord32LE value = askBuffer >>= \buffer -> liftIO (writeWord32LE buffer value)

putWord64LE :: (MemoryWriter m) => Word64 -> m ()
putWord64LE value = askBuffer >>= \buffer -> liftIO (writeWord64LE buffer value)

putFloatLE :: (MemoryWriter m) => Float -> m ()
putFloatLE value = askBuffer >>= \buffer -> liftIO (writeFloatLE buffer value)

putDoubleLE :: (MemoryWriter m) => Double -> m ()
putDoubleLE value = askBuffer >>= \buffer -> liftIO (writeDoubleLE buffer value)

putByteString :: (MemoryWriter m) => ByteString -> m ()
putByteString bytes = askBuffer >>= \buffer -> liftIO (appendByteString buffer bytes)

putGenerated :: (MemoryWriter m) => Int -> (Int -> Word8) -> m ()
putGenerated count at = askBuffer >>= \buffer -> liftIO (appendGeneratedBytes buffer count at)

copyBuffer :: (MemoryWriter m) => MemoryBuffer -> m ()
copyBuffer source = askBuffer >>= \destination -> liftIO (copyBufferInto destination source)

data BufferHandle = BufferHandle
    { bufferPath :: !FilePath
    , bufferHandle :: !WritableBinaryHandle
    , residencyRef :: !(IORef Int)
    , flushedRef :: !(IORef Int)
    }

withFileBuffer :: FilePath -> (BufferHandle -> IO a) -> IO a
withFileBuffer filepath =
    bracket open (hClose . unHandle . bufferHandle)
  where
    open = do
        h <- openBinaryFile filepath ReadWriteMode
        hSetBinaryMode h True
        hSetBuffering h NoBuffering
        res <- newIORef 0
        flushed <- newIORef 0
        pure (BufferHandle filepath (WritableBinaryHandle h) res flushed)

instance HasBuffer (ReaderIO BufferHandle) where
    type Buffer (ReaderIO BufferHandle) = BufferHandle

    askBuffer = ReaderIO pure

    residency = ReaderIO $ \bh -> readIORef bh.residencyRef

    writeBytes bytes = ReaderIO $ \bh -> do
        let WritableBinaryHandle h = bh.bufferHandle
        scratch <- newPinnedByteArray 1
        n <-
            foldM
                ( \count byte -> do
                    writeByteArray scratch 0 byte
                    hPutBuf h (mutableByteArrayContents scratch) 1
                    pure (count + 1)
                )
                0
                bytes
        position <- readIORef bh.residencyRef
        writeIORef bh.residencyRef (position + n)

    -- Flushing from a file to a sink happens using
    -- a reusable 256 KiB buffer closed over this function
    -- Usually for this instance we shoulc be diong only
    -- file to file but file to buffer is also bossible
    -- if you should want to do that, for whatever reason
    -- (don't let me tell you how to live your life)
    flushTo sink = ReaderIO $ \bh -> do
        count <- readIORef bh.residencyRef
        offset <- readIORef bh.flushedRef
        let WritableBinaryHandle h = bh.bufferHandle
            chunkSize = 262144
        chunk <- newPinnedByteArray (min chunkSize count)
        let ptr = mutableByteArrayContents chunk
            pushChunk n = case sink of
                FileSink (WritableBinaryHandle out) -> hPutBuf out ptr n
                MemorySink dest -> do
                    destinationPosition <- readIORef dest.positionRef
                    let newDestinationPosition = destinationPosition + n
                    destinationArray <- ensureCapacity dest newDestinationPosition
                    copyMutableByteArray destinationArray destinationPosition chunk 0 n
                    writeIORef dest.positionRef newDestinationPosition
            go remaining
                | remaining <= 0 = pure ()
                | otherwise = do
                    actual <- hGetBuf h ptr (min chunkSize remaining)
                    if actual <= 0
                        then pure ()
                        else pushChunk actual >> go (remaining - actual)
        hSeek h AbsoluteSeek (fromIntegral offset)
        go count
        writeIORef bh.residencyRef 0
        writeIORef bh.flushedRef (offset + count)

appendByteStringHandle :: BufferHandle -> ByteString -> IO ()
appendByteStringHandle bh bs =
    BU.unsafeUseAsCStringLen bs $ \(source, len) -> do
        let WritableBinaryHandle h = bh.bufferHandle
        hPutBuf h source len
        position <- readIORef bh.residencyRef
        writeIORef bh.residencyRef (position + len)
