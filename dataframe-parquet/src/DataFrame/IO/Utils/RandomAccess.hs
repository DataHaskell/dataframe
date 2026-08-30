{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}

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
    atomicallyWriteFile,
    MemoryBuffer (..),
    mallocBuffer,
    writeByteString,
    appendTextArraySlice,
    writeWord8,
    writeWord32LE,
    writeWord64LE,
    writeInteger64,
    writeFloatLE,
    writeDoubleLE,
    bufferResidency,
    bufferToByteString,
    flushBufferToBuffer,
    resetPosition,
    flushBufferToFile,
    writeByteStringToFile,
) where

import Control.Exception (bracket, bracketOnError, finally)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Primitive (RealWorld)
import Control.Monad.ST (stToIO)
import Data.Bits (shiftR)
import qualified Data.ByteString as BS
import Data.ByteString.Internal (ByteString (PS), create)
import qualified Data.ByteString.Unsafe as BU
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Primitive.ByteArray (
    MutableByteArray,
    copyMutableByteArray,
    getSizeofMutableByteArray,
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
import System.Directory (copyPermissions, doesFileExist, removeFile, renameFile)
import System.FilePath (takeDirectory)
import System.IO (
    BufferMode (NoBuffering),
    Handle,
    IOMode (WriteMode),
    SeekMode (AbsoluteSeek),
    hClose,
    hPutBuf,
    hSetBinaryMode,
    hSetBuffering,
    openBinaryFile,
    openBinaryTempFileWithDefaultPermissions,
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

atomicallyWriteFile :: FilePath -> (FilePath -> IO a) -> IO a
atomicallyWriteFile path action =
    bracketOnError
        openAction
        removeFile
        ( \tmpFile -> do
            result <- action tmpFile
            renameFile tmpFile path
            pure result
        )
  where
    openAction =
        bracketOnError
            ( openBinaryTempFileWithDefaultPermissions
                (takeDirectory path)
                "dataframe-parquet.incomplete"
            )
            (\(tmpFile, h) -> hClose h `finally` removeFile tmpFile)
            ( \(tmpFile, h) -> do
                hClose h
                destinationExists <- doesFileExist path
                when destinationExists (copyPermissions path tmpFile)
                pure tmpFile
            )

withWritableBinaryFile :: FilePath -> (WritableBinaryHandle -> IO a) -> IO a
withWritableBinaryFile filepath =
    bracket
        (openWritableBinaryFile filepath)
        (hClose . unHandle)

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
{-# INLINE ensureCapacity #-}

writeWord8 :: MemoryBuffer -> Word8 -> IO ()
writeWord8 buffer b = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 1)
    writeByteArray array position b
    writeIORef buffer.positionRef (position + 1)
{-# INLINE writeWord8 #-}

writeByteString :: MemoryBuffer -> ByteString -> IO ()
writeByteString buffer bs =
    BU.unsafeUseAsCStringLen bs $ \(source, len) -> do
        position <- readIORef buffer.positionRef
        array <- ensureCapacity buffer (position + len)
        withMutableByteArrayContents array $ \dst ->
            copyBytes (dst `plusPtr` position) (castPtr source) len
        writeIORef buffer.positionRef (position + len)
{-# INLINE writeByteString #-}

writeWord32LE :: MemoryBuffer -> Word32 -> IO ()
writeWord32LE buffer w = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 4)
    writeByteArray array position (fromIntegral w :: Word8)
    writeByteArray array (position + 1) (fromIntegral (w `shiftR` 8) :: Word8)
    writeByteArray array (position + 2) (fromIntegral (w `shiftR` 16) :: Word8)
    writeByteArray array (position + 3) (fromIntegral (w `shiftR` 24) :: Word8)
    writeIORef buffer.positionRef (position + 4)
{-# INLINE writeWord32LE #-}

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
{-# INLINE writeWord64LE #-}

writeInteger64 :: MemoryBuffer -> Integer -> IO ()
writeInteger64 buffer value
    | value < toInteger (minBound :: Int64) = outOfRange
    | value > toInteger (maxBound :: Int64) = outOfRange
    | otherwise = writeWord64LE buffer (fromIntegral value)
  where
    outOfRange = ioError (userError "writeParquet: Integer value is outside the INT64 range")
{-# INLINE writeInteger64 #-}

writeFloatLE :: MemoryBuffer -> Float -> IO ()
writeFloatLE buffer = writeWord32LE buffer . castFloatToWord32
{-# INLINE writeFloatLE #-}

writeDoubleLE :: MemoryBuffer -> Double -> IO ()
writeDoubleLE buffer = writeWord64LE buffer . castDoubleToWord64
{-# INLINE writeDoubleLE #-}

flushBufferToBuffer :: MemoryBuffer -> MemoryBuffer -> IO ()
flushBufferToBuffer source destination
    | source.arrayRef == destination.arrayRef = pure ()
    | otherwise = do
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
        writeIORef source.positionRef 0
{-# INLINE flushBufferToBuffer #-}

bufferToByteString :: MemoryBuffer -> IO ByteString
bufferToByteString buffer = do
    array <- readIORef buffer.arrayRef
    position <- readIORef buffer.positionRef
    create position $ \dst ->
        withMutableByteArrayContents array $ \src ->
            copyBytes dst (castPtr src) position

bufferResidency :: MemoryBuffer -> IO Int
bufferResidency buffer = readIORef buffer.positionRef
{-# INLINE bufferResidency #-}

resetPosition :: MemoryBuffer -> IO ()
resetPosition buffer = writeIORef buffer.positionRef 0
{-# INLINE resetPosition #-}

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
flushBufferToFile :: WritableBinaryHandle -> MemoryBuffer -> IO ()
flushBufferToFile (WritableBinaryHandle h) buffer = do
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

writeByteStringToFile :: WritableBinaryHandle -> ByteString -> IO ()
writeByteStringToFile (WritableBinaryHandle h) bs =
    BU.unsafeUseAsCStringLen bs $ \(source, len) -> do
        let chunkSize = 262144
            go offset
                | offset >= len = pure ()
                | otherwise = do
                    let n = min chunkSize (len - offset)
                    hPutBuf h (source `plusPtr` offset) n
                    go (offset + n)
        go 0

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
