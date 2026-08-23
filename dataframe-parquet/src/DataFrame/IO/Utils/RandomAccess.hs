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


writeWord8 :: MemoryBuffer -> Word8 -> IO ()
writeWord8 buffer b = do
    position <- readIORef buffer.positionRef
    array <- ensureCapacity buffer (position + 1)
    writeByteArray array position b
    writeIORef buffer.positionRef (position + 1)


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

