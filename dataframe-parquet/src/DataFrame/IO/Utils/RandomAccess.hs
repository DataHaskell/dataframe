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
    appendByteStringHandle,
) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString.Internal (ByteString (PS))
import qualified Data.Foldable as Foldable
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Vector.Storable as VS
import Data.Word (Word8)
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable,
    fGet,
    fSeek,
    readLastBytes,
 )
import Control.Exception (bracket)
import Control.Monad (foldM)
import Control.Monad.Primitive (RealWorld)
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
import Foreign (castForeignPtr, castPtr, copyBytes, plusPtr)
import System.IO (
    BufferMode (NoBuffering),
    Handle,
    IOMode (AppendMode, ReadWriteMode),
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

newtype WritableBinaryHandle = WritableBinaryHandle { unHandle :: Handle }

openWritableBinaryFile :: FilePath -> IO WritableBinaryHandle
openWritableBinaryFile filepath = do
  h <- openBinaryFile filepath AppendMode
  hSetBinaryMode h True
  hSetBuffering h NoBuffering
  pure . WritableBinaryHandle $ h

withWritableBinaryFile :: FilePath -> (WritableBinaryHandle -> IO a) -> IO a
withWritableBinaryFile filepath action =
  bracket
    (openWritableBinaryFile filepath)
    (hClose . unHandle)
    action

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
    newPosition <- foldM (\i byte -> writeByteArray array i byte >> pure (i + 1)) position bytes
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
  maxSize <- getSizeofMutableByteArray array -- ensure sequencing in the presence of resizing
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

data BufferHandle = BufferHandle
  { bufferPath :: !FilePath
  , bufferHandle :: !WritableBinaryHandle
  , residencyRef :: !(IORef Int)
  , flushedRef :: !(IORef Int)
  }

withFileBuffer :: FilePath -> (BufferHandle -> IO a) -> IO a
withFileBuffer filepath action =
  bracket open (hClose . unHandle . bufferHandle) action
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
