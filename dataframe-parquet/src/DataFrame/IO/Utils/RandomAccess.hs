{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.IO.Utils.RandomAccess where

import Control.Monad.IO.Class (MonadIO (..))
import Data.ByteString (ByteString)
import Data.ByteString.Internal (ByteString (PS), fromForeignPtr0)
import Data.ByteString.Builder (Builder, byteString)
import qualified Data.Vector.Storable as VS
import Data.Word (Word8)
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable,
    fGet,
    fSeek,
    readLastBytes,
 )
import Foreign (castForeignPtr)
import System.IO (
    SeekMode (AbsoluteSeek),
    Handle,
    WriteMode,
    withBinaryFile
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

openWritableBinaryFile :: (HasBuffer m) => FilePath -> m WritableBinaryHandle
openWritableBinaryFile filepath = liftIO $ do
  h <- openBinaryFile AppendMode
  hSetBinaryMode h True
  hSetBuffering h $ BlockBuffering (Just 65536)
  pure . WritableBinaryHandle $ h

withWritableBinaryFile :: (HasBuffer m) => FilePath -> (WriteableBinaryHandle -> m r) -> m r
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
  
data BufferPointer = BufferPointer
  { pointer :: !(IORef (ForeignPtr Word8)) -- Reallocatoble if we need to grow 
  , size :: !(IORef Int) -- Reallocatable if we need to grow it
  , cursor :: !(IORef Int)
  }

data MemoryBuffer = MemoryBuffer
  { arrayRef :: !(IORef (MutableByteArray RealWorld))
  , positionRef :: !(IORef Int)
  }

data Sink = MemorySink MemoryBuffer | FileSink WritableBinaryHandle

instance HasBuffer (ReaderIO MemoryBuffer) where
  type Buffer (ReaderIO MemoryBuffer) = MemoryBuffer

  askBuffer = ReaderIO id
  
  residency = ReaderIO $ \buffer -> readIORef buffer.positionRef
  
  writeBytes bytes = ReaderIO $ \buffer -> do
    position <- readIORef buffer.positionRef 
    array <- ensureCapacity buffer (position + length bytes)
    newPosition <- foldM (\i byte -> writeByteArray array i byte >> pure (i + 1)) res bytes
    writeIORef buffer.positionRef newPosition

  flushTo (MemorySink destination) = ReaderIO $ \source ->
    guard (destination /= source)
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

  flushTo (FileSink _) = undefined

ensureCapacity :: MemoryBuffer -> Int -> IO MutableByteArray
ensureCapacity buffer needed = do
  array <- readIORef buffer.arrayRef
  if needed <= sizeOfMutableByteArray array
  then pure array
  else do
    grown <- resizeMutableByteArray array (needed + (needed `div` 2))
    writeIORef buffer.arrayRef grown
    pure grown
    
data BufferHandle = BufferHandle
  { handle :: !WritableBinaryHandle
  , cursor :: !(IORef Int)
  }


