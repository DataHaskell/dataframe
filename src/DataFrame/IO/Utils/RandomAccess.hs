{-# LANGUAGE FlexibleInstances #-}

module DataFrame.IO.Utils.RandomAccess where

import Data.ByteString (ByteString, hGet)
import Data.ByteString.Internal (ByteString (PS))
import Data.Functor ((<&>))
import qualified Data.Vector.Storable as VS
import Data.Word (Word8)
import Foreign (castForeignPtr)
import System.IO (
    Handle,
    SeekMode (AbsoluteSeek, SeekFromEnd),
    hFileSize,
    hSeek,
 )
import System.IO.MMap (
    Mode (ReadOnly),
    mmapFileForeignPtr,
 )
import Control.Monad.IO.Class (MonadIO(..))

uncurry_ :: (a -> b -> c -> d) -> (a, b, c) -> d
uncurry_ f (a, b, c) = f a b c

mmapFileVector :: FilePath -> IO (VS.Vector Word8)
mmapFileVector filepath =
    mmapFileForeignPtr filepath ReadOnly Nothing
        <&> uncurry_ VS.unsafeFromForeignPtr

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

type LocalFile = ReaderIO Handle

instance RandomAccess LocalFile where
    readBytes (Range offset length) = ReaderIO $ \handle -> do
        hSeek handle AbsoluteSeek offset
        hGet handle length
    readSuffix n = ReaderIO $ \handle -> do
        hGet handle n
        nMax <- hFileSize handle
        let n' = min (fromIntegral nMax) n
        hSeek handle SeekFromEnd (negate $ fromIntegral n')
        hGet handle n'

type MMappedFile = ReaderIO (VS.Vector Word8)

instance RandomAccess MMappedFile where
    readBytes (Range offset length) =
        ReaderIO $
            pure . unsafeToByteString . VS.slice (fromInteger offset) length
    readSuffix n =
        ReaderIO $ \v ->
            let len = VS.length v
                n' = min n len
                start = len - n'
             in pure . unsafeToByteString $ VS.slice start n' v

unsafeToByteString :: VS.Vector Word8 -> ByteString
unsafeToByteString v = PS (castForeignPtr ptr) offset len
  where
    (ptr, offset, len) = VS.unsafeToForeignPtr v
