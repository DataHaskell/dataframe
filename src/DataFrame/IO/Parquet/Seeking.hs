{- | This module contains low-level utilities around file seeking

potentially also contains all Streamly related low-level utilities.

later this module can be renamed / moved to an internal module.
-}
module DataFrame.IO.Parquet.Seeking (
    SeekableHandle (getSeekableHandle),
    SeekMode (..),
    FileBufferedOrSeekable (..),
    ForceNonSeekable,
    advanceBytes,
    mkFileBufferedOrSeekable,
    mkSeekableHandle,
    readLastBytes,
    seekAndReadBytes,
    seekAndStreamBytes,
    withFileBufferedOrSeekable,
) where

import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.Word
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as S
import qualified Streamly.External.ByteString as SBS
import qualified Streamly.FileSystem.Handle as SHandle
import System.IO

{- | This handle carries a proof that it must be seekable.
Note: Handle and SeekableHandle are not thread safe, should not be
shared across threads, beaware when running parallel/concurrent code.

Not seekable:
  - stdin / stdout
  - pipes / FIFOs

But regular files are always seekable. Parquet fundamentally wants random
access, a non-seekable source will not support effecient access without
buffering the entire file.
-}
newtype SeekableHandle = SeekableHandle {getSeekableHandle :: Handle}

{- | If we truely want to support non-seekable files, we need to also consider the case
to buffer the entire file in memory.

Not thread safe, contains mutable reference (as Handle already is).

If we need concurrent / parallel parsing or something, we need to read into ByteString
first, not sharing the same handle.
-}
data FileBufferedOrSeekable
    = FileBuffered !(IORef Int64) !BS.ByteString
    | FileSeekable !SeekableHandle

-- | Smart constructor for SeekableHandle
mkSeekableHandle :: Handle -> IO (Maybe SeekableHandle)
mkSeekableHandle h = do
    seekable <- hIsSeekable h
    pure $ if seekable then Just (SeekableHandle h) else Nothing

-- | For testing only
type ForceNonSeekable = Maybe Bool

{- | Smart constructor for FileBufferedOrSeekable, tries to keep in the seekable case
if possible.
-}
mkFileBufferedOrSeekable ::
    ForceNonSeekable -> Handle -> IO FileBufferedOrSeekable
mkFileBufferedOrSeekable forceNonSeek h = do
    seekable <- hIsSeekable h
    if not seekable || forceNonSeek == Just True
        then FileBuffered <$> newIORef 0 <*> BS.hGetContents h
        else pure $ FileSeekable $ SeekableHandle h

{- | With / bracket pattern for FileBufferedOrSeekable

Warning: do not return the FileBufferedOrSeekable outside the scope of the action as
it will be closed.
-}
withFileBufferedOrSeekable ::
    ForceNonSeekable ->
    FilePath ->
    IOMode ->
    (FileBufferedOrSeekable -> IO a) ->
    IO a
withFileBufferedOrSeekable forceNonSeek path ioMode action = withFile path ioMode $ \h -> do
    fbos <- mkFileBufferedOrSeekable forceNonSeek h
    action fbos

-- | Read from the end, useful for reading metadata without loading entire file
readLastBytes :: Integer -> FileBufferedOrSeekable -> IO BS.ByteString
readLastBytes n (FileSeekable sh) = do
    let h = getSeekableHandle sh
    hSeek h SeekFromEnd (negate n)
    S.fold SBS.write (SHandle.read h)
readLastBytes n (FileBuffered i bs) = do
    writeIORef i (fromIntegral $ BS.length bs)
    when (n > fromIntegral (BS.length bs)) $ error "lastBytes: n > length bs"
    pure $ BS.drop (BS.length bs - fromIntegral n) bs

-- | Note: this does not guarantee n bytes (if it ends early)
advanceBytes :: Int -> FileBufferedOrSeekable -> IO BS.ByteString
advanceBytes = seekAndReadBytes Nothing

-- | Note: this does not guarantee n bytes (if it ends early)
seekAndReadBytes ::
    Maybe (SeekMode, Integer) -> Int -> FileBufferedOrSeekable -> IO BS.ByteString
seekAndReadBytes mSeek len f = seekAndStreamBytes mSeek len f >>= S.fold SBS.write

{- | Warning: the stream produced from this function accesses to the mutable handler.
if multiple streams are pulled from the same handler at the same time, chaos happen.
Make sure there is only one stream running at one time for each SeekableHandle,
and streams are not read again when they are not used anymore.
-}
seekAndStreamBytes ::
    (MonadIO m) =>
    Maybe (SeekMode, Integer) -> Int -> FileBufferedOrSeekable -> m (Stream m Word8)
seekAndStreamBytes mSeek len f = do
    liftIO $
        case mSeek of
            Nothing -> pure ()
            Just (seekMode, seekTo) -> fSeek f seekMode seekTo
    pure $ S.take len $ fRead f

fSeek :: FileBufferedOrSeekable -> SeekMode -> Integer -> IO ()
fSeek (FileSeekable (SeekableHandle h)) seekMode seekTo = hSeek h seekMode seekTo
fSeek (FileBuffered i bs) AbsoluteSeek seekTo = writeIORef i (fromIntegral seekTo)
fSeek (FileBuffered i bs) RelativeSeek seekTo = modifyIORef' i (+ fromIntegral seekTo)
fSeek (FileBuffered i bs) SeekFromEnd seekTo = writeIORef i (fromIntegral $ BS.length bs + fromIntegral seekTo)

fRead :: (MonadIO m) => FileBufferedOrSeekable -> Stream m Word8
fRead (FileSeekable (SeekableHandle h)) = SHandle.read h
fRead (FileBuffered i bs) = S.concatEffect $ do
    pos <- liftIO $ readIORef i
    pure $
        S.mapM
            ( \x -> do
                liftIO (modifyIORef' i (+ 1))
                pure x
            )
            (S.unfold SBS.reader (BS.drop (fromIntegral pos) bs))
