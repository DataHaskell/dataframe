{- | This module contains low-level utilities around file seeking.

later this module can be renamed / moved to an internal module.
-}
module DataFrame.IO.Parquet.Seeking (
    SeekableHandle (getSeekableHandle),
    SeekMode (..),
    FileBufferedOrSeekable (..),
    ForceNonSeekable,
    mkFileBufferedOrSeekable,
    mkSeekableHandle,
    readLastBytes,
    withFileBufferedOrSeekable,
    fSeek,
    fGet,
) where

import Control.Monad
import qualified Data.ByteString as BS
import Data.ByteString.Unsafe (unsafeDrop, unsafeTake)
import Data.IORef
import Data.Int
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

{- | Read the last @n@ bytes, useful for reading metadata without loading the
entire file. Uses 'BS.hGet' (not @hGetContents@) so the handle stays open for
the subsequent column-chunk reads.
-}
readLastBytes :: Integer -> FileBufferedOrSeekable -> IO BS.ByteString
readLastBytes n (FileSeekable sh) = do
    let h = getSeekableHandle sh
    hSeek h SeekFromEnd (negate n)
    BS.hGet h (fromIntegral n)
readLastBytes n (FileBuffered i bs) = do
    writeIORef i (fromIntegral $ BS.length bs)
    when (n > fromIntegral (BS.length bs)) $ error "lastBytes: n > length bs"
    pure $ BS.drop (BS.length bs - fromIntegral n) bs

fSeek :: FileBufferedOrSeekable -> SeekMode -> Integer -> IO ()
fSeek (FileSeekable (SeekableHandle h)) seekMode seekTo = hSeek h seekMode seekTo
fSeek (FileBuffered i _bs) AbsoluteSeek seekTo = writeIORef i (fromIntegral seekTo)
fSeek (FileBuffered i _bs) RelativeSeek seekTo = modifyIORef' i (+ fromIntegral seekTo)
fSeek (FileBuffered i bs) SeekFromEnd seekTo = writeIORef i (fromIntegral $ BS.length bs + fromIntegral seekTo)

fGet :: FileBufferedOrSeekable -> Int -> IO BS.ByteString
fGet (FileSeekable (SeekableHandle h)) n = BS.hGet h n
fGet (FileBuffered iRef bs) n
    | n == 0 = pure BS.empty
    | n > 0 = do
        i <- fromIntegral <$> readIORef iRef
        if (BS.length bs - i) < n
            then if i <= BS.length bs then pure $ unsafeDrop i bs else pure BS.empty
            else pure . unsafeTake n . unsafeDrop i $ bs
    | otherwise = error "Can't read a negative number of bytes"
