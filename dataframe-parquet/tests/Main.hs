{-# LANGUAGE ScopedTypeVariables #-}

-- | Tests for the writer-buffer logic in "DataFrame.IO.Utils.RandomAccess".

module Main where

import Control.Monad (forM)
import qualified Data.ByteString as BS
import Data.IORef (readIORef)
import Data.Primitive.ByteArray (readByteArray)
import Data.Word (Word8)
import System.FilePath ((</>))
import qualified System.Exit as Exit
import System.IO.Temp (withSystemTempDirectory)
import Test.HUnit

import DataFrame.IO.Utils.RandomAccess

withTempFileBuffer :: FilePath -> String -> (BufferHandle -> IO a) -> IO a
withTempFileBuffer dir name = withFileBuffer (dir </> name)

memoryBufferBytes :: MemoryBuffer -> IO [Word8]
memoryBufferBytes buf = do
    array <- readIORef (arrayRef buf)
    n <- readIORef (positionRef buf)
    forM [0 .. n - 1] (readByteArray array)

residencyTracksWrites :: Test
residencyTracksWrites = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir ->
        withTempFileBuffer dir "residency.bin" $ \bh -> do
            runReaderIO (writeBytes [1, 2, 3, 4, 5 :: Word8]) bh
            r1 <- runReaderIO residency bh
            assertEqual "residency after first write" 5 r1
            runReaderIO (writeBytes [6, 7, 8 :: Word8]) bh
            r2 <- runReaderIO residency bh
            assertEqual "residency accumulates across writes" 8 r2

-- Flushing a file buffer into a FileSink writes exactly the buffered bytes to
-- the output handle and empties the buffer.
flushToFileSink :: Test
flushToFileSink = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "out.bin"
            payload = [10, 20, 30, 40, 50, 60 :: Word8]
        withTempFileBuffer dir "buf.bin" $ \bh -> do
            runReaderIO (writeBytes payload) bh
            withWritableBinaryFile outPath $ \out ->
                runReaderIO (flushTo (FileSink out)) bh
            r <- runReaderIO residency bh
            assertEqual "residency reset after flush" 0 r
        contents <- BS.readFile outPath
        assertEqual "flushed file content" (BS.pack payload) contents

-- Flushing a file buffer into a MemorySink copies the buffered bytes into the
-- destination memory buffer and empties the source.
flushToMemorySink :: Test
flushToMemorySink = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let payload = [7, 6, 5, 4, 3, 2, 1 :: Word8]
        destination <- mallocBuffer 0
        withTempFileBuffer dir "buf.bin" $ \bh -> do
            runReaderIO (writeBytes payload) bh
            runReaderIO (flushTo (MemorySink destination)) bh
            r <- runReaderIO residency bh
            assertEqual "residency reset after flush" 0 r
        memBytes <- memoryBufferBytes destination
        assertEqual "flushed memory content" payload memBytes

-- Successive flushes into the same FileSink append rather than overwrite.
flushAppendsToFileSink :: Test
flushAppendsToFileSink = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "out.bin"
            chunkA = [1, 2, 3 :: Word8]
            chunkB = [4, 5, 6, 7 :: Word8]
        withWritableBinaryFile outPath $ \out -> do
            withTempFileBuffer dir "a.bin" $ \bh -> do
                runReaderIO (writeBytes chunkA) bh
                runReaderIO (flushTo (FileSink out)) bh
            withTempFileBuffer dir "b.bin" $ \bh -> do
                runReaderIO (writeBytes chunkB) bh
                runReaderIO (flushTo (FileSink out)) bh
        contents <- BS.readFile outPath
        assertEqual "appended flushes" (BS.pack (chunkA ++ chunkB)) contents

-- After a flush the buffer is emptied, so re-using it only re-flushes the
-- bytes written since the previous flush (old bytes are not resurrected).
flushEmptiesForReuse :: Test
flushEmptiesForReuse = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let firstPath = dir </> "first.bin"
            secondPath = dir </> "second.bin"
            chunkA = [11, 22, 33 :: Word8]
            chunkB = [44, 55 :: Word8]
        withTempFileBuffer dir "buf.bin" $ \bh -> do
            runReaderIO (writeBytes chunkA) bh
            withWritableBinaryFile firstPath $ \out ->
                runReaderIO (flushTo (FileSink out)) bh
            runReaderIO (writeBytes chunkB) bh
            r <- runReaderIO residency bh
            assertEqual "residency reflects only post-flush bytes" 2 r
            withWritableBinaryFile secondPath $ \out ->
                runReaderIO (flushTo (FileSink out)) bh
        first <- BS.readFile firstPath
        second <- BS.readFile secondPath
        assertEqual "first flush" (BS.pack chunkA) first
        assertEqual "second flush excludes first chunk" (BS.pack chunkB) second

-- A payload larger than the 256 KiB flush chunk round-trips intact, exercising
-- the chunked flush loop.
flushLargePayload :: Test
flushLargePayload = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "big.bin"
            payload = take 300000 (cycle [0 .. 255]) :: [Word8]
        withTempFileBuffer dir "big-buf.bin" $ \bh -> do
            runReaderIO (writeBytes payload) bh
            r <- runReaderIO residency bh
            assertEqual "residency for large payload" 300000 r
            withWritableBinaryFile outPath $ \out ->
                runReaderIO (flushTo (FileSink out)) bh
        contents <- BS.readFile outPath
        assertEqual "large payload round-trips" (BS.pack payload) contents

-- Memory buffer instance ------------------------------------------------------
-- The same observable contract as the file buffer, plus the memory-specific
-- self-flush no-op.

-- residency reports the running byte count and accumulates across writes.
memResidencyTracksWrites :: Test
memResidencyTracksWrites = TestCase $ do
    buf <- mallocBuffer 0
    runReaderIO (writeBytes [1, 2, 3, 4, 5 :: Word8]) buf
    r1 <- runReaderIO residency buf
    assertEqual "residency after first write" 5 r1
    runReaderIO (writeBytes [6, 7, 8 :: Word8]) buf
    r2 <- runReaderIO residency buf
    assertEqual "residency accumulates across writes" 8 r2

-- Flushing a memory buffer into a FileSink writes the buffered bytes to the
-- output handle and empties the buffer.
memFlushToFileSink :: Test
memFlushToFileSink = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "out.bin"
            payload = [10, 20, 30, 40, 50, 60 :: Word8]
        buf <- mallocBuffer 0
        runReaderIO (writeBytes payload) buf
        withWritableBinaryFile outPath $ \out ->
            runReaderIO (flushTo (FileSink out)) buf
        r <- runReaderIO residency buf
        assertEqual "residency reset after flush" 0 r
        contents <- BS.readFile outPath
        assertEqual "flushed file content" (BS.pack payload) contents

-- Flushing a memory buffer into another MemorySink copies the buffered bytes
-- into the destination and empties the source.
memFlushToMemorySink :: Test
memFlushToMemorySink = TestCase $ do
    let payload = [7, 6, 5, 4, 3, 2, 1 :: Word8]
    source <- mallocBuffer 0
    destination <- mallocBuffer 0
    runReaderIO (writeBytes payload) source
    runReaderIO (flushTo (MemorySink destination)) source
    rSrc <- runReaderIO residency source
    assertEqual "source residency reset after flush" 0 rSrc
    destBytes <- memoryBufferBytes destination
    assertEqual "flushed memory content" payload destBytes

-- Successive flushes into the same FileSink append rather than overwrite.
memFlushAppendsToFileSink :: Test
memFlushAppendsToFileSink = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "out.bin"
            chunkA = [1, 2, 3 :: Word8]
            chunkB = [4, 5, 6, 7 :: Word8]
        withWritableBinaryFile outPath $ \out -> do
            bufA <- mallocBuffer 0
            runReaderIO (writeBytes chunkA) bufA
            runReaderIO (flushTo (FileSink out)) bufA
            bufB <- mallocBuffer 0
            runReaderIO (writeBytes chunkB) bufB
            runReaderIO (flushTo (FileSink out)) bufB
        contents <- BS.readFile outPath
        assertEqual "appended flushes" (BS.pack (chunkA ++ chunkB)) contents

-- After a flush the buffer is emptied, so re-using it only re-flushes the
-- bytes written since the previous flush.
memFlushEmptiesForReuse :: Test
memFlushEmptiesForReuse = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let firstPath = dir </> "first.bin"
            secondPath = dir </> "second.bin"
            chunkA = [11, 22, 33 :: Word8]
            chunkB = [44, 55 :: Word8]
        buf <- mallocBuffer 0
        runReaderIO (writeBytes chunkA) buf
        withWritableBinaryFile firstPath $ \out ->
            runReaderIO (flushTo (FileSink out)) buf
        runReaderIO (writeBytes chunkB) buf
        r <- runReaderIO residency buf
        assertEqual "residency reflects only post-flush bytes" 2 r
        withWritableBinaryFile secondPath $ \out ->
            runReaderIO (flushTo (FileSink out)) buf
        first <- BS.readFile firstPath
        second <- BS.readFile secondPath
        assertEqual "first flush" (BS.pack chunkA) first
        assertEqual "second flush excludes first chunk" (BS.pack chunkB) second

-- A memory buffer flushing to a MemorySink backed by itself is a no-op: the
-- bytes and residency are left untouched (identity is compared via arrayRef).
memSelfFlushIsNoop :: Test
memSelfFlushIsNoop = TestCase $ do
    let payload = [3, 1, 4, 1, 5, 9, 2, 6 :: Word8]
    buf <- mallocBuffer 0
    runReaderIO (writeBytes payload) buf
    runReaderIO (flushTo (MemorySink buf)) buf
    r <- runReaderIO residency buf
    assertEqual "self-flush leaves residency unchanged" (Prelude.length payload) r
    bytes <- memoryBufferBytes buf
    assertEqual "self-flush leaves content unchanged" payload bytes

-- A payload larger than the 256 KiB flush chunk round-trips intact.
memFlushLargePayload :: Test
memFlushLargePayload = TestCase $
    withSystemTempDirectory "dfpq-buffer" $ \dir -> do
        let outPath = dir </> "big.bin"
            payload = take 300000 (cycle [0 .. 255]) :: [Word8]
        buf <- mallocBuffer 0
        runReaderIO (writeBytes payload) buf
        r <- runReaderIO residency buf
        assertEqual "residency for large payload" 300000 r
        withWritableBinaryFile outPath $ \out ->
            runReaderIO (flushTo (FileSink out)) buf
        contents <- BS.readFile outPath
        assertEqual "large payload round-trips" (BS.pack payload) contents

tests :: Test
tests =
    TestList
        [ TestLabel "file buffer: residency tracks writes" residencyTracksWrites
        , TestLabel "file buffer: flush to file sink" flushToFileSink
        , TestLabel "file buffer: flush to memory sink" flushToMemorySink
        , TestLabel "file buffer: flush appends to file sink" flushAppendsToFileSink
        , TestLabel "file buffer: flush empties for reuse" flushEmptiesForReuse
        , TestLabel "file buffer: flush large payload" flushLargePayload
        , TestLabel "memory buffer: residency tracks writes" memResidencyTracksWrites
        , TestLabel "memory buffer: flush to file sink" memFlushToFileSink
        , TestLabel "memory buffer: flush to memory sink" memFlushToMemorySink
        , TestLabel "memory buffer: flush appends to file sink" memFlushAppendsToFileSink
        , TestLabel "memory buffer: flush empties for reuse" memFlushEmptiesForReuse
        , TestLabel "memory buffer: self-flush is a no-op" memSelfFlushIsNoop
        , TestLabel "memory buffer: flush large payload" memFlushLargePayload
        ]

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
