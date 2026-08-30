{-# LANGUAGE OverloadedRecordDot #-}

module DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    newDefLevels,
    pushDef,
    flushDef,
) where

import Control.Monad (when)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Word (Word64)
import DataFrame.IO.Utils.RandomAccess (MemoryBuffer, mallocBuffer, writeWord8)

data DefLevels = DefLevels
    { dlBuf :: !MemoryBuffer
    , dlValue :: !(IORef Int)
    , dlCount :: !(IORef Int)
    }

newDefLevels :: IO DefLevels
newDefLevels = DefLevels <$> mallocBuffer 64 <*> newIORef 0 <*> newIORef 0

pushDef :: DefLevels -> Int -> IO ()
pushDef dl value = do
    count <- readIORef dl.dlCount
    if count == 0
        then writeIORef dl.dlValue value >> writeIORef dl.dlCount 1
        else do
            current <- readIORef dl.dlValue
            if current == value
                then writeIORef dl.dlCount (count + 1)
                else do
                    writeDefRun dl current count
                    writeIORef dl.dlValue value
                    writeIORef dl.dlCount 1
{-# INLINE pushDef #-}

flushDef :: DefLevels -> IO ()
flushDef dl = do
    count <- readIORef dl.dlCount
    when (count > 0) $ do
        value <- readIORef dl.dlValue
        writeDefRun dl value count
    writeIORef dl.dlCount 0
{-# INLINE flushDef #-}

writeDefRun :: DefLevels -> Int -> Int -> IO ()
writeDefRun dl value count = do
    writeLeb128 dl.dlBuf (fromIntegral (count `shiftL` 1))
    writeWord8 dl.dlBuf (fromIntegral value)
{-# INLINE writeDefRun #-}

writeLeb128 :: MemoryBuffer -> Word64 -> IO ()
writeLeb128 buffer value
    | value < 0x80 = writeWord8 buffer (fromIntegral value)
    | otherwise = do
        writeWord8 buffer (fromIntegral (value .&. 0x7f) .|. 0x80)
        writeLeb128 buffer (value `shiftR` 7)
{-# INLINE writeLeb128 #-}
