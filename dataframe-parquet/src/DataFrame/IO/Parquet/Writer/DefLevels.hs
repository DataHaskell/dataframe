{-# LANGUAGE OverloadedRecordDot #-}

module DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    newDefLevels,
    pushDef,
    flushDef,
) where

import Control.Monad (when)
import Control.Monad.ST (ST)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.STRef (STRef, newSTRef, readSTRef, writeSTRef)
import Data.Word (Word64)
import DataFrame.IO.Utils.RandomAccess (MemoryBuffer, mallocBuffer, writeWord8)

data DefLevels s = DefLevels
    { dlBuf :: !(MemoryBuffer s)
    , dlValue :: !(STRef s Int)
    , dlCount :: !(STRef s Int)
    }

newDefLevels :: ST s (DefLevels s)
newDefLevels = DefLevels <$> mallocBuffer 64 <*> newSTRef 0 <*> newSTRef 0

pushDef :: DefLevels s -> Int -> ST s ()
pushDef dl value = do
    count <- readSTRef dl.dlCount
    if count == 0
        then writeSTRef dl.dlValue value >> writeSTRef dl.dlCount 1
        else do
            current <- readSTRef dl.dlValue
            if current == value
                then writeSTRef dl.dlCount (count + 1)
                else do
                    writeDefRun dl current count
                    writeSTRef dl.dlValue value
                    writeSTRef dl.dlCount 1
{-# INLINE pushDef #-}

flushDef :: DefLevels s -> ST s ()
flushDef dl = do
    count <- readSTRef dl.dlCount
    when (count > 0) $ do
        value <- readSTRef dl.dlValue
        writeDefRun dl value count
    writeSTRef dl.dlCount 0
{-# INLINE flushDef #-}

writeDefRun :: DefLevels s -> Int -> Int -> ST s ()
writeDefRun dl value count = do
    writeLeb128 dl.dlBuf (fromIntegral (count `shiftL` 1))
    writeWord8 dl.dlBuf (fromIntegral value)
{-# INLINE writeDefRun #-}

writeLeb128 :: MemoryBuffer s -> Word64 -> ST s ()
writeLeb128 buffer value
    | value < 0x80 = writeWord8 buffer (fromIntegral value)
    | otherwise = do
        writeWord8 buffer (fromIntegral (value .&. 0x7f) .|. 0x80)
        writeLeb128 buffer (value `shiftR` 7)
{-# INLINE writeLeb128 #-}
