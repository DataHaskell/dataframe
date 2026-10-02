{-# LANGUAGE OverloadedRecordDot #-}

module DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    newDefLevels,
    pushDef,
    flushDef,
) where

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Primitive (PrimMonad, PrimState)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.Primitive.MutVar (MutVar, newMutVar, readMutVar, writeMutVar)
import Data.Word (Word64)
import DataFrame.IO.Utils.RandomAccess (MemoryBuffer, mallocBuffer, writeWord8)

data DefLevels s = DefLevels
    { dlBuf :: !(MemoryBuffer s)
    , dlValue :: !(MutVar s Int)
    , dlCount :: !(MutVar s Int)
    }

newDefLevels :: (PrimMonad m, MonadIO m) => m (DefLevels (PrimState m))
newDefLevels = DefLevels <$> mallocBuffer 64 <*> newMutVar 0 <*> newMutVar 0

pushDef :: (PrimMonad m) => DefLevels (PrimState m) -> Int -> m ()
pushDef dl value = do
    count <- readMutVar dl.dlCount
    if count == 0
        then writeMutVar dl.dlValue value >> writeMutVar dl.dlCount 1
        else do
            current <- readMutVar dl.dlValue
            if current == value
                then writeMutVar dl.dlCount (count + 1)
                else do
                    writeDefRun dl current count
                    writeMutVar dl.dlValue value
                    writeMutVar dl.dlCount 1
{-# INLINE pushDef #-}

flushDef :: (PrimMonad m) => DefLevels (PrimState m) -> m ()
flushDef dl = do
    count <- readMutVar dl.dlCount
    when (count > 0) $ do
        value <- readMutVar dl.dlValue
        writeDefRun dl value count
    writeMutVar dl.dlCount 0
{-# INLINE flushDef #-}

writeDefRun :: (PrimMonad m) => DefLevels (PrimState m) -> Int -> Int -> m ()
writeDefRun dl value count = do
    writeLeb128 dl.dlBuf (fromIntegral (count `shiftL` 1))
    writeWord8 dl.dlBuf (fromIntegral value)
{-# INLINE writeDefRun #-}

writeLeb128 :: (PrimMonad m) => MemoryBuffer (PrimState m) -> Word64 -> m ()
writeLeb128 buffer value
    | value < 0x80 = writeWord8 buffer (fromIntegral value)
    | otherwise = do
        writeWord8 buffer (fromIntegral (value .&. 0x7f) .|. 0x80)
        writeLeb128 buffer (value `shiftR` 7)
{-# INLINE writeLeb128 #-}
