{-# LANGUAGE BangPatterns #-}

module DataFrame.Internal.Binary where

import Data.Bits (Bits (unsafeShiftL, (.|.)))
import Data.ByteString (toStrict)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (toLazyByteString, word32LE, word64LE)
import qualified Data.ByteString.Unsafe as BS
import Data.Int (Int32)
import Data.Word (Word32, Word64, Word8)

littleEndianWord32 :: BS.ByteString -> Word32
littleEndianWord32 bytes
    | len >= 4 =
        assembleWord32
            (BS.unsafeIndex bytes 0)
            (BS.unsafeIndex bytes 1)
            (BS.unsafeIndex bytes 2)
            (BS.unsafeIndex bytes 3)
    | otherwise =
        assembleWord32
            (byteAtOrZero len bytes 0)
            (byteAtOrZero len bytes 1)
            (byteAtOrZero len bytes 2)
            (byteAtOrZero len bytes 3)
  where
    len = BS.length bytes
{-# INLINE littleEndianWord32 #-}

littleEndianWord64 :: BS.ByteString -> Word64
littleEndianWord64 bytes
    | len >= 8 =
        assembleWord64
            (BS.index bytes 0)
            (BS.index bytes 1)
            (BS.index bytes 2)
            (BS.index bytes 3)
            (BS.index bytes 4)
            (BS.index bytes 5)
            (BS.index bytes 6)
            (BS.index bytes 7)
    | otherwise =
        assembleWord64
            (byteAtOrZero len bytes 0)
            (byteAtOrZero len bytes 1)
            (byteAtOrZero len bytes 2)
            (byteAtOrZero len bytes 3)
            (byteAtOrZero len bytes 4)
            (byteAtOrZero len bytes 5)
            (byteAtOrZero len bytes 6)
            (byteAtOrZero len bytes 7)
  where
    len = BS.length bytes
{-# INLINE littleEndianWord64 #-}

littleEndianInt32 :: BS.ByteString -> Int32
littleEndianInt32 = fromIntegral . littleEndianWord32
{-# INLINE littleEndianInt32 #-}

word64ToLittleEndian :: Word64 -> BS.ByteString
word64ToLittleEndian = toStrict . toLazyByteString . word64LE
{-# INLINE word64ToLittleEndian #-}

word32ToLittleEndian :: Word32 -> BS.ByteString
word32ToLittleEndian = toStrict . toLazyByteString . word32LE
{-# INLINE word32ToLittleEndian #-}

byteAtOrZero :: Int -> BS.ByteString -> Int -> Word8
byteAtOrZero len bytes i
    | i >= 0 && i < len = BS.unsafeIndex bytes i
    | otherwise = 0
{-# INLINE byteAtOrZero #-}

assembleWord32 :: Word8 -> Word8 -> Word8 -> Word8 -> Word32
assembleWord32 !b0 !b1 !b2 !b3 =
    fromIntegral b0
        .|. (fromIntegral b1 `unsafeShiftL` 8)
        .|. (fromIntegral b2 `unsafeShiftL` 16)
        .|. (fromIntegral b3 `unsafeShiftL` 24)
{-# INLINE assembleWord32 #-}

assembleWord64 ::
    Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word64
assembleWord64 !b0 !b1 !b2 !b3 !b4 !b5 !b6 !b7 =
    fromIntegral b0
        .|. (fromIntegral b1 `unsafeShiftL` 8)
        .|. (fromIntegral b2 `unsafeShiftL` 16)
        .|. (fromIntegral b3 `unsafeShiftL` 24)
        .|. (fromIntegral b4 `unsafeShiftL` 32)
        .|. (fromIntegral b5 `unsafeShiftL` 40)
        .|. (fromIntegral b6 `unsafeShiftL` 48)
        .|. (fromIntegral b7 `unsafeShiftL` 56)
{-# INLINE assembleWord64 #-}
