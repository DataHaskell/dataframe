{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Parquet.Encoding where

import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.List (foldl')
import qualified Data.Vector.Unboxed as VU
import Data.Word
import DataFrame.IO.Parquet.Binary (readUVarInt)
import DataFrame.Internal.Binary (littleEndianWord32)

ceilLog2 :: Int -> Int
ceilLog2 x
    | x <= 1 = 0
    | otherwise = 1 + ceilLog2 ((x + 1) `div` 2)

bitWidthForMaxLevel :: Int -> Int
bitWidthForMaxLevel maxLevel = ceilLog2 (maxLevel + 1)

bytesForBW :: Int -> Int
bytesForBW bw = (bw + 7) `div` 8

unpackBitPacked :: Int -> Int -> BS.ByteString -> ([Word32], BS.ByteString)
unpackBitPacked bw count bs
    | count <= 0 = ([], bs)
    | BS.null bs = ([], bs)
    | otherwise =
        let totalBytes = (bw * count + 7) `div` 8
            chunk = BS.take totalBytes bs
            rest = BS.drop totalBytes bs
         in (extractBits bw count chunk, rest)

-- | LSB-first bit accumulator: reads each byte once with no intermediate ByteString allocation.
extractBits :: Int -> Int -> BS.ByteString -> [Word32]
extractBits bw count bs = go 0 (0 :: Word64) 0 count
  where
    !mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1 :: Word64
    !len = BS.length bs
    go !byteIdx !acc !accBits !remaining
        | remaining <= 0 = []
        | accBits >= bw =
            fromIntegral (acc .&. mask)
                : go byteIdx (acc `shiftR` bw) (accBits - bw) (remaining - 1)
        | byteIdx >= len = []
        | otherwise =
            let b = fromIntegral (BSU.unsafeIndex bs byteIdx) :: Word64
             in go (byteIdx + 1) (acc .|. (b `shiftL` accBits)) (accBits + 8) remaining

decodeRLEBitPackedHybrid ::
    Int -> Int -> BS.ByteString -> ([Word32], BS.ByteString)
decodeRLEBitPackedHybrid bw need bs
    | bw == 0 = (replicate need 0, bs)
    | otherwise = go need bs []
  where
    mask :: Word32
    mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1
    go :: Int -> BS.ByteString -> [Word32] -> ([Word32], BS.ByteString)
    go 0 rest acc = (reverse acc, rest)
    go n rest acc
        | BS.null rest = (reverse acc, rest)
        | otherwise =
            let (hdr64, afterHdr) = readUVarInt rest
                isPacked = (hdr64 .&. 1) == 1
             in if isPacked
                    then
                        let groups = fromIntegral (hdr64 `shiftR` 1) :: Int
                            totalVals = groups * 8
                            (valsAll, afterRun) = unpackBitPacked bw totalVals afterHdr
                            takeN = min n totalVals
                            actualTaken = take takeN valsAll
                         in go (n - takeN) afterRun (reverse actualTaken ++ acc)
                    else
                        let runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                            nbytes = bytesForBW bw
                            word32 = littleEndianWord32 (BS.take 4 afterHdr)
                            afterV = BS.drop nbytes afterHdr
                            val = word32 .&. mask
                            takeN = min n runLen
                         in go (n - takeN) afterV (replicate takeN val ++ acc)

decodeDictIndicesV1 ::
    Int -> Int -> BS.ByteString -> (VU.Vector Int, BS.ByteString)
decodeDictIndicesV1 need dictCard bs =
    case BS.uncons bs of
        Nothing -> error "empty dictionary index stream"
        Just (w0, rest0) ->
            let bw = fromIntegral w0 :: Int
                (u32s, rest1) = decodeRLEBitPackedHybrid bw need rest0
             in (VU.fromList (map fromIntegral u32s), rest1)
