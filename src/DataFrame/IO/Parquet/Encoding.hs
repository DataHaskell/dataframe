{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}

module DataFrame.IO.Parquet.Encoding (
    -- Kept from the original Encoding module (used by Levels)
    ceilLog2,
    bitWidthForMaxLevel,
    -- Vector-based RLE/bit-packed decoder (from new parser)
    decodeRLEBitPackedHybrid,
    extractBitsInto,
    fillRun,
    decodeDictIndices,
) where

import Control.Monad.ST (ST, runST)
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
#if !MIN_VERSION_base(4,20,0)
import Data.List (foldl')
#endif
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word
import DataFrame.IO.Parquet.Binary (readUVarInt)
import DataFrame.Internal.Binary (littleEndianWord32)

-- ---------------------------------------------------------------------------
-- Level-width helpers (used by Levels.hs)
-- ---------------------------------------------------------------------------

ceilLog2 :: Int -> Int
ceilLog2 x
    | x <= 1 = 0
    | otherwise = 1 + ceilLog2 ((x + 1) `div` 2)

bitWidthForMaxLevel :: Int -> Int
bitWidthForMaxLevel maxLevel = ceilLog2 (maxLevel + 1)

-- ---------------------------------------------------------------------------
-- Vector-based RLE / bit-packed hybrid decoder
-- ---------------------------------------------------------------------------

decodeRLEBitPackedHybrid ::
    -- | Bit width per value (0 = all zeros, use 'VU.replicate')
    Int ->
    -- | Exact number of values to decode
    Int ->
    BS.ByteString ->
    (VU.Vector Word32, BS.ByteString)
decodeRLEBitPackedHybrid bw need bs
    | bw == 0 = (VU.replicate need 0, bs)
    | otherwise = runST $ do
        mv <- VUM.new need
        rest <- go mv 0 bs
        dat <- VU.unsafeFreeze mv
        return (dat, rest)
  where
    !mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1 :: Word32
    go :: VUM.STVector s Word32 -> Int -> BS.ByteString -> ST s BS.ByteString
    go mv !filled !buf
        | filled >= need = return buf
        | BS.null buf = return buf
        | otherwise =
            let (hdr64, afterHdr) = readUVarInt buf
                isPacked = (hdr64 .&. 1) == 1
             in if isPacked
                    then do
                        let groups = fromIntegral (hdr64 `shiftR` 1) :: Int
                            totalVals = groups * 8
                            takeN = min (need - filled) totalVals
                            -- Consume all the bytes for this group even if we
                            -- only need a subset of the values.
                            bytesN = (bw * totalVals + 7) `div` 8
                            (chunk, rest) = BS.splitAt bytesN afterHdr
                        extractBitsInto bw takeN chunk mv filled
                        go mv (filled + takeN) rest
                    else do
                        let runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                            nbytes = (bw + 7) `div` 8
                            val = littleEndianWord32 (BS.take 4 afterHdr) .&. mask
                            takeN = min (need - filled) runLen
                        -- Fill the run directly — no list, no reverse.
                        fillRun mv filled (filled + takeN) val
                        go mv (filled + takeN) (BS.drop nbytes afterHdr)
{-# INLINE decodeRLEBitPackedHybrid #-}

-- | Fill @mv[start..end-1]@ with @val@ using a bulk @memset@-style write.
fillRun :: VUM.STVector s Word32 -> Int -> Int -> Word32 -> ST s ()
fillRun mv i end val = VUM.set (VUM.unsafeSlice i (end - i) mv) val
{-# INLINE fillRun #-}

{- | Write @count@ bit-width-@bw@ values from @bs@ into @mv@ starting at
@offset@, reading the byte buffer with a single-pass LSB-first accumulator.
No intermediate list or ByteString allocation.
-}
extractBitsInto ::
    -- | Bit width
    Int ->
    -- | Number of values to extract
    Int ->
    BS.ByteString ->
    VUM.STVector s Word32 ->
    -- | Write offset into @mv@
    Int ->
    ST s ()
extractBitsInto bw count bs mv off = go 0 (0 :: Word64) 0 0
  where
    !mask = if bw == 32 then maxBound else (1 `unsafeShiftL` bw) - 1 :: Word64
    !len = BS.length bs
    go !byteIdx !acc !accBits !done
        | done >= count = return ()
        | accBits >= bw = do
            VUM.unsafeWrite mv (off + done) (fromIntegral (acc .&. mask))
            go byteIdx (acc `unsafeShiftR` bw) (accBits - bw) (done + 1)
        | byteIdx >= len = return ()
        | otherwise =
            let b = fromIntegral (BSU.unsafeIndex bs byteIdx) :: Word64
             in go (byteIdx + 1) (acc .|. (b `unsafeShiftL` accBits)) (accBits + 8) done
{-# INLINE extractBitsInto #-}

{- | Decode @need@ dictionary indices from a DATA_PAGE bit-width-prefixed
stream (the first byte encodes the bit-width of all subsequent RLE\/bitpacked
values).

Returns the index vector (as 'Int') and the unconsumed bytes.
-}
decodeDictIndices :: Int -> BS.ByteString -> (VU.Vector Int, BS.ByteString)
decodeDictIndices need bs = case BS.uncons bs of
    Nothing -> error "decodeDictIndices: empty stream"
    Just (w0, rest0) ->
        let bw = fromIntegral w0 :: Int
            (raw, rest1) = decodeRLEBitPackedHybrid bw need rest0
         in (VU.map fromIntegral raw, rest1)
{-# INLINE decodeDictIndices #-}
