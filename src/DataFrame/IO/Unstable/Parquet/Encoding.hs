{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Unstable.Parquet.Encoding (
    decodeRLEBitPackedHybridV,
    decodeDictIndicesV,
) where

import Control.Monad.ST (ST, runST)
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word
import DataFrame.IO.Parquet.Binary (readUVarInt)
import DataFrame.Internal.Binary (littleEndianWord32)

decodeRLEBitPackedHybridV ::
    -- | Bit width per value (0 = all zeros, use 'VU.replicate')
    Int ->
    -- | Exact number of values to decode
    Int ->
    BS.ByteString ->
    (VU.Vector Word32, BS.ByteString)
decodeRLEBitPackedHybridV bw need bs
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
                        extractBitsIntoV bw takeN chunk mv filled
                        go mv (filled + takeN) rest
                    else do
                        let runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                            nbytes = (bw + 7) `div` 8
                            val = littleEndianWord32 (BS.take 4 afterHdr) .&. mask
                            takeN = min (need - filled) runLen
                        -- Fill the run directly — no list, no reverse.
                        fillRun mv filled (filled + takeN) val
                        go mv (filled + takeN) (BS.drop nbytes afterHdr)
{-# INLINE decodeRLEBitPackedHybridV #-}

-- | Fill @mv[start..end-1]@ with @val@.
fillRun :: VUM.STVector s Word32 -> Int -> Int -> Word32 -> ST s ()
fillRun mv !i !end !val
    | i >= end = return ()
    | otherwise = VUM.unsafeWrite mv i val >> fillRun mv (i + 1) end val
{-# INLINE fillRun #-}

{- | Write @count@ bit-width-@bw@ values from @bs@ into @mv@ starting at
@offset@, reading the byte buffer with a single-pass LSB-first accumulator.
No intermediate list or ByteString allocation.
-}
extractBitsIntoV ::
    -- | Bit width
    Int ->
    -- | Number of values to extract
    Int ->
    BS.ByteString ->
    VUM.STVector s Word32 ->
    -- | Write offset into @mv@
    Int ->
    ST s ()
extractBitsIntoV bw count bs mv off = go 0 (0 :: Word64) 0 0
  where
    !mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1 :: Word64
    !len = BS.length bs
    go !byteIdx !acc !accBits !done
        | done >= count = return ()
        | accBits >= bw = do
            VUM.unsafeWrite mv (off + done) (fromIntegral (acc .&. mask))
            go byteIdx (acc `shiftR` bw) (accBits - bw) (done + 1)
        | byteIdx >= len = return ()
        | otherwise =
            let b = fromIntegral (BSU.unsafeIndex bs byteIdx) :: Word64
             in go (byteIdx + 1) (acc .|. (b `shiftL` accBits)) (accBits + 8) done
{-# INLINE extractBitsIntoV #-}

{- | Decode @need@ dictionary indices from a DATA_PAGE bit-width-prefixed
stream (the first byte encodes the bit-width of all subsequent RLE\/bitpacked
values).

Returns the index vector (as 'Int') and the unconsumed bytes.
-}
decodeDictIndicesV :: Int -> BS.ByteString -> (VU.Vector Int, BS.ByteString)
decodeDictIndicesV need bs = case BS.uncons bs of
    Nothing -> error "decodeDictIndicesV: empty stream"
    Just (w0, rest0) ->
        let bw = fromIntegral w0 :: Int
            (raw, rest1) = decodeRLEBitPackedHybridV bw need rest0
         in (VU.map fromIntegral raw, rest1)
{-# INLINE decodeDictIndicesV #-}
