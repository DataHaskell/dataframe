{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Shared byte-level helpers for the fast slice parsers. All operate on
@(buf, start, end)@ slices; the caller guarantees
@0 <= start <= end <= length buf@ so everything below uses 'unsafeIndex'.
-}
module DataFrame.Internal.Parsing.Fast.Common (
    isStripByte,
    isDigitByte,
    skipStrip,
    skipStripEnd,
    skipZeroes,
    takeDigits64,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU

import Data.Word (Word64, Word8)
import GHC.Exts (
    Int (..),
    Int#,
    RuntimeRep,
    TYPE,
    Word64#,
    isTrue#,
    plusWord64#,
    timesWord64#,
    wordToWord64#,
    (+#),
    (>=#),
 )
import GHC.Word (Word64 (..))

{- | Bytes removed by 'Data.ByteString.Char8.strip' (Latin-1 'isSpace'):
HT LF VT FF CR SP and NBSP 0xA0. Probed over all 256 bytes; parity
with the strip-based reference parsers depends on this exact set.
-}
isStripByte :: Word8 -> Bool
isStripByte w = w == 0x20 || (w - 0x09) <= 4 || w == 0xA0
{-# INLINE isStripByte #-}

isDigitByte :: Word8 -> Bool
isDigitByte w = (w - 0x30) <= 9
{-# INLINE isDigitByte #-}

-- | Index of the first non-strip byte in @[i, end)@.
skipStrip :: BS.ByteString -> Int -> Int -> Int
skipStrip bs = go
  where
    go !i !end
        | i < end && isStripByte (BSU.unsafeIndex bs i) = go (i + 1) end
        | otherwise = i
{-# INLINE skipStrip #-}

-- | New exclusive end after dropping trailing strip bytes in @[i, end)@.
skipStripEnd :: BS.ByteString -> Int -> Int -> Int
skipStripEnd bs = go
  where
    go !i !end
        | end > i && isStripByte (BSU.unsafeIndex bs (end - 1)) = go i (end - 1)
        | otherwise = end
{-# INLINE skipStripEnd #-}

-- | Index of the first non-@\'0\'@ byte in @[i, end)@.
skipZeroes :: BS.ByteString -> Int -> Int -> Int
skipZeroes bs = go
  where
    go !i !end
        | i < end && BSU.unsafeIndex bs i == 0x30 = go (i + 1) end
        | otherwise = i
{-# INLINE skipZeroes #-}

{- | Consume ASCII digits from @i@, passing the stop index and the wrapping
'Word64' accumulation to the continuation. Callers must bound the
significant digit count before trusting the value.
-}
takeDigits64 ::
    forall (rep :: RuntimeRep) (r :: TYPE rep).
    BS.ByteString ->
    Int ->
    Int ->
    (Int -> Word64 -> r) ->
    r
takeDigits64 bs (I# i0) (I# end) k = go i0 (wordToWord64# 0##)
  where
    go :: Int# -> Word64# -> r
    go i acc
        | isTrue# (i >=# end) = k (I# i) (W64# acc)
        | isDigitByte w =
            case fromIntegral (w - 0x30) :: Word64 of
                W64# d ->
                    go
                        (i +# 1#)
                        ((acc `timesWord64#` wordToWord64# 10##) `plusWord64#` d)
        | otherwise = k (I# i) (W64# acc)
      where
        w = BSU.unsafeIndex bs (I# i)
{-# INLINE takeDigits64 #-}
