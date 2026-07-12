{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Non-backtracking @Int@ slice parser, bit-compatible with
@readByteStringInt@: grammar @WS* sign? digit+ WS*@, whole slice consumed,
'Nothing' on 64-bit overflow.
-}
module DataFrame.Internal.Parsing.Fast.Int (parseIntField#) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU

import GHC.Exts (Int (..), Int#)

import DataFrame.Internal.Parsing.Fast.Common

{- | Result is @(# ok, value #)@ with @ok@ 0 or 1. Caller guarantees
@0 <= start <= end <= length buf@.
-}
parseIntField# :: BS.ByteString -> Int -> Int -> (# Int#, Int# #)
parseIntField# bs start end0
    | i0 >= end = none
    | otherwise =
        let !c0 = BSU.unsafeIndex bs i0
            !neg = c0 == 0x2D
            !i1 = if neg || c0 == 0x2B then i0 + 1 else i0
         in if i1 >= end || not (isDigitByte (BSU.unsafeIndex bs i1))
                then none
                else
                    let !iz = skipZeroes bs i1 end
                     in takeDigits64 bs iz end $ \dEnd w ->
                            if dEnd /= end || dEnd - iz > 19
                                then none
                                else
                                    if neg
                                        then
                                            if w <= 9223372036854775808
                                                then done (negate (fromIntegral w))
                                                else none
                                        else
                                            if w <= 9223372036854775807
                                                then done (fromIntegral w)
                                                else none
  where
    !i0 = skipStrip bs start end0
    !end = skipStripEnd bs i0 end0
    none = (# 0#, 0# #)
    done :: Int -> (# Int#, Int# #)
    done (I# n) = (# 1#, n #)
    {-# INLINE done #-}
{-# INLINE parseIntField# #-}
