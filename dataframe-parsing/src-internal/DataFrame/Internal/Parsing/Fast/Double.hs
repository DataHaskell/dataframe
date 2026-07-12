{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Fast @Double@ slice parser, bit-exact with @readByteStringDouble@.
Replays the reference parser's exact floating-point operations via 'Word64'
digit accumulation and 10^k tables, falling back when exactness is in doubt.
-}
module DataFrame.Internal.Parsing.Fast.Double (parseDoubleField#) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.Vector.Unboxed as VU

import Data.Word (Word64)
import GHC.Exts (Double (..), Double#, Int#)

import DataFrame.Internal.Parsing (readByteStringDouble)
import DataFrame.Internal.Parsing.Fast.Common

{- | @10 ^ k@ for @k <= tableMax@, computed with the same @(^)@ the reference
parser uses, so every entry is bit-identical. Entries from @10^309@ up are
@Infinity@, so clamping larger exponents to 'tableMax' is exact.
-}
pow10Table :: VU.Vector Double
pow10Table = VU.generate (tableMax + 1) (10 ^)
{-# NOINLINE pow10Table #-}

-- | @recip (10 ^ k)@, replaying @10 ^^ negate k@ bit-for-bit.
recipPow10Table :: VU.Vector Double
recipPow10Table = VU.map recip pow10Table
{-# NOINLINE recipPow10Table #-}

tableMax :: Int
tableMax = 1024

{- | 'Word64' to 'Double' exactly as the reference parser's
'fromInteger' rounds it; values up to @2^53@ take the exact
'Int' conversion (int2Double#), larger ones the 'Integer' route.
-}
w2d :: Word64 -> Double
w2d w
    | w <= 9007199254740991 = fromIntegral (fromIntegral w :: Int)
    | otherwise = fromInteger (toInteger w)
{-# INLINE w2d #-}

-- | Exactness in doubt: hand the raw slice to the reference parser.
referenceSlice :: BS.ByteString -> Int -> Int -> (# Int#, Double# #)
referenceSlice bs start end =
    case readByteStringDouble (BSU.unsafeTake (end - start) (BSU.unsafeDrop start bs)) of
        Just (D# d) -> (# 1#, d #)
        Nothing -> (# 0#, 0.0## #)
{-# NOINLINE referenceSlice #-}

{- | Result is @(# ok, value #)@ with @ok@ 0 or 1. Caller guarantees
@0 <= start <= end <= length buf@.
-}
parseDoubleField# :: BS.ByteString -> Int -> Int -> (# Int#, Double# #)
parseDoubleField# bs start end0
    | i0 >= end = none
    | otherwise =
        let !c0 = BSU.unsafeIndex bs i0
            !neg = c0 == 0x2D
            !i1 = if neg || c0 == 0x2B then i0 + 1 else i0
         in if i1 >= end || not (isDigitByte (BSU.unsafeIndex bs i1))
                then none
                else
                    let !iz = skipZeroes bs i1 end
                     in takeDigits64 bs iz end $ \wEnd w ->
                            if wEnd - iz > 19
                                then referenceSlice bs start end0
                                else afterWhole neg w wEnd
  where
    !i0 = skipStrip bs start end0
    !end = skipStripEnd bs i0 end0

    none = (# 0#, 0.0## #)

    afterWhole !neg !w !i
        | i < end && BSU.unsafeIndex bs i == 0x2E =
            let !f0 = i + 1
                !fz = skipZeroes bs f0 end
             in takeDigits64 bs fz end $ \fEnd p ->
                    if fEnd == f0
                        then none
                        else
                            if fEnd - fz > 19
                                then referenceSlice bs start end0
                                else afterExponent neg (w2d w + (w2d p / pow10 (fEnd - f0))) fEnd
        | otherwise = afterExponent neg (w2d w) i

    afterExponent !neg !val !i
        | i >= end = done neg val
        | BSU.unsafeIndex bs i == 0x65 || BSU.unsafeIndex bs i == 0x45 =
            let !i1 = i + 1
                !eneg = i1 < end && BSU.unsafeIndex bs i1 == 0x2D
                !i2 = if i1 < end && (eneg || BSU.unsafeIndex bs i1 == 0x2B) then i1 + 1 else i1
             in if i2 >= end || not (isDigitByte (BSU.unsafeIndex bs i2))
                    then none
                    else
                        let !ez = skipZeroes bs i2 end
                         in takeDigits64 bs ez end $ \eEnd e ->
                                if eEnd /= end
                                    then none
                                    else
                                        if eEnd - ez > 18
                                            then referenceSlice bs start end0
                                            else done neg (val * scale eneg (fromIntegral e))
        | otherwise = none

    scale !eneg !ex
        | eneg = VU.unsafeIndex recipPow10Table k
        | otherwise = VU.unsafeIndex pow10Table k
      where
        !k = min ex tableMax
    {-# INLINE scale #-}

    pow10 !k = VU.unsafeIndex pow10Table (min k tableMax)
    {-# INLINE pow10 #-}

    done !neg !v = case if neg then negate v else v of
        D# d -> (# 1#, d #)
    {-# INLINE done #-}
{-# INLINE parseDoubleField# #-}
