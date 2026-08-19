{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Fast @Double@ slice parser, bit-exact with @readByteStringDouble@.
Fields whose significand and scale fit Clinger's exact window are one
correctly-rounded machine op; everything else falls back to the exact
reference parser.
-}
module DataFrame.Internal.Parsing.Fast.Double (parseDoubleField#) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.Vector.Unboxed as VU

import Data.Word (Word64)
import GHC.Exts (Double (..), Double#, Int#)

import DataFrame.Internal.Parsing (readByteStringDouble)
import DataFrame.Internal.Parsing.Fast.Common

{- | @10 ^ k@ for @k <= 22@: every entry is exactly representable (5^22
fits in 53 bits), so multiplying or dividing by one is a single
correctly-rounded op.
-}
pow10Table :: VU.Vector Double
pow10Table = VU.generate (exactPow10 + 1) (10 ^)
{-# NOINLINE pow10Table #-}

-- | @10 ^ k@ for @k <= 19@ in 'Word64'; @10^19 < 2^64@.
pow10w :: VU.Vector Word64
pow10w = VU.generate 20 (10 ^)
{-# NOINLINE pow10w #-}

-- | Largest @k@ with @10^k@ exactly representable as a 'Double'.
exactPow10 :: Int
exactPow10 = 22

{- | 'Word64' to 'Double'; callers only pass values @<= 2^53@, where the
conversion is exact.
-}
w2d :: Word64 -> Double
w2d w = fromIntegral (fromIntegral w :: Int)
{-# INLINE w2d #-}

-- | Outside the exact window: hand the raw slice to the reference parser.
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
    -- The one non-numeric token the reference accepts ('show' writes it).
    | isInfinityAt i0 = done False (1 / 0)
    | BSU.unsafeIndex bs i0 == 0x2D && isInfinityAt (i0 + 1) = done True (1 / 0)
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
                                else afterWhole neg w (wEnd - iz) wEnd
  where
    !i0 = skipStrip bs start end0
    !end = skipStripEnd bs i0 end0

    none = (# 0#, 0.0## #)

    -- "Infinity", exactly, to the end of the slice.
    isInfinityAt i =
        end - i == 8
            && BSU.unsafeIndex bs i == 0x49
            && BSU.unsafeIndex bs (i + 1) == 0x6E
            && BSU.unsafeIndex bs (i + 2) == 0x66
            && BSU.unsafeIndex bs (i + 3) == 0x69
            && BSU.unsafeIndex bs (i + 4) == 0x6E
            && BSU.unsafeIndex bs (i + 5) == 0x69
            && BSU.unsafeIndex bs (i + 6) == 0x74
            && BSU.unsafeIndex bs (i + 7) == 0x79

    -- Fold the fraction into the significand so there is one scale step.
    afterWhole !neg !w !wd !i
        | i < end && BSU.unsafeIndex bs i == 0x2E =
            let !f0 = i + 1
                !fz = skipZeroes bs f0 end
             in takeDigits64 bs fz end $ \fEnd p ->
                    if fEnd == f0
                        then none
                        else
                            let !fracLen = fEnd - f0
                             in if wd + fracLen > 19
                                    then referenceSlice bs start end0
                                    else
                                        afterExponent
                                            neg
                                            (w * VU.unsafeIndex pow10w fracLen + p)
                                            fracLen
                                            fEnd
        | otherwise = afterExponent neg w 0 i

    afterExponent !neg !sig !fracLen !i
        | i >= end = finish neg sig (negate fracLen)
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
                                            else
                                                let !ev = fromIntegral e
                                                 in finish
                                                        neg
                                                        sig
                                                        ( (if eneg then negate ev else ev)
                                                            - fracLen
                                                        )
        | otherwise = none

    -- Clinger: exact significand times an exact power of ten is one
    -- correctly-rounded op; anything wider goes to the reference.
    finish !neg !sig !dexp
        | sig == 0 = done neg 0
        | sig <= 9007199254740991 && dexp >= negate exactPow10 && dexp <= exactPow10 =
            done
                neg
                ( if dexp >= 0
                    then w2d sig * VU.unsafeIndex pow10Table dexp
                    else w2d sig / VU.unsafeIndex pow10Table (negate dexp)
                )
        | otherwise = referenceSlice bs start end0
    {-# INLINE finish #-}

    done !neg !v = case if neg then negate v else v of
        D# d -> (# 1#, d #)
    {-# INLINE done #-}
{-# INLINE parseDoubleField# #-}
