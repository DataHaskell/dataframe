{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}

{- |
A poor-man's hash used by 'DataFrame.Internal.Grouping' to bucket rows
without depending on the @hashable@ package.

Each value is folded into an 'Int' accumulator with an FxHash-style step
(rotate, xor, multiply). It is intentionally small and not cryptographically
strong — it only needs to spread group-key tuples well enough that
'Data.IntMap' bucketing produces sensible groups.
-}
module DataFrame.Internal.Hash (
    fnvOffset,
    nullSalt,
    mixInt,
    mixDouble,
    mixBool,
    mixChar,
    mixText,
    mixShow,
) where

import Data.Bits (rotateL, unsafeShiftL, unsafeShiftR, xor)
import Data.Char (ord)
import qualified Data.Text as T
import Data.Text.Array (Array (ByteArray))
import Data.Text.Internal (Text (Text))
import GHC.Exts (Int (I#), indexWord8Array#, indexWord8ArrayAsWord64#)
import GHC.Word (Word64 (W64#), Word8 (W8#))

{- | FNV-1a 64-bit offset basis (used as the initial accumulator).
The literal is unsigned and exceeds 'Int' range, so we round-trip through
'Word64' to get the well-defined two's-complement bit pattern.
-}
fnvOffset :: Int
fnvOffset = fromIntegral (0xcbf29ce484222325 :: Word64)

-- | FNV-1a 64-bit prime.
fnvPrime :: Int
fnvPrime = 0x00000100000001b3

{- | Sentinel mixed in for a /null/ slot of a nullable column, so that a
@Nothing@ does not hash to the same value as a present @Just x@ that happens to
store the same underlying bits (notably @Just 0@). A fixed distinctive constant
(the 64-bit golden-ratio mix constant) keeps null hashing deterministic; a real
value equal to it collides only as rarely as any other hash collision.
-}
nullSalt :: Int
nullSalt = fromIntegral (0x9E3779B97F4A7C15 :: Word64)

{- | Mix an 'Int' into the accumulator.

An FxHash-style step (rotate the accumulator, xor the value, multiply by a large
odd constant). The rotate diffuses each value's bits across all positions before
the next is folded in, so small/adjacent integers — common as group keys — do
not produce the structured collisions that a plain @(acc `xor` x) * prime@ does
once several columns are combined. Grouping trusts hash equality, so this
robustness is what keeps distinct rows in distinct groups.
-}
mixInt :: Int -> Int -> Int
mixInt acc x = (rotateL acc 13 `xor` x) * fnvPrime
{-# INLINE mixInt #-}

{- | Mix a 'Double' into the accumulator. Loses sub-millisecond precision
but matches the bucketing the old hashable-based code used.
-}
mixDouble :: Int -> Double -> Int
mixDouble acc d = mixInt acc (floor (d * 1000))
{-# INLINE mixDouble #-}

mixBool :: Int -> Bool -> Int
mixBool acc b = mixInt acc (if b then 1 else 0)
{-# INLINE mixBool #-}

mixChar :: Int -> Char -> Int
mixChar acc = mixInt acc . ord
{-# INLINE mixChar #-}

{- | Mix a 'T.Text' value into the accumulator over its raw UTF-8 bytes,
eight at a time. Reading a whole 'Word64' per step (rather than decoding and
mixing one codepoint at a time) cuts the multiply count ~8x on long keys while
staying collision-equivalent: UTF-8 is injective, so equal 'T.Text's mix to the
same value and distinct ones almost never collide. The trailing @len `mod` 8@
bytes are folded in individually.
-}
mixText :: Int -> T.Text -> Int
mixText !acc (Text (ByteArray ba) off len) = goBytes (goWords acc off) wordsEnd
  where
    !nWords = len `unsafeShiftR` 3
    !wordsEnd = off + (nWords `unsafeShiftL` 3)
    !end = off + len
    goWords !h !i
        | i >= wordsEnd = h
        | otherwise =
            let !(I# i#) = i
                !w = fromIntegral (W64# (indexWord8ArrayAsWord64# ba i#)) :: Int
             in goWords (mixInt h w) (i + 8)
    goBytes !h !i
        | i >= end = h
        | otherwise =
            let !(I# i#) = i
                !b = fromIntegral (W8# (indexWord8Array# ba i#)) :: Int
             in goBytes (mixInt h b) (i + 1)
{-# INLINE mixText #-}

{- | Fallback for arbitrary 'Show'-able values. Slower but covers types
without a dedicated combinator (e.g. 'Day', 'UTCTime').
-}
mixShow :: (Show a) => Int -> a -> Int
mixShow acc = mixText acc . T.pack . show
{-# INLINE mixShow #-}
