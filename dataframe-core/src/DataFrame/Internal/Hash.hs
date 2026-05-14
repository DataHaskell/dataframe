{- |
A poor-man's hash used by 'DataFrame.Internal.Grouping' to bucket rows
without depending on the @hashable@ package.

The hash is FNV-1a-shaped: an accumulator is repeatedly @xor@ed with the
next chunk and multiplied by an FNV prime. It is intentionally small and
not cryptographically strong — it only needs to spread group-key tuples
well enough that 'Data.IntMap' bucketing produces sensible groups.
-}
module DataFrame.Internal.Hash (
    fnvOffset,
    mixInt,
    mixDouble,
    mixBool,
    mixChar,
    mixText,
    mixShow,
) where

import Data.Bits (xor)
import Data.Char (ord)
import qualified Data.Text as T

-- | FNV-1a 64-bit offset basis (used as the initial accumulator).
fnvOffset :: Int
fnvOffset = 0xcbf29ce484222325

-- | FNV-1a 64-bit prime.
fnvPrime :: Int
fnvPrime = 0x00000100000001b3

-- | Mix an 'Int' into the accumulator.
mixInt :: Int -> Int -> Int
mixInt acc x = (acc `xor` x) * fnvPrime
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

-- | Mix a 'T.Text' value into the accumulator, byte by byte.
mixText :: Int -> T.Text -> Int
mixText = T.foldl' (\a c -> mixInt a (ord c))
{-# INLINE mixText #-}

{- | Fallback for arbitrary 'Show'-able values. Slower but covers types
without a dedicated combinator (e.g. 'Day', 'UTCTime').
-}
mixShow :: (Show a) => Int -> a -> Int
mixShow acc = mixText acc . T.pack . show
{-# INLINE mixShow #-}
