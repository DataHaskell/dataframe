{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | A flat, unboxed, open-addressing (linear-probe) hash table mapping a row's
key-hash to a dense group id, re-verifying the real key on every hash hit to
reject collisions. Runs in any 'PrimMonad' ('ST' for grouping, 'IO' per worker).
-}
module DataFrame.Internal.HashTable (
    HashTable (..),
    newHashTable,
    htInsert,
    nextPow2Above,
) where

import Control.Monad.Primitive (PrimMonad, PrimState)
import Data.Bits (popCount, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)

{- | An open-addressing linear-probe table. @htMask@ is @capacity - 1@ (capacity
is a power of two) and maps a hash to its home slot.
-}
data HashTable s = HashTable
    { htHash :: !(VUM.MVector s Int)
    , htGroup :: !(VUM.MVector s Int)
    , htRep :: !(VUM.MVector s Int)
    , htMask :: !Int
    }

{- | Smallest power of two strictly greater than @n@, at least 2. Sizes the
table so the load factor stays below ~0.5 even when every row is a distinct
group.
-}
nextPow2Above :: Int -> Int
nextPow2Above n = go 2
  where
    go !p
        | p > n = p
        | otherwise = go (p * 2)
{-# INLINE nextPow2Above #-}

{- | Allocate an empty table able to hold up to @n@ distinct groups while
keeping the load factor under ~0.5 (capacity @= nextPow2Above (2*n)@). All
group slots start empty (@-1@).
-}
newHashTable :: (PrimMonad m) => Int -> m (HashTable (PrimState m))
newHashTable n = do
    let !cap = nextPow2Above (2 * max 1 n)
    h <- VUM.unsafeNew cap
    g <- VUM.replicate cap (-1)
    r <- VUM.unsafeNew cap
    pure (HashTable h g r (cap - 1))
{-# INLINE newHashTable #-}

{- | Look up @row@ (with precomputed @hash@) and return its dense group id: an
empty slot starts a new group via @nextGroup@, a stored-hash match is re-verified
with @eqRow@ before reuse. The 'Bool' is 'True' when a new group was created.
-}
htInsert ::
    (PrimMonad m) =>
    HashTable (PrimState m) ->
    -- | @eqRow a b@: do rows @a@ and @b@ have equal key columns?
    (Int -> Int -> Bool) ->
    -- | Next dense group id to assign if this row starts a new group.
    Int ->
    -- | Row index being inserted.
    Int ->
    -- | Precomputed hash of the row's key.
    Int ->
    m (Int, Bool)
htInsert ht eqRow nextGroup row hash = go (homeSlot mask hash)
  where
    !mask = htMask ht
    !hs = htHash ht
    !gs = htGroup ht
    !rs = htRep ht
    go !slot = do
        g <- VUM.unsafeRead gs slot
        if g < 0
            then do
                VUM.unsafeWrite hs slot hash
                VUM.unsafeWrite gs slot nextGroup
                VUM.unsafeWrite rs slot row
                pure (nextGroup, True)
            else do
                h <- VUM.unsafeRead hs slot
                if h == hash
                    then do
                        rep <- VUM.unsafeRead rs slot
                        if eqRow rep row
                            then pure (g, False)
                            else go ((slot + 1) .&. mask)
                    else go ((slot + 1) .&. mask)
{-# INLINE htInsert #-}

{- | Home slot of a hash: the top @log2 cap@ bits of a Fibonacci multiply. The
row hash's final FxHash step is a multiply, which leaves its LOW bits poorly
diffused — raw-text keys cluster into contiguous linear-probe pileups when
slotted by @hash .&. mask@, so the home slot must come from the top bits.
-}
homeSlot :: Int -> Int -> Int
homeSlot !mask !hash =
    fromIntegral
        ( (fromIntegral hash * (0x9E3779B97F4A7C15 :: Word64))
            `unsafeShiftR` (64 - popCount mask)
        )
{-# INLINE homeSlot #-}
