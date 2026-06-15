{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
A flat, unboxed, open-addressing (linear-probe) hash table that maps a row's
key-hash to a /dense group id/, verifying the real key on every hash hit.

The table is three parallel unboxed 'VUM.MVector's keyed by hash slot:

  * @htHash@   — the stored hash at each slot.
  * @htGroup@  — the dense group id stored at each slot (@-1@ marks an empty
    slot, since real group ids are @>= 0@).
  * @htRep@    — the representative row index of that group, used to re-verify
    the real key columns on a hash hit and so reject collisions.

It is 'PrimMonad'-polymorphic: it runs in 'Control.Monad.ST.ST' for the current
single-threaded 'DataFrame.Internal.Grouping.groupBy' and can run in 'IO' inside
a per-worker partition once grouping is parallelised. The lookup-or-insert loop
('htInsert') trusts the caller-supplied @eqRow@ predicate to compare the key
columns of two rows by index, fixing the hash-only bucketing that previously
merged colliding keys.
-}
module DataFrame.Internal.HashTable (
    HashTable (..),
    newHashTable,
    htInsert,
    nextPow2Above,
) where

import Control.Monad.Primitive (PrimMonad, PrimState)
import Data.Bits ((.&.))
import qualified Data.Vector.Unboxed.Mutable as VUM

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

{- | Look up @row@ (with precomputed @hash@) in the table, returning its dense
group id. On an empty slot the row starts a new group: the caller's
@nextGroup@ thunk supplies the next dense id, and the row is recorded as that
group's representative. On a stored-hash match the real key is re-verified with
@eqRow rep row@ before the existing id is returned; a mismatch is a hash
collision and probing continues. The returned 'Bool' is 'True' when a new group
was created, letting the caller bump its group counter without a second read.
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
htInsert ht eqRow nextGroup row hash = go (hash .&. mask)
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
