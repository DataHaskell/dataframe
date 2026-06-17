{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Stable rank of a set of group representatives by the ascending unsigned order of
their hash. Shared by the sequential ('DataFrame.Internal.Grouping') and parallel
('DataFrame.Internal.GroupingPar') group-by canonical-ordering steps so they
stay bit-for-bit identical.

@rankByHash readHash ng@ returns @rank@ with @rank[gid] = position@ of group
@gid@ when groups are ordered by ascending unsigned 'sortKey' of @readHash gid@.
A stable LSD radix sort (8 bits per pass, 8 passes) keeps groups with equal
hash in their original @gid@ order; callers number @gid@s so that this matches
the @repRow@ tie-break of the old comparison sort. @O(ng)@, no boxed tuples or
comparison closures — the lever for the @1e7@-distinct-group case (Q10).
-}
module DataFrame.Internal.RadixRank (
    rankByHash,
    sortKey,
) where

import Control.Monad (when)
import Control.Monad.Primitive (PrimMonad)
import Data.Bits (unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)

{- | Unsigned sort key of a hash: ascending 'Word64' order of @sortKey h@ equals
ascending signed-'Int' order of @h@. Reinterpreted back to 'Int' for the
byte-wise radix passes (the @.&. 0xff@ byte mask makes the arithmetic shift's
sign extension irrelevant).
-}
sortKey :: Int -> Int
sortKey h = fromIntegral (fromIntegral h + 0x8000000000000000 :: Word64)
{-# INLINE sortKey #-}

-- | See the module header. @readHash@ supplies the hash of local group @gid@.
rankByHash ::
    forall m. (PrimMonad m) => (Int -> m Int) -> Int -> m (VU.Vector Int)
rankByHash readHash ng = do
    rankM <- VUM.new (max 1 ng)
    if ng <= 1
        then when (ng == 1) (VUM.unsafeWrite rankM 0 0)
        else do
            keysA <- VUM.new ng
            orderA <- VUM.new ng
            let seed !i
                    | i >= ng = pure ()
                    | otherwise = do
                        h <- readHash i
                        VUM.unsafeWrite keysA i (sortKey h)
                        VUM.unsafeWrite orderA i i
                        seed (i + 1)
            seed 0
            keysB <- VUM.new ng
            orderB <- VUM.new ng
            counts <- VUM.new 256
            let pass ::
                    Int ->
                    VUM.MVector (VUM.PrimState m) Int ->
                    VUM.MVector (VUM.PrimState m) Int ->
                    VUM.MVector (VUM.PrimState m) Int ->
                    VUM.MVector (VUM.PrimState m) Int ->
                    m ()
                pass !shiftBits !srcK !srcO !dstK !dstO = do
                    VUM.set counts 0
                    let count !i
                            | i >= ng = pure ()
                            | otherwise = do
                                k <- VUM.unsafeRead srcK i
                                let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                                VUM.unsafeRead counts b >>= VUM.unsafeWrite counts b . (+ 1)
                                count (i + 1)
                    count 0
                    let scan !b !acc
                            | b >= 256 = pure ()
                            | otherwise = do
                                c <- VUM.unsafeRead counts b
                                VUM.unsafeWrite counts b acc
                                scan (b + 1) (acc + c)
                    scan 0 0
                    let place !i
                            | i >= ng = pure ()
                            | otherwise = do
                                k <- VUM.unsafeRead srcK i
                                o <- VUM.unsafeRead srcO i
                                let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                                pos <- VUM.unsafeRead counts b
                                VUM.unsafeWrite counts b (pos + 1)
                                VUM.unsafeWrite dstK pos k
                                VUM.unsafeWrite dstO pos o
                                place (i + 1)
                    place 0
            -- 8 stable passes over the 64-bit key; ping-pong so the final
            -- sorted order lands back in (keysA, orderA).
            pass 0 keysA orderA keysB orderB
            pass 8 keysB orderB keysA orderA
            pass 16 keysA orderA keysB orderB
            pass 24 keysB orderB keysA orderA
            pass 32 keysA orderA keysB orderB
            pass 40 keysB orderB keysA orderA
            pass 48 keysA orderA keysB orderB
            pass 56 keysB orderB keysA orderA
            -- orderA[rank] = gid; invert to rank[gid] = rank.
            let inv !r
                    | r >= ng = pure ()
                    | otherwise = do
                        g <- VUM.unsafeRead orderA r
                        VUM.unsafeWrite rankM g r
                        inv (r + 1)
            inv 0
    VU.unsafeFreeze rankM
{-# INLINEABLE rankByHash #-}
