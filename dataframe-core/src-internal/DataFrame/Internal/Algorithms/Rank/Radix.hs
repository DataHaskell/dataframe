{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Stable rank of a set of group representatives by ascending unsigned hash
order. Shared by the sequential and parallel group-by canonical-ordering steps
so they stay bit-for-bit identical. @O(ng)@ stable LSD radix sort.
-}
module DataFrame.Internal.Algorithms.Rank.Radix (
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
ascending signed-'Int' order of @h@. Reinterpreted to 'Int' for the byte-wise
radix passes (the byte mask makes the sign extension irrelevant).
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
            pass 0 keysA orderA keysB orderB
            pass 8 keysB orderB keysA orderA
            pass 16 keysA orderA keysB orderB
            pass 24 keysB orderB keysA orderA
            pass 32 keysA orderA keysB orderB
            pass 40 keysB orderB keysA orderA
            pass 48 keysA orderA keysB orderB
            pass 56 keysB orderB keysA orderA
            let inv !r
                    | r >= ng = pure ()
                    | otherwise = do
                        g <- VUM.unsafeRead orderA r
                        VUM.unsafeWrite rankM g r
                        inv (r + 1)
            inv 0
    VU.unsafeFreeze rankM
{-# INLINEABLE rankByHash #-}
