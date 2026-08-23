{-# LANGUAGE BangPatterns #-}

module DataFrame.Internal.Column.Bitmap where

import Control.Monad (foldM_, forM_, when)
import Control.Monad.ST (ST, runST)
import Data.Bits (complement, setBit, shiftL, shiftR, testBit, (.&.), (.|.))
import Data.List (foldl')
import Data.Maybe (fromMaybe, isNothing)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word8)

-- | A bit-packed validity bitmap. Bit @i@ = 1 means row @i@ is valid (not null).
type Bitmap = VU.Vector Word8

-- | A bitmap attached to its row counts so we can splice it.
data Validity = Validity {-# UNPACK #-} !(Maybe Bitmap) {-# UNPACK #-} !Int

vBitmap :: Validity -> Maybe Bitmap
vBitmap (Validity bm _) = bm

vRowCount :: Validity -> Int
vRowCount (Validity _ n) = n

{- | Test whether row @i@ is valid (not null) in a bitmap.

The bit-level arithmetric is dense but read this as:
`shiftR` 3 is equivalent to `div` 8, and `.&. 7` is equivalent to `mod` 8.
-}
bitmapTestBit :: Bitmap -> Int -> Bool
bitmapTestBit bm i = testBit (VU.unsafeIndex bm (i `shiftR` 3)) (i .&. 7)
{-# INLINE bitmapTestBit #-}

-- | Build a fully-valid bitmap for @n@ rows (all bits set).
allValidBitmap :: Int -> Bitmap
allValidBitmap n = runST (allValidBitmap' n >>= VU.unsafeFreeze)
{-# INLINE allValidBitmap #-}

allValidBitmap' :: Int -> ST s (VUM.MVector s Word8)
allValidBitmap' n =
    let
        bytes = (n + 7) `shiftR` 3
        lastBits = n .&. 7
        lastByte = if lastBits == 0 then 0xFF else (1 `shiftL` lastBits) - 1
     in
        if bytes == 0
            then VUM.new 0
            else do
                mv <- VUM.replicate bytes (0xFF :: Word8) :: ST s (VUM.MVector s Word8)
                when (lastBits /= 0) $ VUM.unsafeWrite mv (bytes - 1) lastByte
                pure mv
{-# INLINE allValidBitmap' #-}

{- | Build a bitmap from a @VU.Vector Word8@ validity vector
(1 = valid, 0 = null), as produced by Arrow / Parquet decoders.
-}
buildBitmapFromValid :: VU.Vector Word8 -> Bitmap
buildBitmapFromValid valid =
    let n = VU.length valid
        bytes = (n + 7) `shiftR` 3
     in VU.generate bytes $ \b ->
            let base = b `shiftL` 3
                setBitIf acc bit =
                    let idx = base + bit
                     in if idx < n && VU.unsafeIndex valid idx /= 0
                            then setBit acc bit
                            else acc
             in foldl' setBitIf (0 :: Word8) [0 .. 7]

{- | Build a bitmap from a list of null-row indices.
@nullIdxs@ are the positions that are NULL.
-}
buildBitmapFromNulls :: Int -> [Int] -> Bitmap
buildBitmapFromNulls n idxs = buildBitmapFromNulls' n (VU.fromList idxs)

buildBitmapFromNulls' :: Int -> VU.Vector Int -> VU.Vector Word8
buildBitmapFromNulls' n nullIdxs = runST $ do
    bm' <- allValidBitmap' n
    VU.forM_ nullIdxs $ \i -> do
        let byteIdx = i `shiftR` 3
            bitIdx = i .&. 7
        v <- VUM.unsafeRead bm' byteIdx
        VUM.unsafeWrite bm' byteIdx (clearBit8 v bitIdx)
    VU.unsafeFreeze bm'
  where
    clearBit8 :: Word8 -> Int -> Word8
    clearBit8 b bit = b .&. complement (1 `shiftL` bit)

-- | Slice a bitmap for rows @[start .. start+len-1]@.
bitmapSlice :: Int -> Int -> Bitmap -> Bitmap
bitmapSlice start len bm
    | start .&. 7 == 0 =
        let startByte = start `shiftR` 3
            bytes = min ((len + 7) `shiftR` 3) (VU.length bm - startByte)
         in VU.slice startByte bytes bm
    | otherwise =
        let n = min len (VU.length bm `shiftL` 3 - start)
         in buildBitmapFromValid $
                VU.generate n $
                    \i -> if bitmapTestBit bm (start + i) then 1 else 0

-- | Concatenate two bitmaps covering @n1@ and @n2@ rows respectively.
bitmapConcat :: Int -> Bitmap -> Int -> Bitmap -> Bitmap
bitmapConcat n1 bm1 n2 bm2 =
    buildBitmapFromValid $
        VU.generate (n1 + n2) $ \i ->
            if i < n1
                then if bitmapTestBit bm1 i then 1 else 0
                else if bitmapTestBit bm2 (i - n1) then 1 else 0

-- | Combine two bitmaps with AND (both must be valid for result to be valid).
andBitmaps :: Bitmap -> Bitmap -> Bitmap
andBitmaps = VU.zipWith (.&.)

{- | Splice chunk bitmaps end to end at the bit level. 'Nothing' if no chunk
carries a bitmap; chunks without one count as all-valid otherwise.
-}
concatValidity :: [Validity] -> Maybe Bitmap
concatValidity parts
    | all (isNothing . vBitmap) parts = Nothing
    | otherwise = Just $ VU.create $ do
        let total = sum (map vRowCount parts)
            outBytes = (total + 7) `shiftR` 3
        mv <- VUM.replicate outBytes 0
        let orInto i w =
                when (i < outBytes && w /= 0) $ do
                    old <- VUM.unsafeRead mv i
                    VUM.unsafeWrite mv i (old .|. w)
            splice !bitPos (Validity !mb !len) = do
                let bm = fromMaybe (allValidBitmap len) mb
                    sh = bitPos .&. 7
                    byte0 = bitPos `shiftR` 3
                    lastIdx = ((len + 7) `shiftR` 3) - 1
                    tailBits = len .&. 7
                    lastMask =
                        if tailBits == 0 then 0xFF else (1 `shiftL` tailBits) - 1
                forM_ [0 .. lastIdx] $ \k -> do
                    let raw = VU.unsafeIndex bm k
                        masked = if k == lastIdx then raw .&. lastMask else raw
                        w = fromIntegral masked :: Word
                    orInto (byte0 + k) (fromIntegral (w `shiftL` sh))
                    when (sh /= 0) $
                        orInto (byte0 + k + 1) (fromIntegral (w `shiftR` (8 - sh)))
                pure (bitPos + len)
        foldM_ splice 0 parts
        pure mv

-- | Pack a 0\/1 byte-per-row validity prefix into a bit-packed 'Bitmap'.
packValidity :: Int -> VUM.MVector s Word8 -> ST s Bitmap
packValidity n val = do
    bytes <- VU.unsafeFreeze (VUM.slice 0 n val)
    let assemble b =
            let base = b `shiftL` 3
                m = min 8 (n - base)
                go !acc !k
                    | k >= m = acc
                    | VU.unsafeIndex bytes (base + k) /= 0 =
                        go (acc .|. (1 `shiftL` k)) (k + 1)
                    | otherwise = go acc (k + 1)
             in go (0 :: Word8) 0
    pure $! VU.generate ((n + 7) `shiftR` 3) assemble
