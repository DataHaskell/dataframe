{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Mutable, growable column builders for high-throughput ingest. No
per-append @IORef@ traffic: hot counters live in an unboxed vector, payloads
double on demand, and validity is only materialized once a null is seen.
-}
module DataFrame.Internal.ColumnBuilder (
    ColumnBuilder (..),
    NumBuilder,
    IntBuilder,
    DoubleBuilder,
    TextBuilder,
    TextChunk (..),
    newIntBuilder,
    newDoubleBuilder,
    newNumBuilder,
    newTextBuilder,
    appendInt,
    appendDouble,
    appendNum,
    appendText,
    appendTextSlice,
    appendTextSliceFromPtr,
    freezeTextChunk,
    mergeColumns,
    mergeTextChunks,
) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (when)
import Control.Monad.ST (ST)
import Data.Bits (shiftR)
import Data.STRef
import Data.Text.Internal (Text (..))
import Data.Word (Word8)
import DataFrame.Internal.Column hiding (mergeColumns)
import DataFrame.Internal.ColumnMerge (
    TextChunk (..),
    mergeColumns,
    mergeTextChunks,
    packValidity,
 )
import Foreign.Ptr (Ptr)

{- | Operations shared by all column builders. A builder must not be used
again after 'freezeBuilder' (its storage is frozen in place, not copied).
-}
class ColumnBuilder b where
    -- | Append a null row (sentinel payload + invalid bit).
    appendNull :: b s -> ST s ()

    -- | Rows appended so far.
    builderLength :: b s -> ST s Int

    -- | Freeze into a fully-forced 'Column'; bitmap only when a null was seen.
    freezeBuilder :: b s -> ST s Column

-- Counter slots shared by the builders: rows, any-null flag, text bytes used.
cRows, cAnyNull, cBytes :: Int
cRows = 0
cAnyNull = 1
cBytes = 2

{- | Builder for unboxed numeric payloads ('Int', 'Double', ...). 'nbNull'
is the sentinel written into null slots (protected by the bitmap).
-}
data NumBuilder a s = NumBuilder
    { nbNull :: !a
    , nbCounters :: !(VUM.MVector s Int)
    , nbArrays :: !(STRef s (NumArrays a s))
    }

data NumArrays a s = NumArrays
    { naData :: !(VUM.MVector s a)
    , naValid :: !(VUM.MVector s Word8)
    }

type IntBuilder = NumBuilder Int

type DoubleBuilder = NumBuilder Double

-- | New numeric builder with a row-capacity hint and a null sentinel.
newNumBuilder :: (VU.Unbox a) => a -> Int -> ST s (NumBuilder a s)
newNumBuilder nullValue hint = do
    let cap = max 16 hint
    counters <- VUM.replicate 2 0
    dat <- VUM.unsafeNew cap
    val <- VUM.unsafeNew cap
    NumBuilder nullValue counters <$> newSTRef (NumArrays dat val)

newIntBuilder :: Int -> ST s (IntBuilder s)
newIntBuilder = newNumBuilder 0

newDoubleBuilder :: Int -> ST s (DoubleBuilder s)
newDoubleBuilder = newNumBuilder 0

appendNum :: (VU.Unbox a) => NumBuilder a s -> a -> ST s ()
appendNum b !x = do
    n <- VUM.unsafeRead (nbCounters b) cRows
    anyNull <- VUM.unsafeRead (nbCounters b) cAnyNull
    NumArrays dat val <- reserveNum b n
    VUM.unsafeWrite dat n x
    when (anyNull /= 0) $ VUM.unsafeWrite val n 1
    VUM.unsafeWrite (nbCounters b) cRows (n + 1)
{-# INLINE appendNum #-}

appendInt :: IntBuilder s -> Int -> ST s ()
appendInt = appendNum
{-# INLINE appendInt #-}

appendDouble :: DoubleBuilder s -> Double -> ST s ()
appendDouble = appendNum
{-# INLINE appendDouble #-}

-- Fetch the arrays, growing (doubling) first if row @n@ would not fit.
reserveNum :: (VU.Unbox a) => NumBuilder a s -> Int -> ST s (NumArrays a s)
reserveNum b n = do
    arrs <- readSTRef (nbArrays b)
    if n < VUM.length (naData arrs) then pure arrs else growNum b arrs
{-# INLINE reserveNum #-}

growNum ::
    (VU.Unbox a) => NumBuilder a s -> NumArrays a s -> ST s (NumArrays a s)
growNum b (NumArrays dat val) = do
    let cap = VUM.length dat
    dat' <- VUM.unsafeGrow dat cap
    val' <- VUM.unsafeGrow val cap
    let arrs = NumArrays dat' val'
    writeSTRef (nbArrays b) arrs
    pure arrs

instance (Columnable a, VU.Unbox a) => ColumnBuilder (NumBuilder a) where
    appendNull b = do
        n <- VUM.unsafeRead (nbCounters b) cRows
        anyNull <- VUM.unsafeRead (nbCounters b) cAnyNull
        NumArrays dat val <- reserveNum b n
        VUM.unsafeWrite dat n (nbNull b)
        when (anyNull == 0) $ do
            VUM.set (VUM.slice 0 n val) 1
            VUM.unsafeWrite (nbCounters b) cAnyNull 1
        VUM.unsafeWrite val n 0
        VUM.unsafeWrite (nbCounters b) cRows (n + 1)
    {-# INLINE appendNull #-}

    builderLength b = VUM.unsafeRead (nbCounters b) cRows

    freezeBuilder b = do
        n <- VUM.unsafeRead (nbCounters b) cRows
        anyNull <- VUM.unsafeRead (nbCounters b) cAnyNull
        NumArrays dat val <- readSTRef (nbArrays b)
        !vs <- freezeTrimmed n dat
        if anyNull /= 0
            then do
                !bm <- packValidity n val
                pure $! UnboxedColumn (Just bm) vs
            else pure $! UnboxedColumn Nothing vs

-- Zero-copy freeze; copies to exact size when slack exceeds a quarter of n.
freezeTrimmed :: (VU.Unbox a) => Int -> VUM.MVector s a -> ST s (VU.Vector a)
freezeTrimmed n mv
    | VUM.length mv - n <= n `shiftR` 2 = VU.unsafeFreeze (VUM.slice 0 n mv)
    | otherwise = VU.freeze (VUM.slice 0 n mv)

{- | Builder for 'Text' columns. All field bytes go into one exponentially
grown byte array; rows are recorded as offsets, so an append is a memcpy
and freezing slices 'Text' values off the shared array without copying.
-}
data TextBuilder s = TextBuilder
    { tbCounters :: !(VUM.MVector s Int)
    , tbArrays :: !(STRef s (TextArrays s))
    }

data TextArrays s = TextArrays
    { taBytes :: !(A.MArray s)
    , taByteCap :: !Int
    , taOffsets :: !(VUM.MVector s Int)
    -- ^ Row @i@ spans bytes @[offsets!i, offsets!(i+1))@.
    , taValid :: !(VUM.MVector s Word8)
    }

-- | New text builder with row-count and total-byte capacity hints.
newTextBuilder :: Int -> Int -> ST s (TextBuilder s)
newTextBuilder rowHint byteHint = do
    let rcap = max 16 rowHint
        bcap = max 64 byteHint
    counters <- VUM.replicate 3 0
    bytes <- A.new bcap
    offsets <- VUM.unsafeNew (rcap + 1)
    VUM.unsafeWrite offsets 0 0
    val <- VUM.unsafeNew rcap
    TextBuilder counters <$> newSTRef (TextArrays bytes bcap offsets val)

-- | Append @len@ raw bytes at @off@ in @src@ as one field (one memcpy).
appendTextSlice :: TextBuilder s -> A.Array -> Int -> Int -> ST s ()
appendTextSlice b src off len = do
    (n, pos, arrs) <- reserveText b len
    A.copyI len (taBytes arrs) pos src off
    finishTextAppend b arrs n (pos + len)
{-# INLINE appendTextSlice #-}

-- | 'appendTextSlice' from foreign memory (e.g. an mmapped file buffer).
appendTextSliceFromPtr :: TextBuilder s -> Ptr Word8 -> Int -> ST s ()
appendTextSliceFromPtr b ptr len = do
    (n, pos, arrs) <- reserveText b len
    A.copyFromPointer (taBytes arrs) pos ptr len
    finishTextAppend b arrs n (pos + len)
{-# INLINE appendTextSliceFromPtr #-}

-- | Append an already-decoded 'Text' (its bytes are UTF-8 already).
appendText :: TextBuilder s -> T.Text -> ST s ()
appendText b (Text src off len) = appendTextSlice b src off len
{-# INLINE appendText #-}

finishTextAppend :: TextBuilder s -> TextArrays s -> Int -> Int -> ST s ()
finishTextAppend b arrs n endPos = do
    anyNull <- VUM.unsafeRead (tbCounters b) cAnyNull
    when (anyNull /= 0) $ VUM.unsafeWrite (taValid arrs) n 1
    VUM.unsafeWrite (taOffsets arrs) (n + 1) endPos
    VUM.unsafeWrite (tbCounters b) cRows (n + 1)
    VUM.unsafeWrite (tbCounters b) cBytes endPos
{-# INLINE finishTextAppend #-}

reserveText :: TextBuilder s -> Int -> ST s (Int, Int, TextArrays s)
reserveText b extra = do
    n <- VUM.unsafeRead (tbCounters b) cRows
    pos <- VUM.unsafeRead (tbCounters b) cBytes
    arrs <- readSTRef (tbArrays b)
    arrs' <-
        if n < VUM.length (taValid arrs) && pos + extra <= taByteCap arrs
            then pure arrs
            else growText b arrs (n + 1) (pos + extra)
    pure (n, pos, arrs')
{-# INLINE reserveText #-}

growText :: TextBuilder s -> TextArrays s -> Int -> Int -> ST s (TextArrays s)
growText b (TextArrays bytes bcap offsets val) needRows needBytes = do
    let rcap = VUM.length val
    (offsets', val') <-
        if needRows > rcap
            then do
                let rcap' = max (2 * rcap) needRows
                o <- VUM.unsafeGrow offsets (rcap' - rcap)
                v <- VUM.unsafeGrow val (rcap' - rcap)
                pure (o, v)
            else pure (offsets, val)
    (bytes', bcap') <-
        if needBytes > bcap
            then do
                let cap' = max (2 * bcap) needBytes
                bs <- A.resizeM bytes cap'
                pure (bs, cap')
            else pure (bytes, bcap)
    let arrs = TextArrays bytes' bcap' offsets' val'
    writeSTRef (tbArrays b) arrs
    pure arrs

{- | Freeze a 'TextBuilder' into a raw 'TextChunk' for byte-level merging
('mergeTextChunks'): no 'T.Text' values are created until chunks merge.
-}
freezeTextChunk :: TextBuilder s -> ST s TextChunk
freezeTextChunk b = do
    n <- VUM.unsafeRead (tbCounters b) cRows
    anyNull <- VUM.unsafeRead (tbCounters b) cAnyNull
    used <- VUM.unsafeRead (tbCounters b) cBytes
    TextArrays bytes bcap offsets val <- readSTRef (tbArrays b)
    when (used < bcap) (A.shrinkM bytes used)
    arr <- A.unsafeFreeze bytes
    offs <- VU.unsafeFreeze (VUM.slice 0 (n + 1) offsets)
    bm <-
        if anyNull /= 0
            then Just <$> packValidity n val
            else pure Nothing
    pure (TextChunk arr used offs bm)

instance ColumnBuilder TextBuilder where
    appendNull b = do
        (n, pos, arrs) <- reserveText b 0
        anyNull <- VUM.unsafeRead (tbCounters b) cAnyNull
        when (anyNull == 0) $ do
            VUM.set (VUM.slice 0 n (taValid arrs)) 1
            VUM.unsafeWrite (tbCounters b) cAnyNull 1
        VUM.unsafeWrite (taValid arrs) n 0
        VUM.unsafeWrite (taOffsets arrs) (n + 1) pos
        VUM.unsafeWrite (tbCounters b) cRows (n + 1)
        VUM.unsafeWrite (tbCounters b) cBytes pos
    {-# INLINE appendNull #-}

    builderLength b = VUM.unsafeRead (tbCounters b) cRows

    freezeBuilder b = do
        chunk <- freezeTextChunk b
        pure $! mergeTextChunks [chunk]
