{-# LANGUAGE BangPatterns #-}

{- | Packed-text payload + byte-slice primitives. A 'PackedTextData' shares a
single UTF-8 byte buffer across all rows of a string column, with @n+1@ row
offsets, so no per-row 'Data.Text.Text' header is materialized at freeze.
'Data.Text.Text' is produced only on demand (display, typed extraction) via
the same decode path that the boxed-Text builder used.

A gathered/joined/sorted result keeps sharing that buffer: instead of copying
bytes it carries a @ptSel@ selection vector that reindexes the base rows, so a
permuted or row-exploded column stays a 'PackedText' (shared buffer + permuted
indices) rather than materializing back to boxed 'Data.Text.Text'.
-}
module DataFrame.Internal.PackedText (
    PackedTextData (..),
    mkPackedContiguous,
    packedGather,
    packedTake,
    packedRowOffsetVec,
    packedLength,
    packedSlice,
    packedIndexText,
    sliceEqBytes,
    sliceCmpBytes,
) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU

import Data.Ord (comparing)
import Data.Text.Internal (Text (Text))
import DataFrame.Internal.Utf8 (isValidUtf8Slice, lenientDecodeSlice)

{- | A shared UTF-8 byte buffer plus @n+1@ row offsets (base row @r@ spans bytes
@[offsets!r, offsets!(r+1))@). Validity lives in the enclosing column's
@Maybe Bitmap@, mirroring 'BoxedColumn'/'UnboxedColumn'.

@ptSel@ is an optional selection layer: when @Nothing@ the column is the
contiguous base (row @i@ == base row @i@). When @Just sel@, logical row @i@ is
base row @sel!i@; this is how a gather/join/sort result shares the buffer
without copying bytes. Out-of-range entries in @sel@ (e.g. a join @-1@
sentinel) decode to the empty slice and are masked by the column bitmap.
-}
data PackedTextData = PackedTextData
    { ptBytes :: {-# UNPACK #-} !A.Array
    , ptOffsets :: {-# UNPACK #-} !(VU.Vector Int)
    , ptSel :: !(Maybe (VU.Vector Int))
    }

-- | Build a contiguous packed payload (no selection): the freeze-path shape.
mkPackedContiguous :: A.Array -> VU.Vector Int -> PackedTextData
mkPackedContiguous arr offs = PackedTextData arr offs Nothing
{-# INLINE mkPackedContiguous #-}

{- | Reindex a packed payload by a selection vector, sharing the byte buffer
and base offsets. Logical row @i@ becomes base row @indices!i@. A negative or
out-of-range index decodes to the empty slice (callers mask it with a bitmap).
Composes with an existing selection so a gather of a gather still shares the
buffer.
-}
packedGather :: VU.Vector Int -> PackedTextData -> PackedTextData
packedGather indices (PackedTextData arr offs msel) =
    let !base = VU.length offs - 1
        clamp r = if r >= 0 && r < base then r else -1
        sel' = case msel of
            Nothing -> VU.map clamp indices
            Just s ->
                VU.map
                    (\i -> if i >= 0 && i < VU.length s then clamp (VU.unsafeIndex s i) else -1)
                    indices
     in PackedTextData arr offs (Just sel')
{-# INLINE packedGather #-}

{- | Take the first @k@ logical rows, sharing the byte buffer. With a selection
layer the selection is sliced to @k@ entries; without one a base-row selection
@[0 .. k-1]@ is installed (slicing the contiguous offsets would still leave the
trailing bytes addressable, but a short selection caps 'packedLength' to @k@).
O(k), no byte copy or decode — the fix for cheap @take@/display on a 1e7-row
packed column.
-}
packedTake :: Int -> PackedTextData -> PackedTextData
packedTake k (PackedTextData arr offs msel) =
    let !base = VU.length offs - 1
        !k' = max 0 k
     in case msel of
            Just s -> PackedTextData arr offs (Just (VU.take k' s))
            Nothing -> PackedTextData arr offs (Just (VU.enumFromN 0 (min k' base)))
{-# INLINE packedTake #-}

-- | Map a logical row index to its base row, honoring any selection layer.
baseRow :: PackedTextData -> Int -> Int
baseRow (PackedTextData _ _ Nothing) i = i
baseRow (PackedTextData _ _ (Just sel)) i = VU.unsafeIndex sel i
{-# INLINE baseRow #-}

-- | Row count: @length sel@ when selected, else @length offsets - 1@.
packedLength :: PackedTextData -> Int
packedLength (PackedTextData _ offs Nothing) = VU.length offs - 1
packedLength (PackedTextData _ _ (Just sel)) = VU.length sel
{-# INLINE packedLength #-}

-- | Raw byte slice for logical row @i@: @(buffer, offset, length)@. The hot accessor.
packedSlice :: PackedTextData -> Int -> (A.Array, Int, Int)
packedSlice p@(PackedTextData arr offs _) i =
    let !r = baseRow p i
     in if r < 0
            then (arr, 0, 0)
            else
                let o = VU.unsafeIndex offs r in (arr, o, VU.unsafeIndex offs (r + 1) - o)
{-# INLINE packedSlice #-}

{- | The shared buffer + contiguous @n+1@ offsets when the payload is the
unselected base. A selected (gathered) payload has non-contiguous rows that a
single offset vector cannot express, so this returns @Nothing@ and the caller
decodes per-row via 'packedIndexText'. Lets the boxed-Text fallback take the
fast contiguous 'sliceTextVector' path when possible.
-}
packedRowOffsetVec :: PackedTextData -> Maybe (A.Array, VU.Vector Int)
packedRowOffsetVec (PackedTextData arr offs Nothing) = Just (arr, offs)
packedRowOffsetVec _ = Nothing
{-# INLINE packedRowOffsetVec #-}

{- | On-demand single 'Data.Text.Text' for row @i@, using the same
validate-or-lenient decode as 'sliceTextVector' so output is bit-identical.
-}
packedIndexText :: PackedTextData -> Int -> T.Text
packedIndexText p i =
    let (arr, o, l) = packedSlice p i
     in decodeField arr o l
{-# INLINE packedIndexText #-}

-- Decode one field exactly as 'sliceTextVector' does per row.
decodeField :: A.Array -> Int -> Int -> T.Text
decodeField arr o l
    | l == 0 = T.empty
    | isValidUtf8Slice arr o l = Text arr o l
    | otherwise = lenientDecodeSlice arr o l
{-# INLINE decodeField #-}

{- | Byte-wise equality of two slices. UTF-8 is injective on valid scalar
sequences and lenient decode is deterministic, so this agrees with
@Text@'s '==' on the decoded values.
-}
sliceEqBytes :: A.Array -> Int -> Int -> A.Array -> Int -> Int -> Bool
sliceEqBytes a ao al b bo bl
    | al /= bl = False
    | otherwise = go 0
  where
    go !k
        | k >= al = True
        | A.unsafeIndex a (ao + k) == A.unsafeIndex b (bo + k) = go (k + 1)
        | otherwise = False
{-# INLINE sliceEqBytes #-}

{- | Unsigned byte-lexicographic comparison (memcmp semantics). For
well-formed UTF-8 this matches 'Data.Text.compare' exactly, since UTF-8
byte order equals codepoint order for all valid scalars.
-}
sliceCmpBytes :: A.Array -> Int -> Int -> A.Array -> Int -> Int -> Ordering
sliceCmpBytes a ao al b bo bl = go 0
  where
    !m = min al bl
    go !k
        | k >= m = compare al bl
        | otherwise = case comparing id (A.unsafeIndex a (ao + k)) (A.unsafeIndex b (bo + k)) of
            EQ -> go (k + 1)
            r -> r
{-# INLINE sliceCmpBytes #-}
