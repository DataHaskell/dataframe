{-# LANGUAGE BangPatterns #-}

{- | Packed-text payload + byte-slice primitives. A 'PackedTextData' shares one
UTF-8 byte buffer across all rows of a string column, with @n+1@ row offsets, so
no per-row 'Data.Text.Text' header is materialized until decode is demanded.
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
@[offsets!r, offsets!(r+1))@); validity lives in the column's bitmap. @ptSel@ is
an optional selection layer letting a gather/join/sort result share the buffer.
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

{- | Reindex a packed payload by a selection vector, sharing the byte buffer;
logical row @i@ becomes base row @indices!i@. A negative or out-of-range index
decodes to the empty slice. Composes with an existing selection.
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

{- | Take the first @k@ logical rows, sharing the byte buffer via a capped
selection layer. O(k), no byte copy or decode — cheap @take@/display on a
large packed column.
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
unselected base; a selected (gathered) payload returns 'Nothing' (its rows are
non-contiguous). Lets the boxed-Text fallback take the fast contiguous path.
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
