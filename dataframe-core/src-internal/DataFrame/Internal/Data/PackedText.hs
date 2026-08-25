{-# LANGUAGE BangPatterns #-}

{- | Packed-text payload + byte-slice primitives. A 'PackedTextData' shares one
UTF-8 byte buffer across all rows of a string column, with @n+1@ row offsets, so
no per-row 'Data.Text.Text' header is materialized until decode is demanded.
Offsets and selection vectors are stored 'Int32' whenever their values fit
(Arrow-style), halving the per-row footprint of large string columns.
-}
module DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    PackedOffsets (..),
    PackedSel (..),
    offAt,
    offCount,
    selAt,
    selLength,
    mkPackedContiguous,
    mkPackedContiguous32,
    mkOffsets,
    mkSel,
    packedGather,
    packedTake,
    packedRowOffsets,
    packedLength,
    packedSlice,
    packedIndexText,
    sliceEqBytes,
    sliceCmpBytes,
) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU

import Data.Int (Int32)
import Data.Ord (comparing)
import Data.Text.Internal (Text (Text))
import DataFrame.Internal.Data.PackedText.Utf8 (
    isValidUtf8Slice,
    lenientDecodeSlice,
 )

{- | Row byte-offsets, physically 'Int32' when every value fits (total buffer
bytes < 2^31) and 'Int' otherwise. Values are non-negative byte positions.
-}
data PackedOffsets
    = Offs32 {-# UNPACK #-} !(VU.Vector Int32)
    | Offs64 {-# UNPACK #-} !(VU.Vector Int)

-- | Offset at index @i@, widened to 'Int'.
offAt :: PackedOffsets -> Int -> Int
offAt (Offs32 v) i = fromIntegral (VU.unsafeIndex v i)
offAt (Offs64 v) i = VU.unsafeIndex v i
{-# INLINE offAt #-}

-- | Number of offset entries (row count + 1).
offCount :: PackedOffsets -> Int
offCount (Offs32 v) = VU.length v
offCount (Offs64 v) = VU.length v
{-# INLINE offCount #-}

{- | A selection layer mapping logical rows to base rows; @-1@ marks an
invalid/null row. 'Int32' when the base row count fits.
-}
data PackedSel
    = Sel32 {-# UNPACK #-} !(VU.Vector Int32)
    | Sel64 {-# UNPACK #-} !(VU.Vector Int)

-- | Base row for logical row @i@ (may be @-1@).
selAt :: PackedSel -> Int -> Int
selAt (Sel32 v) i = fromIntegral (VU.unsafeIndex v i)
selAt (Sel64 v) i = VU.unsafeIndex v i
{-# INLINE selAt #-}

selLength :: PackedSel -> Int
selLength (Sel32 v) = VU.length v
selLength (Sel64 v) = VU.length v
{-# INLINE selLength #-}

{- | A shared UTF-8 byte buffer plus @n+1@ row offsets (base row @r@ spans bytes
@[offsets!r, offsets!(r+1))@); validity lives in the column's bitmap. @ptSel@ is
an optional selection layer letting a gather/join/sort result share the buffer.

@ptCanonicalSel@ marks a selection that is a canonical dictionary encoding:
equal byte slices always map to the same base row (codes). Set by dictionary
compaction; preserved by gather/take over an already-canonical selection (a
row keeps its code); 'False' for a gather over an unselected base, where two
logical rows can select different but equal-byted base rows. Grouping keys on
codes directly when it holds.
-}
data PackedTextData = PackedTextData
    { ptBytes :: {-# UNPACK #-} !A.Array
    , ptOffsets :: !PackedOffsets
    , ptSel :: !(Maybe PackedSel)
    , ptCanonicalSel :: !Bool
    }

int32Max :: Int
int32Max = fromIntegral (maxBound :: Int32)

-- | Narrow an 'Int' offset vector when the final offset (total bytes) fits.
mkOffsets :: VU.Vector Int -> PackedOffsets
mkOffsets offs
    | not (VU.null offs) && VU.last offs <= int32Max =
        Offs32 (VU.map fromIntegral offs)
    | otherwise = Offs64 offs
{-# INLINE mkOffsets #-}

{- | Narrow an 'Int' base-row vector (@-1@ sentinels allowed) when the base
row count fits in 'Int32'.
-}
mkSel :: Int -> VU.Vector Int -> PackedSel
mkSel base rows
    | base <= int32Max = Sel32 (VU.map fromIntegral rows)
    | otherwise = Sel64 rows
{-# INLINE mkSel #-}

-- | Build a contiguous packed payload (no selection): the freeze-path shape.
mkPackedContiguous :: A.Array -> VU.Vector Int -> PackedTextData
mkPackedContiguous arr offs = PackedTextData arr (mkOffsets offs) Nothing False
{-# INLINE mkPackedContiguous #-}

-- | 'mkPackedContiguous' from offsets already produced at 'Int32' width.
mkPackedContiguous32 :: A.Array -> VU.Vector Int32 -> PackedTextData
mkPackedContiguous32 arr offs = PackedTextData arr (Offs32 offs) Nothing False
{-# INLINE mkPackedContiguous32 #-}

{- | Reindex a packed payload by a selection vector, sharing the byte buffer;
logical row @i@ becomes base row @indices!i@. A negative or out-of-range index
decodes to the empty slice. Composes with an existing selection; canonicality
survives composition (a kept row keeps its code) but not a first selection
over the unselected base.
-}
packedGather :: VU.Vector Int -> PackedTextData -> PackedTextData
packedGather indices (PackedTextData arr offs msel canon) =
    let !base = offCount offs - 1
        clamp r = if r >= 0 && r < base then r else -1
        (sel', canon') = case msel of
            Nothing -> (VU.map clamp indices, False)
            Just s ->
                let !sn = selLength s
                 in ( VU.map
                        (\i -> if i >= 0 && i < sn then clamp (selAt s i) else -1)
                        indices
                    , canon
                    )
     in PackedTextData arr offs (Just (mkSel base sel')) canon'
{-# INLINE packedGather #-}

{- | Take the first @k@ logical rows, sharing the byte buffer via a capped
selection layer. O(k), no byte copy or decode — cheap @take@/display on a
large packed column.
-}
packedTake :: Int -> PackedTextData -> PackedTextData
packedTake k (PackedTextData arr offs msel canon) =
    let !base = offCount offs - 1
        !k' = max 0 k
        (sel', canon') = case msel of
            Just (Sel32 s) -> (Sel32 (VU.take k' s), canon)
            Just (Sel64 s) -> (Sel64 (VU.take k' s), canon)
            Nothing -> (mkSel base (VU.enumFromN 0 (min k' base)), False)
     in PackedTextData arr offs (Just sel') canon'
{-# INLINE packedTake #-}

-- | Map a logical row index to its base row, honoring any selection layer.
baseRow :: PackedTextData -> Int -> Int
baseRow (PackedTextData _ _ Nothing _) i = i
baseRow (PackedTextData _ _ (Just sel) _) i = selAt sel i
{-# INLINE baseRow #-}

-- | Row count: @length sel@ when selected, else @length offsets - 1@.
packedLength :: PackedTextData -> Int
packedLength (PackedTextData _ offs Nothing _) = offCount offs - 1
packedLength (PackedTextData _ _ (Just sel) _) = selLength sel
{-# INLINE packedLength #-}

-- | Raw byte slice for logical row @i@: @(buffer, offset, length)@. The hot accessor.
packedSlice :: PackedTextData -> Int -> (A.Array, Int, Int)
packedSlice p@(PackedTextData arr offs _ _) i =
    let !r = baseRow p i
     in if r < 0
            then (arr, 0, 0)
            else
                let o = offAt offs r in (arr, o, offAt offs (r + 1) - o)
{-# INLINE packedSlice #-}

{- | The shared buffer + contiguous @n+1@ offsets when the payload is the
unselected base; a selected (gathered) payload returns 'Nothing' (its rows are
non-contiguous). Lets contiguous consumers skip the selection indirection.
-}
packedRowOffsets :: PackedTextData -> Maybe (A.Array, PackedOffsets)
packedRowOffsets (PackedTextData arr offs Nothing _) = Just (arr, offs)
packedRowOffsets _ = Nothing
{-# INLINE packedRowOffsets #-}

{- | On-demand single 'Data.Text.Text' for row @i@, using the same
validate-or-lenient decode as the freeze path so output is bit-identical.
-}
packedIndexText :: PackedTextData -> Int -> T.Text
packedIndexText p i =
    let (arr, o, l) = packedSlice p i
     in decodeField arr o l
{-# INLINE packedIndexText #-}

-- Decode one field exactly as the boxed freeze path does per row.
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
