{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Concatenation of per-chunk 'Column's (e.g. from parallel CSV chunks). Text
columns merge at the byte level via 'TextChunk' \/ 'mergeTextChunks', so no
per-chunk 'Data.Text.Text' values are ever materialized.
-}
module DataFrame.Internal.ColumnMerge (
    TextChunk (..),
    mergeColumns,
    mergeTextChunks,
    packedFromTextChunk,
    packValidity,
    spliceBitmaps,
    tcRows,
) where

import qualified Data.Text.Array as A
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (foldM_, forM_, when)
import Control.Monad.ST (ST, runST)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.Maybe (fromMaybe, isNothing)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Data.Word (Word8)
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    Columnable,
    allValidBitmap,
    isMergedColumn,
    isPackedText,
    materializeMerged,
    materializePacked,
 )
import DataFrame.Internal.PackedText (mkPackedContiguous)
import Type.Reflection (typeRep)

{- | A frozen text-builder chunk: raw UTF-8 bytes plus row offsets (row @i@
spans bytes @[offsets!i, offsets!(i+1))@) and an optional validity bitmap.
'Data.Text.Text' values are only created when chunks merge into a 'Column'.
-}
data TextChunk = TextChunk
    { tcBytes :: !A.Array
    , tcUsed :: !Int
    , tcOffsets :: !(VU.Vector Int)
    , tcBitmap :: !(Maybe Bitmap)
    }

tcRows :: TextChunk -> Int
tcRows c = VU.length (tcOffsets c) - 1

{- | Freeze a builder chunk directly into a packed-text column: no
'Data.Text.Text' materialization, no UTF-8 validation pass (deferred to decode).
Not yet called by any reader.
-}
packedFromTextChunk :: TextChunk -> Column
packedFromTextChunk (TextChunk arr _used offs bm) =
    PackedText bm (mkPackedContiguous arr offs)

{- | Merge text chunks into one packed-text 'Column': one byte-array copy per
chunk, one offset rebase, then wrap the shared buffer + offsets as 'PackedText'
(no per-row header, decode deferred).
-}
mergeTextChunks :: [TextChunk] -> Column
mergeTextChunks [] = error "DataFrame.Internal.ColumnMerge.mergeTextChunks: empty list"
mergeTextChunks [c] = packedFromTextChunk c
mergeTextChunks cs = runST $ do
    let totalBytes = sum (map tcUsed cs)
        totalRows = sum (map tcRows cs)
    arr <- A.new (max 1 totalBytes)
    offs <- VUM.unsafeNew (totalRows + 1)
    VUM.unsafeWrite offs 0 0
    let splice !byteBase !rowBase c = do
            let n = tcRows c
                co = tcOffsets c
            A.copyI (tcUsed c) arr byteBase (tcBytes c) 0
            forM_ [1 .. n] $ \i ->
                VUM.unsafeWrite offs (rowBase + i) (byteBase + VU.unsafeIndex co i)
            pure (byteBase + tcUsed c, rowBase + n)
    foldM_ (\(b, r) c -> splice b r c) (0, 0) cs
    farr <- A.unsafeFreeze arr
    foffs <- VU.unsafeFreeze offs
    let !bm = spliceBitmaps [(tcBitmap c, tcRows c) | c <- cs]
    pure (PackedText bm (mkPackedContiguous farr foffs))

{- | Merge per-chunk columns into one column: one allocation + memcpy per
payload, with bitmaps spliced across non-byte-aligned chunk boundaries.
All chunks must have the same element type.
-}
mergeColumns :: [Column] -> Column
mergeColumns [] = error "DataFrame.Internal.ColumnBuilder.mergeColumns: empty list"
mergeColumns [c] = c
-- Normalize on the whole list, not the head: a packed or merged chunk in any
-- position must demote every chunk to the common boxed form.
mergeColumns cols@(c0 : _)
    | any isMergedColumn cols = mergeColumns (map materializeMerged cols)
    | any isPackedText cols = mergeColumns (map materializePacked cols)
mergeColumns cols@(c0 : _) = case c0 of
    PackedText _ _ -> mergeColumns (map materializePacked cols)
    MergedColumn _ _ -> mergeColumns (map materializeMerged cols)
    UnboxedColumn _ (_ :: VU.Vector a) ->
        let parts = map (unboxedPart @a) cols
            !merged = VU.concat (map snd parts)
            !bm = spliceBitmaps [(mb, VU.length v) | (mb, v) <- parts]
         in UnboxedColumn bm merged
    BoxedColumn _ (_ :: VB.Vector a) ->
        let parts = map (boxedPart @a) cols
            !merged = VB.concat (map snd parts)
            !bm = spliceBitmaps [(mb, VB.length v) | (mb, v) <- parts]
         in BoxedColumn bm merged

unboxedPart ::
    forall a. (Columnable a, VU.Unbox a) => Column -> (Maybe Bitmap, VU.Vector a)
unboxedPart (UnboxedColumn mb (v :: VU.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> (mb, v)
        Nothing -> mergeMismatch
unboxedPart _ = mergeMismatch

boxedPart ::
    forall a. (Columnable a) => Column -> (Maybe Bitmap, VB.Vector a)
boxedPart (BoxedColumn mb (v :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> (mb, v)
        Nothing -> mergeMismatch
boxedPart _ = mergeMismatch

mergeMismatch :: a
mergeMismatch =
    error "DataFrame.Internal.ColumnBuilder.mergeColumns: chunk column types differ"

{- | Splice chunk bitmaps end to end at the bit level. 'Nothing' if no chunk
carries a bitmap; chunks without one count as all-valid otherwise.
-}
spliceBitmaps :: [(Maybe Bitmap, Int)] -> Maybe Bitmap
spliceBitmaps parts
    | all (isNothing . fst) parts = Nothing
    | otherwise = Just $ VU.create $ do
        let total = sum (map snd parts)
            outBytes = (total + 7) `shiftR` 3
        mv <- VUM.replicate outBytes 0
        let orInto i w =
                when (i < outBytes && w /= 0) $ do
                    old <- VUM.unsafeRead mv i
                    VUM.unsafeWrite mv i (old .|. w)
            splice !bitPos (mb, len) = do
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
