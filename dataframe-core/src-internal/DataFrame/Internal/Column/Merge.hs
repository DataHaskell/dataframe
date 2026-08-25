{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Concatenation of per-chunk 'Column's (e.g. from parallel CSV chunks). Text
columns merge at the byte level via 'TextChunk' \/ 'mergeTextChunks', so no
per-chunk 'Data.Text.Text' values are ever materialized.
-}
module DataFrame.Internal.Column.Merge (
    TextChunk (..),
    concatColumns,
    mergeTextChunks,
    packedFromTextChunk,
    concatValidity,
    tcRows,
) where

import qualified Data.Text.Array as A
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (foldM_, forM_)
import Control.Monad.ST (runST)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    isMergedColumn,
    isPackedText,
    materializeMerged,
    materializePacked,
 )
import DataFrame.Internal.Column.Bitmap (
    Bitmap,
    Validity (Validity),
    concatValidity,
 )
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)
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
    let !bm = concatValidity [Validity (tcBitmap c) (tcRows c) | c <- cs]
    pure (PackedText bm (mkPackedContiguous farr foffs))

{- | Merge per-chunk columns into one column.

TODO: mchavinda - this is very similar to mappendColumns can could possibly
be defined in terms of it but I'll have to ivnestigate further.
-}
concatColumns :: [Column] -> Column
concatColumns [] = error "DataFrame.Internal.Column.Builder.concatColumns: empty list"
concatColumns [c] = c
-- Normalize on the whole list, not the head: a packed or merged chunk in any
-- position must demote every chunk to the common boxed form.
concatColumns cols@(c0 : _)
    | any isMergedColumn cols = concatColumns (map materializeMerged cols)
    | any isPackedText cols = concatColumns (map materializePacked cols)
concatColumns cols@(c0 : _) = case c0 of
    PackedText _ _ -> concatColumns (map materializePacked cols)
    MergedColumn _ _ -> concatColumns (map materializeMerged cols)
    UnboxedColumn _ (_ :: VU.Vector a) ->
        let parts = map (unboxedPart @a) cols
            !merged = VU.concat (map snd parts)
            !bm = concatValidity [Validity mb (VU.length v) | (mb, v) <- parts]
         in UnboxedColumn bm merged
    BoxedColumn _ (_ :: VB.Vector a) ->
        let parts = map (boxedPart @a) cols
            !merged = VB.concat (map snd parts)
            !bm = concatValidity [Validity mb (VB.length v) | (mb, v) <- parts]
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
    error
        "DataFrame.Internal.Column.Builder.concatColumns: chunk column types differ"
