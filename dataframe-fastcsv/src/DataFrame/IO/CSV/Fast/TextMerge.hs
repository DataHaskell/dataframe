{-# LANGUAGE BangPatterns #-}

{- | Parallel byte-level merge of per-chunk text columns: the byte copies
and offset rebase fan out over the chunk list, so a large text column does
not serialize the merge phase. Wraps the merged shared buffer + offsets as
'PackedText' (no 'Data.Text.Text' spine, no eager UTF-8 validation), exactly
as the pure 'mergeTextChunks' now produces.
-}
module DataFrame.IO.CSV.Fast.TextMerge (
    mergeTextChunksPar,
) where

import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (void)
import Control.Monad.ST (stToIO)

import Data.Int (Int32)
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.Column.Bitmap (Validity (Validity))
import DataFrame.Internal.Column.Merge (
    TextChunk (..),
    concatValidity,
    mergeTextChunks,
    tcRows,
 )
import DataFrame.Internal.Control.Concurrent (pooledRun)
import DataFrame.Internal.Data.PackedText (
    mkPackedContiguous,
    mkPackedContiguous32,
 )

{- | Merge text chunks with @width@-way parallel byte copies + offset
rebase, then wrap the shared buffer as 'PackedText'. Single chunks take
the pure (zero-copy) path.
-}
mergeTextChunksPar :: Int -> [TextChunk] -> IO Column
mergeTextChunksPar _ [c] = pure $! mergeTextChunks [c]
mergeTextChunksPar width cs = do
    let byteOffs = scanl (+) 0 (map tcUsed cs)
        rowOffs = scanl (+) 0 (map tcRows cs)
        totalBytes = last byteOffs
        totalRows = last rowOffs
    marr <- stToIO (A.new (max 1 totalBytes))
    -- Byte copy + offset rebase, parallel over chunks (disjoint ranges).
    let splice writeOff =
            void . pooledRun width $
                [ do
                    stToIO (A.copyI (tcUsed c) marr bOff (tcBytes c) 0)
                    let co = tcOffsets c
                        n = tcRows c
                        fill !i
                            | i > n = pure ()
                            | otherwise = do
                                writeOff (rOff + i) (bOff + VU.unsafeIndex co i)
                                fill (i + 1)
                    fill 1
                | (c, bOff, rOff) <- zip3 cs byteOffs rowOffs
                ]
    -- The final width is known before allocation: Int32 offsets whenever
    -- the merged buffer stays under 2^31 bytes (the common case).
    packed <-
        if totalBytes <= fromIntegral (maxBound :: Int32)
            then do
                offsMV <- VUM.unsafeNew (totalRows + 1) :: IO (VUM.IOVector Int32)
                VUM.unsafeWrite offsMV 0 0
                splice (\i v -> VUM.unsafeWrite offsMV i (fromIntegral v))
                mkPackedContiguous32
                    <$> stToIO (A.unsafeFreeze marr)
                    <*> VU.unsafeFreeze offsMV
            else do
                offsMV <- VUM.unsafeNew (totalRows + 1) :: IO (VUM.IOVector Int)
                VUM.unsafeWrite offsMV 0 0
                splice (VUM.unsafeWrite offsMV)
                mkPackedContiguous
                    <$> stToIO (A.unsafeFreeze marr)
                    <*> VU.unsafeFreeze offsMV
    let !bm = concatValidity [Validity (tcBitmap c) (tcRows c) | c <- cs]
    pure (PackedText bm packed)
