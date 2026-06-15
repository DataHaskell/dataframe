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

import DataFrame.IO.CSV.Fast.Workers (pooledRun)
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.ColumnMerge (
    TextChunk (..),
    mergeTextChunks,
    spliceBitmaps,
    tcRows,
 )
import DataFrame.Internal.PackedText (mkPackedContiguous)

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
    offsMV <- VUM.unsafeNew (totalRows + 1)
    VUM.unsafeWrite offsMV 0 0
    -- Byte copy + offset rebase, parallel over chunks (disjoint ranges).
    void . pooledRun width $
        [ do
            stToIO (A.copyI (tcUsed c) marr bOff (tcBytes c) 0)
            let co = tcOffsets c
                n = tcRows c
                fill !i
                    | i > n = pure ()
                    | otherwise = do
                        VUM.unsafeWrite
                            offsMV
                            (rOff + i)
                            (bOff + VU.unsafeIndex co i)
                        fill (i + 1)
            fill 1
        | (c, bOff, rOff) <- zip3 cs byteOffs rowOffs
        ]
    arr <- stToIO (A.unsafeFreeze marr)
    offs <- VU.unsafeFreeze offsMV
    let !bm = spliceBitmaps [(tcBitmap c, tcRows c) | c <- cs]
    pure (PackedText bm (mkPackedContiguous arr offs))
