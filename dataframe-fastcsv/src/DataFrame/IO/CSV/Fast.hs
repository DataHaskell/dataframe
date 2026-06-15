{- | SIMD-accelerated CSV reader: a C scanner finds delimiter positions,
then each column is parsed directly from the mmap'd byte slices into
unboxed column builders (Round-2 typed extraction). Reads are strict: the
returned 'DataFrame' is fully built, with no deferred parse work.

Extraction is chunk-parallel: inputs of 4 MB and up are split into one
row chunk per capability and parsed by 'Control.Concurrent.forkIO'
workers. The library never chooses RTS settings — to actually get
parallel reads, link the executable with @-threaded@ and run with
@+RTS -N@ (otherwise the reader falls back to the sequential path).
-}
module DataFrame.IO.CSV.Fast (
    fastReadCsv,
    readCsvFast,
    fastReadTsv,
    readTsvFast,
    fastReadCsvWithOpts,
    fastReadTsvWithOpts,
    fastReadCsvWithSchema,
    fastReadCsvProj,
    fastReadCsvFromBytes,
    fastReadCsvFromBytesWithSchema,
    readSeparated,
    readSeparatedFromBytes,
    readSeparatedFromBytesChunks,
    getDelimiterIndices,
    CsvParseError (..),
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Map as M
import qualified Data.Vector as Vector
import qualified Data.Vector.Storable as VS

import Data.Text (Text)
import Data.Word (Word8)
import System.IO.MMap (Mode (WriteCopy), mmapFileForeignPtr)

import DataFrame.IO.CSV (
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
 )
import DataFrame.IO.CSV.Fast.Core (readSeparatedCore)
import DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    comma,
    getDelimiterIndices,
    tab,
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Schema (Schema (..))

readSeparatedDefault :: Word8 -> FilePath -> IO DataFrame
readSeparatedDefault separator =
    readSeparated separator defaultReadOptions

fastReadCsv :: FilePath -> IO DataFrame
fastReadCsv = readSeparatedDefault comma

readCsvFast :: FilePath -> IO DataFrame
readCsvFast = fastReadCsv

fastReadTsv :: FilePath -> IO DataFrame
fastReadTsv = readSeparatedDefault tab

readTsvFast :: FilePath -> IO DataFrame
readTsvFast = fastReadTsv

{- | Like 'fastReadCsv' but takes a 'ReadOptions' record.  Use this when
you need to tune ragged-row handling, unclosed-quote handling, or the
whitespace-trimming knob; otherwise stick with 'fastReadCsv'.
-}
fastReadCsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
fastReadCsvWithOpts = readSeparated comma

-- | TSV counterpart to 'fastReadCsvWithOpts'.
fastReadTsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
fastReadTsvWithOpts = readSeparated tab

{- | Read a CSV and coerce each column to the type declared in the
supplied 'Schema'.  Schema columns bypass inference entirely: they are
parsed straight from the file bytes as the declared type, which is both
faster than inference and guards against the row-1 \"looks like Int\"
misclassification trap.
-}
fastReadCsvWithSchema :: Schema -> FilePath -> IO DataFrame
fastReadCsvWithSchema schema =
    readSeparated
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

-- | In-memory version of 'fastReadCsv'.
fastReadCsvFromBytes :: BS.ByteString -> IO DataFrame
fastReadCsvFromBytes = readSeparatedFromBytes comma defaultReadOptions

fastReadCsvFromBytesWithSchema :: Schema -> BS.ByteString -> IO DataFrame
fastReadCsvFromBytesWithSchema schema =
    readSeparatedFromBytes
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

{- | Read a CSV and return only the named columns, in the order given.
The SIMD scan and delimiter classification still run over the whole
file, but unreferenced columns are dropped from the result.
-}
fastReadCsvProj :: [Text] -> FilePath -> IO DataFrame
fastReadCsvProj projection path = do
    df <- fastReadCsv path
    pure (projectColumns projection df)

{- | Filter a DataFrame's columns down to (and in the order of) the
supplied names.  Missing names are silently dropped rather than
raised; callers that want strict semantics can check the resulting
'columnIndices' map.
-}
projectColumns :: [Text] -> DataFrame -> DataFrame
projectColumns names df =
    let idxs = [(n, i) | n <- names, Just i <- [M.lookup n (columnIndices df)]]
        newCols = Vector.fromListN (length idxs) [columns df Vector.! i | (_, i) <- idxs]
        newIndices = M.fromList (zip (map fst idxs) [0 ..])
        (rows, _) = dataframeDimensions df
     in DataFrame newCols newIndices (rows, length idxs) M.empty

readSeparated ::
    Word8 ->
    ReadOptions ->
    FilePath ->
    IO DataFrame
readSeparated separator opts filePath = do
    -- WriteCopy keeps the mapping private; the scanner never needs to
    -- grow or pad the buffer, so the file is no longer copied (audit F6).
    (bufferPtr, offset, len) <-
        mmapFileForeignPtr
            filePath
            WriteCopy
            Nothing
    readSeparatedCore
        Nothing
        separator
        opts
        (VS.unsafeFromForeignPtr bufferPtr offset len)

{- | Identical to 'readSeparated' but reads from an already-loaded byte
buffer (e.g. a slice of a memory-mapped file). Zero-copy: the bytes are
only read, never modified or grown.
-}
readSeparatedFromBytes ::
    Word8 ->
    ReadOptions ->
    BS.ByteString ->
    IO DataFrame
readSeparatedFromBytes separator opts bs = do
    let (fp, off, len) = BSI.toForeignPtr bs
    readSeparatedCore Nothing separator opts (VS.unsafeFromForeignPtr fp off len)

{- | 'readSeparatedFromBytes' with a forced chunk count, bypassing the
capability/size gate. Intended for tests and benchmarks that need to pin
the parallel split; @1@ forces the sequential path.
-}
readSeparatedFromBytesChunks ::
    Int ->
    Word8 ->
    ReadOptions ->
    BS.ByteString ->
    IO DataFrame
readSeparatedFromBytesChunks nChunks separator opts bs = do
    let (fp, off, len) = BSI.toForeignPtr bs
    readSeparatedCore
        (Just nChunks)
        separator
        opts
        (VS.unsafeFromForeignPtr fp off len)
