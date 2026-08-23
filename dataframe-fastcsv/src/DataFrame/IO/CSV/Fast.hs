{- | SIMD-accelerated CSV reader: a C scanner finds delimiter positions,
then each column is parsed directly from the mmap'd byte slices into
typed column builders. Chunk-parallel with @-threaded@ + @+RTS -N@.
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
import qualified Data.Vector.Storable as VS

import Data.Char (ord)
import Data.Text (Text)
import Data.Word (Word8)
import System.IO.MMap (Mode (WriteCopy), mmapFileForeignPtr)

import DataFrame.IO.CSV (
    CsvReader,
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
    validateReadOptions,
 )
import DataFrame.IO.CSV.Fast.Core (checkSeparatorAgrees, readSeparatedCore)
import DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    comma,
    getDelimiterIndices,
 )
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Schema (Schema (..))

-- | The separator a 'ReadOptions' asks for, as the scanner's byte.
separatorByte :: ReadOptions -> Word8
separatorByte = fromIntegral . ord . columnSeparator

{- | Read a CSV file with the default options.

==== __Example__
@
ghci> D.fastReadCsv ".\/data\/taxi.csv"

@
-}
fastReadCsv :: FilePath -> IO DataFrame
fastReadCsv = fastReadCsvWithOpts defaultReadOptions

{- | Alias for 'fastReadCsv'.

==== __Example__
@
ghci> D.readCsvFast ".\/data\/taxi.csv"

@
-}
readCsvFast :: FilePath -> IO DataFrame
readCsvFast = fastReadCsv

{- | Read a TSV file with the default options.

==== __Example__
@
ghci> D.fastReadTsv ".\/data\/taxi.tsv"

@
-}
fastReadTsv :: FilePath -> IO DataFrame
fastReadTsv = fastReadTsvWithOpts defaultReadOptions

{- | Alias for 'fastReadTsv'.

==== __Example__
@
ghci> D.readTsvFast ".\/data\/taxi.tsv"

@
-}
readTsvFast :: FilePath -> IO DataFrame
readTsvFast = fastReadTsv

{- | Like 'fastReadCsv' but takes a 'ReadOptions' record.  Use this when
you need to tune ragged-row handling, unclosed-quote handling, the
whitespace-trimming knob, or the column selection; otherwise stick with
'fastReadCsv'.  The separator comes from 'columnSeparator', so this is a
'CsvReader' the lazy scan can drive.

==== __Example__
@
ghci> D.fastReadCsvWithOpts D.defaultReadOptions{D.readColumns = Just ["id", "name"]} ".\/data\/customers.csv"

@
-}
fastReadCsvWithOpts :: CsvReader
fastReadCsvWithOpts opts = readSeparated (separatorByte opts) opts

{- | TSV counterpart to 'fastReadCsvWithOpts'; pins 'columnSeparator' to tab.

==== __Example__
@
ghci> D.fastReadTsvWithOpts D.defaultReadOptions{D.safeRead = D.MaybeRead} ".\/data\/taxi.tsv"

@
-}
fastReadTsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
fastReadTsvWithOpts opts = fastReadCsvWithOpts opts{columnSeparator = '\t'}

{- | Read a CSV and coerce each column to the type declared in the
supplied 'Schema'. Schema columns bypass inference: parsed straight from
file bytes as the declared type, avoiding row-1 misclassification.

==== __Example__
@
ghci> schema = D.makeSchema [("id", D.schemaType \@Int)]
ghci> D.fastReadCsvWithSchema schema ".\/data\/customers.csv"

@
-}
fastReadCsvWithSchema :: Schema -> FilePath -> IO DataFrame
fastReadCsvWithSchema schema =
    readSeparated
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

{- | In-memory version of 'fastReadCsv'.

==== __Example__
@
ghci> D.fastReadCsvFromBytes "id,name\\n1,Ada\\n"

@
-}
fastReadCsvFromBytes :: BS.ByteString -> IO DataFrame
fastReadCsvFromBytes = readSeparatedFromBytes comma defaultReadOptions

{- | In-memory version of 'fastReadCsvWithSchema'.

==== __Example__
@
ghci> schema = D.makeSchema [("id", D.schemaType \@Int)]
ghci> D.fastReadCsvFromBytesWithSchema schema "id,name\\n1,Ada\\n"

@
-}
fastReadCsvFromBytesWithSchema :: Schema -> BS.ByteString -> IO DataFrame
fastReadCsvFromBytesWithSchema schema =
    readSeparatedFromBytes
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

{- | Read a CSV and return only the named columns, in the order given.
Shorthand for 'readColumns'. The SIMD scan and delimiter
classification still run over the whole file, but unnamed columns are
never extracted, parsed or allocated. Naming a column the file does not
have raises a 'ColumnsNotFoundException'.

==== __Example__
@
ghci> D.fastReadCsvProj ["id", "name"] ".\/data\/customers.csv"

@
-}
fastReadCsvProj :: [Text] -> FilePath -> IO DataFrame
fastReadCsvProj projection =
    readSeparated comma defaultReadOptions{readColumns = Just projection}

{- | Reads via a private ('WriteCopy') mmap so the buffer is never copied.
The @separator@ byte and 'columnSeparator' in @opts@ must agree; use
'fastReadCsvWithOpts' unless you need to pass a delimiter byte directly.

==== __Example__
@
ghci> D.readSeparated 44 D.defaultReadOptions ".\/data\/taxi.csv"  -- 44 = ','

@
-}
readSeparated ::
    Word8 ->
    ReadOptions ->
    FilePath ->
    IO DataFrame
readSeparated separator opts filePath = do
    validateReadOptions opts
    checkSeparatorAgrees separator opts
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

==== __Example__
@
ghci> D.readSeparatedFromBytes 44 D.defaultReadOptions "id,name\\n1,Ada\\n"  -- 44 = ','

@
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

==== __Example__
@
ghci> D.readSeparatedFromBytesChunks 1 44 D.defaultReadOptions "id,name\\n1,Ada\\n"  -- 44 = ','

@
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
