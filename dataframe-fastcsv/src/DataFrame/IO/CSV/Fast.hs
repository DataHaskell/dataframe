{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}

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
    getDelimiterIndices,
    CsvParseError (..),
) where

import qualified Data.Vector as Vector
import qualified Data.Vector.Storable as VS
import Data.Vector.Storable.Mutable (
    grow,
    unsafeFromForeignPtr,
 )
import qualified Data.Vector.Storable.Mutable as VSM
import System.IO.MMap (
    Mode (WriteCopy),
    mmapFileForeignPtr,
 )

import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Foreign (
    Ptr,
    castForeignPtr,
    castPtr,
 )
import qualified Foreign
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Storable (peek, poke)

import qualified Data.ByteString as BS
import Data.ByteString.Internal (ByteString (PS))
import qualified Data.ByteString.Internal as BSI
import qualified Data.Map as M
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as TextEncoding
import Data.Word (Word8)

import Control.Parallel.Strategies (parList, rpar, using)
import Data.Array.IArray (array, (!))
import Data.Array.Unboxed (UArray)
import Data.Ix (range)

import DataFrame.IO.CSV (
    HeaderSpec (..),
    RaggedRowPolicy (..),
    ReadOptions (..),
    TypeSpec (..),
    UnclosedQuotePolicy (..),
    defaultReadOptions,
    schemaTypeMap,
    shouldInferFromSample,
    typeInferenceSampleSize,
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Schema (Schema (..))
import DataFrame.Operations.Typing (
    ParseOptions (..),
    effectiveSafeRead,
    parseFromExamples,
    parseWithTypes,
 )

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
supplied 'Schema'.  Columns mentioned in the schema bypass inference;
columns absent from the schema fall back to the default inference
path.  Use when the schema is known (DDL, prior runs) — it's both
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

{- | Read a CSV and return only the named columns, in the order given.
The SIMD scan and delimiter classification still run over the whole
file, but 'extractField' and 'parseFromExamples' are skipped for
unreferenced columns, so the end-to-end cost scales with the size of
the projection rather than the row width.
-}

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
    -- We use write copy mode so that we can append
    -- padding to the end of the memory space
    (bufferPtr, offset, len) <-
        mmapFileForeignPtr
            filePath
            WriteCopy
            Nothing
    let mutableFile = unsafeFromForeignPtr bufferPtr offset len
    readSeparatedFromMVec separator opts mutableFile len

{- | Identical to 'readSeparated' but takes an already-loaded byte buffer
(e.g. a slice of a memory-mapped file).  The bytes are copied once into
a fresh mutable storable vector so the SIMD code can grow it by 64
bytes of trailing padding without disturbing the caller's input.
-}
readSeparatedFromBytes ::
    Word8 ->
    ReadOptions ->
    BS.ByteString ->
    IO DataFrame
readSeparatedFromBytes separator opts bs = do
    let len = BS.length bs
    mvec <- VSM.unsafeNew len
    -- Copy ByteString → mutable vector (one pass, ~1 ns/byte).
    let (fp, off, _) = BSI.toForeignPtr bs
    Foreign.withForeignPtr fp $ \srcPtr ->
        VSM.unsafeWith mvec $ \dstPtr ->
            copyBytes (castPtr dstPtr) (srcPtr `Foreign.plusPtr` off) len
    readSeparatedFromMVec separator opts mvec len

{- | Shared core: takes a mutable storable vector containing exactly @len@
bytes of CSV (no trailing padding required by the caller — it is added
here) and produces a 'DataFrame'.
-}
readSeparatedFromMVec ::
    Word8 ->
    ReadOptions ->
    VSM.IOVector Word8 ->
    Int ->
    IO DataFrame
readSeparatedFromMVec separator opts mutableFile len = do
    paddedMutableFile <- grow mutableFile 64
    paddedCSVFileRaw <- VS.unsafeFreeze paddedMutableFile
    let (paddedCSVFile, bomLen) = stripBom paddedCSVFileRaw
        contentLen = len - bomLen
    indices <-
        getDelimiterIndicesPolicy
            (fastCsvOnUnclosedQuote opts)
            separator
            contentLen
            paddedCSVFile
    let rowEnds = classifyRowEnds paddedCSVFile contentLen indices
        totalRows = VS.length rowEnds
    if totalRows == 0
        then return emptyDataFrame
        else do
            let headerNumCol = fieldsInRow rowEnds 0
                trim = fastCsvTrimUnquoted opts
                extractAt =
                    extractField trim paddedCSVFile indices rowEnds contentLen
                (columnNames, dataStartRow, numCol) = case headerSpec opts of
                    NoHeader ->
                        ( Vector.fromList $
                            map (Text.pack . show) [0 .. headerNumCol - 1]
                        , 0
                        , headerNumCol
                        )
                    UseFirstRow ->
                        ( Vector.fromList $
                            map (extractAt 0) [0 .. headerNumCol - 1]
                        , 1
                        , headerNumCol
                        )
                    ProvideNames ns ->
                        (Vector.fromList ns, 0, length ns)
            if numCol == 0
                then return emptyDataFrame
                else do
                    let dataRows =
                            collectDataRows
                                paddedCSVFile
                                indices
                                rowEnds
                                contentLen
                                dataStartRow
                                totalRows
                        numRow = VS.length dataRows
                    when (fastCsvOnRaggedRow opts == RaiseOnRagged) $
                        checkNoRaggedRows rowEnds dataRows numCol
                    let parseTypes name col =
                            let n =
                                    if shouldInferFromSample (typeSpec opts)
                                        then typeInferenceSampleSize (typeSpec opts)
                                        else 0
                                mode =
                                    effectiveSafeRead
                                        (safeRead opts)
                                        (safeReadOverrides opts)
                                        name
                                parseOpts =
                                    ParseOptions
                                        { missingValues = missingIndicators opts
                                        , sampleSize = n
                                        , parseSafe = mode
                                        , parseSafeOverrides = []
                                        , parseDateFormat = dateFormat opts
                                        }
                             in parseFromExamples parseOpts col
                        generateColumn col =
                            parseTypes (columnNames Vector.! col) $
                                Vector.generate numRow $ \i ->
                                    extractAt (dataRows VS.! i) col
                        columns =
                            Vector.fromListN
                                numCol
                                ( map generateColumn [0 .. numCol - 1]
                                    `using` parList rpar
                                )
                        columnIndices =
                            M.fromList $
                                zip (Vector.toList columnNames) [0 ..]
                        dataframeDimensions = (numRow, numCol)
                    let rawDf =
                            DataFrame
                                columns
                                columnIndices
                                dataframeDimensions
                                M.empty
                        schemaMap = schemaTypeMap (typeSpec opts)
                        resolveMode =
                            effectiveSafeRead
                                (safeRead opts)
                                (safeReadOverrides opts)
                    return $!
                        if M.null schemaMap
                            then rawDf
                            else parseWithTypes resolveMode schemaMap rawDf

{- | An empty 'DataFrame' — returned when the input has no delimiters
(empty file or single line with no separator and no newline). Guards
against a divide-by-zero in the row-stride math when 'numCol == 0'.
-}
emptyDataFrame :: DataFrame
emptyDataFrame = DataFrame Vector.empty M.empty (0, 0) M.empty

{- | Strip a leading UTF-8 BOM (EF BB BF) if present. Returns the trimmed
vector and the number of bytes removed (0 or 3).
-}
{-# INLINE stripBom #-}
stripBom :: VS.Vector Word8 -> (VS.Vector Word8, Int)
stripBom v
    | VS.length v >= 3
    , VS.unsafeIndex v 0 == 0xEF
    , VS.unsafeIndex v 1 == 0xBB
    , VS.unsafeIndex v 2 == 0xBF =
        (VS.drop 3 v, 3)
    | otherwise = (v, 0)

{- | Classify each entry in the flat delimiter vector as either a row
terminator or a field terminator, and return the indices-into-the-vector
of every row terminator.  A position counts as a row break if either

  * the byte at that position is @\\n@, or
  * the position is beyond the original content length, which only
    happens for the synthetic end-of-file delimiter written when a file
    does not end in a newline.

Field terminators (commas / tabs / the configured separator) are left
implicit: anything between consecutive row terminators is a field.
-}
{-# INLINE classifyRowEnds #-}
classifyRowEnds :: VS.Vector Word8 -> Int -> VS.Vector CSize -> VS.Vector Int
classifyRowEnds file contentLen =
    VS.findIndices isRowBreak
  where
    isRowBreak pos =
        let p = fromIntegral pos :: Int
         in p >= contentLen || VS.unsafeIndex file p == lf

{- | Number of fields that row @r@ contains, derived directly from the
gap between consecutive entries in @rowEnds@.
-}
{-# INLINE fieldsInRow #-}
fieldsInRow :: VS.Vector Int -> Int -> Int
fieldsInRow rowEnds r =
    let endIdx = VS.unsafeIndex rowEnds r
        startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
     in endIdx - startIdx + 1

{- | Does row @r@ contain exactly one, empty field?  That is the case for
a bare @\\n@ or a @\\r\\n@ \u2014 blank lines, which we skip by default to
match pandas / polars @skip_blank_lines@ semantics.  We also treat a
row whose only byte is @\\r@ as blank, so CRLF-terminated blank lines
don't leak through when only the @\\n@ counts as a row break.
-}
{-# INLINE isBlankRow #-}
isBlankRow ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    VS.Vector Int ->
    Int ->
    Int ->
    Bool
isBlankRow file delimiters rowEnds contentLen r =
    let endIdx = VS.unsafeIndex rowEnds r
        startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
        numFields = endIdx - startIdx + 1
     in numFields == 1
            && let fieldEndRaw =
                    fromIntegral (VS.unsafeIndex delimiters startIdx) :: Int
                   fieldEnd = min fieldEndRaw contentLen
                   fieldStart =
                    if startIdx == 0
                        then 0
                        else
                            fromIntegral
                                (VS.unsafeIndex delimiters (startIdx - 1))
                                + 1
                   fieldLen = fieldEnd - fieldStart
                in fieldLen == 0
                    || ( fieldLen == 1
                            && VS.unsafeIndex file fieldStart == cr
                       )

{- | Select the row indices that contain actual data: skip @[0 .. skip - 1]@
and drop any blank rows from the remainder.
-}
{-# INLINE collectDataRows #-}
collectDataRows ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    VS.Vector Int ->
    Int ->
    Int ->
    Int ->
    VS.Vector Int
collectDataRows file delimiters rowEnds contentLen skip total =
    VS.filter
        (not . isBlankRow file delimiters rowEnds contentLen)
        (VS.generate (total - skip) (+ skip))

{- | Extract field @col@ of row @r@ as a 'Text'.

Semantics follow RFC 4180:

  * If the raw field is wrapped in @\"..\"@, the outer quotes are
    stripped and any embedded @\"\"@ is unescaped to a single @\"@.
    Whitespace inside the quotes is preserved verbatim.

  * If the raw field is unquoted, it is returned as-is; whitespace
    is preserved by default (matching pandas / polars).  Callers that
    want legacy trim-everything behaviour pass @trimUnquoted = True@.

  * A trailing @\\r@ is dropped before decoding so CRLF files produce
    clean Text on every column, not only when @Text.strip@ happens to
    sweep it up.

If @col@ is out of range (a ragged short row), returns the empty text;
callers convert that into a null via the missing-indicator list that
'parseFromExamples' honours.
-}
{-# INLINE extractField #-}
extractField ::
    Bool ->
    VS.Vector Word8 ->
    VS.Vector CSize ->
    VS.Vector Int ->
    Int ->
    Int ->
    Int ->
    Text
extractField trimUnquoted file delimiters rowEnds contentLen r col
    | col >= numFields = Text.empty
    | fieldEnd - fieldStart >= 2
    , VS.unsafeIndex file fieldStart == quote
    , VS.unsafeIndex file (fieldEnd - 1) == quote =
        unescapeDoubledQuotes
            . TextEncoding.decodeUtf8Lenient
            . unsafeToByteString
            $ VS.slice (fieldStart + 1) (fieldEnd - fieldStart - 2) file
    | otherwise =
        (if trimUnquoted then Text.strip else id)
            . TextEncoding.decodeUtf8Lenient
            . unsafeToByteString
            $ VS.slice fieldStart (fieldEnd - fieldStart) file
  where
    endIdx = VS.unsafeIndex rowEnds r
    startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
    numFields = endIdx - startIdx + 1
    boundaryIdx = startIdx + col
    fieldEndRaw =
        fromIntegral (VS.unsafeIndex delimiters boundaryIdx) :: Int
    fieldEndClamped = min fieldEndRaw contentLen
    fieldStart =
        if boundaryIdx == 0
            then 0
            else
                fromIntegral
                    (VS.unsafeIndex delimiters (boundaryIdx - 1))
                    + 1
    -- Strip a trailing \r (CRLF line endings) from the last field of a row
    -- when the SIMD scanner, which only tracks \n, leaves it behind.
    fieldEnd =
        if fieldEndClamped > fieldStart
            && VS.unsafeIndex file (fieldEndClamped - 1) == cr
            then fieldEndClamped - 1
            else fieldEndClamped
    unsafeToByteString :: VS.Vector Word8 -> BS.ByteString
    unsafeToByteString v = PS (castForeignPtr ptr) 0 n
      where
        (ptr, n) = VS.unsafeToForeignPtr0 v

{- | Walk every data row and throw 'CsvRaggedRow' on the first one whose
field count differs from the header.  Only called when the user opts
into 'RaiseOnRagged' via 'fastCsvOnRaggedRow'.
-}
{-# INLINE checkNoRaggedRows #-}
checkNoRaggedRows ::
    VS.Vector Int ->
    VS.Vector Int ->
    Int ->
    IO ()
checkNoRaggedRows rowEnds dataRows numCol =
    VS.mapM_
        ( \r ->
            let actual = fieldsInRow rowEnds r
             in when (actual /= numCol) $
                    throwIO (CsvRaggedRow r numCol actual)
        )
        dataRows

{- | RFC 4180 inner-quote unescape: @\"\"@ → @\"@.  'Text.replace' on a
two-char needle does a single linear pass and is allocation-free when
no doubled quote is present (short-circuits at the first miss).
-}
{-# INLINE unescapeDoubledQuotes #-}
unescapeDoubledQuotes :: Text -> Text
unescapeDoubledQuotes t
    | Text.isInfixOf doubledQuote t = Text.replace doubledQuote singleQuote t
    | otherwise = t
  where
    doubledQuote = Text.pack "\"\""
    singleQuote = Text.singleton '"'

{- | Exceptions raised by the fast CSV parser.  Catchable with
'Control.Exception.catch' or 'Control.Exception.try'.
-}
data CsvParseError
    = {- | The input ends with a quoted field that was never closed.  The
      PCLMUL quote-parity chain in the SIMD scanner treats an unmatched
      @\"@ as if the rest of the file were inside quotes, so we refuse
      to return a silently corrupted 'DataFrame' and raise instead.
      -}
      CsvUnclosedQuote
    | {- | A row has a different number of fields from the header.  Only
      raised when 'fastCsvOnRaggedRow' is set to 'RaiseOnRagged'.
      Carries the 0-based row index, the expected field count, and the
      actual field count.
      -}
      CsvRaggedRow !Int !Int !Int
    deriving (Eq, Show)

instance Exception CsvParseError

-- Status codes reported via the out-parameter of 'get_delimiter_indices'.
-- Must stay in sync with @GDI_*@ in @cbits/process_csv.h@.
gdiOk, gdiUnclosedQuote :: CInt
gdiOk = 0
gdiUnclosedQuote = 1

{- | Return value that the C helper uses to tell Haskell \"SIMD isn't
available on this build / CPU, run the pure-Haskell state machine.\"
Mirrors @GDI_SIMD_UNAVAILABLE@ in @cbits/process_csv.h@.  'CSize' is
unsigned so this has the same bit pattern as 'maxBound'.
-}
simdUnavailable :: CSize
simdUnavailable = maxBound

foreign import capi "process_csv.h get_delimiter_indices"
    get_delimiter_indices ::
        Ptr CUChar -> -- input
        CSize -> -- input size
        CUChar -> -- separator character
        Ptr CSize -> -- result array
        Ptr CInt -> -- status out-parameter
        IO CSize -- occupancy of result array

{- | Locate delimiter byte positions in @csvFile@.  Treats an unclosed
quoted field at EOF as a hard error; callers that want to suppress the
exception can use 'getDelimiterIndicesPolicy' with 'BestEffort'.
-}
{-# INLINE getDelimiterIndices #-}
getDelimiterIndices ::
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndices = getDelimiterIndicesPolicy RaiseOnUnclosedQuote

{-# INLINE getDelimiterIndicesPolicy #-}
getDelimiterIndicesPolicy ::
    UnclosedQuotePolicy ->
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndicesPolicy policy separator originalLen csvFile =
    VS.unsafeWith csvFile $ \buffer -> do
        let paddedLen = VS.length csvFile
        -- GC-managed pinned memory: freed automatically, no leak in streaming use.
        resultMV <- VSM.unsafeNew paddedLen
        (num_fields, status) <- alloca $ \statusPtr -> do
            poke statusPtr gdiOk
            n <-
                VSM.unsafeWith resultMV $ \indicesPtr ->
                    get_delimiter_indices
                        (castPtr buffer)
                        (fromIntegral paddedLen)
                        (fromIntegral separator)
                        (castPtr indicesPtr)
                        statusPtr
            s <- peek statusPtr
            return (n, s)
        if num_fields == simdUnavailable
            then do
                -- Haskell state-machine fallback, writing directly into resultMV.
                let trans = stateTransitionTable separator
                    processChar (!state, !idx) i byte =
                        case state of
                            UnEscaped ->
                                if byte == lf || byte == separator
                                    then do
                                        VSM.unsafeWrite resultMV idx (fromIntegral i)
                                        return (toEnum (trans ! (fromEnum state, byte)), idx + 1)
                                    else return (toEnum (trans ! (fromEnum state, byte)), idx)
                            Escaped ->
                                return (toEnum (trans ! (fromEnum state, byte)), idx)
                (finalState, finalIdx) <-
                    VS.ifoldM' processChar (UnEscaped, 0 :: Int) csvFile
                case finalState of
                    Escaped
                        | policy == RaiseOnUnclosedQuote ->
                            throwIO CsvUnclosedQuote
                    _ -> return ()
                finalLen <-
                    if originalLen > 0 && csvFile VS.! (originalLen - 1) /= lf
                        then do
                            VSM.unsafeWrite resultMV finalIdx (fromIntegral originalLen)
                            return (finalIdx + 1)
                        else return finalIdx
                VS.unsafeFreeze (VSM.slice 0 finalLen resultMV)
            else do
                when
                    ( status == gdiUnclosedQuote
                        && policy == RaiseOnUnclosedQuote
                    )
                    (throwIO CsvUnclosedQuote)
                let n = fromIntegral num_fields
                finalLen <-
                    if originalLen > 0 && csvFile VS.! (originalLen - 1) /= lf
                        then do
                            VSM.write resultMV n (fromIntegral originalLen)
                            return (n + 1)
                        else return n
                VS.unsafeFreeze (VSM.slice 0 finalLen resultMV)

-- We have a Native version in case the C version
-- cannot be used. For example if neither ARM_NEON
-- nor AVX2 are available

lf, cr, comma, tab, quote :: Word8
lf = 0x0A
cr = 0x0D
comma = 0x2C
tab = 0x09
quote = 0x22

-- We parse using a state machine
data State
    = UnEscaped -- non quoted
    | Escaped -- quoted
    deriving (Enum)

{-# INLINE stateTransitionTable #-}
stateTransitionTable :: Word8 -> UArray (Int, Word8) Int
stateTransitionTable separator = array ((0, 0), (1, 255)) [(i, f i) | i <- range ((0, 0), (1, 255))]
  where
    f (0, character)
        -- Unescaped newline
        | character == 0x0A = fromEnum UnEscaped
        -- Unescaped separator
        | character == separator = fromEnum UnEscaped
        -- Unescaped quote
        | character == 0x22 = fromEnum Escaped
        | otherwise = fromEnum UnEscaped
    -- Escaped quote
    -- escaped quote in fields are dealt as
    -- consecutive quoted sections of a field
    -- example: If we have
    -- field1, "abc""def""ghi, field3
    -- we end up processing abc, def, and ghi
    -- as consecutive quoted strings.
    f (1, 0x22) = fromEnum UnEscaped
    -- Everything else
    f (state, _) = state
