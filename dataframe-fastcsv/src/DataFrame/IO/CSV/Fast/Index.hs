{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}

{- | Delimiter-index construction for the fast CSV reader. The index buffer
is sized from an exact byte count (not 8 bytes per input byte), the SIMD
scan runs over the 64-byte-aligned prefix of the unpadded mmap, and a
scalar Haskell tail finishes the final partial chunk — so the input is
never copied or grown.
-}
module DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    getDelimiterIndices,
    getDelimiterIndicesPolicy,
    scalarScan,
    classifyRowEnds,
    fieldsInRow,
    isBlankRow,
    collectDataRows,
    checkNoRaggedRows,
    byteStringView,
    lf,
    cr,
    comma,
    tab,
    quote,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM

import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Data.Word (Word8)
import Foreign (Ptr, castPtr)
import Foreign.C.Types (CInt (..), CSize (..), CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)

import DataFrame.IO.CSV (UnclosedQuotePolicy (..))

{- | Exceptions raised by the fast CSV parser.  Catchable with
'Control.Exception.catch' or 'Control.Exception.try'.
-}
data CsvParseError
    = {- | The input ends with a quoted field that was never closed.  The
      PCLMUL quote-parity chain in the SIMD scanner treats an unmatched
      @\"@ as if the rest of the file were inside quotes, so we refuse
      to return a silently corrupted 'DataFrame.Internal.DataFrame.DataFrame'
      and raise instead.
      -}
      CsvUnclosedQuote
    | {- | A row has a different number of fields from the header.  Only
      raised when 'DataFrame.IO.CSV.fastCsvOnRaggedRow' is set to
      'RaiseOnRagged'.  Carries the 0-based row index, the expected field
      count, and the actual field count.
      -}
      CsvRaggedRow !Int !Int !Int
    deriving (Eq, Show)

instance Exception CsvParseError

lf, cr, comma, tab, quote :: Word8
lf = 0x0A
cr = 0x0D
comma = 0x2C
tab = 0x09
quote = 0x22

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

-- | Zero-copy 'BS.ByteString' view of the first @n@ bytes of a vector.
byteStringView :: Int -> VS.Vector Word8 -> BS.ByteString
byteStringView n v = BSI.fromForeignPtr (fst (VS.unsafeToForeignPtr0 v)) 0 n
{-# INLINE byteStringView #-}

{- | Locate delimiter byte positions in the first @originalLen@ bytes of
@csvFile@.  Treats an unclosed quoted field at EOF as a hard error; callers
that want to suppress the exception can use 'getDelimiterIndicesPolicy'
with 'BestEffort'.
-}
{-# INLINE getDelimiterIndices #-}
getDelimiterIndices ::
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndices = getDelimiterIndicesPolicy RaiseOnUnclosedQuote

getDelimiterIndicesPolicy ::
    UnclosedQuotePolicy ->
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndicesPolicy policy separator contentLen csvFile = do
    -- Exact upper bound on the index count: every output index is an
    -- unquoted separator or newline byte, so two memchr-speed counts
    -- (plus one synthetic EOF slot) right-size the buffer (audit F4).
    let contentBS = byteStringView contentLen csvFile
        capacity = BS.count separator contentBS + BS.count lf contentBS + 1
    resultMV <- VSM.unsafeNew capacity
    -- The C scanner reads whole 64-byte chunks; cap it at the largest
    -- chunk boundary inside the content so no padding is needed and the
    -- input never has to be grown or copied (audit F6). The loop in C
    -- runs while i < len - 64, hence the + 64.
    let simdLen = (contentLen `div` 64) * 64
    (numFound, status) <- VS.unsafeWith csvFile $ \buffer ->
        alloca $ \statusPtr -> do
            poke statusPtr gdiOk
            n <- VSM.unsafeWith resultMV $ \indicesPtr ->
                get_delimiter_indices
                    (castPtr buffer)
                    (fromIntegral (simdLen + 64))
                    (fromIntegral separator)
                    (castPtr indicesPtr)
                    statusPtr
            s <- peek statusPtr
            pure (n, s)
    -- The status reports the quote parity at the end of the SIMD region,
    -- which is exactly the starting state for the scalar tail.
    (found, inQuote) <-
        if numFound == simdUnavailable
            then scalarScan separator csvFile resultMV 0 0 contentLen False
            else
                scalarScan
                    separator
                    csvFile
                    resultMV
                    (fromIntegral numFound)
                    simdLen
                    contentLen
                    (status == gdiUnclosedQuote)
    when (inQuote && policy == RaiseOnUnclosedQuote) (throwIO CsvUnclosedQuote)
    -- Synthetic EOF delimiter when the file does not end in a newline.
    finalLen <-
        if contentLen > 0 && VS.unsafeIndex csvFile (contentLen - 1) /= lf
            then do
                VSM.unsafeWrite resultMV found (fromIntegral contentLen)
                pure (found + 1)
            else pure found
    VS.unsafeFreeze (VSM.slice 0 finalLen resultMV)

{- | RFC 4180 quote-parity state machine over bytes @[from, to)@, appending
delimiter indices at @writeIdx@.  Returns the next write index and whether
the scan ended inside a quoted region. Serves as both the SIMD tail and the
full fallback when SIMD is unavailable.
-}
scalarScan ::
    Word8 ->
    VS.Vector Word8 ->
    VSM.IOVector CSize ->
    Int ->
    Int ->
    Int ->
    Bool ->
    IO (Int, Bool)
scalarScan separator file out writeIdx from to = go writeIdx from
  where
    go !idx !i !inQ
        | i >= to = pure (idx, inQ)
        | otherwise =
            let b = VS.unsafeIndex file i
             in if inQ
                    then go idx (i + 1) (b /= quote)
                    else
                        if b == quote
                            then go idx (i + 1) True
                            else
                                if b == lf || b == separator
                                    then do
                                        VSM.unsafeWrite out idx (fromIntegral i)
                                        go (idx + 1) (i + 1) False
                                    else go idx (i + 1) False

{- | Walk every data row and throw 'CsvRaggedRow' on the first one whose
field count differs from the header.  Only called when the user opts
into 'RaiseOnRagged' via 'DataFrame.IO.CSV.fastCsvOnRaggedRow'.
-}
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
a bare @\\n@ or a @\\r\\n@ \x2014 blank lines, which we skip by default to
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
