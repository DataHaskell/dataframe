{-# LANGUAGE BangPatterns #-}

{- | Field-slice resolution for the fast CSV reader: maps @(row, col)@ to a
byte range of the input (quote-stripped, CR-stripped, optionally trimmed)
without materializing any intermediate value. 'extractField' is the legacy
Text materialization used for headers, inference samples and cold paths.
-}
module DataFrame.IO.CSV.Fast.Slice (
    FieldCtx (..),
    withFieldSlice,
    withParseSlice,
    sliceBS,
    decodeSlice,
    extractField,
    unescapeDoubledQuotes,
    stripBom,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.Text as Text
import qualified Data.Text.Encoding as TextEncoding
import qualified Data.Vector.Storable as VS

import Data.Text (Text)
import Data.Word (Word8)
import Foreign.C.Types (CSize)

import DataFrame.IO.CSV.Fast.Index (cr, quote)

-- | Everything needed to resolve a field slice, shared by all columns.
data FieldCtx = FieldCtx
    { fcFile :: !(VS.Vector Word8)
    -- ^ File content (BOM already stripped), no padding.
    , fcBS :: !BS.ByteString
    -- ^ Zero-copy 'BS.ByteString' view of 'fcFile'.
    , fcDelims :: !(VS.Vector CSize)
    -- ^ Delimiter byte positions (field and row terminators, flat).
    , fcRowEnds :: !(VS.Vector Int)
    -- ^ Indices into 'fcDelims' that terminate a row.
    , fcContentLen :: !Int
    , fcTrim :: !Bool
    -- ^ 'DataFrame.IO.CSV.fastCsvTrimUnquoted'.
    }

{- | Resolve field @col@ of row @r@ and continue with @k start end quoted@.
Quoted fields yield the bytes between the outer quotes (embedded @\"\"@ is
NOT unescaped here); a trailing @\\r@ is stripped first; a column beyond the
row's field count (ragged short row) yields the empty slice. CPS so the hot
loops never box the bounds.
-}
{-# INLINE withFieldSlice #-}
withFieldSlice :: FieldCtx -> Int -> Int -> (Int -> Int -> Bool -> r) -> r
withFieldSlice ctx r col k =
    let rowEnds = fcRowEnds ctx
        endIdx = VS.unsafeIndex rowEnds r
        startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
        numFields = endIdx - startIdx + 1
     in if col >= numFields
            then k 0 0 False
            else
                let boundaryIdx = startIdx + col
                    fieldEndRaw =
                        fromIntegral (VS.unsafeIndex (fcDelims ctx) boundaryIdx) :: Int
                    fieldEndClamped = min fieldEndRaw (fcContentLen ctx)
                    fieldStart =
                        if boundaryIdx == 0
                            then 0
                            else
                                fromIntegral
                                    (VS.unsafeIndex (fcDelims ctx) (boundaryIdx - 1))
                                    + 1
                    file = fcFile ctx
                    fieldEnd =
                        if fieldEndClamped > fieldStart
                            && VS.unsafeIndex file (fieldEndClamped - 1) == cr
                            then fieldEndClamped - 1
                            else fieldEndClamped
                 in if fieldEnd - fieldStart >= 2
                        && VS.unsafeIndex file fieldStart == quote
                        && VS.unsafeIndex file (fieldEnd - 1) == quote
                        then k (fieldStart + 1) (fieldEnd - 1) True
                        else k fieldStart fieldEnd False

{- | 'withFieldSlice' with the trim knob applied: when 'fcTrim' is set,
unquoted slices have ASCII whitespace stripped from both ends before the
continuation runs. This is the slice the typed parsers and null tests see.
-}
{-# INLINE withParseSlice #-}
withParseSlice :: FieldCtx -> Int -> Int -> (Int -> Int -> r) -> r
withParseSlice ctx r col k =
    withFieldSlice ctx r col $ \s e q ->
        if fcTrim ctx && not q
            then
                let file = fcFile ctx
                    s' = skipWsForward file s e
                    e' = skipWsBackward file s' e
                 in k s' e'
            else k s e

{-# INLINE isAsciiWs #-}
isAsciiWs :: Word8 -> Bool
isAsciiWs w = w == 0x20 || (w - 0x09) <= 4

{-# INLINE skipWsForward #-}
skipWsForward :: VS.Vector Word8 -> Int -> Int -> Int
skipWsForward file = go
  where
    go !i !e
        | i < e && isAsciiWs (VS.unsafeIndex file i) = go (i + 1) e
        | otherwise = i

{-# INLINE skipWsBackward #-}
skipWsBackward :: VS.Vector Word8 -> Int -> Int -> Int
skipWsBackward file = go
  where
    go !s !e
        | e > s && isAsciiWs (VS.unsafeIndex file (e - 1)) = go s (e - 1)
        | otherwise = e

-- | O(1) sub-'BS.ByteString' of the file view (no copy).
{-# INLINE sliceBS #-}
sliceBS :: BS.ByteString -> Int -> Int -> BS.ByteString
sliceBS bs s e = BSU.unsafeTake (e - s) (BSU.unsafeDrop s bs)

-- | Lenient UTF-8 decode of a slice (allocates; cold paths only).
{-# INLINE decodeSlice #-}
decodeSlice :: BS.ByteString -> Int -> Int -> Text
decodeSlice bs s e = TextEncoding.decodeUtf8Lenient (sliceBS bs s e)

{- | Extract field @col@ of row @r@ as a 'Text' with the original fastcsv
semantics: quotes stripped and @\"\"@ unescaped for quoted fields, optional
'Text.strip' (full Unicode) for unquoted fields when 'fcTrim' is set.
Used for header names, inference samples and the legacy (EitherRead /
unsupported-schema) column paths.
-}
{-# INLINE extractField #-}
extractField :: FieldCtx -> Int -> Int -> Text
extractField ctx r col =
    withFieldSlice ctx r col $ \s e q ->
        if q
            then unescapeDoubledQuotes (decodeSlice (fcBS ctx) s e)
            else
                (if fcTrim ctx then Text.strip else id)
                    (decodeSlice (fcBS ctx) s e)

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
