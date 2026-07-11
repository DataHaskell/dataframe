{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Fast, non-backtracking field parsers for CSV ingest. Each is a pure
function over a @(buf, start, end)@ byte slice, bit-exact with the reference
parsers in "DataFrame.Internal.Parsing". Unboxed @#@ variants avoid boxing.
-}
module DataFrame.Internal.Parsing.Fast (
    -- * Int fields
    parseIntField,
    parseIntFieldSlice,
    parseIntField#,

    -- * Double fields
    parseDoubleField,
    parseDoubleFieldSlice,
    parseDoubleField#,

    -- * Bool fields
    parseBoolField,
    parseBoolFieldSlice,
    parseBoolField#,

    -- * Date fields (default @%Y-%m-%d@ format)
    parseDateField,
    parseDateFieldSlice,

    -- * Missing-token test
    isMissingField,
    isMissingFieldSlice,
    isMissingFieldIn,
) where

import qualified Data.ByteString as BS

import Data.Time (Day)
import GHC.Exts (Double (..), Int (..))

import DataFrame.Internal.Parsing.Fast.Double (parseDoubleField#)
import DataFrame.Internal.Parsing.Fast.Int (parseIntField#)
import DataFrame.Internal.Parsing.Fast.Token (
    isMissingFieldIn,
    isMissingFieldSlice,
    parseBoolField#,
    parseDateFieldSlice,
 )

{- | Strip-tolerant @Int@ parse of a whole field; rejects overflow,
matching @readByteStringInt@ exactly.
-}
parseIntField :: BS.ByteString -> Maybe Int
parseIntField bs = parseIntFieldSlice bs 0 (BS.length bs)
{-# INLINE parseIntField #-}

parseIntFieldSlice :: BS.ByteString -> Int -> Int -> Maybe Int
parseIntFieldSlice bs start end =
    case parseIntField# bs start end of
        (# 0#, _ #) -> Nothing
        (# _, n #) -> Just (I# n)
{-# INLINE parseIntFieldSlice #-}

{- | Strip-tolerant @Double@ parse of a whole field; bit-exact with
@readByteStringDouble@ (falls back to it outside the fast window).
-}
parseDoubleField :: BS.ByteString -> Maybe Double
parseDoubleField bs = parseDoubleFieldSlice bs 0 (BS.length bs)
{-# INLINE parseDoubleField #-}

parseDoubleFieldSlice :: BS.ByteString -> Int -> Int -> Maybe Double
parseDoubleFieldSlice bs start end =
    case parseDoubleField# bs start end of
        (# 0#, _ #) -> Nothing
        (# _, d #) -> Just (D# d)
{-# INLINE parseDoubleFieldSlice #-}

-- | Exact-match Bool parse (@True|true|TRUE|False|false|FALSE@, no strip).
parseBoolField :: BS.ByteString -> Maybe Bool
parseBoolField bs = parseBoolFieldSlice bs 0 (BS.length bs)
{-# INLINE parseBoolField #-}

parseBoolFieldSlice :: BS.ByteString -> Int -> Int -> Maybe Bool
parseBoolFieldSlice bs start end =
    case parseBoolField# bs start end of
        (# 0#, _ #) -> Nothing
        (# _, b #) -> Just (I# b /= 0)
{-# INLINE parseBoolFieldSlice #-}

{- | @%Y-%m-%d@ date parse: byte-level fast path for the padded
10-byte shape, 'Data.Time.parseTimeM' fallback for everything else.
-}
parseDateField :: BS.ByteString -> Maybe Day
parseDateField bs = parseDateFieldSlice bs 0 (BS.length bs)
{-# INLINE parseDateField #-}

{- | Byte-level test against the canonical missing-token list
(@Nothing NULL \"\" \" \" nan null N\/A NaN NAN NA@), no Text decode.
-}
isMissingField :: BS.ByteString -> Bool
isMissingField bs = isMissingFieldSlice bs 0 (BS.length bs)
{-# INLINE isMissingField #-}
