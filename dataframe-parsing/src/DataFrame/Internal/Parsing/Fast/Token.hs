{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Byte-level token tests: Bool fields, the canonical missing-token
set, and the default @%Y-%m-%d@ date shape. All length-bucketed with
first-byte dispatch; no Text decode, no list walk.
-}
module DataFrame.Internal.Parsing.Fast.Token (
    parseBoolField#,
    isMissingFieldSlice,
    isMissingFieldIn,
    parseDateFieldSlice,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU

import Data.Time (Day, fromGregorianValid)
import Data.Word (Word8)
import GHC.Exts (Int#)

import DataFrame.Internal.Parsing (readByteStringDate)
import DataFrame.Internal.Parsing.Fast.Common (isDigitByte)

{- | Exactly @True|true|TRUE|False|false|FALSE@, no strip (the
'readByteStringBool' grammar). Result is @(# ok, bool #)@.
-}
parseBoolField# :: BS.ByteString -> Int -> Int -> (# Int#, Int# #)
parseBoolField# bs s e = case e - s of
    4
        | ix 0 == 0x54 && rue 0x72 0x75 0x65 -> (# 1#, 1# #) -- True
        | ix 0 == 0x54 && rue 0x52 0x55 0x45 -> (# 1#, 1# #) -- TRUE
        | ix 0 == 0x74 && rue 0x72 0x75 0x65 -> (# 1#, 1# #) -- true
    5
        | ix 0 == 0x46 && alse 0x61 0x6C 0x73 0x65 -> (# 1#, 0# #) -- False
        | ix 0 == 0x46 && alse 0x41 0x4C 0x53 0x45 -> (# 1#, 0# #) -- FALSE
        | ix 0 == 0x66 && alse 0x61 0x6C 0x73 0x65 -> (# 1#, 0# #) -- false
    _ -> (# 0#, 0# #)
  where
    ix d = BSU.unsafeIndex bs (s + d)
    rue a b c = ix 1 == a && ix 2 == b && ix 3 == c
    alse a b c d = ix 1 == a && ix 2 == b && ix 3 == c && ix 4 == d
{-# INLINE parseBoolField# #-}

{- | Membership in the canonical missing list
@[\"Nothing\",\"NULL\",\"\",\" \",\"nan\",\"null\",\"N\/A\",\"NaN\",\"NAN\",\"NA\"]@
(case-sensitive, exact), dispatched on length then first byte.
-}
isMissingFieldSlice :: BS.ByteString -> Int -> Int -> Bool
isMissingFieldSlice bs s e = case e - s of
    0 -> True
    1 -> ix 0 == 0x20 -- " "
    2 -> ix 0 == 0x4E && ix 1 == 0x41 -- NA
    3 -> case ix 0 of
        0x6E -> ix 1 == 0x61 && ix 2 == 0x6E -- nan
        0x4E ->
            (ix 2 == 0x4E && (ix 1 == 0x61 || ix 1 == 0x41)) -- NaN NAN
                || (ix 1 == 0x2F && ix 2 == 0x41) -- N/A
        _ -> False
    4 ->
        (ix 0 == 0x4E && ix 1 == 0x55 && ix 2 == 0x4C && ix 3 == 0x4C) -- NULL
            || (ix 0 == 0x6E && ix 1 == 0x75 && ix 2 == 0x6C && ix 3 == 0x6C) -- null
    7 ->
        ix 0 == 0x4E -- Nothing
            && ix 1 == 0x6F
            && ix 2 == 0x74
            && ix 3 == 0x68
            && ix 4 == 0x69
            && ix 5 == 0x6E
            && ix 6 == 0x67
    _ -> False
  where
    ix :: Int -> Word8
    ix d = BSU.unsafeIndex bs (s + d)
    {-# INLINE ix #-}
{-# INLINE isMissingFieldSlice #-}

-- | Generic fallback for user-supplied missing-indicator lists.
isMissingFieldIn :: [BS.ByteString] -> BS.ByteString -> Bool
isMissingFieldIn toks f = f `elem` toks
{-# INLINE isMissingFieldIn #-}

{- | @%Y-%m-%d@: byte-level fast path for the padded 10-byte shape
(@dddd-dd-dd@); anything else (unpadded, whitespace-tolerant, long
years, invalid) falls back to 'readByteStringDate'.
-}
parseDateFieldSlice :: BS.ByteString -> Int -> Int -> Maybe Day
parseDateFieldSlice bs s e
    | e - s == 10
        && isDigitByte (ix 0)
        && isDigitByte (ix 1)
        && isDigitByte (ix 2)
        && isDigitByte (ix 3)
        && ix 4 == 0x2D
        && isDigitByte (ix 5)
        && isDigitByte (ix 6)
        && ix 7 == 0x2D
        && isDigitByte (ix 8)
        && isDigitByte (ix 9) =
        fromGregorianValid
            (toInteger (dig 0 * 1000 + dig 1 * 100 + dig 2 * 10 + dig 3))
            (dig 5 * 10 + dig 6)
            (dig 8 * 10 + dig 9)
    | otherwise =
        readByteStringDate "%Y-%m-%d" (BSU.unsafeTake (e - s) (BSU.unsafeDrop s bs))
  where
    ix d = BSU.unsafeIndex bs (s + d)
    dig :: Int -> Int
    dig d = fromIntegral (ix d) - 0x30
{-# INLINE parseDateFieldSlice #-}
