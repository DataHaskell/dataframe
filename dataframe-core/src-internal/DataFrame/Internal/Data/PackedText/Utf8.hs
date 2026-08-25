{-# LANGUAGE BangPatterns #-}

{- | UTF-8 validation and @decodeUtf8Lenient@-parity slice decoding used by
'DataFrame.Internal.Column.Builder' to turn shared byte buffers into 'Text'.
-}
module DataFrame.Internal.Data.PackedText.Utf8 (
    isValidUtf8Slice,
    isUtf8Boundary,
    lenientDecodeSlice,
    sliceTextVector,
) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU

import Data.Text.Internal (Text (..))
import Data.Text.Internal.Encoding.Utf8 (
    DecoderResult (..),
    utf8DecodeContinue,
    utf8DecodeStart,
 )
import Data.Text.Internal.Validate (isValidUtf8ByteArray)
import Data.Word (Word8)

-- | Whether @len@ bytes starting at @off@ are well-formed UTF-8.
isValidUtf8Slice :: A.Array -> Int -> Int -> Bool
isValidUtf8Slice = isValidUtf8ByteArray
{-# INLINE isValidUtf8Slice #-}

{- | Whether a byte may start a code point (i.e. is not a continuation
byte). Field slices of a valid buffer are themselves valid iff every
field starts on a boundary.
-}
isUtf8Boundary :: Word8 -> Bool
isUtf8Boundary w = w < 0x80 || w >= 0xC0
{-# INLINE isUtf8Boundary #-}

{- | Decode a byte slice exactly like @decodeUtf8Lenient@: greedy decode at
each position; any byte that cannot begin a complete, valid sequence within
the slice becomes one U+FFFD and decoding resumes at the next byte.
-}
lenientDecodeSlice :: A.Array -> Int -> Int -> T.Text
lenientDecodeSlice arr off len = T.pack (go off)
  where
    !end = off + len
    go !i
        | i >= end = []
        | otherwise = case tryDecode i of
            Just (c, i') -> c : go i'
            Nothing -> '\xFFFD' : go (i + 1)
    tryDecode !i = loop (utf8DecodeStart (A.unsafeIndex arr i)) (i + 1)
      where
        loop (Accept c) !j = Just (c, j)
        loop Reject _ = Nothing
        loop (Incomplete st cp) !j
            | j >= end = Nothing
            | otherwise = loop (utf8DecodeContinue (A.unsafeIndex arr j) st cp) (j + 1)

{- | Slice forced 'Text' values off a shared array; row @i@ spans bytes
@[offs!i, offs!(i+1))@. Fast path validates the whole span once when every field
starts on a code-point boundary; else per-field validation with lenient decode.
-}
sliceTextVector :: A.Array -> VU.Vector Int -> VB.Vector T.Text
sliceTextVector arr offs = VB.create $ do
    mv <- VBM.unsafeNew n
    let fill dec = go 0
          where
            go !i
                | i >= n = pure ()
                | otherwise = do
                    let o = VU.unsafeIndex offs i
                        !t = dec o (VU.unsafeIndex offs (i + 1) - o)
                    VBM.unsafeWrite mv i t
                    go (i + 1)
    if fast then fill mkSlice else fill decodeField
    pure mv
  where
    n = VU.length offs - 1
    base = VU.unsafeIndex offs 0
    used = VU.unsafeIndex offs n
    boundariesOk !i
        | i >= n = True
        | otherwise =
            let o = VU.unsafeIndex offs i
             in (o >= used || isUtf8Boundary (A.unsafeIndex arr o))
                    && boundariesOk (i + 1)
    fast = isValidUtf8Slice arr base (used - base) && boundariesOk 0
    mkSlice o l = if l == 0 then T.empty else Text arr o l
    decodeField o l
        | l == 0 = T.empty
        | isValidUtf8Slice arr o l = Text arr o l
        | otherwise = lenientDecodeSlice arr o l
