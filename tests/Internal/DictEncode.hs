{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Correctness oracle for the dictionary-encode building block
('DataFrame.Internal.Column.Encode'). A text column encodes to dense
first-appearance @Int@ codes: two rows share a code iff their text is equal, the
codes are a contiguous @0 .. card-1@ range in first-appearance order, packed and
boxed Text encode identically, and the cap parameter bails past a cardinality.
Pins the step that the group-by planner can route a text key through.
-}
module Internal.DictEncode (tests) where

import qualified Data.ByteString as B
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU

import Control.Monad (zipWithM_)
import Data.Maybe (isJust)
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word8)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Column.Encode (dictEncodeColumn, dictEncodeColumnUpTo)
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)
import Test.HUnit

sampleRows :: [T.Text]
sampleRows =
    [ "apple"
    , "banana"
    , ""
    , "apple"
    , "Zebra"
    , "café"
    , "naïve"
    , "日本語"
    , "banana"
    , "apple"
    ]

arrayFromBytes :: [Word8] -> A.Array
arrayFromBytes ws = A.run $ do
    m <- A.new (length ws)
    zipWithM_ (A.unsafeWrite m) [0 ..] ws
    pure m

packedFromTexts :: [T.Text] -> DI.Column
packedFromTexts ts =
    let bytess = map (B.unpack . encodeUtf8) ts
        flat = concat bytess
        offs = scanl (+) 0 (map length bytess)
        arr = arrayFromBytes flat
     in DI.PackedText Nothing (mkPackedContiguous arr (VU.fromList offs))

boxedFromTexts :: [T.Text] -> DI.Column
boxedFromTexts = DI.fromList

{- | The reference dense first-appearance encoding computed with a plain
'Data.Map': scan in row order, assign the next id to each new value.
-}
oracleCodes :: [T.Text] -> ([Int], Int)
oracleCodes ts = go ts M.empty 0 []
  where
    go [] _ next acc = (reverse acc, next)
    go (x : xs) m next acc = case M.lookup x m of
        Just c -> go xs m next (c : acc)
        Nothing -> go xs (M.insert x next m) (next + 1) (next : acc)

codesMatchOracle :: String -> DI.Column -> Test
codesMatchOracle lbl col =
    TestCase $ case dictEncodeColumn col of
        Nothing -> assertFailure (lbl ++ ": expected Just codes")
        Just (codes, card) -> do
            let (refCodes, refCard) = oracleCodes sampleRows
            assertEqual (lbl ++ " codes") refCodes (VU.toList codes)
            assertEqual (lbl ++ " cardinality") refCard card

packedBoxedAgree :: Test
packedBoxedAgree =
    TestCase $
        assertEqual
            "packed and boxed encode identically"
            (dictEncodeColumn (boxedFromTexts sampleRows))
            (dictEncodeColumn (packedFromTexts sampleRows))

denseRange :: Test
denseRange =
    TestCase $ case dictEncodeColumn (packedFromTexts sampleRows) of
        Nothing -> assertFailure "expected Just"
        Just (codes, card) -> do
            assertBool "codes in [0,card)" (VU.all (\c -> c >= 0 && c < card) codes)
            assertEqual
                "all codes used"
                [0 .. card - 1]
                (M.keys (M.fromList [(c, ()) | c <- VU.toList codes]))

equalIffSameText :: Test
equalIffSameText =
    TestCase $ case dictEncodeColumn (packedFromTexts sampleRows) of
        Nothing -> assertFailure "expected Just"
        Just (codes, _) ->
            let pairs = [(i, j) | i <- idxs, j <- idxs, i < j]
                idxs = [0 .. length sampleRows - 1]
                ok (i, j) =
                    (VU.unsafeIndex codes i == VU.unsafeIndex codes j)
                        == (sampleRows !! i == sampleRows !! j)
             in assertBool "code equality matches text equality" (all ok pairs)

capBails :: Test
capBails =
    TestCase $ do
        -- sampleRows has 7 distinct values; a cap below that must bail.
        assertEqual
            "cap 3 bails"
            Nothing
            (dictEncodeColumnUpTo 3 (packedFromTexts sampleRows))
        assertEqual
            "cap bails when the final row crosses it"
            Nothing
            (dictEncodeColumnUpTo 2 (packedFromTexts ["a", "b", "c"]))
        -- a generous cap still encodes.
        assertBool
            "cap 100 encodes"
            (isJust (dictEncodeColumnUpTo 100 (packedFromTexts sampleRows)))

nonTextIsNothing :: Test
nonTextIsNothing =
    TestCase $
        assertEqual
            "int column is not dict-encoded"
            Nothing
            (dictEncodeColumn (DI.fromList [1 :: Int, 2, 3]))

tests :: [Test]
tests =
    [ TestLabel
        "dictCodesPacked"
        (codesMatchOracle "packed" (packedFromTexts sampleRows))
    , TestLabel
        "dictCodesBoxed"
        (codesMatchOracle "boxed" (boxedFromTexts sampleRows))
    , TestLabel "dictPackedBoxedAgree" packedBoxedAgree
    , TestLabel "dictDenseRange" denseRange
    , TestLabel "dictEqualIffSameText" equalIffSameText
    , TestLabel "dictCapBails" capBails
    , TestLabel "dictNonTextNothing" nonTextIsNothing
    ]
