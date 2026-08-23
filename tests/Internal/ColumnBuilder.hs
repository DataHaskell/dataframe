{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Internal.ColumnBuilder (tests, props) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU

import Control.Monad (forM_, zipWithM_)
import Control.Monad.ST (runST, stToIO)
import Data.Maybe (catMaybes, isNothing)
import Data.Text.Encoding (decodeUtf8Lenient, encodeUtf8)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Data.Word (Word8)
import DataFrame.Internal.Column (
    Column (BoxedColumn, PackedText, UnboxedColumn),
    Columnable,
    columnElemIsNull,
    columnVersionString,
    fromVector,
    materializePacked,
 )
import DataFrame.Internal.Column.Bitmap (bitmapTestBit)
import DataFrame.Internal.Column.Builder (
    ColumnBuilder (appendNull, builderLength, freezeBuilder),
    appendDouble,
    appendInt,
    appendText,
    appendTextSlice,
    appendTextSliceFromPtr,
    concatColumns,
    newDoubleBuilder,
    newIntBuilder,
    newTextBuilder,
 )
import Foreign.Ptr (castPtr)
import Test.HUnit (
    Test (TestCase, TestLabel),
    assertEqual,
    assertFailure,
 )
import Test.QuickCheck (
    Gen,
    Property,
    Testable (property),
    chooseInt,
    forAll,
    (.&&.),
    (===),
 )
import Type.Reflection (typeRep)

-- Build a column by appending each list element (Nothing => appendNull).
buildIntColumn :: [Maybe Int] -> Column
buildIntColumn xs = runST $ do
    b <- newIntBuilder (length xs)
    mapM_ (maybe (appendNull b) (appendInt b)) xs
    freezeBuilder b

buildDoubleColumn :: [Maybe Double] -> Column
buildDoubleColumn xs = runST $ do
    b <- newDoubleBuilder (length xs)
    mapM_ (maybe (appendNull b) (appendDouble b)) xs
    freezeBuilder b

buildTextColumn :: [Maybe T.Text] -> Column
buildTextColumn xs = runST $ do
    b <- newTextBuilder (length xs) 16
    mapM_ (maybe (appendNull b) (appendText b)) xs
    freezeBuilder b

-- Build a text column from raw byte fields via appendTextSlice.
buildTextColumnFromBytes :: [[Word8]] -> Column
buildTextColumnFromBytes fields = runST $ do
    b <- newTextBuilder (length fields) 16
    forM_ fields $ \ws ->
        appendTextSlice b (arrayFromBytes ws) 0 (length ws)
    freezeBuilder b

arrayFromBytes :: [Word8] -> A.Array
arrayFromBytes ws = A.run $ do
    m <- A.new (length ws)
    zipWithM_ (A.unsafeWrite m) [0 ..] ws
    pure m

-- Reference column: fromVector of the plain list (no nulls) or Maybe list.
expectedColumn ::
    forall a.
    (Columnable a, Columnable (Maybe a)) =>
    [Maybe a] -> Column
expectedColumn xs
    | any isNothing xs = fromVector (VB.fromList xs)
    | otherwise = fromVector (VB.fromList (catMaybes xs))

-- Read a text column back as a Maybe list, respecting the bitmap.
-- Ingest now freezes text to 'PackedText'; materialize to the boxed form.
columnTexts :: Column -> [Maybe T.Text]
columnTexts c@(PackedText _ _) = columnTexts (materializePacked c)
columnTexts (BoxedColumn mb (v :: VB.Vector b)) =
    case testEquality (typeRep @b) (typeRep @T.Text) of
        Just Refl ->
            [ if maybe True (`bitmapTestBit` i) mb
                then Just (v VB.! i)
                else Nothing
            | i <- [0 .. VB.length v - 1]
            ]
        Nothing -> error "columnTexts: not a Text column"
columnTexts _ = error "columnTexts: not a boxed column"

-- Split a list into arbitrary contiguous chunks (at least one chunk).
splitsOf :: [a] -> Gen [[a]]
splitsOf [] = pure [[]]
splitsOf xs = go xs
  where
    go [] = pure []
    go ys = do
        k <- chooseInt (1, length ys)
        let (a, b) = splitAt k ys
        (a :) <$> go b

nullableVersion :: [Maybe a] -> String -> String -> String
nullableVersion xs nullable plain =
    if any isNothing xs then nullable else plain

-- HUnit tests

intAllValidHasNoBitmap :: Test
intAllValidHasNoBitmap =
    TestCase $
        assertEqual
            "all-valid int column is Unboxed with no bitmap"
            "Unboxed"
            (columnVersionString (buildIntColumn (map Just [1, 2, 3])))

intWithNullHasBitmap :: Test
intWithNullHasBitmap =
    TestCase $
        assertEqual
            "int column with null is NullableUnboxed"
            "NullableUnboxed"
            (columnVersionString (buildIntColumn [Just 1, Nothing]))

intNullSentinelIsZero :: Test
intNullSentinelIsZero = TestCase $
    case buildIntColumn [Just 5, Nothing, Just 7] of
        UnboxedColumn (Just _) (v :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Int) of
                Just Refl -> assertEqual "null slot holds 0" 0 (v VU.! 1)
                Nothing -> assertFailure "expected an Int column"
        _ -> assertFailure "expected a nullable unboxed column"

textAllValidHasNoBitmap :: Test
textAllValidHasNoBitmap =
    TestCase $
        assertEqual
            "all-valid text column is Boxed with no bitmap"
            "Boxed"
            (columnVersionString (buildTextColumn [Just "a", Just "bc"]))

builderLengthCounts :: Test
builderLengthCounts = TestCase $ do
    let n = runST $ do
            b <- newIntBuilder 4
            appendInt b 1
            appendNull b
            appendInt b 3
            builderLength b
    assertEqual "builderLength counts nulls and values" 3 n

growthPastCapacityHint :: Test
growthPastCapacityHint = TestCase $ do
    let xs = map Just [0 .. 9999 :: Int]
        col = runST $ do
            b <- newIntBuilder 1
            mapM_ (maybe (appendNull b) (appendInt b)) xs
            freezeBuilder b
    assertEqual "doubling growth preserves data" (expectedColumn xs) col

multibyteTextRoundTrip :: Test
multibyteTextRoundTrip = TestCase $ do
    let ts = ["héllo", "wörld", "日本語", "😀😃", ""]
    assertEqual
        "multibyte text round trips"
        (map Just ts)
        (columnTexts (buildTextColumn (map Just ts)))

-- A multibyte sequence split across two fields: the whole buffer is valid
-- UTF-8 but each field alone is not. Both must lenient-decode to U+FFFD.
splitMultibyteAcrossFields :: Test
splitMultibyteAcrossFields = TestCase $ do
    let fields = [[0xC2], [0x80]]
    assertEqual
        "fields splitting a sequence decode leniently"
        (map (Just . decodeUtf8Lenient . B.pack) fields)
        (columnTexts (buildTextColumnFromBytes fields))

splitEuroAcrossFields :: Test
splitEuroAcrossFields = TestCase $ do
    let fields = [[0xE2, 0x82], [0xAC, 0x61]]
    assertEqual
        "euro sign split across fields decodes leniently"
        (map (Just . decodeUtf8Lenient . B.pack) fields)
        (columnTexts (buildTextColumnFromBytes fields))

invalidUtf8EdgeCases :: Test
invalidUtf8EdgeCases = TestCase $ do
    let fields =
            [ [0xE2, 0x28, 0xA1]
            , [0xF0, 0x9F, 0x98]
            , [0xF0, 0x28]
            , [0xC2]
            , [0x80]
            , [0xED, 0xA0, 0x80]
            , [0xF4, 0x90, 0x80, 0x80]
            , [0xC0, 0xAF]
            , [0x61, 0xF1, 0x80, 0x80, 0xE1, 0x80, 0xC2, 0x62]
            , [0xF0, 0x9F, 0x98, 0x80]
            ]
    assertEqual
        "invalid sequences match decodeUtf8Lenient"
        (map (Just . decodeUtf8Lenient . B.pack) fields)
        (columnTexts (buildTextColumnFromBytes fields))

appendFromPtrMatchesText :: Test
appendFromPtrMatchesText = TestCase $ do
    let t = "héllo wörld 😀" :: T.Text
    col <- BU.unsafeUseAsCStringLen (encodeUtf8 t) $ \(p, len) ->
        stToIO $ do
            b <- newTextBuilder 1 0
            appendTextSliceFromPtr b (castPtr p) len
            freezeBuilder b
    assertEqual "ptr slice decodes to original text" [Just t] (columnTexts col)

mergeBitmapSpliceUnaligned :: Test
mergeBitmapSpliceUnaligned = TestCase $ do
    let nullIdx = [1, 4, 9, 10, 14] :: [Int]
        xs = [if i `elem` nullIdx then Nothing else Just i | i <- [0 .. 14]]
        chunks = [take 3 xs, take 5 (drop 3 xs), drop 8 xs]
        merged = concatColumns (map buildIntColumn chunks)
    assertEqual "merged equals unsplit" (buildIntColumn xs) merged
    forM_ [0 .. 14] $ \i ->
        assertEqual
            ("null at index " ++ show i)
            (i `elem` nullIdx)
            (columnElemIsNull merged i)

tests :: [Test]
tests =
    [ TestLabel "intAllValidHasNoBitmap" intAllValidHasNoBitmap
    , TestLabel "intWithNullHasBitmap" intWithNullHasBitmap
    , TestLabel "intNullSentinelIsZero" intNullSentinelIsZero
    , TestLabel "textAllValidHasNoBitmap" textAllValidHasNoBitmap
    , TestLabel "builderLengthCounts" builderLengthCounts
    , TestLabel "growthPastCapacityHint" growthPastCapacityHint
    , TestLabel "multibyteTextRoundTrip" multibyteTextRoundTrip
    , TestLabel "splitMultibyteAcrossFields" splitMultibyteAcrossFields
    , TestLabel "splitEuroAcrossFields" splitEuroAcrossFields
    , TestLabel "invalidUtf8EdgeCases" invalidUtf8EdgeCases
    , TestLabel "appendFromPtrMatchesText" appendFromPtrMatchesText
    , TestLabel "mergeBitmapSpliceUnaligned" mergeBitmapSpliceUnaligned
    ]

-- QuickCheck properties

prop_intMatchesFromVector :: [Maybe Int] -> Property
prop_intMatchesFromVector xs =
    buildIntColumn xs === expectedColumn xs
        .&&. columnVersionString (buildIntColumn xs)
            === nullableVersion xs "NullableUnboxed" "Unboxed"

prop_doubleMatchesFromVector :: [Maybe Double] -> Property
prop_doubleMatchesFromVector xs =
    buildDoubleColumn xs === expectedColumn xs

prop_textMatchesFromVector :: [Maybe String] -> Property
prop_textMatchesFromVector ss =
    let xs = map (fmap T.pack) ss
     in buildTextColumn xs === expectedColumn xs
            .&&. columnVersionString (buildTextColumn xs)
                === nullableVersion xs "NullableBoxed" "Boxed"

prop_textSliceMatchesLenientDecode :: [[Word8]] -> Property
prop_textSliceMatchesLenientDecode fields =
    columnTexts (buildTextColumnFromBytes fields)
        === map (Just . decodeUtf8Lenient . B.pack) fields

prop_mergeIntMatchesUnsplit :: [Maybe Int] -> Property
prop_mergeIntMatchesUnsplit xs = forAll (splitsOf xs) $ \chunks ->
    let merged = concatColumns (map buildIntColumn chunks)
        unsplit = buildIntColumn xs
     in merged === unsplit
            .&&. columnVersionString merged === columnVersionString unsplit

prop_mergeDoubleMatchesUnsplit :: [Maybe Double] -> Property
prop_mergeDoubleMatchesUnsplit xs = forAll (splitsOf xs) $ \chunks ->
    concatColumns (map buildDoubleColumn chunks) === buildDoubleColumn xs

prop_mergeTextMatchesUnsplit :: [Maybe String] -> Property
prop_mergeTextMatchesUnsplit ss =
    let xs = map (fmap T.pack) ss
     in forAll (splitsOf xs) $ \chunks ->
            let merged = concatColumns (map buildTextColumn chunks)
                unsplit = buildTextColumn xs
             in merged === unsplit
                    .&&. columnTexts merged === columnTexts unsplit

prop_mergeNullsLandAtRightRows :: [Maybe Int] -> Property
prop_mergeNullsLandAtRightRows xs = forAll (splitsOf xs) $ \chunks ->
    let merged = concatColumns (map buildIntColumn chunks)
     in map isNothing xs
            === [columnElemIsNull merged i | i <- [0 .. length xs - 1]]

props :: [Property]
props =
    [ property prop_intMatchesFromVector
    , property prop_doubleMatchesFromVector
    , property prop_textMatchesFromVector
    , property prop_textSliceMatchesLenientDecode
    , property prop_mergeIntMatchesUnsplit
    , property prop_mergeDoubleMatchesUnsplit
    , property prop_mergeTextMatchesUnsplit
    , property prop_mergeNullsLandAtRightRows
    ]
