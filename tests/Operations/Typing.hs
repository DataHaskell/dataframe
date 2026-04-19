{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Typing where

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (getColumn)
import qualified DataFrame.Operations.Typing as D

import Data.Time (Day, fromGregorian)
import Test.HUnit (Test (TestCase, TestLabel), assertEqual, assertFailure)

testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("test1", DI.fromList ([1 .. 26] :: [Int]))
        , ("test2", DI.fromList ['a' .. 'z'])
        ]

-- Dimensions
correctDimensions :: Test
correctDimensions = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions testData))

emptyDataframeDimensions :: Test
emptyDataframeDimensions = TestCase (assertEqual "should be (0, 0)" (0, 0) (D.dimensions D.empty))

dimensionsTest :: [Test]
dimensionsTest =
    [ TestLabel "dimensions_correctDimensions" correctDimensions
    , TestLabel "dimensions_emptyDataframeDimensions" emptyDataframeDimensions
    ]

-- Shared data directory
typingDataDir :: FilePath
typingDataDir = "./tests/data/typing/"

-- Shared input/expected data
intsInput :: [T.Text]
intsInput = T.pack . show <$> ([1 .. 50] :: [Int])

intsExpected :: [Int]
intsExpected = [1 .. 50]

doublesSpecial :: [Double]
doublesSpecial = [3.14, 2.22, 8.55, 23.3, 12.22222235049450945049504950]

doublesSpecialInput :: [T.Text]
doublesSpecialInput = ["3.14", "2.22", "8.55", "23.3", "12.22222235049451"]

doublesExpected :: [Double]
doublesExpected = [1.0 .. 50.0] ++ doublesSpecial

doublesInput :: [T.Text]
doublesInput = (T.pack . show <$> ([1.0 .. 50.0] :: [Double])) ++ doublesSpecialInput

intsAndDoublesExpected :: [Double]
intsAndDoublesExpected = ([1 .. 50] :: [Double]) ++ doublesExpected

intsAndDoublesInput :: [T.Text]
intsAndDoublesInput = intsInput ++ doublesInput

datesExpected :: [Day]
datesExpected = [fromGregorian 2020 02 12 .. fromGregorian 2020 02 26]

datesInput :: [T.Text]
datesInput = T.pack . show <$> datesExpected

-- PARSING TESTS
------- 1. SIMPLE CASES
parseBools :: Test
parseBools =
    let afterParse :: [Bool]
        afterParse = [True, True, True] ++ [False, False, False]
        beforeParse :: [T.Text]
        beforeParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as OptionalColumn of Maybe Bools"
                expected
                actual
            )

parseInts :: Test
parseInts =
    let afterParse :: [Int]
        afterParse = [1 .. 50]
        beforeParse :: [T.Text]
        beforeParse = T.pack . show <$> ([1 .. 50] :: [Int])
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as OptionalColumn of Maybe Ints"
                expected
                actual
            )

parseDoubles :: Test
parseDoubles =
    let afterParse :: [Double]
        afterParse = [1.0 .. 50.0] ++ [3.14, 2.22, 8.55, 23.3, 12.22222235049450945049504950]
        beforeParse :: [T.Text]
        beforeParse =
            T.pack . show
                <$> ( [1.0 .. 50.0] ++ [3.14, 2.22, 8.55, 23.3, 12.22222235049450945049504950] ::
                        [Double]
                    )
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Doubles without missing values as OptionalColumn of Maybe Doubles"
                expected
                actual
            )

parseDates :: Test
parseDates =
    let afterParse = datesExpected
        beforeParse = datesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as OptionalColumn of Maybe Days"
                expected
                actual
            )

parseTexts :: Test
parseTexts = TestCase $ do
    texts <- T.lines <$> TIO.readFile (typingDataDir <> "texts.txt")
    let expected = DI.fromVector $ V.fromList texts
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList texts
    assertEqual
        "Correctly parses Text without missing values as OptionalColumn of Maybe Text"
        expected
        actual

--- 2. COMBINATION CASES
parseBoolsAndIntsAsTexts :: Test
parseBoolsAndIntsAsTexts =
    let afterParse :: [T.Text]
        afterParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"] ++ ["1", "0", "1"]
        beforeParse :: [T.Text]
        beforeParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"] ++ ["1", "0", "1"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses mixture of Bools and Ints as Maybe Text"
                expected
                actual
            )

parseIntsAndDoublesAsDoubles :: Test
parseIntsAndDoublesAsDoubles =
    let afterParse = intsAndDoublesExpected
        beforeParse = intsAndDoublesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as OptionalColumn of Maybe Doubles"
                expected
                actual
            )

parseIntsAndDatesAsTexts :: Test
parseIntsAndDatesAsTexts =
    let data30 = T.pack . show <$> ([1 .. 30] :: [Int])
        data9dates = take 9 datesInput
        afterParse :: [T.Text]
        afterParse = data30 ++ data9dates
        beforeParse :: [T.Text]
        beforeParse = data30 ++ data9dates
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Dates as OptionalColumn of Maybe Texts"
                expected
                actual
            )

parseTextsAndDoublesAsTexts :: Test
parseTextsAndDoublesAsTexts =
    let shortTexts = ["To", "protect", "the", "world", "from", "devastation"]
        afterParse :: [T.Text]
        afterParse = shortTexts ++ doublesInput
        beforeParse :: [T.Text]
        beforeParse = shortTexts ++ doublesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Texts and Doubles as OptionalColumn of Maybe Texts"
                expected
                actual
            )

parseDatesAndTextsAsTexts :: Test
parseDatesAndTextsAsTexts =
    let extra = ["Jessie", "James", "Meowth"]
        afterParse :: [T.Text]
        afterParse = datesInput ++ extra
        beforeParse :: [T.Text]
        beforeParse = datesInput ++ extra
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Dates and Texts as OptionalColumn of Maybe Texts"
                expected
                actual
            )

-- 3A. PARSING WITH SAFEREAD OFF

parseBoolsWithoutSafeRead :: Test
parseBoolsWithoutSafeRead =
    let afterParse :: [Bool]
        afterParse = replicate 10 True ++ replicate 10 False
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "true" ++ replicate 10 "false"
        expected = DI.UnboxedColumn Nothing $ VU.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as UnboxedColumn of Bools, when safeRead is off"
                expected
                actual
            )

parseIntsWithoutSafeRead :: Test
parseIntsWithoutSafeRead =
    let afterParse = intsExpected
        beforeParse = intsInput
        expected = DI.UnboxedColumn Nothing $ VU.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints, when safeRead is off"
                expected
                actual
            )

parseDoublesWithoutSafeRead :: Test
parseDoublesWithoutSafeRead =
    let afterParse = doublesExpected
        beforeParse = doublesInput
        expected = DI.UnboxedColumn Nothing $ VU.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Doubles without missing values as UnboxedColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseDatesWithoutSafeRead :: Test
parseDatesWithoutSafeRead =
    let afterParse = datesExpected
        beforeParse = datesInput
        expected = DI.BoxedColumn Nothing $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as BoxedColumn of Days"
                expected
                actual
            )

parseTextsWithoutSafeRead :: Test
parseTextsWithoutSafeRead = TestCase $ do
    texts <- T.lines <$> TIO.readFile (typingDataDir <> "texts.txt")
    let expected = DI.BoxedColumn Nothing $ V.fromList texts
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList texts
    assertEqual
        "Correctly parses Text without missing values as BoxedColumn of Text"
        expected
        actual

parseBoolsAndEmptyStringsWithoutSafeRead :: Test
parseBoolsAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe Bool]
        afterParse = replicate 10 Nothing ++ replicate 10 (Just True) ++ replicate 10 (Just False)
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "" ++ replicate 10 "true" ++ replicate 10 "false"
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools and empty Strings as OptionalColumn of Bools, when safeRead is off"
                expected
                actual
            )

parseIntsAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndEmptyStringsWithoutSafeRead =
    let beforeParse = replicate 5 "" ++ intsInput
        afterParse :: [Maybe Int]
        afterParse = replicate 5 Nothing ++ map Just intsExpected
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and empty strings as OptionalColumn of Ints, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndDoublesAndEmptyStringsWithoutSafeRead =
    let intGrp = T.pack . show <$> ([1 .. 10] :: [Int])
        dblGrps =
            [ T.pack . show <$> ([11.0 .. 20.0] :: [Double])
            , T.pack . show <$> ([21.0 .. 30.0] :: [Double])
            , T.pack . show <$> ([31.0 .. 40.0] :: [Double])
            , T.pack . show <$> ([41.0 .. 50.0] :: [Double])
            ]
        beforeParse = intGrp ++ [""] ++ concatMap (\g -> g ++ [""]) dblGrps ++ doublesSpecialInput
        afterParse :: [Maybe Double]
        afterParse =
            map Just ([1.0 .. 10.0] :: [Double])
                ++ [Nothing]
                ++ concatMap
                    (\g -> map Just g ++ [Nothing])
                    [[11.0 .. 20.0], [21.0 .. 30.0], [31.0 .. 40.0], [41.0 .. 50.0]]
                ++ map Just doublesSpecial
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseDatesAndEmptyStringsWithoutSafeRead :: Test
parseDatesAndEmptyStringsWithoutSafeRead =
    -- Pattern: 3 dates, empty, 3 dates, empty, 3 dates, empty, 3 dates, empty, 3 dates, empty
    let groups = chunksOf3 datesExpected
        beforeParse = concatMap (\g -> map (T.pack . show) g ++ [""]) groups
        afterParse :: [Maybe Day]
        afterParse = concatMap (\g -> map Just g ++ [Nothing]) groups
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates and Empty Strings as OptionalColumn of Dates, with safeRead off"
                expected
                actual
            )
  where
    chunksOf3 [] = []
    chunksOf3 xs = take 3 xs : chunksOf3 (drop 3 xs)

parseTextsAndEmptyStringsWithoutSafeRead :: Test
parseTextsAndEmptyStringsWithoutSafeRead = TestCase $ do
    raw <- T.lines <$> TIO.readFile (typingDataDir <> "texts_with_empties.txt")
    let afterParse = map (\t -> if t == "" then Nothing else Just t) raw
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList raw
    assertEqual
        "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
        expected
        actual

parseBoolsAndNullishStringsWithoutSafeRead :: Test
parseBoolsAndNullishStringsWithoutSafeRead =
    let afterParse :: [T.Text]
        afterParse = replicate 10 "N/A" ++ replicate 10 "True" ++ replicate 10 "False"
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "N/A" ++ replicate 10 "True" ++ replicate 10 "False"
        expected = DI.BoxedColumn Nothing $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools with nullish values as BoxedColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishStringsWithoutSafeRead :: Test
parseIntsAndNullishStringsWithoutSafeRead =
    let beforeParse = replicate 5 "N/A" ++ intsInput
        afterParse :: [T.Text]
        afterParse = beforeParse
        expected = DI.BoxedColumn Nothing $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values as BoxedColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndNullishStringsWithoutSafeRead :: Test
parseIntsAndDoublesAndNullishStringsWithoutSafeRead =
    -- Nullish separators are literal text strings (not interpreted as null when safeRead=False)
    let nullishSeps = ["Nothing", "N/A", "NULL", "null", "NAN"]
        intGrp = T.pack . show <$> ([1 .. 10] :: [Int])
        dblGrps =
            [ T.pack . show <$> ([11.0 .. 20.0] :: [Double])
            , T.pack . show <$> ([21.0 .. 30.0] :: [Double])
            , T.pack . show <$> ([31.0 .. 40.0] :: [Double])
            , T.pack . show <$> ([41.0 .. 50.0] :: [Double])
            ]
        beforeParse =
            intGrp
                ++ concat (zipWith (:) nullishSeps dblGrps)
                ++ doublesSpecialInput
        afterParse :: [T.Text]
        afterParse = beforeParse
        expected = DI.BoxedColumn Nothing $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndNullishAndEmptyStringsWithoutSafeRead =
    -- Pattern: 5x"N/A", then groups of ("" : 10 ints), with trailing ""
    let groups = [[1 .. 10], [11 .. 20], [21 .. 30], [31 .. 40], [41 .. 50]] :: [[Int]]
        beforeParse =
            replicate 5 "N/A"
                ++ concatMap (\g -> "" : map (T.pack . show) g) groups
                ++ [""]
        afterParse :: [Maybe T.Text]
        afterParse =
            replicate 5 (Just "N/A")
                ++ concatMap (\g -> Nothing : map (Just . T.pack . show) g) groups
                ++ [Nothing]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values AND empty strings as OptionalColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseTextsAndEmptyAndNullishStringsWithoutSafeRead :: Test
parseTextsAndEmptyAndNullishStringsWithoutSafeRead = TestCase $ do
    raw <-
        T.lines <$> TIO.readFile (typingDataDir <> "texts_with_empties_and_nullish.txt")
    -- safeRead=False: empty strings -> Nothing, nullish text stays as Just
    let afterParse = map (\t -> if t == "" then Nothing else Just t) raw
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList raw
    assertEqual
        "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
        expected
        actual

-- 3B. PARSING WITH SAFEREAD ON
parseBoolsAndEmptyStringsWithSafeRead :: Test
parseBoolsAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Bool]
        afterParse = replicate 10 Nothing ++ replicate 10 (Just True)
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "" ++ replicate 10 "true"
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools and empty strings as OptionalColumn of Bools, when safeRead is on"
                expected
                actual
            )

parseIntsAndEmptyStringsWithSafeRead :: Test
parseIntsAndEmptyStringsWithSafeRead =
    let beforeParse = replicate 5 "" ++ intsInput
        afterParse :: [Maybe Int]
        afterParse = replicate 5 Nothing ++ map Just intsExpected
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and empty strings as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsWithSafeRead :: Test
parseIntsAndDoublesAndEmptyStringsWithSafeRead =
    let intGrp = T.pack . show <$> ([1 .. 10] :: [Int])
        dblGrps =
            [ T.pack . show <$> ([11.0 .. 20.0] :: [Double])
            , T.pack . show <$> ([21.0 .. 30.0] :: [Double])
            , T.pack . show <$> ([31.0 .. 40.0] :: [Double])
            , T.pack . show <$> ([41.0 .. 50.0] :: [Double])
            ]
        beforeParse = intGrp ++ [""] ++ concatMap (\g -> g ++ [""]) dblGrps ++ doublesSpecialInput
        afterParse :: [Maybe Double]
        afterParse =
            map Just ([1.0 .. 10.0] :: [Double])
                ++ [Nothing]
                ++ concatMap
                    (\g -> map Just g ++ [Nothing])
                    [[11.0 .. 20.0], [21.0 .. 30.0], [31.0 .. 40.0], [41.0 .. 50.0]]
                ++ map Just doublesSpecial
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is on"
                expected
                actual
            )

parseDatesAndEmptyStringsWithSafeRead :: Test
parseDatesAndEmptyStringsWithSafeRead =
    let groups = chunksOf3 datesExpected
        beforeParse = concatMap (\g -> map (T.pack . show) g ++ [""]) groups
        afterParse :: [Maybe Day]
        afterParse = concatMap (\g -> map Just g ++ [Nothing]) groups
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates and Empty Strings as OptionalColumn of Dates, with safeRead on"
                expected
                actual
            )
  where
    chunksOf3 [] = []
    chunksOf3 xs = take 3 xs : chunksOf3 (drop 3 xs)

parseTextsAndEmptyStringsWithSafeRead :: Test
parseTextsAndEmptyStringsWithSafeRead = TestCase $ do
    raw <- T.lines <$> TIO.readFile (typingDataDir <> "texts_with_empties.txt")
    let afterParse = map (\t -> if t == "" then Nothing else Just t) raw
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList raw
    assertEqual
        "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead on"
        expected
        actual

parseIntsAndNullishStringsWithSafeRead :: Test
parseIntsAndNullishStringsWithSafeRead =
    let beforeParse = replicate 5 "N/A" ++ intsInput
        afterParse :: [Maybe Int]
        afterParse = replicate 5 Nothing ++ map Just intsExpected
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndNullishStringsWithSafeRead :: Test
parseIntsAndDoublesAndNullishStringsWithSafeRead =
    -- Note: this test uses different special doubles and nullish separators than the safeRead=False version
    let nullishSeps = ["Nothing", "N/A", "NULL", "null", "NAN"]
        intGrp = T.pack . show <$> ([1 .. 10] :: [Int])
        dblGrps =
            [ T.pack . show <$> ([11.0 .. 20.0] :: [Double])
            , T.pack . show <$> ([21.0 .. 30.0] :: [Double])
            , T.pack . show <$> ([31.0 .. 40.0] :: [Double])
            , T.pack . show <$> ([41.0 .. 50.0] :: [Double])
            ]
        beforeParse =
            intGrp
                ++ concat (zipWith (:) nullishSeps dblGrps)
                ++ [nullishSeps !! 4]
                ++ ["3.14", "2.22", "8.55", "23.3", "12.03"]
        afterParse :: [Maybe Double]
        afterParse =
            map Just ([1.0 .. 10.0] :: [Double])
                ++ [Nothing]
                ++ concatMap
                    (\g -> map Just g ++ [Nothing])
                    [[11.0 .. 20.0], [21.0 .. 30.0], [31.0 .. 40.0], [41.0 .. 50.0]]
                ++ map Just [3.14, 2.22, 8.55, 23.3, 12.03]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishAndEmptyStringsWithSafeRead :: Test
parseIntsAndNullishAndEmptyStringsWithSafeRead =
    -- safeRead=True: N/A, Nothing -> Nothing; "" -> Nothing
    let groups = [[1 .. 10], [11 .. 20], [21 .. 30], [31 .. 40], [41 .. 50]] :: [[Int]]
        beforeParse =
            ["N/A", "N/A", "N/A", "N/A", "Nothing"]
                ++ concatMap (\g -> "" : map (T.pack . show) g) groups
                ++ [""]
        afterParse :: [Maybe Int]
        afterParse =
            replicate 5 Nothing
                ++ concatMap (\g -> Nothing : map Just g) groups
                ++ [Nothing]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values AND empty strings as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead :: Test
parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead =
    -- Data: 4xN/A, "Nothing", "", then 10 ints (1-10), "", 10 ints (11-20), "", 10 ints (21-30), "", 3.14
    let beforeParse =
            ["N/A", "N/A", "N/A", "N/A", "Nothing", ""]
                ++ map (T.pack . show) ([1 .. 10] :: [Int])
                ++ [""]
                ++ map (T.pack . show) ([11 .. 20] :: [Int])
                ++ [""]
                ++ map (T.pack . show) ([21 .. 30] :: [Int])
                ++ ["", "3.14"]
        afterParse :: [Maybe Double]
        afterParse =
            replicate 6 Nothing
                ++ map Just ([1 .. 10] :: [Double])
                ++ [Nothing]
                ++ map Just ([11 .. 20] :: [Double])
                ++ [Nothing]
                ++ map Just ([21 .. 30] :: [Double])
                ++ [Nothing, Just 3.14]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and Doubles with nullish values AND empty strings as OptionalColumn of Doubles, when safeRead is on"
                expected
                actual
            )

parseTextsAndEmptyAndNullishStringsWithSafeRead :: Test
parseTextsAndEmptyAndNullishStringsWithSafeRead = TestCase $ do
    raw <-
        T.lines <$> TIO.readFile (typingDataDir <> "texts_with_empties_and_nullish.txt")
    -- safeRead=True: empty strings and nullish tokens (NaN, Nothing, N/A) -> Nothing
    let isNullish t = t `elem` ["NaN", "Nothing", "N/A", "nan", "null", "NULL", "NA", "na", "NAN"]
        afterParse = map (\t -> if t == "" || isNullish t then Nothing else Just t) raw
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList raw
    assertEqual
        "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
        expected
        actual

-- 3C. PARSING WITH SAFEREAD = EitherRead
--
-- EitherRead wraps every column as @Either Text a@: successful parses become
-- @Right v@; failures (including nullish/empty cells) become @Left <raw>@
-- preserving the original input verbatim.

parseBoolsWithEitherRead :: Test
parseBoolsWithEitherRead =
    let beforeParse :: [T.Text]
        beforeParse = ["true", "", "false", "N/A", "True"]
        afterParse :: [Either T.Text Bool]
        afterParse =
            [Right True, Left "", Right False, Left "N/A", Right True]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 5, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead wraps Bools as Either Text Bool; failures carry raw text"
                expected
                actual
            )

parseIntsWithEitherRead :: Test
parseIntsWithEitherRead =
    -- Sample (first 5 rows) is cleanly Int; later rows exercise the Left path.
    let beforeParse :: [T.Text]
        beforeParse = ["1", "2", "3", "4", "42", "abc", "", "N/A"]
        afterParse :: [Either T.Text Int]
        afterParse =
            [ Right 1
            , Right 2
            , Right 3
            , Right 4
            , Right 42
            , Left "abc"
            , Left ""
            , Left "N/A"
            ]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 5, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead wraps Ints as Either Text Int; failures carry raw text"
                expected
                actual
            )

parseDoublesWithEitherRead :: Test
parseDoublesWithEitherRead =
    -- Sample is cleanly Double; "oops" / "" come after and must surface as Left.
    let beforeParse :: [T.Text]
        beforeParse = ["1.5", "2.0", "3.5", "4.25", "3.14", "oops", ""]
        afterParse :: [Either T.Text Double]
        afterParse =
            [ Right 1.5
            , Right 2.0
            , Right 3.5
            , Right 4.25
            , Right 3.14
            , Left "oops"
            , Left ""
            ]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 5, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead wraps Doubles as Either Text Double; failures carry raw text"
                expected
                actual
            )

parseTextsWithEitherRead :: Test
parseTextsWithEitherRead =
    let beforeParse :: [T.Text]
        beforeParse = ["hello", "", "world"]
        afterParse :: [Either T.Text T.Text]
        afterParse = [Right "hello", Left "", Right "world"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 3, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead wraps Texts as Either Text Text; empty cells become Left \"\""
                expected
                actual
            )

parseDatesWithEitherRead :: Test
parseDatesWithEitherRead =
    -- Sample is clean dates; "not-a-date" and "" come after and become Left.
    let beforeParse :: [T.Text]
        beforeParse =
            (T.pack . show <$> take 5 datesExpected) ++ ["not-a-date", ""]
        expectedGood :: [Either T.Text Day]
        expectedGood = Right <$> take 5 datesExpected
        expectedBad :: [Either T.Text Day]
        expectedBad = [Left "not-a-date", Left ""]
        expected = DI.fromVector $ V.fromList (expectedGood ++ expectedBad)
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 5, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead wraps Dates as Either Text Day; failures carry raw text"
                expected
                actual
            )

-- parseDefaults honours per-column overrides: 'ages' stays strict Int,
-- 'notes' is wrapped as Either Text Text.
parseDefaultsPerColumnOverrides :: Test
parseDefaultsPerColumnOverrides = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("ages", DI.fromList @T.Text ["1", "2", "3"])
                , ("notes", DI.fromList @T.Text ["hello", "", "world"])
                ]
        opts =
            D.defaultParseOptions
                { D.sampleSize = 3
                , D.parseSafe = D.MaybeRead
                , D.parseSafeOverrides =
                    [ ("ages", D.NoSafeRead)
                    , ("notes", D.EitherRead)
                    ]
                }
        result = D.parseDefaults opts df
    case getColumn "ages" result of
        Just (DI.UnboxedColumn Nothing _) -> pure () -- strict Int
        Just col ->
            assertFailure $
                "'ages' should be bare UnboxedColumn, got "
                    <> show col
        Nothing -> assertFailure "ages column missing"
    let expectedNotes =
            DI.fromList @(Either T.Text T.Text)
                [Right "hello", Left "", Right "world"]
    case getColumn "notes" result of
        Just col ->
            assertEqual
                "'notes' override EitherRead → Either Text Text"
                expectedNotes
                col
        Nothing -> assertFailure "notes column missing"

-- Default=NoSafeRead, single override to MaybeRead.
parseDefaultsNoSafeReadWithMaybeOverride :: Test
parseDefaultsNoSafeReadWithMaybeOverride = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList @T.Text ["1", "2", "3"])
                , ("y", DI.fromList @T.Text ["a", "", "c"])
                ]
        opts =
            D.defaultParseOptions
                { D.sampleSize = 3
                , D.parseSafe = D.NoSafeRead
                , D.parseSafeOverrides = [("y", D.MaybeRead)]
                }
        result = D.parseDefaults opts df
    -- 'x': NoSafeRead default → bare Int, no bitmap.
    case getColumn "x" result of
        Just (DI.UnboxedColumn Nothing _) -> pure ()
        Just col ->
            assertFailure $
                "'x' should be bare UnboxedColumn, got " <> show col
        Nothing -> assertFailure "x column missing"
    -- 'y': MaybeRead override → nullable Text with bitmap.
    case getColumn "y" result of
        Just (DI.BoxedColumn (Just _) _) -> pure ()
        Just col ->
            assertFailure $
                "'y' MaybeRead override should yield nullable column, got " <> show col
        Nothing -> assertFailure "y column missing"

-- Default=EitherRead, single override to NoSafeRead.
parseDefaultsEitherReadWithNoSafeOverride :: Test
parseDefaultsEitherReadWithNoSafeOverride = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("nums", DI.fromList @T.Text ["10", "20", "30"])
                , ("tags", DI.fromList @T.Text ["foo", "", "baz"])
                ]
        opts =
            D.defaultParseOptions
                { D.sampleSize = 3
                , D.parseSafe = D.EitherRead
                , D.parseSafeOverrides = [("nums", D.NoSafeRead)]
                }
        result = D.parseDefaults opts df
    -- 'nums': NoSafeRead override → bare Int.
    case getColumn "nums" result of
        Just (DI.UnboxedColumn Nothing _) -> pure ()
        Just col ->
            assertFailure $
                "'nums' NoSafeRead override should give bare UnboxedColumn, got "
                    <> show col
        Nothing -> assertFailure "nums column missing"
    -- 'tags': default EitherRead → Either Text Text.
    let expectedTags =
            DI.fromList @(Either T.Text T.Text) [Right "foo", Left "", Right "baz"]
    case getColumn "tags" result of
        Just col ->
            assertEqual "'tags' inherits EitherRead default" expectedTags col
        Nothing -> assertFailure "tags column missing"

-- Empty overrides: behaves identically to no overrides.
parseDefaultsEmptyOverrides :: Test
parseDefaultsEmptyOverrides = TestCase $ do
    let df =
            D.fromNamedColumns
                [("v", DI.fromList @T.Text ["1", "2", "3"])]
        opts1 = D.defaultParseOptions{D.sampleSize = 3}
        opts2 = opts1{D.parseSafeOverrides = []}
    assertEqual
        "empty parseSafeOverrides ≡ no overrides"
        (D.parseDefaults opts1 df)
        (D.parseDefaults opts2 df)

-- Override for non-existent column: silently ignored, no crash.
parseDefaultsNonExistentOverride :: Test
parseDefaultsNonExistentOverride = TestCase $ do
    let df =
            D.fromNamedColumns
                [("a", DI.fromList @T.Text ["1", "2", "3"])]
        opts =
            D.defaultParseOptions
                { D.sampleSize = 3
                , D.parseSafeOverrides = [("z", D.EitherRead)]
                }
        result = D.parseDefaults opts df
    -- 'a' should parse normally under the default (MaybeRead).
    case getColumn "a" result of
        Just (DI.UnboxedColumn (Just _) _) -> pure ()
        Just col ->
            assertFailure $
                "'a' should be nullable UnboxedColumn under MaybeRead default, got "
                    <> show col
        Nothing -> assertFailure "a column missing"

parseAllNullWithEitherRead :: Test
parseAllNullWithEitherRead =
    -- NoAssumption path: sample is all empty/nullish so inference can't
    -- pick a numeric type. EitherRead still wraps as Either Text Text.
    let beforeParse :: [T.Text]
        beforeParse = ["", "", "", "N/A", ""]
        afterParse :: [Either T.Text T.Text]
        afterParse = [Left "", Left "", Left "", Right "N/A", Left ""]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 5, D.parseSafe = D.EitherRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "EitherRead with all-null sample produces Either Text Text"
                expected
                actual
            )

-- 4. PARSING SHOULD NOT DEPEND ON THE NUMBER OF EXAMPLES.
parseBoolsWithOneExample :: Test
parseBoolsWithOneExample =
    let afterParse :: [Bool]
        afterParse = False : replicate 50 True
        beforeParse :: [T.Text]
        beforeParse = "false" : replicate 50 "true"
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 1}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as OptionalColumn of Maybe Bools with only one example"
                expected
                actual
            )

parseBoolsWithManyExamples :: Test
parseBoolsWithManyExamples =
    let afterParse :: [Bool]
        afterParse = False : replicate 50 True
        beforeParse :: [T.Text]
        beforeParse = "false" : replicate 50 "true"
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 49}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as OptionalColumn of Maybe Bools with only one example"
                expected
                actual
            )

parseIntsWithOneExample :: Test
parseIntsWithOneExample =
    let afterParse = intsExpected
        beforeParse = intsInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 1}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as OptionalColumn of Maybe Ints with only one example"
                expected
                actual
            )

parseIntsWithTwentyFiveExamples :: Test
parseIntsWithTwentyFiveExamples =
    let afterParse = intsExpected
        beforeParse = intsInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 25}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as OptionalColumn of Maybe Ints with some examples"
                expected
                actual
            )

parseIntsWithFortyNineExamples :: Test
parseIntsWithFortyNineExamples =
    let afterParse = intsExpected
        beforeParse = intsInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 49}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as OptionalColumn of Maybe Ints with many examples"
                expected
                actual
            )

parseDatesWithOneExample :: Test
parseDatesWithOneExample =
    let afterParse = datesExpected
        beforeParse = datesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 1}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as OptionalColumn of Maybe Days with only one example"
                expected
                actual
            )

parseDatesWithFifteenExamples :: Test
parseDatesWithFifteenExamples =
    let afterParse = datesExpected
        beforeParse = datesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 15}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as OptionalColumn of Maybe Days with many examples"
                expected
                actual
            )

parseIntsAndDoublesAsDoublesWithOneExample :: Test
parseIntsAndDoublesAsDoublesWithOneExample =
    let afterParse = intsAndDoublesExpected
        beforeParse = intsAndDoublesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 1}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as OptionalColumn of Maybe Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAsDoublesWithManyExamples :: Test
parseIntsAndDoublesAsDoublesWithManyExamples =
    let afterParse = intsAndDoublesExpected
        beforeParse = intsAndDoublesInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 50}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as OptionalColumn of Maybe Doubles with many examples"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff :: Test
parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff =
    -- Pattern: 10 empties, 50 ints (as int text), 50 doubles (as double text), 5 specials
    let beforeParse =
            replicate 10 ""
                ++ intsInput
                ++ (T.pack . show <$> ([1.0 .. 50.0] :: [Double]))
                ++ doublesSpecialInput
        afterParse :: [Maybe Double]
        afterParse =
            replicate 10 Nothing
                ++ map Just ([1.0 .. 50.0] :: [Double])
                ++ map Just ([1.0 .. 50.0] :: [Double])
                ++ map Just doublesSpecial
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 1, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff =
    let beforeParse =
            replicate 10 ""
                ++ intsInput
                ++ (T.pack . show <$> ([1.0 .. 50.0] :: [Double]))
                ++ doublesSpecialInput
        afterParse :: [Maybe Double]
        afterParse =
            replicate 10 Nothing
                ++ map Just ([1.0 .. 50.0] :: [Double])
                ++ map Just ([1.0 .. 50.0] :: [Double])
                ++ map Just doublesSpecial
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 30, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff =
    -- Pattern: 10 empties, 5x(NaN,N/A), 20 ints (1-20), 30 doubles (1.0-30.0), 5 specials
    let nullishPairs = concat (replicate 5 ["NaN", "N/A"])
        beforeParse =
            replicate 10 ""
                ++ nullishPairs
                ++ (T.pack . show <$> ([1 .. 20] :: [Int]))
                ++ (T.pack . show <$> ([1.0 .. 30.0] :: [Double]))
                ++ doublesSpecialInput
        afterParse :: [Maybe T.Text]
        afterParse =
            replicate 10 Nothing
                ++ map Just nullishPairs
                ++ map (Just . T.pack . show) ([1 .. 20] :: [Int])
                ++ map (Just . T.pack . show) ([1.0 .. 30.0] :: [Double])
                ++ map Just doublesSpecialInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 1, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints, Doubles, empty strings, nullish as OptionalColumn of Text with just one example, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff =
    let nullishPairs = concat (replicate 5 ["NaN", "N/A"])
        beforeParse =
            replicate 10 ""
                ++ nullishPairs
                ++ (T.pack . show <$> ([1 .. 20] :: [Int]))
                ++ (T.pack . show <$> ([1.0 .. 30.0] :: [Double]))
                ++ doublesSpecialInput
        afterParse :: [Maybe T.Text]
        afterParse =
            replicate 10 Nothing
                ++ map Just nullishPairs
                ++ map (Just . T.pack . show) ([1 .. 20] :: [Int])
                ++ map (Just . T.pack . show) ([1.0 .. 30.0] :: [Double])
                ++ map Just doublesSpecialInput
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 30, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector
                $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints, Doubles, empty strings, nullish as OptionalColumn of Text with many examples, when safeRead is off"
                expected
                actual
            )

-- 5. EDGE CASES THAT HAVE TO BE INTERPRETED CORRECTLY

parseManyNullishAndOneInt :: Test
parseManyNullishAndOneInt =
    let afterParse :: [Maybe Int]
        afterParse = replicate 100 Nothing ++ [Just 100000]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["100000"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndOneDouble :: Test
parseManyNullishAndOneDouble =
    let afterParse :: [Maybe Double]
        afterParse = replicate 100 Nothing ++ [Just 3.14]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["3.14"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndOneDate :: Test
parseManyNullishAndOneDate =
    let afterParse :: [Maybe Day]
        afterParse = replicate 100 Nothing ++ [Just $ fromGregorian 2024 12 25]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["2024-12-25"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndIncorrectDates :: Test
parseManyNullishAndIncorrectDates =
    let afterParse :: [Maybe T.Text]
        afterParse = replicate 100 Nothing ++ [Just "2024-12-25", Just "2024-12-w6"]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["2024-12-25", "2024-12-w6"]
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseRepeatedNullish :: Test
parseRepeatedNullish =
    let afterParse :: [Maybe T.Text]
        afterParse = replicate 100 Nothing
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN"
        expected = DI.fromVector $ V.fromList afterParse
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector $
                    V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

tests :: [Test]
tests =
    dimensionsTest
        ++ [
             -- 1. SIMPLE CASES
             TestLabel "parseBools" parseBools
           , TestLabel "parseInts" parseInts
           , TestLabel "parseDoubles" parseDoubles
           , TestLabel "parseDates" parseDates
           , TestLabel "parseTexts" parseTexts
           , -- 2. COMBINATION CASES
             TestLabel "parseBoolsAndIntsAsTexts" parseBoolsAndIntsAsTexts
           , TestLabel "parseIntsAndDoublesAsDoubles" parseIntsAndDoublesAsDoubles
           , TestLabel "parseIntsAndDatesAsTexts" parseIntsAndDatesAsTexts
           , TestLabel "parseTextsAndDoublesAsTexts" parseTextsAndDoublesAsTexts
           , TestLabel "parseDatesAndTextsAsTexts" parseDatesAndTextsAsTexts
           , -- 3A. PARSING WITH SAFEREAD OFF
             TestLabel "parseBoolsWithoutSafeRead" parseBoolsWithoutSafeRead
           , TestLabel "parseIntsWithoutSafeRead" parseIntsWithoutSafeRead
           , TestLabel "parseDoublesWithoutSafeRead" parseDoublesWithoutSafeRead
           , TestLabel "parseDatesWithoutSafeRead" parseDatesWithoutSafeRead
           , TestLabel "parseTextsWithoutSafeRead" parseTextsWithoutSafeRead
           , TestLabel
                "parseBoolsAndEmptyStringsWithoutSafeRead"
                parseBoolsAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseIntsAndEmptyStringsWithoutSafeRead"
                parseIntsAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsWithoutSafeRead"
                parseIntsAndDoublesAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseDatesAndEmptyStringsWithoutSafeRead"
                parseDatesAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseTextsAndEmptyStringsWithoutSafeRead"
                parseTextsAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseBoolsAndNullishStringsWithoutSafeRead"
                parseBoolsAndNullishStringsWithoutSafeRead
           , TestLabel
                "parseIntsAndNullishStringsWithoutSafeRead"
                parseIntsAndNullishStringsWithoutSafeRead
           , TestLabel
                "parseIntsAndDoublesAndNullishStringsWithoutSafeRead"
                parseIntsAndDoublesAndNullishStringsWithoutSafeRead
           , TestLabel
                "parseIntsAndNullishAndEmptyStringsWithoutSafeRead"
                parseIntsAndNullishAndEmptyStringsWithoutSafeRead
           , TestLabel
                "parseTextsAndEmptyAndNullishStringsWithoutSafeRead"
                parseTextsAndEmptyAndNullishStringsWithoutSafeRead
           , -- 3B. PARSING WITH SAFEREAD ON
             TestLabel
                "parseBoolsAndEmptyStringsWithSafeRead"
                parseBoolsAndEmptyStringsWithSafeRead
           , TestLabel
                "parseIntsAndEmptyStringsWithSafeRead"
                parseIntsAndEmptyStringsWithSafeRead
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsWithSafeRead"
                parseIntsAndDoublesAndEmptyStringsWithSafeRead
           , TestLabel
                "parseDatesAndEmptyStringsWithSafeRead"
                parseDatesAndEmptyStringsWithSafeRead
           , TestLabel
                "parseTextsAndEmptyStringsWithSafeRead"
                parseTextsAndEmptyStringsWithSafeRead
           , TestLabel
                "parseIntsAndNullishStringsWithSafeRead"
                parseIntsAndNullishStringsWithSafeRead
           , TestLabel
                "parseIntsAndDoublesAndNullishStringsWithSafeRead"
                parseIntsAndDoublesAndNullishStringsWithSafeRead
           , TestLabel
                "parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead"
                parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead
           , TestLabel
                "parseIntsAndNullishAndEmptyStringsWithSafeRead"
                parseIntsAndNullishAndEmptyStringsWithSafeRead
           , TestLabel
                "parseTextsAndEmptyAndNullishStringsWithSafeRead"
                parseTextsAndEmptyAndNullishStringsWithSafeRead
           , -- 3C. PARSING WITH EitherRead
             TestLabel "parseBoolsWithEitherRead" parseBoolsWithEitherRead
           , TestLabel "parseIntsWithEitherRead" parseIntsWithEitherRead
           , TestLabel "parseDoublesWithEitherRead" parseDoublesWithEitherRead
           , TestLabel "parseTextsWithEitherRead" parseTextsWithEitherRead
           , TestLabel "parseDatesWithEitherRead" parseDatesWithEitherRead
           , TestLabel "parseAllNullWithEitherRead" parseAllNullWithEitherRead
           , TestLabel
                "parseDefaultsPerColumnOverrides"
                parseDefaultsPerColumnOverrides
           , TestLabel
                "parseDefaultsNoSafeReadWithMaybeOverride"
                parseDefaultsNoSafeReadWithMaybeOverride
           , TestLabel
                "parseDefaultsEitherReadWithNoSafeOverride"
                parseDefaultsEitherReadWithNoSafeOverride
           , TestLabel
                "parseDefaultsEmptyOverrides"
                parseDefaultsEmptyOverrides
           , TestLabel
                "parseDefaultsNonExistentOverride"
                parseDefaultsNonExistentOverride
           , -- 4. PARSING MUST NOT DEPEND ON THE NUMBER OF EXAMPLES
             TestLabel "parseBoolsWithOneExample" parseBoolsWithOneExample
           , TestLabel "parseBoolsWithManyExamples" parseBoolsWithManyExamples
           , TestLabel "parseIntsWithOneExample" parseIntsWithOneExample
           , TestLabel "parseIntsWithTwentyFiveExamples" parseIntsWithTwentyFiveExamples
           , TestLabel "parseIntsWithFortyNineExamples" parseIntsWithFortyNineExamples
           , TestLabel "parseDatesWithOneExample" parseDatesWithOneExample
           , TestLabel "parseDatesWithFifteenExamples" parseDatesWithFifteenExamples
           , TestLabel
                "parseIntsAndDoublesAsDoublesWithOneExample"
                parseIntsAndDoublesAsDoublesWithOneExample
           , TestLabel
                "parseIntsAndDoublesAsDoublesWithManyExamples"
                parseIntsAndDoublesAsDoublesWithManyExamples
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff"
                parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff"
                parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff"
                parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff
           , TestLabel
                "parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff"
                parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff
           , -- 5. EDGE CASES THAT HAVE TO BE PARSED CORRECTLY
             TestLabel "parseManyNullishAndOneInt" parseManyNullishAndOneInt
           , TestLabel "parseManyNullishAndOneDouble" parseManyNullishAndOneDouble
           , TestLabel "parseRepeatedNullish" parseRepeatedNullish
           ]
