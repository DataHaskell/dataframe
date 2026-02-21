{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Operations.Typing as D
import qualified System.Exit as Exit

import Data.Time
import GenDataFrame ()
import Test.HUnit
import Test.QuickCheck

import qualified Functions
import qualified Monad
import qualified Operations.Aggregations
import qualified Operations.Apply
import qualified Operations.Core
import qualified Operations.Derive
import qualified Operations.Filter
import qualified Operations.GroupBy
import qualified Operations.InsertColumn
import qualified Operations.Join
import qualified Operations.Merge
import qualified Operations.ReadCsv
import qualified Operations.Sort
import qualified Operations.Statistics
import qualified Operations.Subset
import qualified Operations.Take
import qualified Parquet

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

-- PARSING TESTS
------- 1. SIMPLE CASES
parseBools :: Test
parseBools =
    let afterParse :: [Bool]
        afterParse = [True, True, True] ++ [False, False, False]
        beforeParse :: [T.Text]
        beforeParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as UnboxedColumn of Bools"
                expected
                actual
            )

parseInts :: Test
parseInts =
    let afterParse :: [Int]
        afterParse = [1 .. 50]
        beforeParse :: [T.Text]
        beforeParse = T.pack . show <$> [1 .. 50]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints"
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
                <$> [1.0 .. 50.0] ++ [3.14, 2.22, 8.55, 23.3, 12.22222235049450945049504950]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Doubles without missing values as UnboxedColumn of Doubles"
                expected
                actual
            )

parseDates :: Test
parseDates =
    let afterParse :: [Day]
        afterParse =
            [ fromGregorian 2020 02 12
            , fromGregorian 2020 02 13
            , fromGregorian 2020 02 14
            , fromGregorian 2020 02 15
            , fromGregorian 2020 02 16
            , fromGregorian 2020 02 17
            , fromGregorian 2020 02 18
            , fromGregorian 2020 02 19
            , fromGregorian 2020 02 20
            , fromGregorian 2020 02 21
            , fromGregorian 2020 02 22
            , fromGregorian 2020 02 23
            , fromGregorian 2020 02 24
            , fromGregorian 2020 02 25
            , fromGregorian 2020 02 26
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as BoxedColumn of Days"
                expected
                actual
            )

parseTexts :: Test
parseTexts =
    let afterParse :: [T.Text]
        afterParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , "Surrender now or prepare to fight!"
            , "Meowth, that's right!"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , "Surrender now or prepare to fight!"
            , "Meowth, that's right!"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Text without missing values as BoxedColumn of Text"
                expected
                actual
            )

--- 2. COMBINATION CASES
parseBoolsAndIntsAsTexts :: Test
parseBoolsAndIntsAsTexts =
    let afterParse :: [T.Text]
        afterParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"] ++ ["1", "0", "1"]
        beforeParse :: [T.Text]
        beforeParse = ["True", "true", "TRUE"] ++ ["False", "false", "FALSE"] ++ ["1", "0", "1"]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses mixture of Bools and Ints as Text"
                expected
                actual
            )

parseIntsAndDoublesAsDoubles :: Test
parseIntsAndDoublesAsDoubles =
    let afterParse :: [Double]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            , 1.0
            , 2.0
            , 3.0
            , 4.0
            , 5.0
            , 6.0
            , 7.0
            , 8.0
            , 9.0
            , 10.0
            , 11.0
            , 12.0
            , 13.0
            , 14.0
            , 15.0
            , 16.0
            , 17.0
            , 18.0
            , 19.0
            , 20.0
            , 21.0
            , 22.0
            , 23.0
            , 24.0
            , 25.0
            , 26.0
            , 27.0
            , 28.0
            , 29.0
            , 30.0
            , 31.0
            , 32.0
            , 33.0
            , 34.0
            , 35.0
            , 36.0
            , 37.0
            , 38.0
            , 39.0
            , 40.0
            , 41.0
            , 42.0
            , 43.0
            , 44.0
            , 45.0
            , 46.0
            , 47.0
            , 48.0
            , 49.0
            , 50.0
            , 3.14
            , 2.22
            , 8.55
            , 23.3
            , 12.22222235049450945049504950
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles"
                expected
                actual
            )

parseIntsAndDatesAsTexts :: Test
parseIntsAndDatesAsTexts =
    let afterParse :: [T.Text]
        afterParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Dates as BoxedColumn of Texts"
                expected
                actual
            )

parseTextsAndDoublesAsTexts :: Test
parseTextsAndDoublesAsTexts =
    let afterParse :: [T.Text]
        afterParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Texts and Doubles as BoxedColumn of Texts"
                expected
                actual
            )

parseDatesAndTextsAsTexts :: Test
parseDatesAndTextsAsTexts =
    let afterParse :: [T.Text]
        afterParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            , "Jessie"
            , "James"
            , "Meowth"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            , "Jessie"
            , "James"
            , "Meowth"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Dates and Texts as BoxedColumn of Texts"
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
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as UnboxedColumn of Bools, when safeRead is off"
                expected
                actual
            )

parseIntsWithoutSafeRead :: Test
parseIntsWithoutSafeRead =
    let afterParse :: [Int]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints, when safeRead is off"
                expected
                actual
            )

parseDoublesWithoutSafeRead :: Test
parseDoublesWithoutSafeRead =
    let afterParse :: [Double]
        afterParse =
            [ 1.0
            , 2.0
            , 3.0
            , 4.0
            , 5.0
            , 6.0
            , 7.0
            , 8.0
            , 9.0
            , 10.0
            , 11.0
            , 12.0
            , 13.0
            , 14.0
            , 15.0
            , 16.0
            , 17.0
            , 18.0
            , 19.0
            , 20.0
            , 21.0
            , 22.0
            , 23.0
            , 24.0
            , 25.0
            , 26.0
            , 27.0
            , 28.0
            , 29.0
            , 30.0
            , 31.0
            , 32.0
            , 33.0
            , 34.0
            , 35.0
            , 36.0
            , 37.0
            , 38.0
            , 39.0
            , 40.0
            , 41.0
            , 42.0
            , 43.0
            , 44.0
            , 45.0
            , 46.0
            , 47.0
            , 48.0
            , 49.0
            , 50.0
            , 3.14
            , 2.22
            , 8.55
            , 23.3
            , 12.22222235049450945049504950
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Doubles without missing values as UnboxedColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseDatesWithoutSafeRead :: Test
parseDatesWithoutSafeRead =
    let afterParse :: [Day]
        afterParse =
            [ fromGregorian 2020 02 12
            , fromGregorian 2020 02 13
            , fromGregorian 2020 02 14
            , fromGregorian 2020 02 15
            , fromGregorian 2020 02 16
            , fromGregorian 2020 02 17
            , fromGregorian 2020 02 18
            , fromGregorian 2020 02 19
            , fromGregorian 2020 02 20
            , fromGregorian 2020 02 21
            , fromGregorian 2020 02 22
            , fromGregorian 2020 02 23
            , fromGregorian 2020 02 24
            , fromGregorian 2020 02 25
            , fromGregorian 2020 02 26
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as BoxedColumn of Days"
                expected
                actual
            )

parseTextsWithoutSafeRead :: Test
parseTextsWithoutSafeRead =
    let afterParse :: [T.Text]
        afterParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , "Surrender now or prepare to fight!"
            , "Meowth, that's right!"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , "Surrender now or prepare to fight!"
            , "Meowth, that's right!"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Text without missing values as BoxedColumn of Text"
                expected
                actual
            )

parseBoolsAndEmptyStringsWithoutSafeRead :: Test
parseBoolsAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe Bool]
        afterParse = replicate 10 Nothing ++ replicate 10 (Just True) ++ replicate 10 (Just False)
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "" ++ replicate 10 "true" ++ replicate 10 "false"
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools and empty Strings as OptionalColumn of Bools, when safeRead is off"
                expected
                actual
            )

parseIntsAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe Int]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1
            , Just 2
            , Just 3
            , Just 4
            , Just 5
            , Just 6
            , Just 7
            , Just 8
            , Just 9
            , Just 10
            , Just 11
            , Just 12
            , Just 13
            , Just 14
            , Just 15
            , Just 16
            , Just 17
            , Just 18
            , Just 19
            , Just 20
            , Just 21
            , Just 22
            , Just 23
            , Just 24
            , Just 25
            , Just 26
            , Just 27
            , Just 28
            , Just 29
            , Just 30
            , Just 31
            , Just 32
            , Just 33
            , Just 34
            , Just 35
            , Just 36
            , Just 37
            , Just 38
            , Just 39
            , Just 40
            , Just 41
            , Just 42
            , Just 43
            , Just 44
            , Just 45
            , Just 46
            , Just 47
            , Just 48
            , Just 49
            , Just 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and empty strings as OptionalColumn of Ints, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndDoublesAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Nothing
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Nothing
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Nothing
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Nothing
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Nothing
            , Just 3.14
            , Just 2.22
            , Just 8.55
            , Just 23.3
            , Just 12.22222235049451
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , ""
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , ""
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , ""
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , ""
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , ""
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseDatesAndEmptyStringsWithoutSafeRead :: Test
parseDatesAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe Day]
        afterParse =
            [ Just $ fromGregorian 2020 02 12
            , Just $ fromGregorian 2020 02 13
            , Just $ fromGregorian 2020 02 14
            , Nothing
            , Just $ fromGregorian 2020 02 15
            , Just $ fromGregorian 2020 02 16
            , Just $ fromGregorian 2020 02 17
            , Nothing
            , Just $ fromGregorian 2020 02 18
            , Just $ fromGregorian 2020 02 19
            , Just $ fromGregorian 2020 02 20
            , Nothing
            , Just $ fromGregorian 2020 02 21
            , Just $ fromGregorian 2020 02 22
            , Just $ fromGregorian 2020 02 23
            , Nothing
            , Just $ fromGregorian 2020 02 24
            , Just $ fromGregorian 2020 02 25
            , Just $ fromGregorian 2020 02 26
            , Nothing
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , ""
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , ""
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , ""
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , ""
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            , ""
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates and Empty Strings as OptionalColumn of Dates, with safeRead off"
                expected
                actual
            )

parseTextsAndEmptyStringsWithoutSafeRead :: Test
parseTextsAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Just "To"
            , Just "protect"
            , Just "the"
            , Just "world"
            , Just "from"
            , Just "devastation"
            , Nothing
            , Just "To"
            , Just "unite"
            , Just "all"
            , Just "people"
            , Just "within"
            , Just "our"
            , Just "nation"
            , Nothing
            , Just "To"
            , Just "denounce"
            , Just "the"
            , Just "evils"
            , Just "of"
            , Just "truth"
            , Just "and"
            , Just "love"
            , Nothing
            , Just "To"
            , Just "extend"
            , Just "our"
            , Just "reach"
            , Just "to"
            , Just "the"
            , Just "stars"
            , Just "above"
            , Nothing
            , Just "JESSIE!"
            , Just "JAMES!"
            , Just "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , Nothing
            , Just "Surrender now or prepare to fight!"
            , Nothing
            , Just "Meowth, that's right!"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , ""
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , ""
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , ""
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , ""
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , ""
            , "Surrender now or prepare to fight!"
            , ""
            , "Meowth, that's right!"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
                expected
                actual
            )

parseBoolsAndNullishStringsWithoutSafeRead :: Test
parseBoolsAndNullishStringsWithoutSafeRead =
    let afterParse :: [T.Text]
        afterParse = replicate 10 "N/A" ++ replicate 10 "True" ++ replicate 10 "False"
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "N/A" ++ replicate 10 "True" ++ replicate 10 "False"
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools with nullish values as BoxedColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishStringsWithoutSafeRead :: Test
parseIntsAndNullishStringsWithoutSafeRead =
    let afterParse :: [T.Text]
        afterParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values as BoxedColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndNullishStringsWithoutSafeRead :: Test
parseIntsAndDoublesAndNullishStringsWithoutSafeRead =
    let afterParse :: [T.Text]
        afterParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "Nothing"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "N/A"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "NULL"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "null"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "NAN"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "Nothing"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "N/A"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "NULL"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "null"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "NAN"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishAndEmptyStringsWithoutSafeRead :: Test
parseIntsAndNullishAndEmptyStringsWithoutSafeRead =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Just "N/A"
            , Just "N/A"
            , Just "N/A"
            , Just "N/A"
            , Just "N/A"
            , Nothing
            , Just "1"
            , Just "2"
            , Just "3"
            , Just "4"
            , Just "5"
            , Just "6"
            , Just "7"
            , Just "8"
            , Just "9"
            , Just "10"
            , Nothing
            , Just "11"
            , Just "12"
            , Just "13"
            , Just "14"
            , Just "15"
            , Just "16"
            , Just "17"
            , Just "18"
            , Just "19"
            , Just "20"
            , Nothing
            , Just "21"
            , Just "22"
            , Just "23"
            , Just "24"
            , Just "25"
            , Just "26"
            , Just "27"
            , Just "28"
            , Just "29"
            , Just "30"
            , Nothing
            , Just "31"
            , Just "32"
            , Just "33"
            , Just "34"
            , Just "35"
            , Just "36"
            , Just "37"
            , Just "38"
            , Just "39"
            , Just "40"
            , Nothing
            , Just "41"
            , Just "42"
            , Just "43"
            , Just "44"
            , Just "45"
            , Just "46"
            , Just "47"
            , Just "48"
            , Just "49"
            , Just "50"
            , Nothing
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , ""
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , ""
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , ""
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , ""
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , ""
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values AND empty strings as OptionalColumn of Texts, when safeRead is off"
                expected
                actual
            )

parseTextsAndEmptyAndNullishStringsWithoutSafeRead :: Test
parseTextsAndEmptyAndNullishStringsWithoutSafeRead =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Just "To"
            , Just "protect"
            , Just "the"
            , Just "world"
            , Just "from"
            , Just "devastation"
            , Nothing
            , Just "To"
            , Just "unite"
            , Just "all"
            , Just "people"
            , Just "within"
            , Just "our"
            , Just "nation"
            , Nothing
            , Just "To"
            , Just "denounce"
            , Just "the"
            , Just "evils"
            , Just "of"
            , Just "truth"
            , Just "and"
            , Just "love"
            , Nothing
            , Just "To"
            , Just "extend"
            , Just "our"
            , Just "reach"
            , Just "to"
            , Just "the"
            , Just "stars"
            , Just "above"
            , Nothing
            , Just "JESSIE!"
            , Just "JAMES!"
            , Just "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , Nothing
            , Just "Surrender now or prepare to fight!"
            , Nothing
            , Just "Meowth, that's right!"
            , Just "NaN"
            , Just "Nothing"
            , Just "N/A"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , ""
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , ""
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , ""
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , ""
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , ""
            , "Surrender now or prepare to fight!"
            , ""
            , "Meowth, that's right!"
            , "NaN"
            , "Nothing"
            , "N/A"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 10 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
                expected
                actual
            )

-- 3B. PARSING WITH SAFEREAD ON
parseBoolsAndEmptyStringsWithSafeRead :: Test
parseBoolsAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Bool]
        afterParse = replicate 10 Nothing ++ replicate 10 (Just True)
        beforeParse :: [T.Text]
        beforeParse = replicate 10 "" ++ replicate 10 "true"
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools and empty strings as OptionalColumn of Bools, when safeRead is on"
                expected
                actual
            )

parseIntsAndEmptyStringsWithSafeRead :: Test
parseIntsAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Int]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1
            , Just 2
            , Just 3
            , Just 4
            , Just 5
            , Just 6
            , Just 7
            , Just 8
            , Just 9
            , Just 10
            , Just 11
            , Just 12
            , Just 13
            , Just 14
            , Just 15
            , Just 16
            , Just 17
            , Just 18
            , Just 19
            , Just 20
            , Just 21
            , Just 22
            , Just 23
            , Just 24
            , Just 25
            , Just 26
            , Just 27
            , Just 28
            , Just 29
            , Just 30
            , Just 31
            , Just 32
            , Just 33
            , Just 34
            , Just 35
            , Just 36
            , Just 37
            , Just 38
            , Just 39
            , Just 40
            , Just 41
            , Just 42
            , Just 43
            , Just 44
            , Just 45
            , Just 46
            , Just 47
            , Just 48
            , Just 49
            , Just 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and empty strings as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsWithSafeRead :: Test
parseIntsAndDoublesAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Nothing
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Nothing
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Nothing
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Nothing
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Nothing
            , Just 3.14
            , Just 2.22
            , Just 8.55
            , Just 23.3
            , Just 12.22222235049451
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , ""
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , ""
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , ""
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , ""
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , ""
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is on"
                expected
                actual
            )

parseDatesAndEmptyStringsWithSafeRead :: Test
parseDatesAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Day]
        afterParse =
            [ Just $ fromGregorian 2020 02 12
            , Just $ fromGregorian 2020 02 13
            , Just $ fromGregorian 2020 02 14
            , Nothing
            , Just $ fromGregorian 2020 02 15
            , Just $ fromGregorian 2020 02 16
            , Just $ fromGregorian 2020 02 17
            , Nothing
            , Just $ fromGregorian 2020 02 18
            , Just $ fromGregorian 2020 02 19
            , Just $ fromGregorian 2020 02 20
            , Nothing
            , Just $ fromGregorian 2020 02 21
            , Just $ fromGregorian 2020 02 22
            , Just $ fromGregorian 2020 02 23
            , Nothing
            , Just $ fromGregorian 2020 02 24
            , Just $ fromGregorian 2020 02 25
            , Just $ fromGregorian 2020 02 26
            , Nothing
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , ""
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , ""
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , ""
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , ""
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            , ""
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates and Empty Strings as OptionalColumn of Dates, with safeRead on"
                expected
                actual
            )

parseTextsAndEmptyStringsWithSafeRead :: Test
parseTextsAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Just "To"
            , Just "protect"
            , Just "the"
            , Just "world"
            , Just "from"
            , Just "devastation"
            , Nothing
            , Just "To"
            , Just "unite"
            , Just "all"
            , Just "people"
            , Just "within"
            , Just "our"
            , Just "nation"
            , Nothing
            , Just "To"
            , Just "denounce"
            , Just "the"
            , Just "evils"
            , Just "of"
            , Just "truth"
            , Just "and"
            , Just "love"
            , Nothing
            , Just "To"
            , Just "extend"
            , Just "our"
            , Just "reach"
            , Just "to"
            , Just "the"
            , Just "stars"
            , Just "above"
            , Nothing
            , Just "JESSIE!"
            , Just "JAMES!"
            , Just "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , Nothing
            , Just "Surrender now or prepare to fight!"
            , Nothing
            , Just "Meowth, that's right!"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , ""
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , ""
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , ""
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , ""
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , ""
            , "Surrender now or prepare to fight!"
            , ""
            , "Meowth, that's right!"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead on"
                expected
                actual
            )

parseIntsAndNullishStringsWithSafeRead :: Test
parseIntsAndNullishStringsWithSafeRead =
    let afterParse :: [Maybe Int]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1
            , Just 2
            , Just 3
            , Just 4
            , Just 5
            , Just 6
            , Just 7
            , Just 8
            , Just 9
            , Just 10
            , Just 11
            , Just 12
            , Just 13
            , Just 14
            , Just 15
            , Just 16
            , Just 17
            , Just 18
            , Just 19
            , Just 20
            , Just 21
            , Just 22
            , Just 23
            , Just 24
            , Just 25
            , Just 26
            , Just 27
            , Just 28
            , Just 29
            , Just 30
            , Just 31
            , Just 32
            , Just 33
            , Just 34
            , Just 35
            , Just 36
            , Just 37
            , Just 38
            , Just 39
            , Just 40
            , Just 41
            , Just 42
            , Just 43
            , Just 44
            , Just 45
            , Just 46
            , Just 47
            , Just 48
            , Just 49
            , Just 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndNullishStringsWithSafeRead :: Test
parseIntsAndDoublesAndNullishStringsWithSafeRead =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Nothing
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Nothing
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Nothing
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Nothing
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Nothing
            , Just 3.14
            , Just 2.22
            , Just 8.55
            , Just 23.3
            , Just 12.03
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "Nothing"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "N/A"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "NULL"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "null"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "NAN"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.03"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses combination of Ints, Doubles and empty strings as OptionalColumn of Doubles, when safeRead is off"
                expected
                actual
            )

parseIntsAndNullishAndEmptyStringsWithSafeRead :: Test
parseIntsAndNullishAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Int]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1
            , Just 2
            , Just 3
            , Just 4
            , Just 5
            , Just 6
            , Just 7
            , Just 8
            , Just 9
            , Just 10
            , Nothing
            , Just 11
            , Just 12
            , Just 13
            , Just 14
            , Just 15
            , Just 16
            , Just 17
            , Just 18
            , Just 19
            , Just 20
            , Nothing
            , Just 21
            , Just 22
            , Just 23
            , Just 24
            , Just 25
            , Just 26
            , Just 27
            , Just 28
            , Just 29
            , Just 30
            , Nothing
            , Just 31
            , Just 32
            , Just 33
            , Just 34
            , Just 35
            , Just 36
            , Just 37
            , Just 38
            , Just 39
            , Just 40
            , Nothing
            , Just 41
            , Just 42
            , Just 43
            , Just 44
            , Just 45
            , Just 46
            , Just 47
            , Just 48
            , Just 49
            , Just 50
            , Nothing
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "Nothing"
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , ""
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , ""
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , ""
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , ""
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , ""
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints with nullish values AND empty strings as OptionalColumn of Ints, when safeRead is on"
                expected
                actual
            )

parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead :: Test
parseIntsAndDoublesAndNullishAndEmptyStringsWithSafeRead =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1
            , Just 2
            , Just 3
            , Just 4
            , Just 5
            , Just 6
            , Just 7
            , Just 8
            , Just 9
            , Just 10
            , Nothing
            , Just 11
            , Just 12
            , Just 13
            , Just 14
            , Just 15
            , Just 16
            , Just 17
            , Just 18
            , Just 19
            , Just 20
            , Nothing
            , Just 21
            , Just 22
            , Just 23
            , Just 24
            , Just 25
            , Just 26
            , Just 27
            , Just 28
            , Just 29
            , Just 30
            , Nothing
            , Just 3.14
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "N/A"
            , "N/A"
            , "N/A"
            , "N/A"
            , "Nothing"
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , ""
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , ""
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , ""
            , "3.14"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints and Doubles with nullish values AND empty strings as OptionalColumn of Doubles, when safeRead is on"
                expected
                actual
            )

parseTextsAndEmptyAndNullishStringsWithSafeRead :: Test
parseTextsAndEmptyAndNullishStringsWithSafeRead =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Just "To"
            , Just "protect"
            , Just "the"
            , Just "world"
            , Just "from"
            , Just "devastation"
            , Nothing
            , Just "To"
            , Just "unite"
            , Just "all"
            , Just "people"
            , Just "within"
            , Just "our"
            , Just "nation"
            , Nothing
            , Just "To"
            , Just "denounce"
            , Just "the"
            , Just "evils"
            , Just "of"
            , Just "truth"
            , Just "and"
            , Just "love"
            , Nothing
            , Just "To"
            , Just "extend"
            , Just "our"
            , Just "reach"
            , Just "to"
            , Just "the"
            , Just "stars"
            , Just "above"
            , Nothing
            , Just "JESSIE!"
            , Just "JAMES!"
            , Just "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , Nothing
            , Just "Surrender now or prepare to fight!"
            , Nothing
            , Just "Meowth, that's right!"
            , Nothing
            , Nothing
            , Nothing
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , "To"
            , "protect"
            , "the"
            , "world"
            , "from"
            , "devastation"
            , ""
            , "To"
            , "unite"
            , "all"
            , "people"
            , "within"
            , "our"
            , "nation"
            , ""
            , "To"
            , "denounce"
            , "the"
            , "evils"
            , "of"
            , "truth"
            , "and"
            , "love"
            , ""
            , "To"
            , "extend"
            , "our"
            , "reach"
            , "to"
            , "the"
            , "stars"
            , "above"
            , ""
            , "JESSIE!"
            , "JAMES!"
            , "TEAM ROCKET BLASTS OFF AT THE SPEED OF LIGHT!"
            , ""
            , "Surrender now or prepare to fight!"
            , ""
            , "Meowth, that's right!"
            , "NaN"
            , "Nothing"
            , "N/A"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Texts and Empty Strings as OptionalColumn of Text, with safeRead off"
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
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 1 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as UnboxedColumn of Ints with only one example"
                expected
                actual
            )

parseBoolsWithManyExamples :: Test
parseBoolsWithManyExamples =
    let afterParse :: [Bool]
        afterParse = False : replicate 50 True
        beforeParse :: [T.Text]
        beforeParse = "false" : replicate 50 "true"
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 49 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Bools without missing values as UnboxedColumn of Ints with only one example"
                expected
                actual
            )

parseIntsWithOneExample :: Test
parseIntsWithOneExample =
    let afterParse :: [Int]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 1 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints with only one example"
                expected
                actual
            )

parseIntsWithTwentyFiveExamples :: Test
parseIntsWithTwentyFiveExamples =
    let afterParse :: [Int]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 25 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints with some examples"
                expected
                actual
            )

parseIntsWithFortyNineExamples :: Test
parseIntsWithFortyNineExamples =
    let afterParse :: [Int]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 49 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Ints without missing values as UnboxedColumn of Ints with many examples"
                expected
                actual
            )

parseDatesWithOneExample :: Test
parseDatesWithOneExample =
    let afterParse :: [Day]
        afterParse =
            [ fromGregorian 2020 02 12
            , fromGregorian 2020 02 13
            , fromGregorian 2020 02 14
            , fromGregorian 2020 02 15
            , fromGregorian 2020 02 16
            , fromGregorian 2020 02 17
            , fromGregorian 2020 02 18
            , fromGregorian 2020 02 19
            , fromGregorian 2020 02 20
            , fromGregorian 2020 02 21
            , fromGregorian 2020 02 22
            , fromGregorian 2020 02 23
            , fromGregorian 2020 02 24
            , fromGregorian 2020 02 25
            , fromGregorian 2020 02 26
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 1 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as BoxedColumn of Days with only one example"
                expected
                actual
            )

parseDatesWithFifteenExamples :: Test
parseDatesWithFifteenExamples =
    let afterParse :: [Day]
        afterParse =
            [ fromGregorian 2020 02 12
            , fromGregorian 2020 02 13
            , fromGregorian 2020 02 14
            , fromGregorian 2020 02 15
            , fromGregorian 2020 02 16
            , fromGregorian 2020 02 17
            , fromGregorian 2020 02 18
            , fromGregorian 2020 02 19
            , fromGregorian 2020 02 20
            , fromGregorian 2020 02 21
            , fromGregorian 2020 02 22
            , fromGregorian 2020 02 23
            , fromGregorian 2020 02 24
            , fromGregorian 2020 02 25
            , fromGregorian 2020 02 26
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "2020-02-12"
            , "2020-02-13"
            , "2020-02-14"
            , "2020-02-15"
            , "2020-02-16"
            , "2020-02-17"
            , "2020-02-18"
            , "2020-02-19"
            , "2020-02-20"
            , "2020-02-21"
            , "2020-02-22"
            , "2020-02-23"
            , "2020-02-24"
            , "2020-02-25"
            , "2020-02-26"
            ]
        expected = DI.BoxedColumn $ V.fromList afterParse
        actual = D.parseDefault [] 15 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses Dates without missing values as BoxedColumn of Days with many examples"
                expected
                actual
            )

parseIntsAndDoublesAsDoublesWithOneExample :: Test
parseIntsAndDoublesAsDoublesWithOneExample =
    let afterParse :: [Double]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            , 1.0
            , 2.0
            , 3.0
            , 4.0
            , 5.0
            , 6.0
            , 7.0
            , 8.0
            , 9.0
            , 10.0
            , 11.0
            , 12.0
            , 13.0
            , 14.0
            , 15.0
            , 16.0
            , 17.0
            , 18.0
            , 19.0
            , 20.0
            , 21.0
            , 22.0
            , 23.0
            , 24.0
            , 25.0
            , 26.0
            , 27.0
            , 28.0
            , 29.0
            , 30.0
            , 31.0
            , 32.0
            , 33.0
            , 34.0
            , 35.0
            , 36.0
            , 37.0
            , 38.0
            , 39.0
            , 40.0
            , 41.0
            , 42.0
            , 43.0
            , 44.0
            , 45.0
            , 46.0
            , 47.0
            , 48.0
            , 49.0
            , 50.0
            , 3.14
            , 2.22
            , 8.55
            , 23.3
            , 12.22222235049450945049504950
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 1 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAsDoublesWithManyExamples :: Test
parseIntsAndDoublesAsDoublesWithManyExamples =
    let afterParse :: [Double]
        afterParse =
            [ 1
            , 2
            , 3
            , 4
            , 5
            , 6
            , 7
            , 8
            , 9
            , 10
            , 11
            , 12
            , 13
            , 14
            , 15
            , 16
            , 17
            , 18
            , 19
            , 20
            , 21
            , 22
            , 23
            , 24
            , 25
            , 26
            , 27
            , 28
            , 29
            , 30
            , 31
            , 32
            , 33
            , 34
            , 35
            , 36
            , 37
            , 38
            , 39
            , 40
            , 41
            , 42
            , 43
            , 44
            , 45
            , 46
            , 47
            , 48
            , 49
            , 50
            , 1.0
            , 2.0
            , 3.0
            , 4.0
            , 5.0
            , 6.0
            , 7.0
            , 8.0
            , 9.0
            , 10.0
            , 11.0
            , 12.0
            , 13.0
            , 14.0
            , 15.0
            , 16.0
            , 17.0
            , 18.0
            , 19.0
            , 20.0
            , 21.0
            , 22.0
            , 23.0
            , 24.0
            , 25.0
            , 26.0
            , 27.0
            , 28.0
            , 29.0
            , 30.0
            , 31.0
            , 32.0
            , 33.0
            , 34.0
            , 35.0
            , 36.0
            , 37.0
            , 38.0
            , 39.0
            , 40.0
            , 41.0
            , 42.0
            , 43.0
            , 44.0
            , 45.0
            , 46.0
            , 47.0
            , 48.0
            , 49.0
            , 50.0
            , 3.14
            , 2.22
            , 8.55
            , 23.3
            , 12.22222235049450945049504950
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.UnboxedColumn $ VU.fromList afterParse
        actual = D.parseDefault [] 50 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff :: Test
parseIntsAndDoublesAndEmptyStringsAsDoublesWithOneExampleWithSafeReadOff =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Just 3.14
            , Just 2.22
            , Just 8.55
            , Just 23.3
            , Just 12.22222235049451
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 1 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAsDoublesWithManyExamplesWithSafeReadOff =
    let afterParse :: [Maybe Double]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Just 1.0
            , Just 2.0
            , Just 3.0
            , Just 4.0
            , Just 5.0
            , Just 6.0
            , Just 7.0
            , Just 8.0
            , Just 9.0
            , Just 10.0
            , Just 11.0
            , Just 12.0
            , Just 13.0
            , Just 14.0
            , Just 15.0
            , Just 16.0
            , Just 17.0
            , Just 18.0
            , Just 19.0
            , Just 20.0
            , Just 21.0
            , Just 22.0
            , Just 23.0
            , Just 24.0
            , Just 25.0
            , Just 26.0
            , Just 27.0
            , Just 28.0
            , Just 29.0
            , Just 30.0
            , Just 31.0
            , Just 32.0
            , Just 33.0
            , Just 34.0
            , Just 35.0
            , Just 36.0
            , Just 37.0
            , Just 38.0
            , Just 39.0
            , Just 40.0
            , Just 41.0
            , Just 42.0
            , Just 43.0
            , Just 44.0
            , Just 45.0
            , Just 46.0
            , Just 47.0
            , Just 48.0
            , Just 49.0
            , Just 50.0
            , Just 3.14
            , Just 2.22
            , Just 8.55
            , Just 23.3
            , Just 12.22222235049451
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "21"
            , "22"
            , "23"
            , "24"
            , "25"
            , "26"
            , "27"
            , "28"
            , "29"
            , "30"
            , "31"
            , "32"
            , "33"
            , "34"
            , "35"
            , "36"
            , "37"
            , "38"
            , "39"
            , "40"
            , "41"
            , "42"
            , "43"
            , "44"
            , "45"
            , "46"
            , "47"
            , "48"
            , "49"
            , "50"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "31.0"
            , "32.0"
            , "33.0"
            , "34.0"
            , "35.0"
            , "36.0"
            , "37.0"
            , "38.0"
            , "39.0"
            , "40.0"
            , "41.0"
            , "42.0"
            , "43.0"
            , "44.0"
            , "45.0"
            , "46.0"
            , "47.0"
            , "48.0"
            , "49.0"
            , "50.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 30 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints and Doubles as UnboxedColumn of Doubles with just one example"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithOneExampleWithSafeReadOff =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "1"
            , Just "2"
            , Just "3"
            , Just "4"
            , Just "5"
            , Just "6"
            , Just "7"
            , Just "8"
            , Just "9"
            , Just "10"
            , Just "11"
            , Just "12"
            , Just "13"
            , Just "14"
            , Just "15"
            , Just "16"
            , Just "17"
            , Just "18"
            , Just "19"
            , Just "20"
            , Just "1.0"
            , Just "2.0"
            , Just "3.0"
            , Just "4.0"
            , Just "5.0"
            , Just "6.0"
            , Just "7.0"
            , Just "8.0"
            , Just "9.0"
            , Just "10.0"
            , Just "11.0"
            , Just "12.0"
            , Just "13.0"
            , Just "14.0"
            , Just "15.0"
            , Just "16.0"
            , Just "17.0"
            , Just "18.0"
            , Just "19.0"
            , Just "20.0"
            , Just "21.0"
            , Just "22.0"
            , Just "23.0"
            , Just "24.0"
            , Just "25.0"
            , Just "26.0"
            , Just "27.0"
            , Just "28.0"
            , Just "29.0"
            , Just "30.0"
            , Just "3.14"
            , Just "2.22"
            , Just "8.55"
            , Just "23.3"
            , Just "12.22222235049451"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 1 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            ( assertEqual
                "Correctly parses a mixture of Ints, Doubles, empty strings, nullish as OptionalColumn of Text with just one example, when safeRead is off"
                expected
                actual
            )

parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff ::
    Test
parseIntsAndDoublesAndEmptyStringsAndNullishAsStringssWithManyExamplesWithSafeReadOff =
    let afterParse :: [Maybe T.Text]
        afterParse =
            [ Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Nothing
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "NaN"
            , Just "N/A"
            , Just "1"
            , Just "2"
            , Just "3"
            , Just "4"
            , Just "5"
            , Just "6"
            , Just "7"
            , Just "8"
            , Just "9"
            , Just "10"
            , Just "11"
            , Just "12"
            , Just "13"
            , Just "14"
            , Just "15"
            , Just "16"
            , Just "17"
            , Just "18"
            , Just "19"
            , Just "20"
            , Just "1.0"
            , Just "2.0"
            , Just "3.0"
            , Just "4.0"
            , Just "5.0"
            , Just "6.0"
            , Just "7.0"
            , Just "8.0"
            , Just "9.0"
            , Just "10.0"
            , Just "11.0"
            , Just "12.0"
            , Just "13.0"
            , Just "14.0"
            , Just "15.0"
            , Just "16.0"
            , Just "17.0"
            , Just "18.0"
            , Just "19.0"
            , Just "20.0"
            , Just "21.0"
            , Just "22.0"
            , Just "23.0"
            , Just "24.0"
            , Just "25.0"
            , Just "26.0"
            , Just "27.0"
            , Just "28.0"
            , Just "29.0"
            , Just "30.0"
            , Just "3.14"
            , Just "2.22"
            , Just "8.55"
            , Just "23.3"
            , Just "12.22222235049451"
            ]
        beforeParse :: [T.Text]
        beforeParse =
            [ ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , ""
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "NaN"
            , "N/A"
            , "1"
            , "2"
            , "3"
            , "4"
            , "5"
            , "6"
            , "7"
            , "8"
            , "9"
            , "10"
            , "11"
            , "12"
            , "13"
            , "14"
            , "15"
            , "16"
            , "17"
            , "18"
            , "19"
            , "20"
            , "1.0"
            , "2.0"
            , "3.0"
            , "4.0"
            , "5.0"
            , "6.0"
            , "7.0"
            , "8.0"
            , "9.0"
            , "10.0"
            , "11.0"
            , "12.0"
            , "13.0"
            , "14.0"
            , "15.0"
            , "16.0"
            , "17.0"
            , "18.0"
            , "19.0"
            , "20.0"
            , "21.0"
            , "22.0"
            , "23.0"
            , "24.0"
            , "25.0"
            , "26.0"
            , "27.0"
            , "28.0"
            , "29.0"
            , "30.0"
            , "3.14"
            , "2.22"
            , "8.55"
            , "23.3"
            , "12.22222235049451"
            ]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual =
            D.parseDefault [] 30 False "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
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
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndOneDouble :: Test
parseManyNullishAndOneDouble =
    let afterParse :: [Maybe Double]
        afterParse = replicate 100 Nothing ++ [Just 3.14]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["3.14"]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndOneDate :: Test
parseManyNullishAndOneDate =
    let afterParse :: [Maybe Day]
        afterParse = replicate 100 Nothing ++ [Just $ fromGregorian 2024 12 25]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["2024-12-25"]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseManyNullishAndIncorrectDates :: Test
parseManyNullishAndIncorrectDates =
    let afterParse :: [Maybe T.Text]
        afterParse = replicate 100 Nothing ++ [Just "2024-12-25", Just "2024-12-w6"]
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN" ++ ["2024-12-25", "2024-12-w6"]
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseRepeatedNullish :: Test
parseRepeatedNullish =
    let afterParse :: [Maybe T.Text]
        afterParse = replicate 100 Nothing
        beforeParse :: [T.Text]
        beforeParse = replicate 100 "NaN"
        expected = DI.OptionalColumn $ V.fromList afterParse
        actual = D.parseDefault [] 10 True "%Y-%m-%d" $ DI.fromVector $ V.fromList beforeParse
     in TestCase
            (assertEqual "Correctly parses many Nulls followed by one Int" expected actual)

parseTests :: [Test]
parseTests =
    [ -- 1. SIMPLE CASES
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

tests :: Test
tests =
    TestList $
        dimensionsTest
            ++ Operations.Aggregations.tests
            ++ Operations.Apply.tests
            ++ Operations.Core.tests
            ++ Operations.Derive.tests
            ++ Operations.Filter.tests
            ++ Operations.GroupBy.tests
            ++ Operations.InsertColumn.tests
            ++ Operations.Join.tests
            ++ Operations.Merge.tests
            ++ Operations.ReadCsv.tests
            ++ Operations.Sort.tests
            ++ Operations.Statistics.tests
            ++ Operations.Take.tests
            ++ Functions.tests
            ++ Parquet.tests
            ++ parseTests

isSuccessful :: Result -> Bool
isSuccessful (Success{..}) = True
isSuccessful _ = False

main :: IO ()
main = do
    result <- runTestTT tests
    -- Property tests
    propRes <-
        mapM
            (quickCheckWithResult stdArgs)
            Operations.Subset.tests
    monadRes <- mapM (quickCheckWithResult stdArgs) Monad.tests
    if failures result > 0
        || errors result > 0
        || not (all isSuccessful propRes)
        || not (all isSuccessful monadRes)
        then Exit.exitFailure
        else Exit.exitSuccess
