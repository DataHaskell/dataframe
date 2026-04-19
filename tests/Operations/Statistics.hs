{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Statistics where

import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.Statistics as D

import Assertions
import Test.HUnit

medianOfOddLengthDataSet :: Test
medianOfOddLengthDataSet =
    TestCase
        ( assertEqual
            "Median of an odd length data set"
            (D.median' (VU.fromList @Double [179.94, 231.94, 839.06, 534.23, 248.94]))
            248.94
        )

medianOfEvenLengthDataSet :: Test
medianOfEvenLengthDataSet =
    TestCase
        ( assertEqual
            "Median of an even length data set"
            (D.median' (VU.fromList @Double [179.94, 231.94, 839.06, 534.23, 248.94, 276.37]))
            262.655
        )

medianOfEmptyDataSet :: Test
medianOfEmptyDataSet =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.emptyDataSetError "median")
            (print $ D.median' (VU.fromList @Double []))
        )

skewnessOfDataSetWithSameElements :: Test
skewnessOfDataSetWithSameElements =
    TestCase
        ( assertBool
            "Skewness of a data set with the same elements"
            (isNaN (D.skewness' @Double (VU.fromList $ replicate 10 42.0)))
        )

skewnessOfSymmetricDataSet :: Test
skewnessOfSymmetricDataSet =
    TestCase
        ( assertEqual
            "Skewness of a symmetric data set"
            (D.skewness' (VU.fromList [-3.0 :: Double, -2.0, -1.5, 0, 1.5, 2.0, 3.0]))
            0
        )

skewnessOfSimpleDataSet :: Test
skewnessOfSimpleDataSet =
    TestCase
        ( assertBool
            "Skewness of a simple data set"
            ( abs
                ( D.skewness' (VU.fromList [25 :: Int, 28, 26, 30, 40, 50, 40])
                    - 0.566_731_633_676
                )
                < 1e-12
            )
        )

skewnessOfEmptyDataSet :: Test
skewnessOfEmptyDataSet =
    TestCase
        ( assertEqual
            "Skewness of an empty data set"
            (D.skewness' @Double (VU.fromList []))
            0
        )

twoQuantileOfOddLengthDataSet :: Test
twoQuantileOfOddLengthDataSet =
    TestCase
        ( assertEqual
            "2-quantile of an odd length data set"
            ( D.quantiles'
                (VU.fromList [0, 1, 2])
                2
                (VU.fromList [179.94 :: Double, 231.94, 839.06, 534.23, 248.94])
            )
            (VU.fromList [179.94, 248.94, 839.06])
        )

twoQuantileOfEvenLengthDataSet :: Test
twoQuantileOfEvenLengthDataSet =
    TestCase
        ( assertEqual
            "2-quantile of an even length data set"
            ( D.quantiles'
                (VU.fromList [0, 1, 2])
                2
                (VU.fromList [179.94 :: Double, 231.94, 839.06, 534.23, 248.94, 276.37])
            )
            (VU.fromList [179.94, 262.655, 839.06])
        )

quartilesOfOddLengthDataSet :: Test
quartilesOfOddLengthDataSet =
    TestCase
        ( assertEqual
            "Quartiles of an odd length data set"
            ( D.quantiles'
                (VU.fromList [0, 1, 2, 3, 4])
                4
                (VU.fromList [3 :: Int, 6, 7, 8, 8, 9, 10, 13, 15, 16, 20])
            )
            (VU.fromList [3, 7.5, 9, 14, 20])
        )

quartilesOfEvenLengthDataSet :: Test
quartilesOfEvenLengthDataSet =
    TestCase
        ( assertEqual
            "Quartiles of an even length data set"
            ( D.quantiles'
                (VU.fromList [0, 1, 2, 3, 4])
                4
                (VU.fromList [3 :: Int, 6, 7, 8, 8, 10, 13, 15, 16, 20])
            )
            (VU.fromList [3, 7.25, 9, 14.5, 20])
        )

deciles :: Test
deciles =
    TestCase
        ( assertEqual
            "Deciles"
            ( D.quantiles'
                (VU.fromList [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
                10
                (VU.fromList [4 :: Int, 7, 3, 1, 11, 6, 2, 9, 8, 10, 5])
            )
            (VU.fromList [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
        )

interQuartileRangeOfOddLengthDataSet :: Test
interQuartileRangeOfOddLengthDataSet =
    TestCase
        ( assertEqual
            "Inter quartile range of an odd length data set"
            ( D.interQuartileRange'
                (VU.fromList [3 :: Int, 6, 7, 8, 8, 9, 10, 13, 15, 16, 20])
            )
            6.5
        )

interQuartileRangeOfEvenLengthDataSet :: Test
interQuartileRangeOfEvenLengthDataSet =
    TestCase
        ( assertEqual
            "Inter quartile range of an even length data set"
            (D.interQuartileRange' (VU.fromList [3 :: Int, 6, 7, 8, 8, 10, 13, 15, 16, 20]))
            7.25
        )

wrongQuantileNumber :: Test
wrongQuantileNumber =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.wrongQuantileNumberError 1)
            (print $ D.quantiles' (VU.fromList [0]) 1 (VU.fromList [1 :: Int, 2, 3, 4, 5]))
        )

wrongQuantileIndex :: Test
wrongQuantileIndex =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.wrongQuantileIndexError (VU.fromList [5]) 4)
            (print $ D.quantiles' (VU.fromList [5]) 4 (VU.fromList [1 :: Int, 2, 3, 4, 5]))
        )

summarizeOptional :: Test
summarizeOptional =
    TestCase
        ( assertEqual
            "Summarizes `Num a => Maybe a` column"
            3 -- The three columns should be Statistics, A, and B
            ( D.nColumns
                ( D.summarize
                    ( D.fromNamedColumns
                        [ ("A", D.fromList [1 :: Int, 2])
                        , ("B", D.fromList [Just (1 :: Int), Nothing])
                        ]
                    )
                )
            )
        )

-- correlation

correlationDf :: D.DataFrame
correlationDf =
    D.fromNamedColumns
        [ ("x", DI.fromList [1 :: Int, 2, 3, 4, 5])
        , ("y_pos", DI.fromList [1 :: Int, 2, 3, 4, 5])
        , ("y_neg", DI.fromList [5 :: Int, 4, 3, 2, 1])
        ]

-- x perfectly predicts y_pos → r = 1.0
correlationPerfectPositive :: Test
correlationPerfectPositive =
    TestCase
        ( case D.correlation "x" "y_pos" correlationDf of
            Nothing -> assertFailure "Expected Just 1.0, got Nothing"
            Just r ->
                assertBool
                    "Perfect positive correlation should be 1.0"
                    (abs (r - 1.0) < 1e-10)
        )

-- x perfectly anti-predicts y_neg → r = -1.0
correlationPerfectNegative :: Test
correlationPerfectNegative =
    TestCase
        ( case D.correlation "x" "y_neg" correlationDf of
            Nothing -> assertFailure "Expected Just (-1.0), got Nothing"
            Just r ->
                assertBool
                    "Perfect negative correlation should be -1.0"
                    (abs (r + 1.0) < 1e-10)
        )

-- Correlation of a column with itself is 1.0
correlationSelfIdentity :: Test
correlationSelfIdentity =
    TestCase
        ( case D.correlation "x" "x" correlationDf of
            Nothing -> assertFailure "Expected Just 1.0, got Nothing"
            Just r ->
                assertBool
                    "Correlation of a column with itself should be 1.0"
                    (abs (r - 1.0) < 1e-10)
        )

-- Requesting a missing column should throw ColumnsNotFoundException
correlationMissingColumn :: Test
correlationMissingColumn =
    TestCase
        ( assertExpectException
            "[Error Case]"
            "missingcol"
            (print $ D.correlation "x" "missingcol" correlationDf)
        )

tests :: [Test]
tests =
    [ TestLabel "medianOfOddLengthDataSet" medianOfOddLengthDataSet
    , TestLabel "medianOfEvenLengthDataSet" medianOfEvenLengthDataSet
    , TestLabel "medianOfEmptyDataSet" medianOfEmptyDataSet
    , TestLabel "skewnessOfDataSetWithSameElements" skewnessOfDataSetWithSameElements
    , TestLabel "skewnessOfSymmetricDataSet" skewnessOfSymmetricDataSet
    , TestLabel "skewnessOfSimpleDataSet" skewnessOfSimpleDataSet
    , TestLabel "skewnessOfEmptyDataSet" skewnessOfEmptyDataSet
    , TestLabel "twoQuantileOfOddLengthDataSet" twoQuantileOfOddLengthDataSet
    , TestLabel "twoQuantileOfEvenLengthDataSet" twoQuantileOfEvenLengthDataSet
    , TestLabel "quartilesOfOddLengthDataSet" quartilesOfOddLengthDataSet
    , TestLabel "quartilesOfEvenLengthDataSet" quartilesOfEvenLengthDataSet
    , TestLabel "deciles" deciles
    , TestLabel
        "interQuartileRangeOfOddLengthDataSet"
        interQuartileRangeOfOddLengthDataSet
    , TestLabel
        "interQuartileRangeOfEvenLengthDataSet"
        interQuartileRangeOfEvenLengthDataSet
    , TestLabel "wrongQuantileNumber" wrongQuantileNumber
    , TestLabel "wrongQuantileIndex" wrongQuantileIndex
    , TestLabel "summarizeOptional" summarizeOptional
    , TestLabel "correlationPerfectPositive" correlationPerfectPositive
    , TestLabel "correlationPerfectNegative" correlationPerfectNegative
    , TestLabel "correlationSelfIdentity" correlationSelfIdentity
    , TestLabel "correlationMissingColumn" correlationMissingColumn
    ]
