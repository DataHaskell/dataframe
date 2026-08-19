{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Statistics where

import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
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

-- Nullable columns: statistics must skip null slots, not read the sentinel
-- stored there.

nullableDf :: D.DataFrame
nullableDf =
    D.fromNamedColumns [("x", DI.fromList [Just (10 :: Double), Nothing, Just 20])]

nullableIntDf :: D.DataFrame
nullableIntDf =
    D.fromNamedColumns [("n", DI.fromList [Just (10 :: Int), Nothing, Just 20])]

nullableBoxedDf :: D.DataFrame
nullableBoxedDf =
    D.fromNamedColumns [("b", DI.fromList [Just (10 :: Integer), Nothing, Just 20])]

meanIgnoresNulls :: Test
meanIgnoresNulls =
    TestCase
        (assertEqual "mean skips nulls" 15.0 (D.mean (F.col @Double "x") nullableDf))

meanExprIgnoresNulls :: Test
meanExprIgnoresNulls =
    TestCase
        ( assertEqual
            "mean over a derived nullable expression skips nulls"
            30.0
            (D.mean (F.lift (* 2) (F.col @Double "x")) nullableDf)
        )

medianIgnoresNulls :: Test
medianIgnoresNulls =
    TestCase
        (assertEqual "median skips nulls" 15.0 (D.median (F.col @Double "x") nullableDf))

percentileIgnoresNulls :: Test
percentileIgnoresNulls =
    TestCase
        ( assertEqual
            "percentile skips nulls"
            15.0
            (D.percentile 50 (F.col @Double "x") nullableDf)
        )

stdDevIgnoresNulls :: Test
stdDevIgnoresNulls =
    TestCase
        ( assertBool
            "standard deviation skips nulls"
            ( abs (D.standardDeviation (F.col @Double "x") nullableDf - 7.0710678118654755)
                < 1e-12
            )
        )

varianceIgnoresNulls :: Test
varianceIgnoresNulls =
    TestCase
        ( assertEqual
            "variance skips nulls"
            50.0
            (D.variance (F.col @Double "x") nullableDf)
        )

varianceExprIgnoresNulls :: Test
varianceExprIgnoresNulls =
    TestCase
        ( assertEqual
            "variance over a derived nullable expression skips nulls"
            200.0
            (D.variance (F.lift (* 2) (F.col @Double "x")) nullableDf)
        )

iqrIgnoresNulls :: Test
iqrIgnoresNulls =
    TestCase
        ( assertEqual
            "inter-quartile range skips nulls"
            5.0
            (D.interQuartileRange (F.col @Double "x") nullableDf)
        )

skewnessIgnoresNulls :: Test
skewnessIgnoresNulls =
    TestCase
        ( let skewDf =
                D.fromNamedColumns
                    [("s", DI.fromList [Just (10 :: Double), Nothing, Just 20, Just 100, Just 11])]
           in assertBool
                "skewness skips nulls"
                ( abs
                    ( D.skewness (F.col @Double "s") skewDf
                        - D.skewness' (VU.fromList [10 :: Double, 20, 100, 11])
                    )
                    < 1e-12
                )
        )

genericPercentileIgnoresNulls :: Test
genericPercentileIgnoresNulls =
    TestCase
        ( assertEqual
            "genericPercentile skips the sentinel"
            10
            (D.genericPercentile 10 (F.col @Int "n") nullableIntDf)
        )

genericPercentileBoxedNullableDoesNotThrow :: Test
genericPercentileBoxedNullableDoesNotThrow =
    TestCase
        ( assertEqual
            "genericPercentile on a boxed nullable column skips the error thunk"
            20
            (D.genericPercentile 100 (F.col @Integer "b") nullableBoxedDf)
        )

genericPercentileMaybeViewKeepsNothing :: Test
genericPercentileMaybeViewKeepsNothing =
    TestCase
        ( assertEqual
            "a Maybe-typed view still sees its Nothings"
            (Nothing :: Maybe Int)
            (D.genericPercentile 0 (F.col @(Maybe Int) "n") nullableIntDf)
        )

sumUnboxedNullable :: Test
sumUnboxedNullable =
    TestCase
        (assertEqual "sum skips null slots" 30.0 (D.sum (F.col @Double "x") nullableDf))

sumBoxedNullableDoesNotThrow :: Test
sumBoxedNullableDoesNotThrow =
    TestCase
        ( assertEqual
            "sum on a boxed nullable column skips the error thunk"
            (30 :: Integer)
            (D.sum (F.col @Integer "b") nullableBoxedDf)
        )

correlationIgnoresNullRows :: Test
correlationIgnoresNullRows =
    TestCase
        ( let dfc =
                D.fromNamedColumns
                    [ ("a", DI.fromList [Just (1 :: Double), Nothing, Just 3])
                    , ("c", DI.fromList [1 :: Double, 2, 3])
                    ]
           in case D.correlation "a" "c" dfc of
                Nothing -> assertFailure "Expected Just 1.0, got Nothing"
                Just r ->
                    assertBool
                        "null rows are dropped pairwise"
                        (abs (r - 1.0) < 1e-10)
        )

frequenciesSkipsNulls :: Test
frequenciesSkipsNulls =
    TestCase
        ( assertEqual
            "frequencies has no sentinel category"
            3 -- Statistic, 10 and 20
            (D.nColumns (D.frequencies (F.col @Int "n") nullableIntDf))
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
    , TestLabel "meanIgnoresNulls" meanIgnoresNulls
    , TestLabel "meanExprIgnoresNulls" meanExprIgnoresNulls
    , TestLabel "medianIgnoresNulls" medianIgnoresNulls
    , TestLabel "percentileIgnoresNulls" percentileIgnoresNulls
    , TestLabel "stdDevIgnoresNulls" stdDevIgnoresNulls
    , TestLabel "varianceIgnoresNulls" varianceIgnoresNulls
    , TestLabel "varianceExprIgnoresNulls" varianceExprIgnoresNulls
    , TestLabel "iqrIgnoresNulls" iqrIgnoresNulls
    , TestLabel "skewnessIgnoresNulls" skewnessIgnoresNulls
    , TestLabel "genericPercentileIgnoresNulls" genericPercentileIgnoresNulls
    , TestLabel
        "genericPercentileBoxedNullableDoesNotThrow"
        genericPercentileBoxedNullableDoesNotThrow
    , TestLabel
        "genericPercentileMaybeViewKeepsNothing"
        genericPercentileMaybeViewKeepsNothing
    , TestLabel "sumUnboxedNullable" sumUnboxedNullable
    , TestLabel "sumBoxedNullableDoesNotThrow" sumBoxedNullableDoesNotThrow
    , TestLabel "correlationIgnoresNullRows" correlationIgnoresNullRows
    , TestLabel "frequenciesSkipsNulls" frequenciesSkipsNulls
    ]
