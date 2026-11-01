{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Window where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI

import Data.Function ((&))
import DataFrame.Expression.Operators
import Test.HUnit

-- Similar to the example in sumsar.net's pandas-feels-clunky-from-r post.
purchases :: D.DataFrame
purchases =
    D.fromColumns
        [
            ( "country"
            , DI.fromList
                (["US", "US", "US", "UK", "UK", "UK", "France", "France", "France"] :: [T.Text])
            )
        , ("amount", DI.fromList ([100, 200, 5000, 50, 60, 800, 30, 40, 35] :: [Double]))
        , ("discount", DI.fromList ([5, 10, 0, 2, 3, 0, 1, 2, 1] :: [Double]))
        ]

globalFilterTest :: Test
globalFilterTest =
    TestCase
        ( assertEqual
            "Global median filter removes global outliers"
            7
            ( purchases
                & D.filter
                    (F.double "amount" .<=. F.median (F.double "amount") * 10)
                & D.nRows
            )
        )

overMedianFilterTest :: Test
overMedianFilterTest =
    TestCase
        ( assertEqual
            "Per-group median filter via over removes per-country outliers"
            expected
            actual
        )
  where
    actual =
        purchases
            & D.filter
                ( F.double "amount"
                    .<=. F.over ["country"] (F.median (F.double "amount"))
                    * 10
                )
            & D.sortBy [D.Asc (F.text "country"), D.Asc (F.double "amount")]
    expected =
        D.fromColumns
            [
                ( "country"
                , DI.fromList (["France", "France", "France", "UK", "UK", "US", "US"] :: [T.Text])
                )
            , ("amount", DI.fromList ([30, 35, 40, 50, 60, 100, 200] :: [Double]))
            , ("discount", DI.fromList ([1, 1, 2, 2, 3, 5, 10] :: [Double]))
            ]

globalVsOverDifferentResults :: Test
globalVsOverDifferentResults =
    TestCase
        ( assertBool
            "Global filter and per-group over filter should produce different DataFrames"
            (D.nRows globalResult /= D.nRows overResult)
        )
  where
    globalResult =
        purchases
            & D.filter
                (F.double "amount" .<=. F.median (F.double "amount") * 3)
            & D.sortBy [D.Asc (F.text "country"), D.Asc (F.double "amount")]

    overResult =
        purchases
            & D.filter
                ( F.double "amount"
                    .<=. F.over ["country"] (F.median (F.double "amount"))
                    * 3
                )
            & D.sortBy [D.Asc (F.text "country"), D.Asc (F.double "amount")]

overMeanDeriveTest :: Test
overMeanDeriveTest =
    TestCase
        ( assertEqual
            "over with mean broadcasts per-group averages to all rows"
            expectedMeans
            actualMeans
        )
  where
    simpleData =
        D.fromColumns
            [ ("group", DI.fromList (["A", "A", "B", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([10, 20, 30, 60, 90] :: [Double]))
            ]
    result =
        simpleData
            & D.derive "group_mean" (F.over ["group"] (F.mean (F.double "value")))
            & D.sortBy [D.Asc (F.text "group"), D.Asc (F.double "value")]
    -- A mean = 15.0, B mean = 60.0
    expectedMeans = [15.0, 15.0, 60.0, 60.0, 60.0] :: [Double]
    actualMeans = case DI.getColumn "group_mean" result of
        Nothing -> error "group_mean column not found"
        Just c -> DI.toList @Double c

overSumTest :: Test
overSumTest =
    TestCase
        ( assertEqual
            "over with sum broadcasts per-group sums"
            expectedSums
            actualSums
        )
  where
    simpleData =
        D.fromColumns
            [ ("group", DI.fromList (["X", "X", "Y", "Y"] :: [T.Text]))
            , ("value", DI.fromList ([10, 20, 100, 200] :: [Int]))
            ]
    result =
        simpleData
            & D.derive "group_sum" (F.over ["group"] (F.sum (F.int "value")))
            & D.sortBy [D.Asc (F.text "group"), D.Asc (F.int "value")]
    expectedSums = [30, 30, 300, 300] :: [Int]
    actualSums = case DI.getColumn "group_sum" result of
        Nothing -> error "group_sum column not found"
        Just c -> DI.toList @Int c

overCountTest :: Test
overCountTest =
    TestCase
        ( assertEqual
            "over with count broadcasts per-group counts"
            expectedCounts
            actualCounts
        )
  where
    simpleData =
        D.fromColumns
            [ ("group", DI.fromList (["A", "A", "A", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([1, 2, 3, 4, 5] :: [Int]))
            ]
    result =
        simpleData
            & D.derive "group_count" (F.over ["group"] (F.count (F.int "value")))
            & D.sortBy [D.Asc (F.text "group"), D.Asc (F.int "value")]
    expectedCounts = [3, 3, 3, 2, 2] :: [Int]
    actualCounts = case DI.getColumn "group_count" result of
        Nothing -> error "group_count column not found"
        Just c -> DI.toList @Int c

mixedGlobalAndOverTest :: Test
mixedGlobalAndOverTest =
    TestCase
        ( assertEqual
            "Can mix over (per-group) and global expressions"
            expectedDeviations
            actualDeviations
        )
  where
    simpleData =
        D.fromColumns
            [ ("group", DI.fromList (["A", "A", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([10.0, 20.0, 100.0, 200.0] :: [Double]))
            ]
    result =
        simpleData
            & D.derive
                "deviation"
                (F.double "value" - F.over ["group"] (F.mean (F.double "value")))
            & D.sortBy [D.Asc (F.text "group"), D.Asc (F.double "value")]
    expectedDeviations = [-5.0, 5.0, -50.0, 50.0] :: [Double]
    actualDeviations = case DI.getColumn "deviation" result of
        Nothing -> error "deviation column not found"
        Just c -> DI.toList @Double c

-- | Example from the sumsar.net pandas-feels-clunky-from-r post.
blogPostExampleGlobal :: Test
blogPostExampleGlobal =
    TestCase
        ( assertEqual
            "Blog post example: global filter then group+aggregate"
            expected
            actual
        )
  where
    amount = F.double "amount"
    discount = F.double "discount"
    actual =
        purchases
            & D.filter (amount .<=. F.median amount * 10)
            & D.groupBy ["country"]
            & D.aggregate [F.sum (amount - discount) `as` "total"]
            & D.sortBy [D.Asc (F.text "country")]
    -- Global median 60 and threshold 600 remove 5000 and 800.
    expected =
        D.fromColumns
            [ ("country", DI.fromList (["France", "UK", "US"] :: [T.Text]))
            , ("total", DI.fromList ([101, 105, 285] :: [Double]))
            ]

blogPostExampleOver :: Test
blogPostExampleOver =
    TestCase
        ( assertEqual
            "Blog post example: per-group filter (via over) then group+aggregate"
            expected
            actual
        )
  where
    amount = F.double "amount"
    discount = F.double "discount"
    actual =
        purchases
            & D.filter (amount .<=. F.over ["country"] (F.median amount) * 10)
            & D.groupBy ["country"]
            & D.aggregate [F.sum (amount - discount) `as` "total"]
            & D.sortBy [D.Asc (F.text "country")]
    expected =
        D.fromColumns
            [ ("country", DI.fromList (["France", "UK", "US"] :: [T.Text]))
            , ("total", DI.fromList ([101, 105, 285] :: [Double]))
            ]

tests :: [Test]
tests =
    [ TestLabel "globalFilterTest" globalFilterTest
    , TestLabel "overMedianFilterTest" overMedianFilterTest
    , TestLabel "globalVsOverDifferentResults" globalVsOverDifferentResults
    , TestLabel "overMeanDeriveTest" overMeanDeriveTest
    , TestLabel "overSumTest" overSumTest
    , TestLabel "overCountTest" overCountTest
    , TestLabel "mixedGlobalAndOverTest" mixedGlobalAndOverTest
    , TestLabel "blogPostExampleGlobal" blogPostExampleGlobal
    , TestLabel "blogPostExampleOver" blogPostExampleOver
    ]
