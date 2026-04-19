{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Window where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI

import Data.Function ((&))
import DataFrame.Operators
import Test.HUnit

-- Similar to example discussed in https://www.sumsar.net/blog/pandas-feels-clunky-when-coming-from-r/
purchases :: D.DataFrame
purchases =
    D.fromNamedColumns
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
                & D.filterWhere
                    (F.col @Double "amount" .<=. F.median (F.col @Double "amount") * 10)
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
            & D.filterWhere
                ( F.col @Double "amount"
                    .<=. F.over ["country"] (F.median (F.col @Double "amount"))
                    * 10
                )
            & D.sortBy [D.Asc (F.col @T.Text "country"), D.Asc (F.col @Double "amount")]
    expected =
        D.fromNamedColumns
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
            & D.filterWhere
                (F.col @Double "amount" .<=. F.median (F.col @Double "amount") * 3)
            & D.sortBy [D.Asc (F.col @T.Text "country"), D.Asc (F.col @Double "amount")]

    overResult =
        purchases
            & D.filterWhere
                ( F.col @Double "amount"
                    .<=. F.over ["country"] (F.median (F.col @Double "amount"))
                    * 3
                )
            & D.sortBy [D.Asc (F.col @T.Text "country"), D.Asc (F.col @Double "amount")]

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
        D.fromNamedColumns
            [ ("group", DI.fromList (["A", "A", "B", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([10, 20, 30, 60, 90] :: [Double]))
            ]
    result =
        simpleData
            & D.derive "group_mean" (F.over ["group"] (F.mean (F.col @Double "value")))
            & D.sortBy [D.Asc (F.col @T.Text "group"), D.Asc (F.col @Double "value")]
    -- A mean = 15.0, B mean = 60.0
    expectedMeans = [15.0, 15.0, 60.0, 60.0, 60.0] :: [Double]
    actualMeans = case DI.getColumn "group_mean" result of
        Nothing -> error "group_mean column not found"
        Just col -> DI.toList @Double col

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
        D.fromNamedColumns
            [ ("group", DI.fromList (["X", "X", "Y", "Y"] :: [T.Text]))
            , ("value", DI.fromList ([10, 20, 100, 200] :: [Int]))
            ]
    result =
        simpleData
            & D.derive "group_sum" (F.over ["group"] (F.sum (F.col @Int "value")))
            & D.sortBy [D.Asc (F.col @T.Text "group"), D.Asc (F.col @Int "value")]
    expectedSums = [30, 30, 300, 300] :: [Int]
    actualSums = case DI.getColumn "group_sum" result of
        Nothing -> error "group_sum column not found"
        Just col -> DI.toList @Int col

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
        D.fromNamedColumns
            [ ("group", DI.fromList (["A", "A", "A", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([1, 2, 3, 4, 5] :: [Int]))
            ]
    result =
        simpleData
            & D.derive "group_count" (F.over ["group"] (F.count (F.col @Int "value")))
            & D.sortBy [D.Asc (F.col @T.Text "group"), D.Asc (F.col @Int "value")]
    expectedCounts = [3, 3, 3, 2, 2] :: [Int]
    actualCounts = case DI.getColumn "group_count" result of
        Nothing -> error "group_count column not found"
        Just col -> DI.toList @Int col

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
        D.fromNamedColumns
            [ ("group", DI.fromList (["A", "A", "B", "B"] :: [T.Text]))
            , ("value", DI.fromList ([10.0, 20.0, 100.0, 200.0] :: [Double]))
            ]
    result =
        simpleData
            & D.derive
                "deviation"
                (F.col @Double "value" - F.over ["group"] (F.mean (F.col @Double "value")))
            & D.sortBy [D.Asc (F.col @T.Text "group"), D.Asc (F.col @Double "value")]
    expectedDeviations = [-5.0, 5.0, -50.0, 50.0] :: [Double]
    actualDeviations = case DI.getColumn "deviation" result of
        Nothing -> error "deviation column not found"
        Just col -> DI.toList @Double col

-- | Example from https://www.sumsar.net/blog/pandas-feels-clunky-when-coming-from-r/
blogPostExampleGlobal :: Test
blogPostExampleGlobal =
    TestCase
        ( assertEqual
            "Blog post example: global filter then group+aggregate"
            expected
            actual
        )
  where
    amount = F.col @Double "amount"
    discount = F.col @Double "discount"
    actual =
        purchases
            & D.filterWhere (amount .<=. F.median amount * 10)
            & D.groupBy ["country"]
            & D.aggregate [F.sum (amount - discount) `as` "total"]
            & D.sortBy [D.Asc (F.col @T.Text "country")]
    -- Global median = 60, threshold = 600 → removes 5000 and 800
    -- France: (30-1)+(40-2)+(35-1) = 101, UK: (50-2)+(60-3) = 105, US: (100-5)+(200-10) = 285
    expected =
        D.fromNamedColumns
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
    amount = F.col @Double "amount"
    discount = F.col @Double "discount"
    actual =
        purchases
            & D.filterWhere (amount .<=. F.over ["country"] (F.median amount) * 10)
            & D.groupBy ["country"]
            & D.aggregate [F.sum (amount - discount) `as` "total"]
            & D.sortBy [D.Asc (F.col @T.Text "country")]
    expected =
        D.fromNamedColumns
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
