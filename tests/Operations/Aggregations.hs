{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Aggregations where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Typed as DT

import Data.Function
import DataFrame.Operators
import Test.HUnit

values :: [(T.Text, DI.Column)]
values =
    [ ("test1", DI.fromList ([1, 1, 1, 2, 2, 2, 3, 3, 3, 1, 1, 1] :: [Int]))
    , ("test2", DI.fromList ([12, 11 .. 1] :: [Int]))
    , ("test3", DI.fromList ([1 .. 12] :: [Int]))
    , ("test4", DI.fromList ['a' .. 'l'])
    , ("test5", DI.fromList (map show ['a' .. 'l']))
    , ("test6", DI.fromList ([1 .. 12] :: [Integer]))
    ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

foldAggregation :: Test
foldAggregation =
    TestCase
        ( assertEqual
            "Counting elements after grouping gives correct numbers"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [6 :: Int, 3, 3])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate [F.count (F.col @Int "test2") `as` "test2"]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

foldAggregationTyped :: Test
foldAggregationTyped =
    TestCase
        ( assertEqual
            "Typed counting elements after grouping gives correct numbers"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2_count", DI.fromList [6 :: Int, 3, 3])
                ]
            )
            ( testData
                & either (error . show) id
                    . DT.freezeWithError
                        @[ DT.Column "test1" Int
                         , DT.Column "test2" Int
                         , DT.Column "test3" Int
                         , DT.Column "test4" Char
                         , DT.Column "test5" String
                         , DT.Column "test6" Integer
                         ]
                & DT.groupBy @'["test1"]
                & DT.aggregate (DT.agg @"test2_count" (DT.count (DT.col @"test2")) DT.aggNil)
                & DT.sortBy [DT.asc (DT.col @"test1")]
                & DT.thaw
            )
        )

numericAggregation :: Test
numericAggregation =
    TestCase
        ( assertEqual
            "Mean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [6.5 :: Double, 8.0, 5.0])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate [F.mean (F.col @Int "test2") `as` "test2"]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

numericAggregationTyped :: Test
numericAggregationTyped =
    TestCase
        ( assertEqual
            "Typed ean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2_mean", DI.fromList [6.5 :: Double, 8.0, 5.0])
                ]
            )
            ( testData
                & either (error . show) id
                    . DT.freezeWithError
                        @[ DT.Column "test1" Int
                         , DT.Column "test2" Int
                         , DT.Column "test3" Int
                         , DT.Column "test4" Char
                         , DT.Column "test5" String
                         , DT.Column "test6" Integer
                         ]
                & DT.groupBy @'["test1"]
                & DT.aggregate (DT.agg @"test2_mean" (DT.mean (DT.col @"test2")) DT.aggNil)
                & DT.sortBy [DT.asc (DT.col @"test1")]
                & DT.thaw
            )
        )

numericAggregationOfUnaggregatedUnaryOp :: Test
numericAggregationOfUnaggregatedUnaryOp =
    TestCase
        ( assertEqual
            "Mean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [6.5 :: Double, 8.0, 5.0])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate
                    [ F.mean (F.lift (fromIntegral @Int @Double) (F.col @Int "test2")) `as` "test2"
                    ]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

numericAggregationOfUnaggregatedBinaryOp :: Test
numericAggregationOfUnaggregatedBinaryOp =
    TestCase
        ( assertEqual
            "Mean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [13 :: Double, 16, 10])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate [F.mean (F.col @Int "test2" + F.col @Int "test2") `as` "test2"]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

reduceAggregationOfUnaggregatedUnaryOp :: Test
reduceAggregationOfUnaggregatedUnaryOp =
    TestCase
        ( assertEqual
            "Mean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [12 :: Double, 9, 6])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate
                    [ F.maximum (F.lift (fromIntegral @Int @Double) (F.col @Int "test2"))
                        `as` "test2"
                    ]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

reduceAggregationOfUnaggregatedBinaryOp :: Test
reduceAggregationOfUnaggregatedBinaryOp =
    TestCase
        ( assertEqual
            "Mean works for ints"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [1 :: Int, 2, 3])
                , ("test2", DI.fromList [24 :: Int, 18, 12])
                ]
            )
            ( testData
                & D.groupBy ["test1"]
                & D.aggregate
                    [F.maximum (F.col @Int "test2" + F.col @Int "test2") `as` "test2"]
                & D.sortBy [D.Asc (F.col @Int "test1")]
            )
        )

aggregationOnNoRows :: Test
aggregationOnNoRows =
    TestCase
        ( assertEqual
            "Aggregation on DataFrame with no rows"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList ([] :: [Int]))
                , ("sum(test2)", DI.fromList ([] :: [Int]))
                ]
            )
            ( testData
                & D.drop 12
                & D.groupBy ["test1"]
                & D.aggregate
                    [F.sum (F.col @Int "test2") `as` "sum(test2)"]
            )
        )

tests :: [Test]
tests =
    [ TestLabel "foldAggregation" foldAggregation
    , TestLabel "foldAggregationTyped" foldAggregationTyped
    , TestLabel "numericAggregation" numericAggregation
    , TestLabel "numericAggregationTyped" numericAggregationTyped
    , TestLabel
        "numericAggregationOfUnaggregatedUnaryOp"
        numericAggregationOfUnaggregatedUnaryOp
    , TestLabel
        "numericAggregationOfUnaggregatedBinaryOp"
        numericAggregationOfUnaggregatedBinaryOp
    , TestLabel
        "reduceAggregationOfUnaggregatedUnaryOp"
        reduceAggregationOfUnaggregatedUnaryOp
    , TestLabel
        "reduceAggregationOfUnaggregatedBinaryOp"
        reduceAggregationOfUnaggregatedBinaryOp
    , TestLabel
        "aggregationOnNoRows"
        aggregationOnNoRows
    ]
