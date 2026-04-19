{-# LANGUAGE OverloadedStrings #-}

module Operations.GroupBy where

import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as D

import Assertions
import Test.HUnit

values :: [(T.Text, DI.Column)]
values =
    [ ("test1", DI.fromList (concatMap (replicate 10) [1 :: Int, 2, 3, 4]))
    , ("test2", DI.fromList (take 40 $ cycle [1 :: Int, 2]))
    , ("test3", DI.fromList [(1 :: Int) .. 40])
    , ("test4", DI.fromList (reverse [(1 :: Int) .. 40]))
    ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

groupBySingleRowWAI :: Test
groupBySingleRowWAI =
    TestCase
        ( assertEqual
            "Groups by single column"
            -- We don't yet compare offsets and indices
            (D.Grouped testData ["test1"] VU.empty VU.empty VU.empty)
            (D.groupBy ["test1"] testData)
        )

groupByMultipleRowsWAI :: Test
groupByMultipleRowsWAI =
    TestCase
        ( assertEqual
            "Groups by single column"
            -- We don't yet compare offsets and indices
            (D.Grouped testData ["test1", "test2"] VU.empty VU.empty VU.empty)
            (D.groupBy ["test1", "test2"] testData)
        )

groupByColumnDoesNotExist :: Test
groupByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnsNotFound ["test0"] "groupBy" (D.columnNames testData))
            (print $ D.groupBy ["test0"] testData)
        )

tests :: [Test]
tests =
    [ TestLabel "groupBySingleRowWAI" groupBySingleRowWAI
    , TestLabel "groupByMultipleRowsWAI" groupByMultipleRowsWAI
    , TestLabel "groupByColumnDoesNotExist" groupByColumnDoesNotExist
    ]
