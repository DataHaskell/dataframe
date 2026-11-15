{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Sort where

import Assertions
import Control.Exception (SomeException, evaluate, try)
import Data.Char
import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import System.Random
import System.Random.Shuffle (shuffle')
import Test.HUnit

values :: [(T.Text, DI.Column)]
values =
    let
        ns = shuffle' [(1 :: Int) .. 26] 26 $ mkStdGen 252
     in
        [ ("test1", DI.fromList ns)
        , ("test2", DI.fromList (map (chr . (+ 96)) ns))
        ]

testData :: D.DataFrame
testData = D.fromColumns values

moreTestData :: D.DataFrame
moreTestData =
    D.fromColumns
        [ ("test1", DI.fromList $ replicate 10 (0 :: Int) ++ replicate 10 1)
        , ("test2", DI.fromList $ [1 :: Int .. 10] ++ [1 .. 10])
        ]

sortByAscendingWAI :: Test
sortByAscendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by ascending works as intended"
            ( D.fromColumns
                [ ("test1", DI.fromList [(1 :: Int) .. 26])
                , ("test2", DI.fromList ['a' .. 'z'])
                ]
            )
            (D.sortBy [D.Asc (F.int "test1")] testData)
        )

sortByDescendingWAI :: Test
sortByDescendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by descending works as intended"
            ( D.fromColumns
                [ ("test1", DI.fromList $ reverse [(1 :: Int) .. 26])
                , ("test2", DI.fromList $ reverse ['a' .. 'z'])
                ]
            )
            (D.sortBy [D.Desc (F.int "test1")] testData)
        )

sortByTwoColumns :: Test
sortByTwoColumns =
    TestCase
        ( assertEqual
            "Sorting moreTestData (which is already sorted) is idempotent."
            moreTestData
            (D.sortBy [D.Asc (F.int "test1"), D.Asc (F.int "test2")] moreTestData)
        )

sortByOneColumnAscOneColumnDesc :: Test
sortByOneColumnAscOneColumnDesc =
    TestCase
        ( assertEqual
            "Sorting moreTestData by Desc of test2 reverses the order of the second column."
            ( D.fromColumns
                [ ("test1", DI.fromList $ replicate 10 (0 :: Int) ++ replicate 10 1)
                , ("test2", DI.fromList $ [10 :: Int, 9 .. 1] ++ [10, 9 .. 1])
                ]
            )
            (D.sortBy [D.Asc (F.int "test1"), D.Desc (F.int "test2")] moreTestData)
        )

sortByColumnDoesNotExist :: Test
sortByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnsNotFound ["test0"] "sortBy" (D.columnNames testData))
            (print $ D.sortBy [D.Asc (F.int "test0")] testData)
        )

compoundTestData :: D.DataFrame
compoundTestData =
    D.fromColumns
        [ ("a", DI.fromList ([3, 1, 4, 1, 5] :: [Int]))
        , ("b", DI.fromList ([10, 40, 20, 30, 50] :: [Int]))
        ]

sortByCompoundExpression :: Test
sortByCompoundExpression =
    TestCase
        ( assertEqual
            "Sorting by Asc (a + b) orders rows by the sum without leaking a synthetic column"
            ( D.fromColumns
                [ ("a", DI.fromList ([3, 4, 1, 1, 5] :: [Int]))
                , ("b", DI.fromList ([10, 20, 30, 40, 50] :: [Int]))
                ]
            )
            (D.sortBy [D.Asc (F.int "a" + F.int "b")] compoundTestData)
        )

sortByCompoundExpressionDescending :: Test
sortByCompoundExpressionDescending =
    TestCase
        ( assertEqual
            "Sorting by Desc (b - a) orders rows by descending difference"
            ( D.fromColumns
                [ ("a", DI.fromList ([5, 1, 1, 4, 3] :: [Int]))
                , ("b", DI.fromList ([50, 40, 30, 20, 10] :: [Int]))
                ]
            )
            (D.sortBy [D.Desc (F.int "b" - F.int "a")] compoundTestData)
        )

sortByCompoundMixedWithBareColumn :: Test
sortByCompoundMixedWithBareColumn =
    TestCase
        ( assertEqual
            "Mixing a compound Asc key with a bare Desc tie-breaker works"
            ( D.fromColumns
                [ ("a", DI.fromList ([1, 1, 3, 4, 5] :: [Int]))
                , ("b", DI.fromList ([40, 30, 10, 20, 50] :: [Int]))
                ]
            )
            ( D.sortBy
                [D.Asc (F.int "a" * 2), D.Desc (F.int "b")]
                compoundTestData
            )
        )

sortByCompoundMissingColumn :: Test
sortByCompoundMissingColumn =
    TestCase
        ( assertExpectException
            "[Error Case]"
            ( D.columnsNotFound
                ["nope"]
                "sortBy"
                (D.columnNames compoundTestData)
            )
            ( print $
                D.sortBy
                    [D.Asc (F.int "nope" + F.int "a")]
                    compoundTestData
            )
        )

nullableTestData :: D.DataFrame
nullableTestData =
    D.fromColumns
        [ ("a", DI.fromList [Just (3 :: Int), Nothing, Just 1, Just 2])
        , ("t", DI.fromList [Just ("c" :: T.Text), Just "a", Nothing, Just "b"])
        , ("i", DI.fromList [0 :: Int .. 3])
        ]

sortedRowIds :: [D.SortOrder] -> [Int]
sortedRowIds ords = D.columnAsList (F.col @Int "i") (D.sortBy ords nullableTestData)

sortByNullableAscending :: Test
sortByNullableAscending =
    TestCase
        ( assertEqual
            "Asc on a Maybe Int column sorts, nulls first"
            [1, 2, 3, 0]
            (sortedRowIds [D.Asc (F.col @(Maybe Int) "a")])
        )

sortByNullableDescending :: Test
sortByNullableDescending =
    TestCase
        ( assertEqual
            "Desc on a Maybe Int column sorts, nulls last"
            [0, 3, 2, 1]
            (sortedRowIds [D.Desc (F.col @(Maybe Int) "a")])
        )

sortByNullableText :: Test
sortByNullableText =
    TestCase
        ( assertEqual
            "Asc on a Maybe Text column sorts, nulls first"
            [2, 1, 3, 0]
            (sortedRowIds [D.Asc (F.col @(Maybe T.Text) "t")])
        )

topByNullableAscending :: Test
topByNullableAscending =
    TestCase
        ( assertEqual
            "topBy on an ascending nullable key agrees with Maybe's Ord"
            [1, 2]
            ( D.columnAsList
                (F.col @Int "i")
                (D.topBy 0 2 [D.Asc (F.col @(Maybe Int) "a")] nullableTestData)
            )
        )

sortByWrongTypeThrows :: Test
sortByWrongTypeThrows = TestCase $ do
    r <-
        try (evaluate (length (sortedRowIds [D.Asc (F.col @Double "i")]))) ::
            IO (Either SomeException Int)
    assertBool "Sorting with the wrong key type throws" (either (const True) (const False) r)

tests :: [Test]
tests =
    [ TestLabel "sortByAscendingWAI" sortByAscendingWAI
    , TestLabel "sortByDescendingWAI" sortByDescendingWAI
    , TestLabel "sortByColumnDoesNotExist" sortByColumnDoesNotExist
    , TestLabel "sortByTwoColumns" sortByTwoColumns
    , TestLabel "sortByOneColumnAscOneColumnDesc" sortByOneColumnAscOneColumnDesc
    , TestLabel "sortByCompoundExpression" sortByCompoundExpression
    , TestLabel
        "sortByCompoundExpressionDescending"
        sortByCompoundExpressionDescending
    , TestLabel "sortByCompoundMixedWithBareColumn" sortByCompoundMixedWithBareColumn
    , TestLabel "sortByCompoundMissingColumn" sortByCompoundMissingColumn
    , TestLabel "sortByNullableAscending" sortByNullableAscending
    , TestLabel "sortByNullableDescending" sortByNullableDescending
    , TestLabel "sortByNullableText" sortByNullableText
    , TestLabel "topByNullableAscending" topByNullableAscending
    , TestLabel "sortByWrongTypeThrows" sortByWrongTypeThrows
    ]
