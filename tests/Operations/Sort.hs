{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Sort where

import Assertions
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
testData = D.fromNamedColumns values

moreTestData :: D.DataFrame
moreTestData =
    D.fromNamedColumns
        [ ("test1", DI.fromList $ replicate 10 (0 :: Int) ++ replicate 10 1)
        , ("test2", DI.fromList $ [1 :: Int .. 10] ++ [1 .. 10])
        ]

sortByAscendingWAI :: Test
sortByAscendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by ascending works as intended"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [(1 :: Int) .. 26])
                , ("test2", DI.fromList ['a' .. 'z'])
                ]
            )
            (D.sortBy [D.Asc (F.col @Int "test1")] testData)
        )

sortByDescendingWAI :: Test
sortByDescendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by descending works as intended"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList $ reverse [(1 :: Int) .. 26])
                , ("test2", DI.fromList $ reverse ['a' .. 'z'])
                ]
            )
            (D.sortBy [D.Desc (F.col @Int "test1")] testData)
        )

sortByTwoColumns :: Test
sortByTwoColumns =
    TestCase
        ( assertEqual
            "Sorting moreTestData (which is already sorted) is idempotent."
            moreTestData
            (D.sortBy [D.Asc (F.col @Int "test1"), D.Asc (F.col @Int "test2")] moreTestData)
        )

sortByOneColumnAscOneColumnDesc :: Test
sortByOneColumnAscOneColumnDesc =
    TestCase
        ( assertEqual
            "Sorting moreTestData by Desc of test2 reverses the order of the second column."
            ( D.fromNamedColumns
                [ ("test1", DI.fromList $ replicate 10 (0 :: Int) ++ replicate 10 1)
                , ("test2", DI.fromList $ [10 :: Int, 9 .. 1] ++ [10, 9 .. 1])
                ]
            )
            (D.sortBy [D.Asc (F.col @Int "test1"), D.Desc (F.col @Int "test2")] moreTestData)
        )

sortByColumnDoesNotExist :: Test
sortByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnsNotFound ["test0"] "sortBy" (D.columnNames testData))
            (print $ D.sortBy [D.Asc (F.col @Int "test0")] testData)
        )

compoundTestData :: D.DataFrame
compoundTestData =
    D.fromNamedColumns
        [ ("a", DI.fromList ([3, 1, 4, 1, 5] :: [Int]))
        , ("b", DI.fromList ([10, 40, 20, 30, 50] :: [Int]))
        ]

sortByCompoundExpression :: Test
sortByCompoundExpression =
    TestCase
        ( assertEqual
            "Sorting by Asc (a + b) orders rows by the sum without leaking a synthetic column"
            ( D.fromNamedColumns
                [ ("a", DI.fromList ([3, 4, 1, 1, 5] :: [Int]))
                , ("b", DI.fromList ([10, 20, 30, 40, 50] :: [Int]))
                ]
            )
            (D.sortBy [D.Asc (F.col @Int "a" + F.col @Int "b")] compoundTestData)
        )

sortByCompoundExpressionDescending :: Test
sortByCompoundExpressionDescending =
    TestCase
        ( assertEqual
            "Sorting by Desc (b - a) orders rows by descending difference"
            ( D.fromNamedColumns
                [ ("a", DI.fromList ([5, 1, 1, 4, 3] :: [Int]))
                , ("b", DI.fromList ([50, 40, 30, 20, 10] :: [Int]))
                ]
            )
            (D.sortBy [D.Desc (F.col @Int "b" - F.col @Int "a")] compoundTestData)
        )

sortByCompoundMixedWithBareColumn :: Test
sortByCompoundMixedWithBareColumn =
    TestCase
        ( assertEqual
            "Mixing a compound Asc key with a bare Desc tie-breaker works"
            ( D.fromNamedColumns
                [ ("a", DI.fromList ([1, 1, 3, 4, 5] :: [Int]))
                , ("b", DI.fromList ([40, 30, 10, 20, 50] :: [Int]))
                ]
            )
            ( D.sortBy
                [D.Asc (F.col @Int "a" * 2), D.Desc (F.col @Int "b")]
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
                    [D.Asc (F.col @Int "nope" + F.col @Int "a")]
                    compoundTestData
            )
        )

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
    ]
