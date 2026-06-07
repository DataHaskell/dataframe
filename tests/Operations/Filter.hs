{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Filter where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI

import Assertions
import Test.HUnit
import Type.Reflection (typeRep)

values :: [(T.Text, DI.Column)]
values =
    [ ("test1", DI.fromList ([1 .. 26] :: [Int]))
    , ("test2", DI.fromList (map show ['a' .. 'z']))
    , ("test3", DI.fromList ([1 .. 26] :: [Int]))
    , ("test4", DI.fromList ['a' .. 'z'])
    , ("test5", DI.fromList ([1 .. 26] :: [Int]))
    , ("test6", DI.fromList ['a' .. 'z'])
    , ("test7", DI.fromList ([1 .. 26] :: [Int]))
    , ("test8", DI.fromList ['a' .. 'z'])
    ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

filterColumnDoesNotExist :: Test
filterColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnNotFound "test0" "filter" (D.columnNames testData))
            (print $ D.filter (F.col Int "test0") even testData)
        )

filterColumnWrongType :: Test
filterColumnWrongType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.typeMismatchError (show $ typeRep @Integer) (show $ typeRep @Int))
            (print $ D.filter (F.col Integer "test1") even testData)
        )

filterByColumnDoesNotExist :: Test
filterByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnNotFound "test0" "filter" (D.columnNames testData))
            (print $ D.filterBy even (F.col Int "test0") testData)
        )

filterByColumnWrongType :: Test
filterByColumnWrongType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.typeMismatchError (show $ typeRep @Integer) (show $ typeRep @Int))
            (print $ D.filterBy even (F.col Integer "test1") testData)
        )

filterColumnInexistentValues :: Test
filterColumnInexistentValues =
    TestCase
        ( assertEqual
            "Non existent filter value returns no rows"
            (0, 8)
            (D.dimensions $ D.filter (F.col Int "test1") (< 0) testData)
        )

filterColumnAllValues :: Test
filterColumnAllValues =
    TestCase
        ( assertEqual
            "Filters all columns"
            (26, 8)
            (D.dimensions $ D.filter (F.col Int "test1") (const True) testData)
        )

filterJustWAI :: Test
filterJustWAI =
    TestCase
        ( assertEqual
            "Filters out Nothing and unwraps Maybe"
            (D.fromNamedColumns [("test", D.fromList $ replicate 5 (1 :: Int))])
            ( D.filterJust
                "test"
                ( D.fromNamedColumns
                    [("test", D.fromList $ take 10 $ cycle [Just (1 :: Int), Nothing])]
                )
            )
        )

tests :: [Test]
tests =
    [ TestLabel "filterColumnDoesNotExist" filterColumnDoesNotExist
    , TestLabel "filterColumnWrongType" filterColumnWrongType
    , TestLabel "filterByColumnDoesNotExist" filterByColumnDoesNotExist
    , TestLabel "filterByColumnWrongType" filterByColumnWrongType
    , TestLabel "filterColumnInexistentValues" filterColumnInexistentValues
    , TestLabel "filterColumnAllValues" filterColumnAllValues
    , TestLabel "filterJustWAI" filterJustWAI
    ]
