{-# LANGUAGE OverloadedStrings #-}

module Operations.Take where

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as D

import Test.HUnit

testData :: D.DataFrame
testData =
    D.fromColumns
        [ ("test1", DI.fromList ([1 .. 26] :: [Int]))
        , ("test2", DI.fromList ['a' .. 'z'])
        ]

takeWAI :: Test
takeWAI =
    TestCase
        ( assertEqual
            "Gets first 10 numbers"
            (Just $ D.fromList [(1 :: Int) .. 10])
            (D.getColumn "test1" $ D.take 10 testData)
        )

takeLastWAI :: Test
takeLastWAI =
    TestCase
        ( assertEqual
            "Gets first 10 numbers"
            (Just $ D.fromList [(17 :: Int) .. 26])
            (D.getColumn "test1" $ D.takeLast 10 testData)
        )

lengthEqualsTakeParam :: Test
lengthEqualsTakeParam =
    TestCase
        (assertEqual "should be (5, 2)" (5, 2) (D.dimensions $ D.take 5 testData))

lengthGreaterThanTakeParam :: Test
lengthGreaterThanTakeParam =
    TestCase
        (assertEqual "should be (26, 2)" (26, 2) (D.dimensions $ D.take 30 testData))

emptyIsZero :: Test
emptyIsZero =
    TestCase
        (assertEqual "should be (0, 0)" (0, 0) (D.dimensions $ D.take 5 D.empty))

negativeIsZero :: Test
negativeIsZero =
    TestCase
        (assertEqual "should be (0, 2)" (0, 2) (D.dimensions $ D.take (-1) testData))

lengthEqualsTakeLastParam :: Test
lengthEqualsTakeLastParam =
    TestCase
        (assertEqual "should be (5, 2)" (5, 2) (D.dimensions $ D.takeLast 5 testData))

lengthGreaterThanTakeLastParam :: Test
lengthGreaterThanTakeLastParam =
    TestCase
        (assertEqual "should be (26, 2)" (26, 2) (D.dimensions $ D.takeLast 30 testData))

emptyIsZeroTakeLast :: Test
emptyIsZeroTakeLast =
    TestCase
        (assertEqual "should be (0, 0)" (0, 0) (D.dimensions $ D.takeLast 5 D.empty))

negativeIsZeroTakeLast :: Test
negativeIsZeroTakeLast =
    TestCase
        (assertEqual "should be (0, 2)" (0, 2) (D.dimensions $ D.takeLast (-1) testData))

tests :: [Test]
tests =
    [ TestLabel "takeWAI" takeWAI
    , TestLabel "takeLastWAI" takeLastWAI
    , TestLabel "lengthEqualsTakeParam" lengthEqualsTakeParam
    , TestLabel "lengthGreaterThanTakeParam" lengthGreaterThanTakeParam
    , TestLabel "emptyIsZero" emptyIsZero
    , TestLabel "negativeIsZero" negativeIsZero
    , TestLabel "lengthEqualsTakeLastParam" lengthEqualsTakeLastParam
    , TestLabel "lengthGreaterThanTakeLastParam" lengthGreaterThanTakeLastParam
    , TestLabel "emptyIsZeroTakeLast" emptyIsZeroTakeLast
    , TestLabel "negativeIsZeroTakeLast" negativeIsZeroTakeLast
    ]
