{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Functions where

import Data.Time.Calendar (Day, fromGregorian)
import qualified DataFrame as D
import DataFrame.Functions (
    sanitize,
 )
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import Test.HUnit

-- Test cases for the sanitize function
sanitizeIdentifiers :: Test
sanitizeIdentifiers =
    TestList
        [ TestCase $
            assertEqual
                "Reserved word 'Data' should become '_data_'"
                "_data_"
                (sanitize "Data")
        , TestCase $
            assertEqual
                "Spaces should become underscores"
                "my_data"
                (sanitize "My Data")
        , TestCase $
            assertEqual
                "Punctuation and parentheses should be handled"
                "distance_km_h"
                (sanitize "Distance (km/h)")
        , TestCase $
            assertEqual
                "Leading digit should be wrapped"
                "_0_age_"
                (sanitize "0 Age")
        , TestCase $
            assertEqual
                "Valid camelCase should be unchanged"
                "camelCaseStr"
                (sanitize "camelCaseStr")
        , TestCase $
            assertEqual
                "Valid camelCase with invalid characters mixed in"
                "camelcase_str"
                (sanitize "camelCase$Str")
        , TestCase $
            assertEqual
                "Valid snake_case should remain unchanged"
                "snake_case_str"
                (sanitize "snake_case_str")
        , TestCase $
            assertEqual
                "Leading digit with snake_case should be wrapped"
                "_12_snake_case_"
                (sanitize "12_snake_case")
        , TestCase $
            assertEqual
                "All symbols should become underscores"
                "_____"
                (sanitize "***")
        ]
df :: D.DataFrame
df =
    D.fromNamedColumns
        [("A", DI.fromList [(1 :: Int) .. 10])]

testSum :: Test
testSum =
    TestCase
        ( assertEqual
            "Sum first 10 numbers"
            ( D.fromNamedColumns
                [ ("A", DI.fromList [(1 :: Int) .. 10])
                , ("sum", DI.fromList (replicate 10 (55 :: Int)))
                ]
            )
            (D.derive "sum" (F.sum (F.col @Int "A")) df)
        )

testPow :: Test
testPow =
    TestCase
        ( assertEqual
            "pow of a compound base"
            [4, 9, 16, 25, 36, 49, 64, 81, 100, 121]
            ( D.columnAsList @Int
                (F.col "sq")
                (D.derive "sq" (F.pow (F.col @Int "A" + F.lit 1) 2) df)
            )
        )

testDaysBetween :: Test
testDaysBetween =
    TestCase
        ( assertEqual
            "daysBetween d1 d2 is d1 minus d2 in either argument order"
            ([-9], [9])
            ( days (F.col @Day "start") (F.col @Day "end")
            , days (F.col @Day "end") (F.col @Day "start")
            )
        )
  where
    dates =
        D.fromNamedColumns
            [ ("start", DI.fromList [fromGregorian 2024 3 1])
            , ("end", DI.fromList [fromGregorian 2024 3 10])
            ]
    days a b = D.columnAsList @Int (F.col "d") (D.derive "d" (F.daysBetween a b) dates)

tests :: [Test]
tests =
    [ TestLabel "sanitizeIdentifiers" sanitizeIdentifiers
    , TestLabel "testSum" testSum
    , TestLabel "testPow" testPow
    , TestLabel "testDaysBetween" testDaysBetween
    ]
