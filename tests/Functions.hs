{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Functions where

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
            (D.derive @Int "sum" (F.sum (F.col Int "A")) df)
        )

tests :: [Test]
tests =
    [ TestLabel "sanitizeIdentifiers" sanitizeIdentifiers
    , TestLabel "testSum" testSum
    ]
