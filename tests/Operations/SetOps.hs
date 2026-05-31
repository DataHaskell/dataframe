{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.SetOps where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI

import Test.HUnit

{- | Sort by the integer key column so set results (which come out in
hash-bucket order) can be compared deterministically.
-}
sortByA :: D.DataFrame -> D.DataFrame
sortByA = D.sortBy [D.Asc (F.col @Int "A")]

dfA :: D.DataFrame
dfA =
    D.fromNamedColumns
        [ ("A", DI.fromList [1 :: Int, 2, 3, 3])
        , ("B", DI.fromList ['a', 'b', 'c', 'c'])
        ]

dfB :: D.DataFrame
dfB =
    D.fromNamedColumns
        [ ("A", DI.fromList [3 :: Int, 4])
        , ("B", DI.fromList ['c', 'd'])
        ]

expect :: [Int] -> [Char] -> D.DataFrame
expect as bs =
    D.fromNamedColumns
        [ ("A", DI.fromList as)
        , ("B", DI.fromList bs)
        ]

unionWAI :: Test
unionWAI =
    TestCase
        ( assertEqual
            "union is the deduplicated set union"
            (expect [1, 2, 3, 4] "abcd")
            (sortByA (D.union dfA dfB))
        )

intersectWAI :: Test
intersectWAI =
    TestCase
        ( assertEqual
            "intersect keeps rows present in both"
            (expect [3] "c")
            (sortByA (D.intersect dfA dfB))
        )

differenceWAI :: Test
differenceWAI =
    TestCase
        ( assertEqual
            "difference keeps left rows absent from right"
            (expect [1, 2] "ab")
            (sortByA (D.difference dfA dfB))
        )

differenceIsDirectional :: Test
differenceIsDirectional =
    TestCase
        ( assertEqual
            "difference b a is the other complement"
            (expect [4] "d")
            (sortByA (D.difference dfB dfA))
        )

symmetricDifferenceWAI :: Test
symmetricDifferenceWAI =
    TestCase
        ( assertEqual
            "symmetricDifference keeps rows in exactly one input"
            (expect [1, 2, 4] "abd")
            (sortByA (D.symmetricDifference dfA dfB))
        )

intersectWithEmptyIsEmpty :: Test
intersectWithEmptyIsEmpty =
    TestCase
        ( assertEqual
            "intersect with an empty frame is empty (schema preserved)"
            (expect [] "")
            (sortByA (D.intersect dfA (expect [] "")))
        )

differenceWithEmptyIsDistinctSelf :: Test
differenceWithEmptyIsDistinctSelf =
    TestCase
        ( assertEqual
            "difference against an empty frame is the deduplicated self"
            (expect [1, 2, 3] "abc")
            (sortByA (D.difference dfA (expect [] "")))
        )

tests :: [Test]
tests =
    [ TestLabel "unionWAI" unionWAI
    , TestLabel "intersectWAI" intersectWAI
    , TestLabel "differenceWAI" differenceWAI
    , TestLabel "differenceIsDirectional" differenceIsDirectional
    , TestLabel "symmetricDifferenceWAI" symmetricDifferenceWAI
    , TestLabel "intersectWithEmptyIsEmpty" intersectWithEmptyIsEmpty
    , TestLabel "differenceWithEmptyIsDistinctSelf" differenceWithEmptyIsDistinctSelf
    ]
