{-# LANGUAGE OverloadedStrings #-}

module Operations.Merge where

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Operations.Merge ((|||))
import Test.HUnit

df1 :: D.DataFrame
df1 =
    D.fromColumns
        [ ("A", DI.fromList [1 :: Int, 2])
        , ("B", DI.fromList ['a', 'b'])
        ]

df2 :: D.DataFrame
df2 =
    D.fromColumns
        [ ("A", DI.fromList [3 :: Int])
        , ("B", DI.fromList ['c'])
        ]

dfOnlyA :: D.DataFrame
dfOnlyA = D.fromColumns [("A", DI.fromList [1 :: Int, 2])]

dfOnlyB :: D.DataFrame
dfOnlyB = D.fromColumns [("B", DI.fromList ['c', 'd'])]

dfSide1 :: D.DataFrame
dfSide1 =
    D.fromColumns
        [("L", DI.fromList [10 :: Int, 20])]

dfSide2 :: D.DataFrame
dfSide2 =
    D.fromColumns
        [("R", DI.fromList ["x" :: String, "y"])]

mergeVerticalWAI :: Test
mergeVerticalWAI =
    TestCase
        ( assertEqual
            "Vertical merge concatenates matching columns"
            ( D.fromColumns
                [ ("A", DI.fromList [1 :: Int, 2, 3])
                , ("B", DI.fromList ['a', 'b', 'c'])
                ]
            )
            (df1 <> df2)
        )

mergeVerticalDisjointSchemas :: Test
mergeVerticalDisjointSchemas =
    TestCase
        ( assertEqual
            "Disjoint schemas produce optional padding"
            ( D.fromColumns
                [
                    ( "A"
                    , DI.fromList
                        [ Just (1 :: Int)
                        , Just 2
                        , Nothing
                        , Nothing
                        ]
                    )
                ,
                    ( "B"
                    , DI.fromList
                        [ Nothing
                        , Nothing
                        , Just 'c'
                        , Just 'd'
                        ]
                    )
                ]
            )
            (dfOnlyA <> dfOnlyB)
        )

mergeVerticalWithEmptyLeft :: Test
mergeVerticalWithEmptyLeft =
    TestCase
        ( assertEqual
            "Empty <> df = df"
            df1
            (D.empty <> df1)
        )

mergeVerticalWithEmptyRight :: Test
mergeVerticalWithEmptyRight =
    TestCase
        ( assertEqual
            "df <> Empty = df"
            df1
            (df1 <> D.empty)
        )

mergeHorizontalWAI :: Test
mergeHorizontalWAI =
    TestCase
        ( assertEqual
            "Horizontal merge combines columns side by side"
            ( D.fromColumns
                [ ("L", DI.fromList [10 :: Int, 20])
                , ("R", DI.fromList ["x" :: String, "y"])
                ]
            )
            (dfSide1 ||| dfSide2)
        )

mergeHorizontalWithEmptyRight :: Test
mergeHorizontalWithEmptyRight =
    TestCase
        ( assertEqual
            "df ||| empty = df"
            dfSide1
            (dfSide1 ||| D.empty)
        )

mergeHorizontalWithEmptyLeft :: Test
mergeHorizontalWithEmptyLeft =
    TestCase
        ( assertEqual
            "empty ||| df = df"
            dfSide2
            (D.empty ||| dfSide2)
        )

mergeVerticalDifferentTypesSameColumnName :: Test
mergeVerticalDifferentTypesSameColumnName =
    let
        dfIntA = D.fromColumns [("A", DI.fromList [1 :: Int, 2])]

        dfTextA = D.fromColumns [("A", DI.fromList ["x" :: String, "y"])]
     in
        TestCase
            ( assertEqual
                "Merging columns with same name but different types wraps the values in Either"
                ( D.fromColumns
                    [ ("A", DI.fromList [Left (1 :: Int), Left 2, Right ("x" :: String), Right "y"])
                    ]
                )
                (dfIntA <> dfTextA)
            )

tests :: [Test]
tests =
    [ TestLabel "mergeVerticalWAI" mergeVerticalWAI
    , TestLabel "mergeVerticalDisjointSchemas" mergeVerticalDisjointSchemas
    , TestLabel "mergeVerticalWithEmptyLeft" mergeVerticalWithEmptyLeft
    , TestLabel "mergeVerticalWithEmptyRight" mergeVerticalWithEmptyRight
    , TestLabel "mergeHorizontalWAI" mergeHorizontalWAI
    , TestLabel "mergeHorizontalWithEmptyRight" mergeHorizontalWithEmptyRight
    , TestLabel "mergeHorizontalWithEmptyLeft" mergeHorizontalWithEmptyLeft
    , TestLabel
        "mergeVerticalDifferentTypesSameColumnName"
        mergeVerticalDifferentTypesSameColumnName
    ]
