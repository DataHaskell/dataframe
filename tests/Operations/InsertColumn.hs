{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.InsertColumn where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI

import Test.HUnit

testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("test1", DI.fromList ([1 .. 26] :: [Int]))
        , ("test2", DI.fromList ['a' .. 'z'])
        , ("test3", DI.fromList ([1 .. 26] :: [Int]))
        , ("test4", DI.fromList ['a' .. 'z'])
        , ("test5", DI.fromList ([1 .. 26] :: [Int]))
        , ("test6", DI.fromList ['a' .. 'z'])
        , ("test7", DI.fromList ([1 .. 26] :: [Int]))
        , ("test8", DI.fromList ['a' .. 'z'])
        ]

-- Adding a boxed vector to an empty dataframe creates a new column boxed containing the vector elements.
addBoxedColumn :: Test
addBoxedColumn =
    TestCase
        ( assertEqual
            "Two columns should be equal"
            ( Just $
                DI.BoxedColumn Nothing (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"])
            )
            ( DI.getColumn "new" $
                D.insertVector "new" (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"]) D.empty
            )
        )

addBoxedColumn' :: Test
addBoxedColumn' =
    TestCase
        ( assertEqual
            "Two columns should be equal"
            (Just $ DI.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"])
            ( DI.getColumn "new" $
                D.insertColumn
                    "new"
                    (DI.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"])
                    D.empty
            )
        )

-- Adding an boxed vector with an unboxable type (Int/Double) to an empty dataframe creates a new column boxed containing the vector elements.
addUnboxedColumn :: Test
addUnboxedColumn =
    TestCase
        ( assertEqual
            "Value should be boxed"
            (Just $ DI.UnboxedColumn Nothing (VU.fromList [1 :: Int, 2, 3]))
            (DI.getColumn "new" $ D.insertVector "new" (V.fromList [1 :: Int, 2, 3]) D.empty)
        )

addUnboxedColumn' :: Test
addUnboxedColumn' =
    TestCase
        ( assertEqual
            "Value should be boxed"
            (Just $ DI.fromList [1 :: Int, 2, 3])
            (DI.getColumn "new" $ D.insertColumn "new" (DI.fromList [1 :: Int, 2, 3]) D.empty)
        )

-- Adding a column with less values than the current DF dimensions adds column with optionals.
addSmallerColumnBoxed :: Test
addSmallerColumnBoxed =
    TestCase
        ( assertEqual
            "Missing values should be replaced with Nothing"
            ( Just $
                DI.fromVector
                    (V.fromList [Just "a" :: Maybe T.Text, Just "b", Just "c", Nothing, Nothing])
            )
            ( DI.getColumn "newer" $
                D.insertVector "newer" (V.fromList ["a" :: T.Text, "b", "c"]) $
                    D.insertVector "new" (V.fromList ["a" :: T.Text, "b", "c", "d", "e"]) D.empty
            )
        )

addSmallerColumnUnboxed :: Test
addSmallerColumnUnboxed =
    TestCase
        ( assertEqual
            "Missing values should be replaced with Nothing"
            ( Just $
                DI.fromVector
                    (V.fromList [Just 1 :: Maybe Int, Just 2, Just 3, Nothing, Nothing])
            )
            ( DI.getColumn "newer" $
                D.insertVector "newer" (V.fromList [1 :: Int, 2, 3]) $
                    D.insertVector "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty
            )
        )

insertColumnWithDefaultFillsWithDefault :: Test
insertColumnWithDefaultFillsWithDefault =
    TestCase
        ( assertEqual
            "Missing values should be replaced with Nothing"
            (Just $ DI.UnboxedColumn Nothing (VU.fromList [1 :: Int, 2, 3, 0, 0]))
            ( DI.getColumn "newer" $
                D.insertVectorWithDefault 0 "newer" (V.fromList [1 :: Int, 2, 3]) $
                    D.insertVector "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty
            )
        )

insertColumnWithDefaultFillsLargerNoop :: Test
insertColumnWithDefaultFillsLargerNoop =
    TestCase
        ( assertEqual
            "Lists should be the same size"
            (Just $ DI.UnboxedColumn Nothing (VU.fromList [(6 :: Int) .. 10]))
            ( DI.getColumn "newer" $
                D.insertVectorWithDefault 0 "newer" (V.fromList [(6 :: Int) .. 10]) $
                    D.insertVector "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty
            )
        )

addLargerColumnBoxed :: Test
addLargerColumnBoxed =
    TestCase
        ( assertEqual
            "Smaller lists should grow and contain optionals"
            ( D.fromNamedColumns
                [
                    ( "new"
                    , D.fromList [Just "a" :: Maybe T.Text, Just "b", Just "c", Nothing, Nothing]
                    )
                , ("newer", D.fromList ["a" :: T.Text, "b", "c", "d", "e"])
                ]
            )
            ( D.insertVector "newer" (V.fromList ["a" :: T.Text, "b", "c", "d", "e"]) $
                D.insertVector "new" (V.fromList ["a" :: T.Text, "b", "c"]) D.empty
            )
        )

addLargerColumnUnboxed :: Test
addLargerColumnUnboxed =
    TestCase
        ( assertEqual
            "Smaller lists should grow and contain optionals"
            ( D.fromNamedColumns
                [ ("old", D.fromList [Just 1 :: Maybe Int, Just 2, Nothing, Nothing, Nothing])
                , ("new", D.fromList [Just 1 :: Maybe Int, Just 2, Just 3, Nothing, Nothing])
                , ("newer", D.fromList [1 :: Int, 2, 3, 4, 5])
                ]
            )
            ( D.insertVector "newer" (V.fromList [1 :: Int, 2, 3, 4, 5]) $
                D.insertVector "new" (V.fromList [1 :: Int, 2, 3]) $
                    D.insertVector "old" (V.fromList [1 :: Int, 2]) D.empty
            )
        )

dimensionsChangeAfterAdd :: Test
dimensionsChangeAfterAdd =
    TestCase
        ( assertEqual
            "should be (26, 3)"
            (26, 9)
            (D.dimensions $ D.insertVector @Int "new" (V.fromList [1 .. 26]) testData)
        )

dimensionsNotChangedAfterDuplicate :: Test
dimensionsNotChangedAfterDuplicate =
    TestCase
        ( assertEqual
            "should be (26, 3)"
            (26, 9)
            ( D.dimensions $
                D.insertVector @Int "new" (V.fromList [1 .. 26]) $
                    D.insertVector @Int "new" (V.fromList [1 .. 26]) testData
            )
        )

tests :: [Test]
tests =
    [ TestLabel "dimensionsChangeAfterAdd" dimensionsChangeAfterAdd
    , TestLabel
        "dimensionsNotChangedAfterDuplicate"
        dimensionsNotChangedAfterDuplicate
    , TestLabel "addBoxedColunmToEmpty" addBoxedColumn
    , TestLabel "addBoxedColumnAutoUnboxes" addBoxedColumn
    , TestLabel "addSmallerColumnBoxed" addSmallerColumnBoxed
    , TestLabel "addSmallerColumnUnboxed" addSmallerColumnUnboxed
    , TestLabel "addLargerColumnBoxed" addLargerColumnBoxed
    , TestLabel "addLargerColumnUnboxed" addLargerColumnUnboxed
    , TestLabel
        "insertColumnWithDefaultFillsWithDefault"
        insertColumnWithDefaultFillsWithDefault
    , TestLabel
        "insertColumnWithDefaultFillsLargerNoop"
        insertColumnWithDefaultFillsLargerNoop
    ]
