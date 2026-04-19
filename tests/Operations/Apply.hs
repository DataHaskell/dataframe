{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Apply where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame as DE
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.DataFrame as DI
import DataFrame.Operations.Statistics (imputeWith)
import DataFrame.Operations.Transformations (impute)

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

applyBoxedToUnboxed :: Test
applyBoxedToUnboxed =
    TestCase
        ( assertEqual
            "Boxed apply unboxed when result is unboxed"
            (Just $ DI.UnboxedColumn Nothing (VU.fromList (replicate 26 (1 :: Int))))
            (DI.getColumn "test2" $ D.apply @String (const (1 :: Int)) "test2" testData)
        )

applyBoxedToBoxed :: Test
applyBoxedToBoxed =
    TestCase
        ( assertEqual
            "Boxed apply remains in boxed vector"
            (Just $ DI.BoxedColumn Nothing (V.fromList (replicate 26 (1 :: Integer))))
            (DI.getColumn "test2" $ D.apply @String (const (1 :: Integer)) "test2" testData)
        )

applyWrongType :: Test
applyWrongType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            ( show $
                DE.TypeMismatchException
                    ( DE.MkTypeErrorContext
                        { DE.userType = Right (typeRep @Char)
                        , DE.expectedType = Right (typeRep @String)
                        , DE.callingFunctionName = Just "apply"
                        , DE.errorColumnName = Nothing
                        }
                    )
            )
            (print $ DI.getColumn "test2" $ D.apply @Char (const (1 :: Int)) "test2" testData)
        )

applyUnknownColumn :: Test
applyUnknownColumn =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.columnNotFound "test9" "apply" (D.columnNames testData))
            (print $ D.apply @[Char] (const (1 :: Int)) "test9" testData)
        )

applyManyOnlyGivenFields :: Test
applyManyOnlyGivenFields =
    TestCase
        ( assertEqual
            "Applies function to many fields"
            ( D.fromNamedColumns
                ( map (,D.fromList $ replicate 26 (1 :: Integer)) ["test4", "test6"]
                    ++
                    -- All other fields should have their original values.
                    filter (\(name, _col) -> name /= "test4" && name /= "test6") values
                )
            )
            ( D.applyMany @Char
                (const (1 :: Integer))
                ["test4", "test6"]
                testData
            )
        )

applyManyBoxedToBoxed :: Test
applyManyBoxedToBoxed =
    TestCase
        ( assertEqual
            "Applies function to many fields"
            ( D.fromNamedColumns
                (map (,D.fromList $ replicate 26 (1 :: Integer)) ["test4", "test6", "test8"])
            )
            ( D.select ["test4", "test6", "test8"] $
                D.applyMany @Char
                    (const (1 :: Integer))
                    ["test4", "test6", "test8"]
                    testData
            )
        )

applyManyBoxedToUnboxed :: Test
applyManyBoxedToUnboxed =
    TestCase
        ( assertEqual
            "Unboxes fields when necessary"
            ( D.fromNamedColumns
                (map (,D.fromList $ replicate 26 (1 :: Int)) ["test4", "test6", "test8"])
            )
            ( D.select ["test4", "test6", "test8"] $
                D.applyMany @Char
                    (const (1 :: Int))
                    ["test4", "test6", "test8"]
                    testData
            )
        )

applyManyColumnNotFound :: Test
applyManyColumnNotFound =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.columnNotFound "test0" "apply" (D.columnNames testData))
            ( print $
                D.applyMany @Char
                    (const (1 :: Integer))
                    ["test0", "test6", "test8"]
                    testData
            )
        )

applyManyWrongType :: Test
applyManyWrongType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.typeMismatchError (show $ typeRep @Char) (show $ typeRep @[Char]))
            ( print $
                DI.getColumn "test2" $
                    D.applyMany @Char (const (1 :: Int)) ["test2"] testData
            )
        )

applyWhereWrongConditionType :: Test
applyWhereWrongConditionType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.typeMismatchError (show $ typeRep @Integer) (show $ typeRep @Int))
            ( print $
                D.applyWhere (even @Integer) "test1" ((+ 1) :: Int -> Int) "test5" testData
            )
        )

applyWhereWrongTargetType :: Test
applyWhereWrongTargetType =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.typeMismatchError (show $ typeRep @Float) (show $ typeRep @Int))
            ( print $
                D.applyWhere (even @Int) "test1" ((+ 1) :: Float -> Float) "test5" testData
            )
        )

applyWhereConditionColumnNotFound :: Test
applyWhereConditionColumnNotFound =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.columnNotFound "test0" "applyWhere" (D.columnNames testData))
            (print $ D.applyWhere (even @Int) "test0" ((+ 1) :: Int -> Int) "test5" testData)
        )

applyWhereTargetColumnNotFound :: Test
applyWhereTargetColumnNotFound =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.columnNotFound "test0" "applyAtIndex" (D.columnNames testData))
            (print $ D.applyWhere (even @Int) "test1" ((+ 1) :: Int -> Int) "test0" testData)
        )

applyWhereWAI :: Test
applyWhereWAI =
    TestCase
        ( assertEqual
            "applyWhere works as intended"
            ( Just $
                DI.UnboxedColumn
                    Nothing
                    (VU.fromList (zipWith ($) (cycle [id, (+ 1)]) [(1 :: Int) .. 26]))
            )
            ( D.getColumn "test5" $
                D.applyWhere (even @Int) "test1" ((+ 1) :: Int -> Int) "test5" testData
            )
        )

imputeData :: D.DataFrame
imputeData =
    D.fromNamedColumns
        [ ("opt", DI.fromList [Just (1 :: Int), Nothing, Just 3])
        , ("plain", DI.fromList [10 :: Int, 20, 30])
        ]

imputeHappyPath :: Test
imputeHappyPath =
    TestCase
        ( assertEqual
            "impute fills Nothing with the given value"
            (Just $ DI.UnboxedColumn Nothing (VU.fromList [1 :: Int, 0, 3]))
            (DI.getColumn "opt" $ impute (F.col @(Maybe Int) "opt") 0 imputeData)
        )

imputeColumnNotFound :: Test
imputeColumnNotFound =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.columnNotFound "missing" "impute" (D.columnNames imputeData))
            (print $ impute (F.col @(Maybe Int) "missing") 0 imputeData)
        )

imputeOnNonOptional :: Test
imputeOnNonOptional =
    TestCase
        ( assertEqual
            "impute is a no-op on a non-nullable column"
            imputeData
            (impute (F.col @(Maybe Int) "plain") 0 imputeData)
        )

imputePlainNoOp :: Test
imputePlainNoOp =
    TestCase
        ( assertEqual
            "impute with non-Maybe expr is always a no-op"
            imputeData
            (impute (F.col @Int "plain") 0 imputeData)
        )

imputeWithPlainNoOp :: Test
imputeWithPlainNoOp =
    TestCase
        ( assertEqual
            "imputeWith with non-Maybe expr is always a no-op"
            imputeData
            (imputeWith id (F.col @Int "plain") imputeData)
        )

tests :: [Test]
tests =
    [ TestLabel "applyBoxedToUnboxed" applyBoxedToUnboxed
    , TestLabel "applyWrongType" applyWrongType
    , TestLabel "applyUnknownColumn" applyUnknownColumn
    , TestLabel "applyBoxedToBoxed" applyBoxedToBoxed
    , TestLabel "applyManyBoxedToBoxed" applyManyBoxedToBoxed
    , TestLabel "applyManyOnlyGivenFields" applyManyOnlyGivenFields
    , TestLabel "applyManyBoxedToUnboxed" applyManyBoxedToUnboxed
    , TestLabel "applyManyColumnNotFound" applyManyColumnNotFound
    , TestLabel "applyManyWrongType" applyManyWrongType
    , TestLabel "applyWhereWrongConditionType" applyWhereWrongConditionType
    , TestLabel "applyWhereWrongTargetType" applyWhereWrongTargetType
    , TestLabel "applyWhereConditionColumnNotFound" applyWhereConditionColumnNotFound
    , TestLabel "applyWhereTargetColumnNotFound" applyWhereTargetColumnNotFound
    , TestLabel "applyWhereWAI" applyWhereWAI
    , TestLabel "imputeHappyPath" imputeHappyPath
    , TestLabel "imputeColumnNotFound" imputeColumnNotFound
    , TestLabel "imputeOnNonOptional" imputeOnNonOptional
    , TestLabel "imputePlainNoOp" imputePlainNoOp
    , TestLabel "imputeWithPlainNoOp" imputeWithPlainNoOp
    ]
