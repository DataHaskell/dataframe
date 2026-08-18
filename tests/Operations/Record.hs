{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module Operations.Record where

import Data.Int (Int64)
import qualified Data.Map.Strict as M
import qualified Data.Text as T

-- UTF-8 byte-mode IO: the plain Data.Text.IO writer honours the
-- handle's text mode, which on Windows turns \n into \r\n and
-- corrupts inputs meant for byte-level parsers.
import qualified Data.Text.IO.Utf8 as TIO
import GHC.Generics (Generic)

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Operators
import qualified DataFrame.Schema as IS
import DataFrame.Typed (Schema)
import qualified DataFrame.Typed as DT
import System.Directory (getTemporaryDirectory)

import Test.HUnit

-- Basic record with three different-type fields (Int64 / Text / Double).
data Order = Order
    { orderId :: Int64
    , region :: T.Text
    , amount :: Double
    }
    deriving (Show, Eq)

$(DT.deriveSchemaFromType ''Order)
$(D.deriveSchemaValues ''Order)

-- Nullable fields (Maybe Text -> RNullableBoxed; Maybe Int -> RNullableUnboxed).
data User = User
    { userId :: Int64
    , userName :: Maybe T.Text
    , userAge :: Maybe Int
    }
    deriving (Show, Eq)

$(DT.deriveSchemaFromType ''User)
$(D.deriveSchemaValues ''User)

-- Identity-cased: keep the record selector names verbatim.
data Account = Account
    { accountId :: Int64
    , accountName :: T.Text
    }
    deriving (Show, Eq)

$( DT.deriveSchemaFromTypeWith
    DT.defaultSchemaOptions{DT.nameTransform = id}
    ''Account
 )

-- Schema-name override.
data Item = Item
    { itemId :: Int64
    , itemPrice :: Double
    }
    deriving (Show, Eq)

$( DT.deriveSchemaFromTypeWith
    DT.defaultSchemaOptions{DT.schemaTypeName = Just "ItemsSchema"}
    ''Item
 )

-- Wide record (more than zipWith3 can handle).
data Wide = Wide
    { f1 :: Int
    , f2 :: Int
    , f3 :: Int
    , f4 :: Int
    , f5 :: Int
    , f6 :: Int
    , f7 :: Int
    , f8 :: Int
    }
    deriving (Show, Eq)

$(DT.deriveSchemaFromType ''Wide)
$(D.deriveSchemaValues ''Wide)

-- Generics opt-in: derive the schema via Generic, not TH.
data Foo = Foo
    { fooId :: Int64
    , fooName :: T.Text
    , fooValue :: Double
    }
    deriving (Show, Eq, Generic)

type FooSchema = DT.SchemaOf Foo

instance DT.HasSchema Foo where
    type Schema Foo = FooSchema
    toColumns = DT.genericToColumns
    fromColumns = DT.genericFromColumns

orderSample :: [Order]
orderSample =
    [ Order 1 "us" 10.0
    , Order 2 "eu" 20.5
    , Order 3 "ap" 30.0
    ]

basicTypedRoundTrip :: Test
basicTypedRoundTrip = TestCase $ do
    let df :: DT.TypedDataFrame OrderSchema
        df = DT.fromRecordsTyped orderSample
    case DT.toRecordsTyped df of
        Left e -> assertFailure (T.unpack e)
        Right xs -> assertEqual "typed round-trip" orderSample xs

basicUntypedRoundTrip :: Test
basicUntypedRoundTrip = TestCase $ do
    let df = D.fromRecords orderSample
    case D.toRecords df :: Either T.Text [Order] of
        Left e -> assertFailure (T.unpack e)
        Right xs -> assertEqual "untyped round-trip" orderSample xs

emptyRoundTrip :: Test
emptyRoundTrip = TestCase $ do
    let df = D.fromRecords ([] :: [Order])
    case D.toRecords df :: Either T.Text [Order] of
        Left e -> assertFailure (T.unpack e)
        Right xs -> assertEqual "empty round-trip" [] xs

nullableRoundTrip :: Test
nullableRoundTrip = TestCase $ do
    let xs =
            [ User 1 (Just "alice") (Just 30)
            , User 2 Nothing (Just 40)
            , User 3 (Just "carol") Nothing
            , User 4 Nothing Nothing
            ]
        df = D.fromRecords xs
    case D.toRecords df :: Either T.Text [User] of
        Left e -> assertFailure (T.unpack e)
        Right ys -> assertEqual "nullable round-trip" xs ys

identityRoundTrip :: Test
identityRoundTrip = TestCase $ do
    let xs = [Account 1 "alice", Account 2 "bob"]
        df = D.fromRecords xs
    assertEqual
        "identity column names"
        ["accountId", "accountName"]
        (D.columnNames df)
    case D.toRecords df :: Either T.Text [Account] of
        Left e -> assertFailure (T.unpack e)
        Right ys -> assertEqual "identity round-trip" xs ys

schemaNameOverride :: Test
schemaNameOverride = TestCase $ do
    let xs = [Item 1 9.99, Item 2 19.99]
        df :: DT.TypedDataFrame ItemsSchema
        df = DT.fromRecordsTyped xs
    case DT.toRecordsTyped df of
        Left e -> assertFailure (T.unpack e)
        Right ys -> assertEqual "schema-name override round-trip" xs ys

-- order_id is deliberately Int (not the schema's Int64) to force a mismatch.
typeMismatchError :: Test
typeMismatchError = TestCase $ do
    let badDf =
            D.fromNamedColumns
                [ ("order_id", DI.fromList ([1, 2, 3] :: [Int]))
                , ("region", DI.fromList (["us", "eu", "ap"] :: [T.Text]))
                , ("amount", DI.fromList ([10.0, 20.5, 30.0] :: [Double]))
                ]
    case D.toRecords badDf :: Either T.Text [Order] of
        Right _ -> assertFailure "expected error on type mismatch"
        Left e ->
            assertBool
                ("error should mention order_id, got: " ++ T.unpack e)
                ("order_id" `T.isInfixOf` e)

missingColumnError :: Test
missingColumnError = TestCase $ do
    let badDf =
            D.fromNamedColumns
                [ ("region", DI.fromList (["us"] :: [T.Text]))
                , ("amount", DI.fromList ([10.0] :: [Double]))
                ]
    case D.toRecords badDf :: Either T.Text [Order] of
        Right _ -> assertFailure "expected error on missing column"
        Left e ->
            assertBool
                ("error should mention order_id, got: " ++ T.unpack e)
                ("order_id" `T.isInfixOf` e)

wideRoundTrip :: Test
wideRoundTrip = TestCase $ do
    let xs =
            [ Wide 1 2 3 4 5 6 7 8
            , Wide 9 10 11 12 13 14 15 16
            ]
        df = D.fromRecords xs
    case D.toRecords df :: Either T.Text [Wide] of
        Left e -> assertFailure (T.unpack e)
        Right ys -> assertEqual "wide round-trip" xs ys

genericRoundTrip :: Test
genericRoundTrip = TestCase $ do
    let xs = [Foo 1 "a" 1.0, Foo 2 "b" 2.5]
        df = D.fromRecords xs
    case D.toRecords df :: Either T.Text [Foo] of
        Left e -> assertFailure (T.unpack e)
        Right ys -> assertEqual "generic round-trip" xs ys

genericColumnNames :: Test
genericColumnNames = TestCase $ do
    let df = D.fromRecords [Foo 1 "a" 1.0]
    assertEqual
        "generic snake-cased column names"
        ["foo_id", "foo_name", "foo_value"]
        (D.columnNames df)

deriveSchemaSplice :: Test
deriveSchemaSplice = TestCase $ do
    assertEqual
        "orderSchema column names"
        ["amount", "order_id", "region"]
        (M.keys (IS.elements orderSchema))
    assertEqual
        "order_id is Int64"
        (Just (IS.schemaType @Int64))
        (M.lookup "order_id" (IS.elements orderSchema))
    assertEqual
        "region is Text"
        (Just (IS.schemaType @T.Text))
        (M.lookup "region" (IS.elements orderSchema))
    assertEqual
        "amount is Double"
        (Just (IS.schemaType @Double))
        (M.lookup "amount" (IS.elements orderSchema))

deriveSchemaNullable :: Test
deriveSchemaNullable = TestCase $ do
    assertEqual
        "userSchema column names"
        ["user_age", "user_id", "user_name"]
        (M.keys (IS.elements userSchema))
    assertEqual
        "user_id is Int64"
        (Just (IS.schemaType @Int64))
        (M.lookup "user_id" (IS.elements userSchema))
    assertEqual
        "user_name is Maybe Text"
        (Just (IS.schemaType @(Maybe T.Text)))
        (M.lookup "user_name" (IS.elements userSchema))
    assertEqual
        "user_age is Maybe Int"
        (Just (IS.schemaType @(Maybe Int)))
        (M.lookup "user_age" (IS.elements userSchema))

deriveSchemaWide :: Test
deriveSchemaWide = TestCase $ do
    assertEqual
        "wideSchema has 8 keys"
        8
        (M.size (IS.elements wideSchema))
    assertEqual
        "f1 is Int"
        (Just (IS.schemaType @Int))
        (M.lookup "f1" (IS.elements wideSchema))
    assertEqual
        "f8 is Int"
        (Just (IS.schemaType @Int))
        (M.lookup "f8" (IS.elements wideSchema))

deriveSchemaReadsCsv :: Test
deriveSchemaReadsCsv = TestCase $ do
    let csv =
            T.unlines
                [ "order_id,region,amount"
                , "1,us,10.0"
                , "2,eu,20.5"
                , "3,ap,30.0"
                ]
    tmpDir <- getTemporaryDirectory
    let tmp = tmpDir <> "/dataframe_test_deriveSchema.csv"
    TIO.writeFile tmp csv
    df <- D.readCsvWithSchema orderSchema tmp
    assertEqual
        "deriveSchema-driven readCsvWithSchema column names"
        ["order_id", "region", "amount"]
        (D.columnNames df)
    case D.toRecords df :: Either T.Text [Order] of
        Left e -> assertFailure (T.unpack e)
        Right xs ->
            assertEqual
                "deriveSchema-driven CSV parses back to records"
                [Order 1 "us" 10.0, Order 2 "eu" 20.5, Order 3 "ap" 30.0]
                xs

deriveSchemaAccessorFilter :: Test
deriveSchemaAccessorFilter = TestCase $ do
    let df =
            D.fromRecords
                [ Order 1 "us" 20.0
                , Order 2 "eu" 20.5
                , Order 3 "ap" 30.0
                , Order 4 "us" 25.0
                ]
        big =
            D.filterWhere
                (orderAmount .>. F.lit @Double 15.0 .&&. orderRegion .==. F.lit @T.Text "us")
                df
    case D.toRecords big :: Either T.Text [Order] of
        Left e -> assertFailure (T.unpack e)
        Right xs ->
            assertEqual
                "accessor drives D.filter (amount > 15.0 && region == \"us\")"
                [Order 1 "us" 20.0, Order 4 "us" 25.0]
                xs

deriveSchemaAccessorDerive :: Test
deriveSchemaAccessorDerive = TestCase $ do
    let df =
            D.fromRecords
                [ Order 1 "us" 10.0
                , Order 2 "eu" 20.0
                ]
        df' = D.derive "double_amount" (orderAmount + orderAmount) df
    assertEqual
        "accessor composes in derive expression"
        [20.0, 40.0]
        (D.columnAsList (D.col @Double "double_amount") df')

labelColumnFilter :: Test
labelColumnFilter = TestCase $ do
    let df :: DT.TypedDataFrame OrderSchema
        df = DT.fromRecordsTyped orderSample
        usOnly = DT.filterWhere (#region DT..==. "us") df
    case DT.toRecordsTyped usOnly of
        Left e -> assertFailure (T.unpack e)
        Right xs ->
            assertEqual
                "#region OverloadedLabel resolves to col @\"region\""
                [Order 1 "us" 10.0]
                xs

tests :: [Test]
tests =
    [ TestLabel "basicTypedRoundTrip" basicTypedRoundTrip
    , TestLabel "labelColumnFilter" labelColumnFilter
    , TestLabel "basicUntypedRoundTrip" basicUntypedRoundTrip
    , TestLabel "emptyRoundTrip" emptyRoundTrip
    , TestLabel "nullableRoundTrip" nullableRoundTrip
    , TestLabel "identityRoundTrip" identityRoundTrip
    , TestLabel "schemaNameOverride" schemaNameOverride
    , TestLabel "typeMismatchError" typeMismatchError
    , TestLabel "missingColumnError" missingColumnError
    , TestLabel "wideRoundTrip" wideRoundTrip
    , TestLabel "genericRoundTrip" genericRoundTrip
    , TestLabel "genericColumnNames" genericColumnNames
    , TestLabel "deriveSchemaSplice" deriveSchemaSplice
    , TestLabel "deriveSchemaNullable" deriveSchemaNullable
    , TestLabel "deriveSchemaWide" deriveSchemaWide
    , TestLabel "deriveSchemaReadsCsv" deriveSchemaReadsCsv
    , TestLabel "deriveSchemaAccessorFilter" deriveSchemaAccessorFilter
    , TestLabel "deriveSchemaAccessorDerive" deriveSchemaAccessorDerive
    ]
