{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Behavior pins for the typed-extraction fastcsv path: schema bypass,
parser semantics, byte-level missing tests, and the chunk-boundary scalar
tail that replaced the full-file copy.
-}
module Operations.TypedExtraction (tests) where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

-- UTF-8 byte-mode IO: the plain Data.Text.IO writer honours the
-- handle's text mode, which on Windows turns \n into \r\n and
-- corrupts inputs meant for byte-level parsers.
import qualified Data.Text.IO.Utf8 as TIO

import Control.Exception (ErrorCall, evaluate, try)
import Data.Time (Day)
import DataFrame.IO.CSV (
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
 )
import qualified DataFrame.IO.CSV.Fast as D
import DataFrame.Internal.Column (Column)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame, dataframeDimensions, getColumn)
import DataFrame.Operations.Typing (SafeReadMode (..))
import DataFrame.Schema (Schema (..), SchemaType (..), elements)
import System.Directory (removeFile)
import Test.HUnit

tmpDir :: FilePath
tmpDir = "./tests/data/unstable_csv/"

withCsv :: String -> T.Text -> (FilePath -> IO a) -> IO a
withCsv name body action = do
    let path = tmpDir <> "typed_" <> name <> ".csv"
    TIO.writeFile path body
    r <- action path
    removeFile path
    pure r

expectColumn :: String -> T.Text -> DataFrame -> Column -> Assertion
expectColumn what name df expected = case getColumn name df of
    Nothing -> assertFailure (what <> ": column missing")
    Just col -> assertEqual what expected col

-- Divergence #1 (pinned new behavior): Int overflow rejects instead of
-- wrapping, so an overflowing cell demotes the column to Double.
testIntOverflowDemotesToDouble :: Test
testIntOverflowDemotesToDouble = TestLabel "typed_int_overflow_demotes" $
    TestCase $
        withCsv "overflow" "a\n1\n9223372036854775808\n" $ \path -> do
            df <- D.fastReadCsv path
            expectColumn
                "overflowing int column becomes Double"
                "a"
                df
                (DI.fromList @Double [1.0, 9.223372036854776e18])

-- Divergence #2: doubles parse with strip-equivalent semantics, so a padded
-- double past the 100-row inference sample no longer demotes the column to
-- Text. (Inside the sample the classifier still rejects padding.)
testPaddedDoubleStaysDouble :: Test
testPaddedDoubleStaysDouble = TestLabel "typed_padded_double" $
    TestCase $ do
        let clean = [fromIntegral i + 0.5 :: Double | i <- [1 .. 120 :: Int]]
            cleanRows = T.unlines (map (T.pack . show) clean)
        withCsv "padded_double" ("a\n" <> cleanRows <> " 3.5\n") $ \path -> do
            df <- D.fastReadCsv path
            expectColumn
                "padded double cell past the sample stays Double"
                "a"
                df
                (DI.fromList @Double (clean <> [3.5]))

-- Schema bypasses inference: a numeric-looking column declared Text in the
-- schema must come back as Text (old code silently kept the inferred Int).
testSchemaTextBypassesInference :: Test
testSchemaTextBypassesInference = TestLabel "typed_schema_text" $
    TestCase $
        withCsv "schema_text" "a\n1\n2\n" $ \path -> do
            let schema = Schema (M.fromList [("a", SType (P.Proxy @T.Text))])
            df <- D.fastReadCsvWithSchema schema path
            expectColumn
                "schema Text wins over inferred Int"
                "a"
                df
                (DI.fromList @T.Text ["1", "2"])

-- Schema + NoSafeRead: a cell that cannot parse as the declared type
-- raises during the read (strict read), instead of hiding an error thunk.
testSchemaStrictFailureThrows :: Test
testSchemaStrictFailureThrows = TestLabel "typed_schema_strict_throws" $
    TestCase $
        withCsv "schema_strict" "a\n1\nx\n" $ \path -> do
            let schema = Schema (M.fromList [("a", SType (P.Proxy @Int))])
            result <- try @ErrorCall $ do
                df <- D.fastReadCsvWithSchema schema path
                _ <- evaluate (dataframeDimensions df)
                case getColumn "a" df of
                    Just col -> evaluate (show col)
                    Nothing -> pure ""
            case result of
                Left _ -> pure ()
                Right _ ->
                    assertFailure
                        "schema Int over unparseable cell should raise under NoSafeRead"

-- Schema + MaybeRead: unparseable cells become nulls.
testSchemaMaybeReadNullsFailures :: Test
testSchemaMaybeReadNullsFailures = TestLabel "typed_schema_mayberead" $
    TestCase $
        withCsv "schema_maybe" "a\n1\nx\n2\n" $ \path -> do
            let schema = Schema (M.fromList [("a", SType (P.Proxy @Int))])
                opts =
                    defaultReadOptions
                        { typeSpec =
                            SpecifyTypes
                                (M.toList (elements schema))
                                (typeSpec defaultReadOptions)
                        , safeRead = MaybeRead
                        }
            df <- D.fastReadCsvWithOpts opts path
            expectColumn
                "schema Int + MaybeRead nulls the bad cell"
                "a"
                df
                (DI.fromList @(Maybe Int) [Just 1, Nothing, Just 2])

-- Preserved: NoSafeRead treats only empty cells as null, so "NA" still
-- demotes a numeric column to Text.
testNoSafeReadNADemotesToText :: Test
testNoSafeReadNADemotesToText = TestLabel "typed_nosaferead_na" $
    TestCase $
        withCsv "na_demotes" "a\n1\nNA\n" $ \path -> do
            df <- D.fastReadCsv path
            expectColumn
                "NA is data under NoSafeRead"
                "a"
                df
                (DI.fromList @T.Text ["1", "NA"])

-- Preserved: MaybeRead honours the canonical missing-token set.
testMaybeReadMissingTokens :: Test
testMaybeReadMissingTokens = TestLabel "typed_mayberead_missing" $
    TestCase $
        withCsv "maybe_missing" "a\n1\nNA\n2\n" $ \path -> do
            let opts = defaultReadOptions{safeRead = MaybeRead}
            df <- D.fastReadCsvWithOpts opts path
            expectColumn
                "NA is null under MaybeRead"
                "a"
                df
                (DI.fromList @(Maybe Int) [Just 1, Nothing, Just 2])

-- Preserved: custom missing indicators extend the canonical set.
testMaybeReadCustomMissingToken :: Test
testMaybeReadCustomMissingToken = TestLabel "typed_mayberead_custom" $
    TestCase $
        withCsv "maybe_custom" "a\n1\nMISSING\n2\n" $ \path -> do
            let opts =
                    defaultReadOptions
                        { safeRead = MaybeRead
                        , missingIndicators =
                            "MISSING" : missingIndicators defaultReadOptions
                        }
            df <- D.fastReadCsvWithOpts opts path
            expectColumn
                "custom token is null under MaybeRead"
                "a"
                df
                (DI.fromList @(Maybe Int) [Just 1, Nothing, Just 2])

-- Preserved: quoted numerics parse as numbers (quote stripping happens
-- before the typed parse).
testQuotedNumbersParse :: Test
testQuotedNumbersParse = TestLabel "typed_quoted_numbers" $
    TestCase $
        withCsv "quoted_nums" "a,b\n\"42\",\"1.5\"\n\"7\",\"2.5\"\n" $ \path -> do
            df <- D.fastReadCsv path
            expectColumn "quoted ints" "a" df (DI.fromList @Int [42, 7])
            expectColumn "quoted doubles" "b" df (DI.fromList @Double [1.5, 2.5])

-- Preserved: Bool and Date inference still land on typed columns.
testBoolAndDateInference :: Test
testBoolAndDateInference = TestLabel "typed_bool_date" $
    TestCase $
        withCsv "bool_date" "b,d\nTrue,2020-01-01\nFalse,2021-12-31\n" $ \path -> do
            df <- D.fastReadCsv path
            expectColumn "bool column" "b" df (DI.fromList @Bool [True, False])
            expectColumn
                "date column"
                "d"
                df
                (DI.fromList [read @Day "2020-01-01", read "2021-12-31"])

-- Preserved: a null inside a date column yields Maybe Day.
testDateWithNulls :: Test
testDateWithNulls = TestLabel "typed_date_nulls" $
    TestCase $
        withCsv "date_nulls" "d,x\n2020-01-01,a\n,b\n2021-12-31,c\n" $ \path -> do
            df <- D.fastReadCsv path
            expectColumn
                "nullable date column"
                "d"
                df
                ( DI.fromList
                    [ Just (read @Day "2020-01-01")
                    , Nothing
                    , Just (read "2021-12-31")
                    ]
                )

-- The scalar tail (last <64 bytes after the SIMD chunks) must honour the
-- quote state from the SIMD region: a quoted field opening before the
-- boundary and closing in the tail may contain separators and newlines.
testQuoteSpansChunkBoundary :: Test
testQuoteSpansChunkBoundary = TestLabel "typed_quote_spans_boundary" $
    TestCase $ do
        let pad = T.replicate 70 "x"
            body = "a,b\n" <> pad <> ",\"with,comma\nand newline\"\n"
        withCsv "boundary_quote" body $ \path -> do
            df <- D.fastReadCsv path
            assertEqual "one data row" 1 (fst (dataframeDimensions df))
            expectColumn
                "quoted field spanning the SIMD/tail boundary"
                "b"
                df
                (DI.fromList @T.Text ["with,comma\nand newline"])

-- An unclosed quote that opens inside the scalar tail must still raise.
testUnclosedQuoteInTail :: Test
testUnclosedQuoteInTail = TestLabel "typed_unclosed_in_tail" $
    TestCase $ do
        let pad = T.replicate 70 "x"
            body = "a\n" <> pad <> "\n\"dangling"
        withCsv "tail_unclosed" body $ \path -> do
            result <- try @D.CsvParseError (D.fastReadCsv path)
            case result of
                Left D.CsvUnclosedQuote -> pure ()
                other ->
                    assertFailure ("expected CsvUnclosedQuote, got " <> show other)

-- S2 pin: inference must land on exactly the same columns (types and
-- values) as an explicit schema declaring the true types.
testInferredMatchesSchema :: Test
testInferredMatchesSchema = TestLabel "typed_inferred_matches_schema" $
    TestCase $ do
        let rows =
                T.unlines
                    [ T.intercalate
                        ","
                        [ T.pack (show i)
                        , T.pack (show (fromIntegral i + 0.25 :: Double))
                        , "name" <> T.pack (show i)
                        , if even i then "True" else "false"
                        , "2024-01-0" <> T.pack (show (1 + i `mod` 9))
                        ]
                    | i <- [1 .. 150 :: Int]
                    ]
            body = "i,d,t,b,dt\n" <> rows
            schema =
                Schema
                    ( M.fromList
                        [ ("i", SType (P.Proxy @Int))
                        , ("d", SType (P.Proxy @Double))
                        , ("t", SType (P.Proxy @T.Text))
                        , ("b", SType (P.Proxy @Bool))
                        , ("dt", SType (P.Proxy @Day))
                        ]
                    )
        withCsv "inferred_vs_schema" body $ \path -> do
            inferred <- D.fastReadCsv path
            explicit <- D.fastReadCsvWithSchema schema path
            assertEqual "inferred == explicit schema" explicit inferred

-- Ingest pin: an inferred string column freezes to a shared-buffer
-- 'PackedText' (no per-row boxed Text header), values byte-identical.
testTextFreezesPacked :: Test
testTextFreezesPacked = TestLabel "typed_text_freezes_packed" $
    TestCase $
        withCsv "freezes_packed" "a,b\n1,x\n2,y\n" $ \path -> do
            df <- D.fastReadCsv path
            case getColumn "b" df of
                Nothing -> assertFailure "missing text column"
                Just col -> do
                    assertBool "text column is PackedText" (DI.isPackedText col)
                    assertEqual "values byte-identical" (DI.fromList @T.Text ["x", "y"]) col

-- The in-memory entry point must agree with the file-based one.
testFromBytesMatchesFile :: Test
testFromBytesMatchesFile = TestLabel "typed_from_bytes" $
    TestCase $ do
        let body = "a,b,c\n1,x,2.5\n2,y,3.5\n,z,\n"
        withCsv "from_bytes" body $ \path -> do
            fromFile <- D.fastReadCsv path
            fromBytes <- D.fastReadCsvFromBytes (TE.encodeUtf8 body)
            assertEqual "file and bytes parses agree" fromFile fromBytes

tests :: [Test]
tests =
    [ testIntOverflowDemotesToDouble
    , testPaddedDoubleStaysDouble
    , testSchemaTextBypassesInference
    , testSchemaStrictFailureThrows
    , testSchemaMaybeReadNullsFailures
    , testNoSafeReadNADemotesToText
    , testMaybeReadMissingTokens
    , testMaybeReadCustomMissingToken
    , testQuotedNumbersParse
    , testBoolAndDateInference
    , testDateWithNulls
    , testQuoteSpansChunkBoundary
    , testUnclosedQuoteInTail
    , testInferredMatchesSchema
    , testTextFreezesPacked
    , testFromBytesMatchesFile
    ]
