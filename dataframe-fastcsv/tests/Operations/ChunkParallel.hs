{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Round-2 (WS-E2) behavior pins for chunk-parallel extraction: a chunked
read must be indistinguishable from the sequential one — same column types
(promotion resolved across chunks), same values, same errors.
-}
module Operations.ChunkParallel (tests) where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Control.Exception (ErrorCall, try)
import Data.Time (Day)
import Data.Word (Word8)
import DataFrame.IO.CSV (
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
 )
import qualified DataFrame.IO.CSV.Fast as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame, getColumn)
import DataFrame.Internal.Schema (Schema (..), SchemaType (..), elements)
import DataFrame.Operations.Typing (SafeReadMode (..))
import Test.HUnit

comma :: Word8
comma = 0x2C

-- | Read @body@ with a forced chunk count (bypasses the size/caps gate).
readChunked :: Int -> ReadOptions -> T.Text -> IO DataFrame
readChunked n opts body =
    D.readSeparatedFromBytesChunks n comma opts (TE.encodeUtf8 body)

readSequential :: ReadOptions -> T.Text -> IO DataFrame
readSequential opts body =
    D.readSeparatedFromBytes comma opts (TE.encodeUtf8 body)

-- | Assert chunked == sequential for several chunk counts.
assertChunkInvariant :: String -> ReadOptions -> T.Text -> Assertion
assertChunkInvariant what opts body = do
    seqDf <- readSequential opts body
    mapM_
        ( \n -> do
            parDf <- readChunked n opts body
            assertEqual (what <> " (chunks=" <> show n <> ")") seqDf parDf
        )
        [2, 3, 5, 8]

expectColumn :: String -> T.Text -> DataFrame -> DI.Column -> Assertion
expectColumn what name df expected = case getColumn name df of
    Nothing -> assertFailure (what <> ": column missing")
    Just col -> assertEqual what expected col

-- A 200-row mixed dataset: nullable ints, doubles, quoted text with
-- embedded separators/newlines/doubled quotes, bools, nullable dates.
mixedBody :: T.Text
mixedBody = "i,d,t,b,dt\n" <> T.concat (map row [0 .. 199 :: Int])
  where
    row k =
        T.intercalate
            ","
            [ if k `mod` 7 == 3 then "" else T.pack (show k)
            , T.pack (show (fromIntegral k * 0.25 :: Double))
            , textCell k
            , if even k then "True" else "false"
            , if k `mod` 11 == 5
                then ""
                else "2024-01-" <> T.justifyRight 2 '0' (T.pack (show (1 + k `mod` 28)))
            ]
            <> "\n"
    textCell k = case k `mod` 5 of
        0 -> "\"a,b" <> T.pack (show k) <> "\""
        1 -> "\"line\nbreak" <> T.pack (show k) <> "\""
        2 -> "\"quote\"\"inside" <> T.pack (show k) <> "\""
        3 -> ""
        _ -> "plain" <> T.pack (show k)

testParallelMatchesSequentialMixed :: Test
testParallelMatchesSequentialMixed =
    TestLabel "par_matches_seq_mixed" . TestCase $
        assertChunkInvariant "mixed nulls+quoted" defaultReadOptions mixedBody

-- A Double cell far past the sample, in the last chunk: every chunk that
-- resolved Int must be re-parsed as Double by the merger.
testPromotionAcrossChunks :: Test
testPromotionAcrossChunks = TestLabel "par_promotion_int_double" . TestCase $ do
    let body =
            "a\n"
                <> T.concat [T.pack (show k) <> "\n" | k <- [0 .. 189 :: Int]]
                <> "190.5\n"
    assertChunkInvariant "late double promotes ints" defaultReadOptions body
    df <- readChunked 4 defaultReadOptions body
    expectColumn
        "merged column is Double"
        "a"
        df
        (DI.fromList @Double ([fromIntegral k | k <- [0 .. 189 :: Int]] <> [190.5]))

-- A Bool cell in the last chunk of an Int column: candidate escalates past
-- Double (the bool cell fails it) all the way to Text.
testDemotionToTextAcrossChunks :: Test
testDemotionToTextAcrossChunks = TestLabel "par_demotion_text" . TestCase $ do
    let body =
            "a\n"
                <> T.concat [T.pack (show k) <> "\n" | k <- [0 .. 189 :: Int]]
                <> "True\n"
    assertChunkInvariant "late bool demotes ints to Text" defaultReadOptions body
    df <- readChunked 4 defaultReadOptions body
    expectColumn
        "merged column is Text"
        "a"
        df
        (DI.fromList @T.Text ([T.pack (show k) | k <- [0 .. 189 :: Int]] <> ["True"]))

-- An all-null column must merge to the same all-null Maybe Text column.
testAllNullColumnAcrossChunks :: Test
testAllNullColumnAcrossChunks = TestLabel "par_all_null" . TestCase $ do
    let body =
            "a,b\n" <> T.concat [",x" <> T.pack (show k) <> "\n" | k <- [0 .. 199 :: Int]]
    assertChunkInvariant "all-null column" defaultReadOptions body
    df <- readChunked 4 defaultReadOptions body
    expectColumn
        "all-null column is Maybe Text"
        "a"
        df
        (DI.fromList @(Maybe T.Text) (replicate 200 Nothing))

-- All-null sample (NoAssumption), data only in the last chunk: all-null
-- chunks must be re-parsed at the type the data chunk resolved.
testLateDataAfterAllNullSample :: Test
testLateDataAfterAllNullSample = TestLabel "par_late_bool" . TestCase $ do
    let cell k
            | k < 150 = ""
            | even k = "True"
            | otherwise = "false"
        body =
            "a,b\n"
                <> T.concat [cell k <> ",x\n" | k <- [0 .. 199 :: Int]]
    assertChunkInvariant "late bools after null sample" defaultReadOptions body
    df <- readChunked 4 defaultReadOptions body
    expectColumn
        "column is Maybe Bool"
        "a"
        df
        ( DI.fromList @(Maybe Bool)
            [ if k < 150 then Nothing else Just (even k)
            | k <- [0 .. 199 :: Int]
            ]
        )

-- Schema + NoSafeRead failure in a late chunk: same error, same row index
-- as the sequential read.
testSchemaErrorSameRowAsSequential :: Test
testSchemaErrorSameRowAsSequential = TestLabel "par_schema_error" . TestCase $ do
    let body =
            "a\n"
                <> T.concat [T.pack (show k) <> "\n" | k <- [0 .. 179 :: Int]]
                <> "boom\n"
                <> T.concat [T.pack (show k) <> "\n" | k <- [181 .. 199 :: Int]]
        opts =
            defaultReadOptions
                { typeSpec =
                    SpecifyTypes
                        (M.toList (elements (Schema (M.fromList [("a", SType (P.Proxy @Int))]))))
                        (typeSpec defaultReadOptions)
                }
    seqErr <- try @ErrorCall (readSequential opts body)
    parErr <- try @ErrorCall (readChunked 4 opts body)
    case (seqErr, parErr) of
        (Left e1, Left e2) -> assertEqual "same schema error" (show e1) (show e2)
        _ -> assertFailure "both reads should raise on the unparseable schema cell"

-- MaybeRead missing tokens with chunked extraction.
testMaybeReadAcrossChunks :: Test
testMaybeReadAcrossChunks = TestLabel "par_mayberead" . TestCase $ do
    let cell k = if k `mod` 13 == 7 then "NA" else T.pack (show k)
        body = "a\n" <> T.concat [cell k <> "\n" | k <- [0 .. 199 :: Int]]
        opts = defaultReadOptions{safeRead = MaybeRead}
    assertChunkInvariant "MaybeRead missing tokens" opts body
    df <- readChunked 4 opts body
    expectColumn
        "column is Maybe Int"
        "a"
        df
        ( DI.fromList @(Maybe Int)
            [ if k `mod` 13 == 7 then Nothing else Just k
            | k <- [0 .. 199 :: Int]
            ]
        )

-- Ragged rows (PadWithNull default) must behave identically chunked.
testRaggedRowsAcrossChunks :: Test
testRaggedRowsAcrossChunks = TestLabel "par_ragged" . TestCase $ do
    let row k
            | k `mod` 9 == 4 = T.pack (show k) <> "\n"
            | k `mod` 9 == 8 = T.pack (show k) <> ",x,extra\n"
            | otherwise = T.pack (show k) <> ",x" <> T.pack (show k) <> "\n"
        body = "a,b\n" <> T.concat (map row [0 .. 199 :: Int])
    assertChunkInvariant "ragged rows pad with null" defaultReadOptions body

-- Date column with nulls: boxed chunk columns must splice correctly.
testDateNullsAcrossChunks :: Test
testDateNullsAcrossChunks = TestLabel "par_date_nulls" . TestCase $ do
    -- A second column keeps null rows from looking like blank lines
    -- (blank lines are skipped by documented behavior).
    let cell k =
            if k `mod` 17 == 9
                then ""
                else "2023-05-" <> T.justifyRight 2 '0' (T.pack (show (1 + k `mod` 28)))
        body = "a,b\n" <> T.concat [cell k <> ",x\n" | k <- [0 .. 199 :: Int]]
    assertChunkInvariant "nullable dates" defaultReadOptions body
    df <- readChunked 4 defaultReadOptions body
    expectColumn
        "column is Maybe Day"
        "a"
        df
        ( DI.fromList @(Maybe Day)
            [ if k `mod` 17 == 9
                then Nothing
                else Just (read ("2023-05-" <> pad (1 + k `mod` 28)))
            | k <- [0 .. 199 :: Int]
            ]
        )
  where
    pad n = if n < 10 then '0' : show n else show n

-- An unclosed quote must still raise under the chunked (parallel) scan.
testUnclosedQuoteAcrossChunks :: Test
testUnclosedQuoteAcrossChunks = TestLabel "par_unclosed_quote" . TestCase $ do
    let body =
            "a\n"
                <> T.concat [T.pack (show k) <> "\n" | k <- [0 .. 199 :: Int]]
                <> "\"dangling"
    r <- try @D.CsvParseError (readChunked 4 defaultReadOptions body)
    case r of
        Left D.CsvUnclosedQuote -> pure ()
        other -> assertFailure ("expected CsvUnclosedQuote, got " <> show other)

-- WS-E2 ingest pin: the parallel byte-level merge wraps the merged shared
-- buffer as 'PackedText' (no Text spine), so the parallel path is also an
-- RSS win. Values stay identical to the sequential read.
testParallelTextFreezesPacked :: Test
testParallelTextFreezesPacked = TestLabel "par_text_freezes_packed" . TestCase $ do
    let body =
            "a,b\n"
                <> T.concat
                    [ "row" <> T.pack (show k) <> ",t" <> T.pack (show k) <> "\n"
                    | k <- [0 .. 199 :: Int]
                    ]
    df <- readChunked 4 defaultReadOptions body
    case getColumn "b" df of
        Nothing -> assertFailure "missing text column"
        Just col -> do
            assertBool "parallel text column is PackedText" (DI.isPackedText col)
            assertEqual
                "parallel text values byte-identical"
                (DI.fromList @T.Text ["t" <> T.pack (show k) | k <- [0 .. 199 :: Int]])
                col

tests :: [Test]
tests =
    [ testParallelMatchesSequentialMixed
    , testPromotionAcrossChunks
    , testDemotionToTextAcrossChunks
    , testAllNullColumnAcrossChunks
    , testLateDataAfterAllNullSample
    , testSchemaErrorSameRowAsSequential
    , testMaybeReadAcrossChunks
    , testRaggedRowsAcrossChunks
    , testDateNullsAcrossChunks
    , testUnclosedQuoteAcrossChunks
    , testParallelTextFreezesPacked
    ]
