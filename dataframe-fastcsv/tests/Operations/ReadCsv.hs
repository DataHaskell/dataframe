{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- Test fixtures inspired by csv-spectrum (https://github.com/max-mapper/csv-spectrum)

module Operations.ReadCsv where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.IO.CSV.Fast (CsvParseError (..))
import qualified DataFrame.IO.CSV.Fast as D

import Control.Exception (try)
import Data.Function (on)
import qualified Data.Proxy as P
import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.IO.CSV (
    RaggedRowPolicy (..),
    ReadOptions (..),
    defaultReadOptions,
 )
import DataFrame.Internal.Column (Column (..), bitmapTestBit)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columns,
    dataframeDimensions,
    getColumn,
 )
import DataFrame.Internal.Schema (Schema (..), SchemaType (..))
import System.Directory (removeFile)
import System.IO (IOMode (..), withFile)
import Test.HUnit
import Type.Reflection (typeRep)

fixtureDir :: FilePath
fixtureDir = "./tests/data/unstable_csv/"

tempDir :: FilePath
tempDir = "./tests/data/unstable_csv/"

--------------------------------------------------------------------------------
-- Pretty-printer
--------------------------------------------------------------------------------

prettyPrintCsv :: FilePath -> DataFrame -> IO ()
prettyPrintCsv = prettyPrintSeparated ','

prettyPrintTsv :: FilePath -> DataFrame -> IO ()
prettyPrintTsv = prettyPrintSeparated '\t'

prettyPrintSeparated :: Char -> FilePath -> DataFrame -> IO ()
prettyPrintSeparated sep filepath df = withFile filepath WriteMode $ \handle -> do
    let (rows, _) = dataframeDimensions df
    let headers = map fst (L.sortBy (compare `on` snd) (M.toList (columnIndices df)))
    TIO.hPutStrLn
        handle
        (T.intercalate (T.singleton sep) (map (escapeField sep) headers))
    -- Write data rows
    mapM_
        (TIO.hPutStrLn handle . T.intercalate (T.singleton sep) . getRowEscaped sep df)
        [0 .. rows - 1]

-- RFC 4180-compliant CSV field escaping: wrap in quotes if the field
-- contains the separator, a line break, or a quote; double any embedded
-- quotes so the parser can reverse the encoding.
escapeField :: Char -> T.Text -> T.Text
escapeField sep field
    | needsQuoting = T.concat ["\"", T.replace "\"" "\"\"" field, "\""]
    | otherwise = field
  where
    needsQuoting =
        T.any (\c -> c == sep || c == '\n' || c == '\r' || c == '"') field

-- | Get a row from the DataFrame with all fields escaped
getRowEscaped :: Char -> DataFrame -> Int -> [T.Text]
getRowEscaped sep df i = V.ifoldr go [] (columns df)
  where
    go :: Int -> Column -> [T.Text] -> [T.Text]
    go _ (BoxedColumn bm (c :: V.Vector a)) acc = case c V.!? i of
        Just e -> escapeField sep textRep : acc
          where
            isNull = case bm of Just bm' -> not (bitmapTestBit bm' i); Nothing -> False
            textRep =
                if isNull
                    then ""
                    else case testEquality (typeRep @a) (typeRep @T.Text) of
                        Just Refl -> e
                        Nothing -> T.pack (show e)
        Nothing -> acc
    go _ (UnboxedColumn bm c) acc = case c VU.!? i of
        Just e ->
            let isNull = case bm of Just bm' -> not (bitmapTestBit bm' i); Nothing -> False
                textRep = if isNull then "" else T.pack (show e)
             in escapeField sep textRep : acc
        Nothing -> acc

testFastCsv :: String -> FilePath -> Test
testFastCsv name csvPath = TestLabel ("fast_roundtrip_" <> name) $ TestCase $ do
    dfOriginal <- D.fastReadCsv csvPath
    let tempPath = tempDir <> "temp_fast_" <> name <> ".csv"
    prettyPrintCsv tempPath dfOriginal
    dfRoundtrip <- D.fastReadCsv tempPath
    assertEqual
        ("Fast round-trip should produce equivalent DataFrame for " <> name)
        dfOriginal
        dfRoundtrip
    removeFile tempPath

testTsv :: String -> FilePath -> Test
testTsv name tsvPath = TestLabel ("roundtrip_tsv_" <> name) $ TestCase $ do
    dfOriginal <- D.readTsvFast tsvPath
    let tempPath = tempDir <> "temp_" <> name <> ".tsv"
    prettyPrintTsv tempPath dfOriginal
    dfRoundtrip <- D.readTsvFast tempPath
    assertEqual
        ("TSV round-trip should produce equivalent DataFrame for " <> name)
        dfOriginal
        dfRoundtrip
    removeFile tempPath

-- Individual round-trip test cases for each fixture

testSimpleFast :: Test
testSimpleFast = testFastCsv "simple" (fixtureDir <> "simple.csv")

testCommaInQuotesFast :: Test
testCommaInQuotesFast = testFastCsv "comma_in_quotes" (fixtureDir <> "comma_in_quotes.csv")

testEscapedQuotesFast :: Test
testEscapedQuotesFast = testFastCsv "escaped_quotes" (fixtureDir <> "escaped_quotes.csv")

testNewlinesFast :: Test
testNewlinesFast = testFastCsv "newlines" (fixtureDir <> "newlines.csv")

testUtf8Fast :: Test
testUtf8Fast = testFastCsv "utf8" (fixtureDir <> "utf8.csv")

testQuotesAndNewlinesFast :: Test
testQuotesAndNewlinesFast = testFastCsv "quotes_and_newlines" (fixtureDir <> "quotes_and_newlines.csv")

testEmptyValuesFast :: Test
testEmptyValuesFast = testFastCsv "empty_values" (fixtureDir <> "empty_values.csv")

testJsonDataFast :: Test
testJsonDataFast = testFastCsv "json_data" (fixtureDir <> "json_data.csv")

testCrlfCsv :: Test
testCrlfCsv = TestLabel "malformed_crlf_csv" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "crlf.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "crlf.csv: 2 data rows" 2 rows
    assertEqual "crlf.csv: 2 columns" 2 cols
    case getColumn "name" df of
        Nothing -> assertFailure "crlf.csv: column 'name' missing"
        Just col ->
            assertEqual
                "crlf.csv: name has no \\r"
                (DI.fromList @T.Text ["Alice", "Bob"])
                col

testCrlfTsv :: Test
testCrlfTsv = TestLabel "malformed_crlf_tsv" $ TestCase $ do
    df <- D.readTsvFast (fixtureDir <> "crlf.tsv")
    let (rows, _) = dataframeDimensions df
    assertEqual "crlf.tsv: 1 data row" 1 rows
    case getColumn "name" df of
        Nothing -> assertFailure "crlf.tsv: column 'name' missing"
        Just col ->
            assertEqual
                "crlf.tsv: name has no \\r"
                (DI.fromList @T.Text ["Alice"])
                col

testHeaderOnly :: Test
testHeaderOnly = TestLabel "malformed_header_only" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "header_only.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "header_only.csv: 0 data rows" 0 rows
    assertEqual "header_only.csv: 3 columns" 3 cols
    let names = map fst . L.sortBy (compare `on` snd) . M.toList $ columnIndices df
    assertEqual "header_only.csv: column names" ["first", "second", "third"] names

testTrailingBlankLine :: Test
testTrailingBlankLine = TestLabel "malformed_trailing_blank_line" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "trailing_blank_line.csv")
    -- blank line contributes 1 extra delimiter; (3+3+1) div 3 = 2, numRow=1
    assertEqual
        "trailing_blank_line.csv: 1 data row visible"
        1
        (fst (dataframeDimensions df))

testAllEmptyRow :: Test
testAllEmptyRow = TestLabel "malformed_all_empty_row" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "all_empty_row.csv")
    assertEqual "all_empty_row.csv: 1 data row" 1 (fst (dataframeDimensions df))
    let checkEmpty colName =
            case getColumn colName df of
                Nothing -> assertFailure ("column '" <> T.unpack colName <> "' missing")
                Just col ->
                    assertEqual
                        (T.unpack colName <> " is Nothing (empty field → null)")
                        (DI.fromList @(Maybe T.Text) [Nothing])
                        col
    mapM_ checkEmpty ["a", "b", "c"]

testSingleCol :: Test
testSingleCol = TestLabel "malformed_single_col" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "single_col.csv")
    let (rows, cols) = dataframeDimensions df
    assertEqual "single_col.csv: 3 data rows" 3 rows
    assertEqual "single_col.csv: 1 column" 1 cols
    case getColumn "name" df of
        Nothing -> assertFailure "single_col.csv: column 'name' missing"
        Just col ->
            assertEqual
                "single_col.csv: correct values"
                (DI.fromList @T.Text ["Alice", "Bob", "Carol"])
                col

-- After the RFC 4180 alignment, whitespace inside unquoted fields is
-- preserved verbatim.  Users who want leading/trailing whitespace
-- stripped can opt in via the fastCsvTrimUnquoted knob (Step 7).
testWhitespaceFields :: Test
testWhitespaceFields = TestLabel "malformed_whitespace_fields" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "whitespace_fields.csv")
    assertEqual
        "whitespace_fields.csv: 2 data rows"
        2
        (fst (dataframeDimensions df))
    case getColumn "name" df of
        Nothing -> assertFailure "whitespace_fields.csv: 'name' missing"
        Just col ->
            assertEqual
                "name preserves padding"
                (DI.fromList @T.Text ["  Alice  ", "  Bob  "])
                col
    case getColumn "city" df of
        Nothing -> assertFailure "whitespace_fields.csv: 'city' missing"
        Just col ->
            assertEqual
                "city preserves padding"
                (DI.fromList @T.Text ["  New York", "  Los Angeles"])
                col

-- File: a,b,c header; row "1,2" (short); row "X,Y,Z" (full).
-- After the row-index refactor each line is classified individually:
-- the short row survives with a null-padded column 'c'; the full row
-- keeps its value.  This replaces the earlier "X bleeds from next row"
-- behaviour, which was data corruption masquerading as a passing test.
testMissingFields :: Test
testMissingFields = TestLabel "malformed_missing_fields" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "missing_fields.csv")
    assertEqual
        "missing_fields.csv: both rows visible"
        2
        (fst (dataframeDimensions df))
    case getColumn "a" df of
        Nothing -> assertFailure "missing_fields.csv: column 'a' missing"
        Just col ->
            assertEqual
                "missing_fields.csv: col 'a' = [1, X]"
                (DI.fromList @T.Text ["1", "X"])
                col
    case getColumn "c" df of
        Nothing -> assertFailure "missing_fields.csv: column 'c' missing"
        Just col ->
            assertEqual
                "missing_fields.csv: col 'c' pads short row with Nothing"
                (DI.fromList @(Maybe T.Text) [Nothing, Just "Z"])
                col

-- File: a,b,c header; row "1,2,3,EXTRA" (over-long).
-- Under the default ragged-row policy we read columns 0..numCol-1 and
-- silently drop the EXTRA field.  A stricter `RaggedRowPolicy = Error`
-- will come with Step 7's ReadOptions knob.
testExtraFieldsTruncate :: Test
testExtraFieldsTruncate =
    TestLabel "malformed_extra_fields_truncate" $ TestCase $ do
        df <- D.readCsvFast (fixtureDir <> "extra_fields.csv")
        assertEqual
            "extra_fields.csv: 1 data row"
            1
            (fst (dataframeDimensions df))
        assertEqual
            "extra_fields.csv: 3 columns (extras dropped)"
            3
            (snd (dataframeDimensions df))
        case getColumn "c" df of
            Nothing -> assertFailure "extra_fields.csv: 'c' missing"
            Just col ->
                assertEqual
                    "extra_fields.csv: col 'c' = [3]"
                    (DI.fromList @Int [3])
                    col

testNoTrailingNewline :: Test
testNoTrailingNewline = TestLabel "malformed_no_trailing_newline" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "no_trailing_newline.csv")
    assertEqual
        "no_trailing_newline.csv: 1 data row"
        1
        (fst (dataframeDimensions df))
    case getColumn "name" df of
        Nothing -> assertFailure "no_trailing_newline.csv: 'name' missing"
        Just col -> assertEqual "name = Alice" (DI.fromList @T.Text ["Alice"]) col
    case getColumn "city" df of
        Nothing -> assertFailure "no_trailing_newline.csv: 'city' missing"
        Just col ->
            assertEqual
                "city = London (synthetic delimiter worked)"
                (DI.fromList @T.Text ["London"])
                col

-- Regression: a zero-byte CSV used to divide by zero in the row-stride math
-- (`VS.length indices `div` numCol` with numCol == 0). Now it returns an
-- empty DataFrame cleanly.
testEmptyFile :: Test
testEmptyFile = TestLabel "malformed_empty_file" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "empty_file.csv")
    assertEqual "empty_file.csv: 0 rows" 0 (fst (dataframeDimensions df))
    assertEqual "empty_file.csv: 0 columns" 0 (snd (dataframeDimensions df))

-- RFC 4180 quote unescaping: `""` decodes to a single `"`.  The escaped
-- quote fixture contains the two canonical shapes: an embedded doubled
-- quote and a plain quoted number.
testRfc4180EscapedQuote :: Test
testRfc4180EscapedQuote =
    TestLabel "rfc4180_escaped_quote" $ TestCase $ do
        df <- D.readCsvFast (fixtureDir <> "escaped_quotes.csv")
        assertEqual
            "escaped_quotes.csv: 2 data rows"
            2
            (fst (dataframeDimensions df))
        case getColumn "b" df of
            Nothing -> assertFailure "escaped_quotes.csv: 'b' missing"
            Just col ->
                assertEqual
                    "escaped_quotes.csv: doubled quotes unescaped to single quote"
                    (DI.fromList @T.Text ["ha \"ha\" ha", "4"])
                    col

-- Whitespace inside double quotes is data, not padding, so a quoted
-- "  padded  " survives the parser unchanged.
testQuotedWhitespacePreserved :: Test
testQuotedWhitespacePreserved =
    TestLabel "quoted_whitespace_preserved" $ TestCase $ do
        let path = fixtureDir <> "quoted_whitespace.csv"
        TIO.writeFile path "a\n\"  padded  \"\n"
        df <- D.readCsvFast path
        removeFile path
        case getColumn "a" df of
            Nothing -> assertFailure "quoted_whitespace.csv: 'a' missing"
            Just col ->
                assertEqual
                    "quoted_whitespace.csv: whitespace inside quotes preserved"
                    (DI.fromList @T.Text ["  padded  "])
                    col

-- With `fastCsvOnRaggedRow = RaiseOnRagged`, the short row in
-- missing_fields.csv must be rejected with a CsvRaggedRow carrying the
-- row index and the expected/actual field counts.
testRaiseOnRagged :: Test
testRaiseOnRagged = TestLabel "raise_on_ragged" $ TestCase $ do
    let opts = defaultReadOptions{fastCsvOnRaggedRow = RaiseOnRagged}
    result <-
        try (D.fastReadCsvWithOpts opts (fixtureDir <> "missing_fields.csv"))
    case (result :: Either CsvParseError DataFrame) of
        Left (CsvRaggedRow r expected actual) -> do
            assertEqual "ragged row index" 1 r
            assertEqual "expected field count" 3 expected
            assertEqual "actual field count" 2 actual
        Left other ->
            assertFailure
                ("expected CsvRaggedRow, got " <> show other)
        Right _ ->
            assertFailure
                "fastReadCsvWithOpts RaiseOnRagged should have thrown"

-- With `fastCsvTrimUnquoted = True` we recover the legacy behaviour of
-- stripping leading/trailing whitespace from unquoted fields.
testTrimUnquotedOpt :: Test
testTrimUnquotedOpt = TestLabel "trim_unquoted_opt" $ TestCase $ do
    let opts = defaultReadOptions{fastCsvTrimUnquoted = True}
    df <- D.fastReadCsvWithOpts opts (fixtureDir <> "whitespace_fields.csv")
    case getColumn "name" df of
        Nothing -> assertFailure "name missing"
        Just col ->
            assertEqual
                "trim_unquoted_opt: name stripped"
                (DI.fromList @T.Text ["Alice", "Bob"])
                col

-- A declared 'Schema' overrides type inference: the 'a' column is
-- forced to 'Int' and 'b' to 'Double', regardless of what the first few
-- rows look like.
testSchemaPushdown :: Test
testSchemaPushdown = TestLabel "schema_pushdown" $ TestCase $ do
    let path = fixtureDir <> "schema_pushdown.csv"
    TIO.writeFile path "a,b\n1,1.5\n2,2.5\n"
    let schema =
            Schema $
                M.fromList
                    [ ("a", SType (P.Proxy @Int))
                    , ("b", SType (P.Proxy @Double))
                    ]
    df <- D.fastReadCsvWithSchema schema path
    removeFile path
    case getColumn "a" df of
        Nothing -> assertFailure "schema_pushdown: 'a' missing"
        Just col -> assertEqual "a is Int" (DI.fromList @Int [1, 2]) col
    case getColumn "b" df of
        Nothing -> assertFailure "schema_pushdown: 'b' missing"
        Just col -> assertEqual "b is Double" (DI.fromList @Double [1.5, 2.5]) col

-- `fastReadCsvProj` returns only the requested columns, in the order
-- requested; unreferenced columns never appear in the result.
testProjection :: Test
testProjection = TestLabel "projection" $ TestCase $ do
    let path = fixtureDir <> "projection.csv"
    TIO.writeFile path "a,b,c\n1,2,3\n4,5,6\n"
    df <- D.fastReadCsvProj ["c", "a"] path
    removeFile path
    let (rows, cols) = dataframeDimensions df
    assertEqual "projection: 2 rows" 2 rows
    assertEqual "projection: 2 columns" 2 cols
    let names =
            map fst . L.sortBy (compare `on` snd) . M.toList $ columnIndices df
    assertEqual "projection preserves order" ["c", "a"] names

-- An unmatched `"` used to silently swallow the rest of the file: the
-- SIMD scanner's PCLMUL quote-parity chain never resets, so every byte
-- after the stray quote becomes "inside quotes."  We now raise
-- 'CsvUnclosedQuote' rather than returning a corrupted DataFrame.
testUnclosedQuote :: Test
testUnclosedQuote = TestLabel "malformed_unclosed_quote" $ TestCase $ do
    let path = fixtureDir <> "unclosed_quote.csv"
    TIO.writeFile path "a,b\n1,\"unterminated\n"
    result <- try (D.readCsvFast path)
    removeFile path
    case (result :: Either CsvParseError DataFrame) of
        Left CsvUnclosedQuote -> return ()
        Left other ->
            assertFailure
                ("expected CsvUnclosedQuote, got " <> show other)
        Right _ ->
            assertFailure
                "readCsvFast should have thrown CsvUnclosedQuote on input with a stray quote"

-- UTF-8 BOM (EF BB BF) must be stripped before the first header is parsed.
-- Without the strip, `getColumn "name"` fails because the column is keyed
-- under the three-byte-prefixed "\xEF\xBB\xBFname".
testUtf8Bom :: Test
testUtf8Bom = TestLabel "malformed_utf8_bom" $ TestCase $ do
    df <- D.readCsvFast (fixtureDir <> "utf8_bom.csv")
    assertEqual "utf8_bom.csv: 2 data rows" 2 (fst (dataframeDimensions df))
    assertEqual "utf8_bom.csv: 2 columns" 2 (snd (dataframeDimensions df))
    let names =
            map fst . L.sortBy (compare `on` snd) . M.toList $ columnIndices df
    assertEqual "utf8_bom.csv: header names" ["name", "value"] names
    case getColumn "name" df of
        Nothing -> assertFailure "utf8_bom.csv: 'name' missing"
        Just col ->
            assertEqual
                "utf8_bom.csv: name values"
                (DI.fromList @T.Text ["Alice", "Bob"])
                col

tests :: [Test]
tests =
    [ testSimpleFast
    , testCommaInQuotesFast
    , testQuotesAndNewlinesFast
    , testEscapedQuotesFast
    , testNewlinesFast
    , testUtf8Fast
    , testQuotesAndNewlinesFast
    , testEmptyValuesFast
    , testJsonDataFast
    , testCrlfCsv
    , testCrlfTsv
    , testHeaderOnly
    , testTrailingBlankLine
    , testAllEmptyRow
    , testSingleCol
    , testWhitespaceFields
    , testMissingFields
    , testExtraFieldsTruncate
    , testNoTrailingNewline
    , testEmptyFile
    , testUtf8Bom
    , testRfc4180EscapedQuote
    , testQuotedWhitespacePreserved
    , testUnclosedQuote
    , testRaiseOnRagged
    , testTrimUnquotedOpt
    , testSchemaPushdown
    , testProjection
    ]
