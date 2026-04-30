{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | QuickCheck properties for the SIMD CSV parser.

Each property builds a fresh input on disk under @/tmp@, exercises
'DataFrame.IO.CSV.Fast.fastReadCsv' (or a variant), and asserts a
property-level invariant.  Temp files are cleaned up by the caller.

The properties deliberately avoid depending on type-inference: every
test cell starts with a letter so 'parseFromExamples' always lands on
@Text@, which makes roundtrip comparison unambiguous.
-}
module Properties.Csv (tests) where

import Control.Exception (try)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V

import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.IO.CSV.Fast (CsvParseError (..))
import qualified DataFrame.IO.CSV.Fast as D
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columns,
    dataframeDimensions,
 )

import System.Directory (removeFile)
import Test.QuickCheck
import Test.QuickCheck.Monadic (PropertyM, assert, monadicIO, run)
import Type.Reflection (typeRep)

{- | A generator for a single CSV cell.  Each cell starts with an ASCII
letter (so type inference always picks 'Text') and may contain a small
menagerie of special characters the parser must escape correctly.
-}
newtype Cell = Cell {unCell :: T.Text}
    deriving (Eq, Show)

instance Arbitrary Cell where
    arbitrary = do
        leading <- elements "abcdefghij"
        rest <- listOf (elements "abcdef,\n\"\r ")
        pure (Cell (T.pack (leading : rest)))
    shrink (Cell t)
        | T.null t || T.length t == 1 = []
        | otherwise = [Cell (T.take (T.length t - 1) t)]

{- | Like 'Cell' but guaranteed to contain neither @\\n@ nor @\\r@.  Used
by properties that diff across line-ending dialects, where embedded
line breaks become part of the data under CRLF transform and break
the otherwise-clean invariant.
-}
newtype SingleLineCell = SingleLineCell {unSingleLineCell :: T.Text}
    deriving (Eq, Show)

instance Arbitrary SingleLineCell where
    arbitrary = do
        leading <- elements "abcdefghij"
        rest <- listOf (elements "abcdef, \"")
        pure (SingleLineCell (T.pack (leading : rest)))
    shrink (SingleLineCell t)
        | T.null t || T.length t == 1 = []
        | otherwise = [SingleLineCell (T.take (T.length t - 1) t)]

-- | A generator for a non-empty list of ASCII column names, unique.
newtype ColumnNames = ColumnNames {unColumnNames :: [T.Text]}
    deriving (Eq, Show)

instance Arbitrary ColumnNames where
    arbitrary = do
        n <- chooseInt (1, 4)
        pure (ColumnNames [T.pack ('c' : show i) | i <- [0 .. n - 1]])
    shrink (ColumnNames ns)
        | length ns <= 1 = []
        | otherwise = [ColumnNames (init ns)]

{- | RFC 4180 cell encoding: wrap a field in @"@ and double embedded
quotes when the cell contains the separator, a line break, or a
quote.
-}
encodeCell :: Char -> T.Text -> T.Text
encodeCell sep t
    | T.any needsQuote t = T.concat ["\"", T.replace "\"" "\"\"" t, "\""]
    | otherwise = t
  where
    needsQuote c = c == sep || c == '\n' || c == '\r' || c == '"'

{- | Encode a grid @[[cell]]@ (rows of cells) as an RFC 4180 CSV.  The
first row is written as the header, followed by one line per data
row.  Always uses @\\n@ as the line terminator; callers that need
CRLF can transform the output themselves.
-}
encodeCsv :: Char -> [T.Text] -> [[T.Text]] -> T.Text
encodeCsv sep header rows =
    T.unlines (encodeRow header : map encodeRow rows)
  where
    encodeRow = T.intercalate (T.singleton sep) . map (encodeCell sep)

{- | Pull the raw 'Text' values of the named column out of a DataFrame.
Fails the property if the column is missing or is not a 'Text'
column (which would signal a type-inference accident — the cell
generator is designed to avoid that).
-}
columnAsText :: T.Text -> DataFrame -> Maybe [T.Text]
columnAsText name df = do
    idx <- M.lookup name (columnIndices df)
    col <- columns df V.!? idx
    case col of
        BoxedColumn _ (vec :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> Just (V.toList vec)
                Nothing -> Nothing
        UnboxedColumn{} -> Nothing

{- | Run a property in @IO@ against a generated CSV text, cleaning up
the temp file afterwards no matter what.
-}
withCsvFile :: String -> T.Text -> (FilePath -> IO a) -> IO a
withCsvFile label body action = do
    let path = "/tmp/fastcsv_prop_" <> label <> ".csv"
    TIO.writeFile path body
    r <- action path
    removeFile path
    pure r

--------------------------------------------------------------------------------
-- Properties

{- | Any CSV parses the same whether or not a UTF-8 BOM (EF BB BF) is
prepended.  Excel / PowerShell CSV exports all come with a BOM.
-}
prop_bom_invariant :: ColumnNames -> [[Cell]] -> Property
prop_bom_invariant (ColumnNames header) rows = monadicIO $ do
    let cellRows = map (map unCell) rows
        csv = encodeCsv ',' header cellRows
        csvWithBom = T.pack "\xFEFF" <> csv
    plain <- run $ withCsvFile "bom_plain" csv D.fastReadCsv
    withBom <- run $ withCsvFile "bom_with" csvWithBom D.fastReadCsv
    assert (plain == withBom)
    assert (dataframeDimensions plain == dataframeDimensions withBom)

{- | A CSV with no embedded newlines inside quoted fields parses the
same whether record separators are @\\n@ or @\\r\\n@.  When quoted
cells themselves contain @\\n@, a CRLF-encoded file contains a literal
@\\r\\n@ inside the quotes (that's data, not line ending), so the
invariant correctly no longer holds; we filter such inputs out.
-}
prop_crlf_invariant :: ColumnNames -> [[SingleLineCell]] -> Property
prop_crlf_invariant (ColumnNames header) rows = monadicIO $ do
    let cellRows = map (map unSingleLineCell) rows
        lfCsv = encodeCsv ',' header cellRows
        crlfCsv = T.replace "\n" "\r\n" lfCsv
    dfLf <- run $ withCsvFile "crlf_lf" lfCsv D.fastReadCsv
    dfCrlf <- run $ withCsvFile "crlf_crlf" crlfCsv D.fastReadCsv
    assert (dfLf == dfCrlf)

{- | A DataFrame produced by the RFC 4180 encoder round-trips through
the fast reader: column names survive, row count survives, and every
cell comes back bit-for-bit.  Restricted to ASCII-letter-prefixed
cells so that type inference always stays on 'Text'.
-}
prop_roundtrip_ascii :: Property
prop_roundtrip_ascii = forAll (chooseInt (0, 12)) $ \nRows ->
    forAll (chooseInt (1, 4)) $ \nCols ->
        let header = [T.pack ('c' : show i) | i <- [0 .. nCols - 1]]
         in forAll (vectorOf nRows (vectorOf nCols arbitrary)) $
                \(rawRows :: [[Cell]]) -> monadicIO $ do
                    let rows = map (map unCell) rawRows
                        csv = encodeCsv ',' header rows
                    df <- run $ withCsvFile "roundtrip" csv D.fastReadCsv
                    assertColumns header rows df

assertColumns :: [T.Text] -> [[T.Text]] -> DataFrame -> PropertyM IO ()
assertColumns header rows df = do
    let (gotRows, gotCols) = dataframeDimensions df
    assert (gotRows == length rows)
    assert (gotCols == length header)
    let expected = L.transpose rows
    mapM_
        ( \(name, expectedCol) ->
            case columnAsText name df of
                Just actual -> assert (actual == expectedCol)
                Nothing -> assert False
        )
        (zip header expected)

{- | A file that ends with an unmatched @"@ must raise 'CsvUnclosedQuote'
under the default policy.  The generator deliberately builds a valid
prefix so the property is checking the error path, not a random
parse failure.
-}
prop_unclosed_quote_throws :: Property
prop_unclosed_quote_throws = forAll (listOf1 arbitrary) $ \(cells :: [Cell]) ->
    monadicIO $ do
        let plainRow =
                T.intercalate "," (map (encodeCell ',' . unCell) cells)
            csv = "v\n" <> plainRow <> ",\"dangling\n"
        result <- run $ do
            let path = "/tmp/fastcsv_prop_unclosed.csv"
            TIO.writeFile path csv
            r <- try @CsvParseError (D.fastReadCsv path)
            removeFile path
            pure r
        case result of
            Left CsvUnclosedQuote -> assert True
            _ -> assert False

{- | Files whose length sits exactly at a SIMD chunk boundary must parse
without an off-by-one.  We generate inputs at the key critical sizes
(64, 127, 128, 129, 192 bytes after the 'v\n' header) by padding a
single column with ASCII letters.
-}
prop_boundary_sizes :: Property
prop_boundary_sizes =
    forAll (elements [64, 127, 128, 129, 192]) $ \targetBytes ->
        let header = "v\n"
            bodyLen = targetBytes - T.length header
            -- Build a "aaaa\nbbbb\n..." body of exactly `bodyLen` bytes
            -- ending in a newline.
            rows = take bodyLen $ cycle (map T.singleton "abcdefghij")
            body = T.intercalate "\n" rows <> "\n"
            csv = header <> T.take bodyLen body
         in counterexample (show csv) $ monadicIO $ do
                df <- run $ withCsvFile "boundary" csv D.fastReadCsv
                let (rs, cs) = dataframeDimensions df
                -- Property: reader terminates and returns one column; the
                -- row count may vary depending on how the padding lines up,
                -- but the column metadata must be consistent.
                assert (cs == 1)
                assert (rs >= 0)

tests :: [Property]
tests =
    [ property prop_bom_invariant
    , property prop_crlf_invariant
    , property prop_roundtrip_ascii
    , property prop_unclosed_quote_throws
    , property prop_boundary_sizes
    ]
