{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Golden semantics for the default CSV reader, pinned against the
cassava-based oracle (2026-06-12) before the strict-scanner rewrite.
Ragged-row cases encode the one intentional change: pad-with-null (audit D6).
-}
module IO.CsvGolden (tests) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Text as T

import Control.Exception (SomeException, evaluate, try)
import Data.List (isInfixOf)
import Data.Time.Calendar (Day, fromGregorian)
import DataFrame.IO.CSV
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (
    columnIndices,
    dataframeDimensions,
    forceDataFrame,
    getColumn,
 )
import DataFrame.Operations.Typing (SafeReadMode (..))
import DataFrame.Schema (schemaType)
import Test.HUnit

data Expect
    = Cols (Int, Int) [(T.Text, DI.Column)]
    | Err String

ints :: [Int] -> DI.Column
ints = DI.fromList
mints :: [Maybe Int] -> DI.Column
mints = DI.fromList
dbls :: [Double] -> DI.Column
dbls = DI.fromList
texts :: [T.Text] -> DI.Column
texts = DI.fromList
mtexts :: [Maybe T.Text] -> DI.Column
mtexts = DI.fromList
eti :: [Either T.Text Int] -> DI.Column
eti = DI.fromList
ett :: [Either T.Text T.Text] -> DI.Column
ett = DI.fromList
days :: [Day] -> DI.Column
days = DI.fromList
boolsC :: [Bool] -> DI.Column
boolsC = DI.fromList

sample :: Int -> ReadOptions
sample n = defaultReadOptions{typeSpec = InferFromSample n}

goldenCase :: (String, ReadOptions, BS.ByteString, Expect) -> Test
goldenCase (label, opts, input, expect) = TestLabel label $ TestCase $ do
    r <- try $ do
        df <- decodeSeparated opts (BL.fromStrict input)
        evaluate (forceDataFrame df)
    case (expect, r) of
        (Err sub, Left (e :: SomeException)) ->
            assertBool
                (label <> ": error should mention " <> show sub <> ", got " <> show e)
                (sub `isInfixOf` show e)
        (Err _, Right _) -> assertFailure (label <> ": expected an error")
        (Cols _ _, Left (e :: SomeException)) ->
            assertFailure (label <> ": unexpected error " <> show e)
        (Cols dims cols, Right df) -> do
            assertEqual (label <> ": dims") dims (dataframeDimensions df)
            mapM_
                ( \(name, expected) -> case getColumn name df of
                    Nothing -> assertFailure (label <> ": missing column " <> show name)
                    Just actual ->
                        assertEqual (label <> ": column " <> show name) expected actual
                )
                cols

goldenCases :: [(String, ReadOptions, BS.ByteString, Expect)]
goldenCases =
    [
        ( "basic"
        , defaultReadOptions
        , "a,b,c\n1,2.5,x\n2,3.5,y\n"
        , Cols
            (2, 3)
            [("a", ints [1, 2]), ("b", dbls [2.5, 3.5]), ("c", texts ["x", "y"])]
        )
    ,
        ( "quoted_doubled"
        , defaultReadOptions
        , "a,b\n\"x,1\",\"he said \"\"hi\"\"\"\n\"multi\nline\",plain\n"
        , Cols
            (2, 2)
            [("a", texts ["x,1", "multi\nline"]), ("b", texts ["he said \"hi\"", "plain"])]
        )
    ,
        ( "crlf"
        , defaultReadOptions
        , "a,b\r\n1,x\r\n2,y\r\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "lone_cr"
        , defaultReadOptions
        , "a,b\r1,x\r2,y"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "blank_lines"
        , defaultReadOptions
        , "a,b\n\n1,x\n\n\n2,y\n\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "lone_cr_line"
        , defaultReadOptions
        , "a,b\r\n\r1,x\r\n"
        , Cols (1, 2) [("a", ints [1]), ("b", texts ["x"])]
        )
    ,
        ( "no_eof_newline"
        , defaultReadOptions
        , "a,b\n1,x"
        , Cols (1, 2) [("a", ints [1]), ("b", texts ["x"])]
        )
    ,
        ( "missing_numeric"
        , defaultReadOptions
        , "a,b\n1,2\nNA,3\n,4\nnan,5\n"
        , Cols
            (4, 2)
            [("a", mints [Just 1, Nothing, Nothing, Nothing]), ("b", ints [2, 3, 4, 5])]
        )
    ,
        ( "missing_text"
        , defaultReadOptions
        , "t,u\nfoo,1\nNA,2\n ,3\nbar,4\n"
        , Cols
            (4, 2)
            [ ("t", mtexts [Just "foo", Nothing, Nothing, Just "bar"])
            , ("u", ints [1, 2, 3, 4])
            ]
        )
    ,
        ( "ws_padding"
        , defaultReadOptions
        , "a,b\n 1 , x \n2,y\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "tabs_strip"
        , defaultReadOptions
        , "a,b\n\t1\t,\tx\t\n2,y\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "ragged_short_D6"
        , defaultReadOptions
        , "a,b,c\n1,2,3\n4,5\n6,7,8\n"
        , Cols
            (3, 3)
            [ ("a", ints [1, 4, 6])
            , ("b", ints [2, 5, 7])
            , ("c", mints [Just 3, Nothing, Just 8])
            ]
        )
    ,
        ( "ragged_long"
        , defaultReadOptions
        , "a,b\n1,2\n3,4,5\n6,7\n"
        , Cols (3, 2) [("a", ints [1, 3, 6]), ("b", ints [2, 4, 7])]
        )
    ,
        ( "all_rows_ragged_short_D6"
        , defaultReadOptions
        , "a,b,c\n1,2\n3,4\n"
        , Cols
            (2, 3)
            [("a", ints [1, 3]), ("b", ints [2, 4]), ("c", mtexts [Nothing, Nothing])]
        )
    ,
        ( "maybe_read_ragged_D6"
        , defaultReadOptions{safeRead = MaybeRead}
        , "a,b\n1,2\n3\n"
        , Cols (2, 2) [("a", mints [Just 1, Just 3]), ("b", mints [Just 2, Nothing])]
        )
    ,
        ( "either_read_ragged_D6"
        , defaultReadOptions{safeRead = EitherRead}
        , "a,b\n1,2\n3\n"
        , Cols (2, 2) [("a", eti [Right 1, Right 3]), ("b", eti [Right 2, Left ""])]
        )
    , ("stray_quote_mid", defaultReadOptions, "a,b\nx\"y,2\n", Err "")
    , ("quote_garbage", defaultReadOptions, "a,b\n\"x\"y,2\n", Err "")
    , ("space_then_quote", defaultReadOptions, "a,b\n \"x\",2\n", Err "")
    , ("quote_space_garbage", defaultReadOptions, "a,b\n\"x\" ,2\n", Err "")
    , ("quote_at_field_end", defaultReadOptions, "a,b\n1,x\"\n", Err "")
    , ("doubled_then_garbage", defaultReadOptions, "a,b\n\"\"x,2\n", Err "")
    ,
        ( "quoted_empty"
        , defaultReadOptions
        , "a,b\n\"\",2\n"
        , Cols (1, 2) [("a", mtexts [Nothing]), ("b", ints [2])]
        )
    ,
        ( "quoted_empty_row_skipped"
        , defaultReadOptions
        , "a\nx\n\"\"\ny\n"
        , Cols (2, 1) [("a", texts ["x", "y"])]
        )
    ,
        ( "noheader"
        , defaultReadOptions{headerSpec = NoHeader}
        , "1,2\n3,4\n"
        , Cols (2, 2) [("0", ints [1, 3]), ("1", ints [2, 4])]
        )
    ,
        ( "providenames_fewer"
        , defaultReadOptions{headerSpec = ProvideNames ["x"]}
        , "1,2\n3,4\n"
        , Cols (2, 2) [("x", ints [1, 3]), ("1", ints [2, 4])]
        )
    ,
        ( "providenames_more_D6"
        , defaultReadOptions{headerSpec = ProvideNames ["x", "y", "z"]}
        , "1,2\n3,4\n"
        , Cols
            (2, 3)
            [("x", ints [1, 3]), ("y", ints [2, 4]), ("z", mtexts [Nothing, Nothing])]
        )
    ,
        ( "either_read"
        , (sample 2){safeRead = EitherRead}
        , "a,b\n1,x\n2,y\nz,\n"
        , Cols
            (3, 2)
            [ ("a", eti [Right 1, Right 2, Left "z"])
            , ("b", ett [Right "x", Right "y", Left ""])
            ]
        )
    ,
        ( "maybe_read"
        , defaultReadOptions{safeRead = MaybeRead}
        , "a,b\n1,x\nNA,y\n"
        , Cols (2, 2) [("a", mints [Just 1, Nothing]), ("b", mtexts [Just "x", Just "y"])]
        )
    ,
        ( "schema_int_bad"
        , defaultReadOptions{typeSpec = SpecifyTypes [("a", schemaTypeInt)] NoInference}
        , "a,b\n1,p\nx,q\n3,r\n"
        , Cols
            (3, 2)
            [("a", mints [Just 1, Nothing, Just 3]), ("b", texts ["p", "q", "r"])]
        )
    ,
        ( "schema_int_missing"
        , defaultReadOptions{typeSpec = SpecifyTypes [("a", schemaTypeInt)] NoInference}
        , "a\n1\nNA\n\n3\n"
        , Cols (3, 1) [("a", mints [Just 1, Nothing, Just 3])]
        )
    ,
        ( "schema_text_missing"
        , defaultReadOptions{typeSpec = SpecifyTypes [("a", schemaTypeText)] NoInference}
        , "a\nx\nNA\n\nz\n"
        , Cols (3, 1) [("a", mtexts [Just "x", Nothing, Just "z"])]
        )
    ,
        ( "dates"
        , defaultReadOptions
        , "d\n2024-01-02\n2024-02-03\n"
        , Cols (2, 1) [("d", days [fromGregorian 2024 1 2, fromGregorian 2024 2 3])]
        )
    ,
        ( "dates_custom"
        , defaultReadOptions{dateFormat = "%d/%m/%Y"}
        , "d\n02/01/2024\n03/02/2024\n"
        , Cols (2, 1) [("d", days [fromGregorian 2024 1 2, fromGregorian 2024 2 3])]
        )
    ,
        ( "custom_sep"
        , defaultReadOptions{columnSeparator = ';'}
        , "a;b\n1;x\n2;y\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", texts ["x", "y"])]
        )
    ,
        ( "custom_missing"
        , defaultReadOptions{missingIndicators = ["foo"]}
        , "a,b\n1,foo\n2,bar\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", mtexts [Nothing, Just "bar"])]
        )
    ,
        ( "bools"
        , defaultReadOptions
        , "f\nTrue\nFalse\n"
        , Cols (2, 1) [("f", boolsC [True, False])]
        )
    ,
        ( "bool_ws"
        , defaultReadOptions
        , "f\n True\nFalse\n"
        , Cols (2, 1) [("f", boolsC [True, False])]
        )
    ,
        ( "single_space_field"
        , defaultReadOptions
        , "a,b\nx, \ny,z\n"
        , Cols (2, 2) [("a", texts ["x", "y"]), ("b", mtexts [Nothing, Just "z"])]
        )
    ,
        ( "row_cap"
        , defaultReadOptions{numRowsToRead = Just 2}
        , "a\n1\n2\n3\n4\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "row_cap_zero"
        , defaultReadOptions{numRowsToRead = Just 0}
        , "a,b\n1,2\n"
        , Cols (0, 2) [("a", mtexts []), ("b", mtexts [])]
        )
    ,
        ( "selected_subset"
        , defaultReadOptions{readColumns = Just ["c", "a"]}
        , "a,b,c\n1,x,2.5\n3,y,4.5\n"
        , Cols (2, 2) [("c", dbls [2.5, 4.5]), ("a", ints [1, 3])]
        )
    ,
        ( "selected_single_trailing"
        , defaultReadOptions{readColumns = Just ["c"]}
        , "a,b,c\n1,x,2\n3,y,4\n"
        , Cols (2, 1) [("c", ints [2, 4])]
        )
    ,
        ( "selected_all_reordered"
        , defaultReadOptions{readColumns = Just ["b", "a"]}
        , "a,b\n1,x\n2,y\n"
        , Cols (2, 2) [("b", texts ["x", "y"]), ("a", ints [1, 2])]
        )
    ,
        ( "selected_ignores_unselected_values"
        , defaultReadOptions{readColumns = Just ["a"]}
        , "a,b\n1,notanumber\n2,\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "selected_noheader_positional"
        , defaultReadOptions{headerSpec = NoHeader, readColumns = Just ["2", "0"]}
        , "1,x,9\n2,y,8\n"
        , Cols (2, 2) [("2", ints [9, 8]), ("0", ints [1, 2])]
        )
    ,
        ( "selected_with_specified_type"
        , defaultReadOptions
            { readColumns = Just ["b"]
            , typeSpec = SpecifyTypes [("b", schemaTypeText)] (InferFromSample 100)
            }
        , "a,b\n1,2\n3,4\n"
        , Cols (2, 1) [("b", texts ["2", "4"])]
        )
    ,
        ( "selected_missing"
        , defaultReadOptions{readColumns = Just ["a", "nope"]}
        , "a,b\n1,2\n"
        , Err "Column not found"
        )
    ,
        ( "trailing_sep"
        , defaultReadOptions
        , "a,b\n1,\n2,3\n"
        , Cols (2, 2) [("a", ints [1, 2]), ("b", mints [Nothing, Just 3])]
        )
    , ("header_only", defaultReadOptions, "a,b\n", Err "Empty CSV file")
    , ("empty", defaultReadOptions, "", Err "Empty CSV file")
    , ("only_blank_lines", defaultReadOptions, "\n\n\n", Err "Empty CSV file")
    ,
        ( "all_null_col"
        , defaultReadOptions
        , "a,b\n,1\n,2\n"
        , Cols (2, 2) [("a", mtexts [Nothing, Nothing]), ("b", ints [1, 2])]
        )
    ,
        ( "int_overflow"
        , defaultReadOptions
        , "a\n9223372036854775808\n1\n"
        , Cols (2, 1) [("a", dbls [9.223372036854776e18, 1.0])]
        )
    ,
        ( "mixed_int_double"
        , defaultReadOptions
        , "a\n1\n2.5\n"
        , Cols (2, 1) [("a", dbls [1.0, 2.5])]
        )
    ,
        ( "int_then_text"
        , sample 2
        , "a\n1\n2\nx\n"
        , Cols (3, 1) [("a", texts ["1", "2", "x"])]
        )
    ,
        ( "int_then_double"
        , sample 2
        , "a\n1\n2\n3.5\n"
        , Cols (3, 1) [("a", dbls [1.0, 2.0, 3.5])]
        )
    ,
        ( "sample_all_null_then_data"
        , sample 2
        , "a\n\n\n5\n7\n"
        , Cols (2, 1) [("a", ints [5, 7])]
        )
    ,
        ( "quoted_number"
        , defaultReadOptions
        , "a\n\"1\"\n\"2\"\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "quoted_padded_number"
        , defaultReadOptions
        , "a\n\" 1 \"\n\"2\"\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "quoted_header"
        , defaultReadOptions
        , "\"a b\",c\n1,2\n"
        , Cols (1, 2) [("a b", ints [1]), ("c", ints [2])]
        )
    ,
        ( "header_quoted_doubled"
        , defaultReadOptions
        , "\"a\"\"b\",c\n1,2\n"
        , Cols (1, 2) [("a\"b", ints [1]), ("c", ints [2])]
        )
    ,
        ( "quoted_crlf_field"
        , defaultReadOptions
        , "a,b\r\n\"x\r\ny\",2\r\n"
        , Cols (1, 2) [("a", texts ["x\r\ny"]), ("b", ints [2])]
        )
    ,
        ( "quoted_field_with_cr_alone"
        , defaultReadOptions
        , "a,b\n\"x\ry\",2\n"
        , Cols (1, 2) [("a", texts ["x\ry"]), ("b", ints [2])]
        )
    ,
        ( "tab_sep_quoted"
        , defaultReadOptions{columnSeparator = '\t'}
        , "a\tb\n\"x\t1\"\ty\n"
        , Cols (1, 2) [("a", texts ["x\t1"]), ("b", texts ["y"])]
        )
    ,
        ( "either_disables_missing"
        , defaultReadOptions{safeReadOverrides = [("b", EitherRead)]}
        , "a,b\nNA,1\n2,2\n"
        , Cols (2, 2) [("a", texts ["NA", "2"]), ("b", eti [Right 1, Right 2])]
        )
    ,
        ( "unclosed_quote_D6"
        , defaultReadOptions
        , "a,b\n\"x,2\n"
        , Cols (1, 2) [("a", texts ["x,2"]), ("b", mtexts [Nothing])]
        )
    ,
        ( "unclosed_trailing_doubled_D6"
        , defaultReadOptions
        , "a,b\n\"x\"\"\n"
        , Cols (1, 2) [("a", texts ["x\""]), ("b", mtexts [Nothing])]
        )
    ,
        ( "closed_after_doubled_D6"
        , defaultReadOptions
        , "a,b\n\"x\"\"\"\n"
        , Cols (1, 2) [("a", texts ["x\""]), ("b", mtexts [Nothing])]
        )
    ,
        ( "multi_doubled"
        , defaultReadOptions
        , "a\n\"x\"\"y\"\"z\"\nw\n"
        , Cols (2, 1) [("a", texts ["x\"y\"z", "w"])]
        )
    ,
        ( "only_doubled"
        , defaultReadOptions
        , "a\n\"\"\"\"\nw\n"
        , Cols (2, 1) [("a", texts ["\"", "w"])]
        )
    ,
        ( "doubled_at_open"
        , defaultReadOptions
        , "a,b\n\"\"\"x\",2\n"
        , Cols (1, 2) [("a", texts ["\"x"]), ("b", ints [2])]
        )
    ,
        ( "interior_doubled_inferred"
        , defaultReadOptions
        , "a\n\"x\"\"y\"\nz\n"
        , Cols (2, 1) [("a", texts ["x\"y", "z"])]
        )
    ,
        ( "trailing_cr_eof"
        , defaultReadOptions
        , "a,b\n1,x\r"
        , Cols (1, 2) [("a", ints [1]), ("b", texts ["x"])]
        )
    ,
        ( "single_col_blank"
        , defaultReadOptions
        , "a\n1\n\n2\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "single_col_quoted_empty"
        , defaultReadOptions
        , "a\n1\n\"\"\n2\n"
        , Cols (2, 1) [("a", ints [1, 2])]
        )
    ,
        ( "row_only_sep"
        , defaultReadOptions
        , "a,b\n,\n1,2\n"
        , Cols (2, 2) [("a", mints [Nothing, Just 1]), ("b", mints [Nothing, Just 2])]
        )
    ,
        ( "sep_at_eof"
        , defaultReadOptions
        , "a,b\n1,2\n3,"
        , Cols (2, 2) [("a", ints [1, 3]), ("b", mints [Just 2, Nothing])]
        )
    ,
        ( "noheader_ragged_grow"
        , defaultReadOptions{headerSpec = NoHeader}
        , "1,2\n3,4,5\n"
        , Cols (2, 2) [("0", ints [1, 3]), ("1", ints [2, 4])]
        )
    ,
        ( "quoted_last_no_nl"
        , defaultReadOptions
        , "a,b\n1,2\n\"x\",3"
        , Cols (2, 2) [("a", texts ["1", "x"]), ("b", ints [2, 3])]
        )
    ,
        ( "empty_quoted_middle_col"
        , defaultReadOptions
        , "a,b,c\n1,\"\",3\n4,5,6\n"
        , Cols
            (2, 3)
            [("a", ints [1, 4]), ("b", mints [Nothing, Just 5]), ("c", ints [3, 6])]
        )
    ,
        ( "exp_double"
        , defaultReadOptions
        , "a\n1e3\n1E-3\n1e999\n"
        , Cols (3, 1) [("a", dbls [1000.0, 1.0e-3, 1 / 0])]
        )
    ,
        ( "five_field_double"
        , defaultReadOptions
        , "a\n+.5\n5.\n"
        , Cols (2, 1) [("a", texts ["+.5", "5."])]
        )
    ,
        ( "raw_invalid_utf8"
        , defaultReadOptions
        , BS.pack [97, 10, 255, 10, 98, 10]
        , Cols (2, 1) [("a", texts ["\65533", "b"])]
        )
    ,
        ( "raw_nbsp_edges_stripped"
        , defaultReadOptions
        , BS.pack [97, 10, 160, 120, 160, 10, 98, 10]
        , Cols (2, 1) [("a", texts ["x", "b"])]
        )
    ]
  where
    schemaTypeInt = schemaType @Int
    schemaTypeText = schemaType @T.Text

-- Duplicate header names: last index wins in the map; both columns kept.
dupHeaders :: Test
dupHeaders = TestLabel "dup_headers" $ TestCase $ do
    df <- decodeSeparated defaultReadOptions (BL.fromStrict "a,a\n1,2\n3,4\n")
    assertEqual "dims" (2, 2) (dataframeDimensions df)
    assertEqual "index map" [("a", 1)] (M.toList (columnIndices df))

-- Ingest pin: a string column freezes to a shared-buffer 'PackedText'
-- (no per-row boxed Text header), while values stay byte-identical.
textFreezesPacked :: Test
textFreezesPacked = TestLabel "text_freezes_packed" $ TestCase $ do
    df <- decodeSeparated defaultReadOptions (BL.fromStrict "a,b\n1,x\n2,y\n")
    case getColumn "b" df of
        Nothing -> assertFailure "missing text column"
        Just col -> do
            assertBool "text column is PackedText" (DI.isPackedText col)
            assertEqual "values byte-identical" (texts ["x", "y"]) col

tests :: [Test]
tests = dupHeaders : textFreezesPacked : map goldenCase goldenCases
