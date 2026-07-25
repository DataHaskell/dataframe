{-# LANGUAGE OverloadedStrings #-}

{- | Configuration types for the CSV readers. Re-exported through
"DataFrame.IO.CSV"; shared by the default reader, @dataframe-fastcsv@
and @dataframe-lazy@.
-}
module DataFrame.IO.CSV.Internal.Options (
    HeaderSpec (..),
    TypeSpec (..),
    RaggedRowPolicy (..),
    UnclosedQuotePolicy (..),
    ReadOptions (..),
    defaultReadOptions,
    shouldInferFromSample,
    schemaTypeMap,
    typeInferenceSampleSize,
    resolveSelection,
    InvalidReadOptions (..),
    readOptionsErrors,
    validateReadOptions,
) where

import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T

import Control.Exception (Exception, throw, throwIO)
import Control.Monad (unless)
import DataFrame.Errors (DataFrameException (ColumnsNotFoundException))
import DataFrame.Operations.Typing (SafeReadMode (..))
import DataFrame.Schema (SchemaType)

{- | Where a reader's column names come from.

==== __Example__
@
ghci> D.readCsvWithOpts D.defaultReadOptions{D.headerSpec = D.ProvideNames ["a", "b"]} "no_header.csv"

@
-}
data HeaderSpec
    = NoHeader
    | -- | Take names from the first row (the default).
      UseFirstRow
    | -- | Name the leading columns; any beyond the given list are numbered.
      ProvideNames [T.Text]
    deriving (Eq, Show)

{- | How a reader decides each column's type.

==== __Example__
@
ghci> D.readCsvWithOpts D.defaultReadOptions{D.typeSpec = D.InferFromSample 500} "wide.csv"

@
-}
data TypeSpec
    = -- | Infer each column's type from the first @n@ rows.
      InferFromSample Int
    | {- | Pin the listed columns to their given types; fall back to the
      nested 'TypeSpec' for the rest.
      -}
      SpecifyTypes [(T.Text, SchemaType)] TypeSpec
    | -- | Every column is read as 'Data.Text.Text', no inference at all.
      NoInference

{- | How the fast reader should treat a row whose field count does not
match the header row.  Only consulted by @dataframe-fastcsv@; the pure
Haskell reader always pads short rows with nulls and drops extras.
-}
data RaggedRowPolicy
    = {- | Fill missing cells with nulls; silently drop extras.
      Matches pandas / polars lenient defaults.
      -}
      PadWithNull
    | {- | Fill missing cells with nulls; silently drop extras.  Alias
      kept for ergonomic naming when the caller only cares about the
      "don't raise, just forget the extras" half of 'PadWithNull'.
      -}
      Truncate
    | {- | Raise a 'DataFrame.IO.CSV.Fast.CsvParseError' on any row whose
      field count differs from the header.  Matches polars' strict
      schema-bound mode.
      -}
      RaiseOnRagged
    deriving (Eq, Show)

-- | How the fast reader should treat an unclosed quoted field at EOF.
data UnclosedQuotePolicy
    = -- | Raise a 'DataFrame.IO.CSV.Fast.CsvParseError'.  Default.
      RaiseOnUnclosedQuote
    | {- | Return whatever rows were parsed before the stray quote; the
      remainder is silently dropped.
      -}
      BestEffort
    deriving (Eq, Show)

-- | CSV read parameters.
data ReadOptions = ReadOptions
    { headerSpec :: HeaderSpec
    -- ^ Where to get the headers from. (default: UseFirstRow)
    , typeSpec :: TypeSpec
    -- ^ Whether/how to infer types. (default: InferFromSample 100)
    , safeRead :: SafeReadMode
    {- ^ Default 'SafeReadMode' for columns without an entry in
    'safeReadOverrides'. (default: 'NoSafeRead')
    -}
    , safeReadOverrides :: [(T.Text, SafeReadMode)]
    -- ^ Per-column 'SafeReadMode' overrides; takes precedence over 'safeRead'.
    , dateFormat :: String
    {- ^ Format of date fields as recognized by the Data.Time.Format module.

    __Examples:__

    @
    > parseTimeM True defaultTimeLocale "%Y/%-m/%-d" "2010/3/04" :: Maybe Day
    Just 2010-03-04
    > parseTimeM True defaultTimeLocale "%d/%-m/%-Y" "04/3/2010" :: Maybe Day
    Just 2010-03-04
    @
    -}
    , columnSeparator :: Char
    -- ^ Character that separates column values.
    , numRowsToRead :: Maybe Int
    {- ^ Maximum number of data rows to read ('Nothing' = all).

    __Example:__

    @
    ghci> D.readCsvWithOpts D.defaultReadOptions{D.numRowsToRead = Just 5} "big.csv"
    @
    -}
    , readColumns :: Maybe [T.Text]
    {- ^ Columns to read ('Nothing' = all). Unselected columns are never
    decoded, parsed or allocated; the returned frame carries the selected
    columns in the order given here. Naming a column the file does not have
    raises a 'ColumnsNotFoundException'.

    __Example:__

    @
    ghci> D.readCsvWithOpts D.defaultReadOptions{D.readColumns = Just ["id", "name"]} "customers.csv"
    @
    -}
    , missingIndicators :: [T.Text]
    -- ^ Values that should be read as `Nothing`.
    , fastCsvOnRaggedRow :: RaggedRowPolicy
    {- ^ @dataframe-fastcsv@: how to treat rows with a non-header field count.
    (default: 'PadWithNull')
    -}
    , fastCsvOnUnclosedQuote :: UnclosedQuotePolicy
    {- ^ @dataframe-fastcsv@: how to treat an unclosed quoted field at EOF.
    (default: 'RaiseOnUnclosedQuote')
    -}
    , fastCsvTrimUnquoted :: Bool
    {- ^ @dataframe-fastcsv@: if 'True', leading/trailing whitespace is
    stripped from unquoted fields after decoding.  RFC 4180 preserves
    this whitespace, and that is the default ('False').
    -}
    }

{- | Whether a 'TypeSpec' still infers types from a sample, once any
'SpecifyTypes' fallback chain is followed to its end.

==== __Example__
>>> shouldInferFromSample (InferFromSample 100)
True
>>> shouldInferFromSample NoInference
False
-}
shouldInferFromSample :: TypeSpec -> Bool
shouldInferFromSample (InferFromSample _) = True
shouldInferFromSample (SpecifyTypes _ fallback) = shouldInferFromSample fallback
shouldInferFromSample _ = False

{- | The explicit column\/type pairs a 'TypeSpec' declares, if any.

==== __Example__
>>> :set -XTypeApplications
>>> M.toList (schemaTypeMap (SpecifyTypes [("id", schemaType @Int)] (InferFromSample 100)))
[("id",Int)]
-}
schemaTypeMap :: TypeSpec -> M.Map T.Text SchemaType
schemaTypeMap (SpecifyTypes xs _) = M.fromList xs
schemaTypeMap _ = M.empty

{- | A 'ReadOptions' value that contradicts itself, with every problem listed.
Thrown by 'validateReadOptions'; see 'readOptionsErrors' for the check
without the exception.

==== __Example__
>>> show (InvalidReadOptions ["readColumns is an empty selection; omit it to read every column"])
"Invalid ReadOptions:\n  - readColumns is an empty selection; omit it to read every column"
-}
newtype InvalidReadOptions = InvalidReadOptions [T.Text]

instance Show InvalidReadOptions where
    show (InvalidReadOptions problems) =
        T.unpack
            (T.intercalate "\n" ("Invalid ReadOptions:" : map ("  - " <>) problems))

instance Exception InvalidReadOptions

{- | Everything self-contradictory about @opts@, judged without looking at a
file. A type or safe-read override naming an unread column is redundant, not
contradictory, and is not reported.

==== __Example__
>>> readOptionsErrors defaultReadOptions{readColumns = Just []}
["readColumns is an empty selection; omit it to read every column"]
>>> readOptionsErrors defaultReadOptions
[]
-}
readOptionsErrors :: ReadOptions -> [T.Text]
readOptionsErrors opts =
    concat
        [ [ "readColumns is an empty selection; omit it to read every column"
          | Just [] <- [readColumns opts]
          ]
        , [ "readColumns names "
                <> T.intercalate ", " dups
                <> " more than once"
          | not (null dups)
          ]
        , [ "numRowsToRead is negative: " <> T.pack (show n)
          | Just n <- [numRowsToRead opts]
          , n < 0
          ]
        , [ "headerSpec is ProvideNames with no names"
          | ProvideNames [] <- [headerSpec opts]
          ]
        , [ "columnSeparator is " <> T.pack (show sep) <> ", which cannot delimit fields"
          | let sep = columnSeparator opts
          , sep `elem` ['"', '\n', '\r']
          ]
        ]
  where
    dups = maybe [] (\cs -> L.nub (cs L.\\ L.nub cs)) (readColumns opts)

{- | Throw 'InvalidReadOptions' if @opts@ contradicts itself. Both readers
call this before opening the file.

==== __Example__
@
ghci> D.validateReadOptions D.defaultReadOptions{D.readColumns = Just []}
*** Exception: Invalid ReadOptions:
  - readColumns is an empty selection; omit it to read every column
@
-}
validateReadOptions :: ReadOptions -> IO ()
validateReadOptions opts =
    unless (null problems) (throwIO (InvalidReadOptions problems))
  where
    problems = readOptionsErrors opts

{- | Resolve 'readColumns' against a file's header names: the columns to
build, each paired with its field index within a row. 'Nothing' keeps every
column in file order; a selection keeps the order it was written in.

==== __Example__
>>> resolveSelection defaultReadOptions{readColumns = Just ["b", "a"]} ["a", "b", "c"]
[("b",1),("a",0)]
>>> resolveSelection defaultReadOptions ["a", "b"]
[("a",0),("b",1)]
-}
resolveSelection :: ReadOptions -> [T.Text] -> [(T.Text, Int)]
resolveSelection opts names = case readColumns opts of
    Nothing -> indexed
    Just requested -> case filter (`M.notMember` fieldIndex) requested of
        [] -> [(n, fieldIndex M.! n) | n <- requested]
        missing -> throw (ColumnsNotFoundException missing "readCsvWithOpts" names)
  where
    indexed = zip names [0 ..]
    fieldIndex = M.fromList indexed

{- | The sample size a 'TypeSpec' infers from, once any 'SpecifyTypes'
fallback chain is followed to its end. @0@ when the chain ends in
'NoInference'.

==== __Example__
>>> typeInferenceSampleSize (InferFromSample 100)
100
>>> typeInferenceSampleSize NoInference
0
-}
typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize (SpecifyTypes _ fallback) = typeInferenceSampleSize fallback
typeInferenceSampleSize _ = 0

{- | The default 'ReadOptions': infer types from a 100-row sample, treat the
first row as a header, read every row and every column.

==== __Example__
@
ghci> D.readCsvWithOpts D.defaultReadOptions{D.columnSeparator = ';'} "data.csv"
@
-}
defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = NoSafeRead
        , safeReadOverrides = []
        , dateFormat = "%Y-%m-%d"
        , columnSeparator = ','
        , numRowsToRead = Nothing
        , readColumns = Nothing
        , missingIndicators =
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
        , fastCsvOnRaggedRow = PadWithNull
        , fastCsvOnUnclosedQuote = RaiseOnUnclosedQuote
        , fastCsvTrimUnquoted = False
        }
