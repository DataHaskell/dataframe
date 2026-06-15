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
) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T

import DataFrame.Internal.Schema (SchemaType)
import DataFrame.Operations.Typing (SafeReadMode (..))

data HeaderSpec = NoHeader | UseFirstRow | ProvideNames [T.Text]
    deriving (Eq, Show)

data TypeSpec
    = InferFromSample Int
    | SpecifyTypes [(T.Text, SchemaType)] TypeSpec
    | NoInference

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
    , numColumns :: Maybe Int
    {- ^ Maximum number of data rows to read ('Nothing' = all). Note: despite
    the legacy name, this has always capped /rows/, not columns.
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

shouldInferFromSample :: TypeSpec -> Bool
shouldInferFromSample (InferFromSample _) = True
shouldInferFromSample (SpecifyTypes _ fallback) = shouldInferFromSample fallback
shouldInferFromSample _ = False

schemaTypeMap :: TypeSpec -> M.Map T.Text SchemaType
schemaTypeMap (SpecifyTypes xs _) = M.fromList xs
schemaTypeMap _ = M.empty

typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize (SpecifyTypes _ fallback) = typeInferenceSampleSize fallback
typeInferenceSampleSize _ = 0

defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = NoSafeRead
        , safeReadOverrides = []
        , dateFormat = "%Y-%m-%d"
        , columnSeparator = ','
        , numColumns = Nothing
        , missingIndicators =
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
        , fastCsvOnRaggedRow = PadWithNull
        , fastCsvOnUnclosedQuote = RaiseOnUnclosedQuote
        , fastCsvTrimUnquoted = False
        }
