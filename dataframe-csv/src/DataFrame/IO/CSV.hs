{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | CSV reading and writing for dataframes. A strict, single-pass
RFC 4180 scanner (cassava-compatible) parses fields into typed column
builders; ragged rows are padded with nulls and extra fields dropped.
-}
module DataFrame.IO.CSV (
    -- * Reading
    readCsv,
    readTsv,
    readCsvWithOpts,
    readSeparated,
    readCsvWithSchema,
    CsvReader,
    schemaReadOptions,
    decodeSeparated,
    fromCsv,
    fromCsvBytes,

    -- * Options
    module DataFrame.IO.CSV.Internal.Options,

    -- * Writing
    writeCsv,
    writeTsv,
    writeSeparated,

    -- * Helpers
    stripQuotes,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO.Utf8 as TIO

import Control.Exception (SomeException, catch)
import Data.Maybe (fromMaybe)
import DataFrame.IO.CSV.Internal.Options
import DataFrame.IO.CSV.Internal.Read (decodeCsvStrict)
import DataFrame.Internal.DataFrame (DataFrame (..), toSeparated)
import DataFrame.Schema (Schema, elements)

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsv ".\/data\/taxi.csv"

@
-}
readCsv :: FilePath -> IO DataFrame
readCsv = readSeparated defaultReadOptions

{- | A reader the lazy scan can drive: 'readSeparated' here, or
@fastReadCsvWithOpts@ from @dataframe-fastcsv@. The scan owns the
'ReadOptions', so every reader it accepts honours the projection it asks
for.

==== __Example__
@
myReader :: CsvReader
myReader opts path = D.readCsvWithOpts opts path
@
-}
type CsvReader = ReadOptions -> FilePath -> IO DataFrame

{- | Read options a scan derives from its 'Schema': the schema assigns the
column types /and/ selects the columns, so a scan reads only what its
schema names — the same contract as @scanParquet@.

==== __Example__
@
ghci> schema = D.makeSchema [("id", D.schemaType \@Int), ("name", D.schemaType \@Text)]
ghci> D.readCsvWithOpts (schemaReadOptions schema) ".\/data\/customers.csv"

@
-}
schemaReadOptions :: Schema -> ReadOptions
schemaReadOptions schema =
    defaultReadOptions
        { typeSpec =
            SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
        , readColumns = Just (M.keys (elements schema))
        }

{- | Schema-driven CSV reader. Coerces each column to the type declared
in 'Schema'; columns absent from the schema fall back to inference and are
still returned. To read /only/ the schema's columns, pass
'schemaReadOptions' to 'readCsvWithOpts'.

@
import qualified DataFrame as D
df <- D.readCsvWithSchema schema "input.csv"
@
-}
readCsvWithSchema :: Schema -> FilePath -> IO DataFrame
readCsvWithSchema schema =
    readSeparated
        defaultReadOptions
            { typeSpec =
                SpecifyTypes
                    (M.toList (elements schema))
                    (typeSpec defaultReadOptions)
            }

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsvWithOpts ".\/data\/taxi.csv" (D.defaultReadOptions { dateFormat = "%d/%-m/%-Y" })

@
-}
readCsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
readCsvWithOpts = readSeparated

{- | Read TSV (tab separated) file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readTsv ".\/data\/taxi.tsv"

@
-}
readTsv :: FilePath -> IO DataFrame
readTsv = readSeparated (defaultReadOptions{columnSeparator = '\t'})

{- | Read text file with specified delimiter into a dataframe.

==== __Example__
@
ghci> D.readSeparated (D.defaultReadOptions { columnSeparator = ';' }) ".\/data\/taxi.txt"

@
-}
readSeparated :: ReadOptions -> FilePath -> IO DataFrame
readSeparated opts path = do
    validateReadOptions opts
    let stripUtf8Bom b = fromMaybe b (BS.stripPrefix "\xEF\xBB\xBF" b)
    csvData <- stripUtf8Bom <$> BS.readFile path
    decodeCsvStrict opts csvData

{- | Decode in-memory CSV bytes into a dataframe. The result is fully
forced. (Note: unlike 'readSeparated', no UTF-8 BOM is stripped.)

==== __Example__
@
ghci> D.decodeSeparated D.defaultReadOptions "id,name\\n1,Ada\\n"

@
-}
decodeSeparated :: ReadOptions -> BL.ByteString -> IO DataFrame
decodeSeparated opts csvData = decodeCsvStrict opts (BL.toStrict csvData)

{- | Write a dataframe to a comma-separated file.

==== __Example__
@
ghci> D.writeCsv ".\/out.csv" df
@
-}
writeCsv :: FilePath -> DataFrame -> IO ()
writeCsv = writeSeparated ','

{- | Write a dataframe to a tab-separated file.

==== __Example__
@
ghci> D.writeTsv ".\/out.tsv" df
@
-}
writeTsv :: FilePath -> DataFrame -> IO ()
writeTsv = writeSeparated '\t'

{- | Write a dataframe to a file using the given field separator.

==== __Example__
@
ghci> D.writeSeparated ';' ".\/out.txt" df
@
-}
writeSeparated ::
    -- | Separator
    Char ->
    -- | Path to write to
    FilePath ->
    DataFrame ->
    IO ()
writeSeparated c filepath df = TIO.writeFile filepath (toSeparated c df)

{- | Parse a CSV string into a DataFrame using default options.

==== __Example__
@
ghci> D.fromCsv "id,name\\n1,Ada\\n"
Right (DataFrame ...)

@
-}
fromCsv :: String -> IO (Either String DataFrame)
fromCsv s = do
    let bs = BL.fromStrict (TE.encodeUtf8 (T.pack s))
    (Right <$> decodeSeparated defaultReadOptions bs)
        `catch` (\(e :: SomeException) -> pure (Left (show e)))

{- | Parse a lazy 'ByteString' containing CSV data into a DataFrame using
default options.

==== __Example__
@
ghci> D.fromCsvBytes "id,name\\n1,Ada\\n"

@
-}
fromCsvBytes :: BL.ByteString -> IO DataFrame
fromCsvBytes = decodeSeparated defaultReadOptions

{- | Strip one layer of double-quote delimiters from a field, if present.

==== __Example__
>>> stripQuotes "\"hello\""
"hello"
>>> stripQuotes "hello"
"hello"
-}
stripQuotes :: T.Text -> T.Text
stripQuotes txt =
    case T.uncons txt of
        Just ('"', rest) ->
            case T.unsnoc rest of
                Just (middle, '"') -> middle
                _ -> txt
        _ -> txt
