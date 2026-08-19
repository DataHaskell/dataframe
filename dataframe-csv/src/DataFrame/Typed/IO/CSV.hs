{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Typed CSV reading.

The schema is the whole specification of the read: it names the columns to
fetch and gives their types, so these readers touch only the columns @cols@
declares and never infer a type the schema already knows.

@
type Customer = '[ '(\"customer_id\", Int), '(\"customer_name\", Text)]

customers <- readCsv \@Customer \"customers.csv\"  -- reads 2 columns, whatever
                                                 -- else the file holds
@
-}
module DataFrame.Typed.IO.CSV (
    readCsv,
    readCsvWithError,
    readTsv,
    readCsvWithOpts,
    writeCsv,
    writeTsv,
) where

import Control.Applicative ((<|>))
import Control.Exception (SomeException, try)
import qualified Data.Text as T

import DataFrame.IO.CSV (ReadOptions (..), TypeSpec (..), defaultReadOptions)
import qualified DataFrame.IO.CSV as CSV
import DataFrame.Schema (RuntimeSchema (..), elements)
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError, thaw)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

import qualified Data.Map.Strict as M

{- | Fold a type-level schema into read options: fetch exactly its columns,
and parse them at its types. Anything else the caller asked for is kept,
including a wider 'typeSpec' for columns the schema does not name.
-}
schemaOptions :: forall cols. (RuntimeSchema cols) => ReadOptions -> ReadOptions
schemaOptions opts =
    opts
        { typeSpec = SpecifyTypes (M.toList declared) (typeSpec opts)
        , readColumns = readColumns opts <|> Just (M.keys declared)
        }
  where
    declared = elements (runtimeSchema @cols)

{- | Read a CSV file into a typed DataFrame, throwing on schema mismatch.
Reads only the columns @cols@ names, typed as @cols@ says.

==== __Example__
@
ghci> type Customer = '[ '(\"id\", Int), '(\"name\", Text)]
ghci> customers <- readCsv \@Customer \"customers.csv\"
@
-}
readCsv ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (TypedDataFrame cols)
readCsv = readCsvWithOpts @cols defaultReadOptions

{- | Read a CSV file, returning a descriptive error on schema mismatch or a
missing column instead of throwing.

==== __Example__
@
ghci> readCsvWithError \@Customer \"customers.csv\"
Right (TDF ...)
@
-}
readCsvWithError ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readCsvWithError path = do
    -- The projection can fail before the freeze does (a column the file does
    -- not have); this reader reports rather than throws either way.
    r <- try (CSV.readCsvWithOpts (schemaOptions @cols defaultReadOptions) path)
    pure $ case r of
        Left (e :: SomeException) -> Left (T.pack (show e))
        Right df -> freezeWithError df

{- | Read a tab-separated file into a typed DataFrame, throwing on mismatch.

==== __Example__
@
ghci> customers <- readTsv \@Customer \"customers.tsv\"
@
-}
readTsv ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (TypedDataFrame cols)
readTsv = readCsvWithOpts @cols defaultReadOptions{columnSeparator = '\t'}

{- | Read a CSV file with custom options, throwing on schema mismatch. The
schema still supplies the column selection and types; an explicit
'readColumns' takes precedence, and is then checked by the freeze.

==== __Example__
@
ghci> customers <- readCsvWithOpts \@Customer defaultReadOptions{safeRead = MaybeRead} \"customers.csv\"
@
-}
readCsvWithOpts ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    ReadOptions -> FilePath -> IO (TypedDataFrame cols)
readCsvWithOpts opts path =
    CSV.readCsvWithOpts (schemaOptions @cols opts) path >>= freezeOrThrow @cols

{- | Write a typed DataFrame to a CSV file.

==== __Example__
@
ghci> writeCsv \"customers.csv\" customers
@
-}
writeCsv :: FilePath -> TypedDataFrame cols -> IO ()
writeCsv path = CSV.writeCsv path . thaw

{- | Write a typed DataFrame to a tab-separated file.

==== __Example__
@
ghci> writeTsv \"customers.tsv\" customers
@
-}
writeTsv :: FilePath -> TypedDataFrame cols -> IO ()
writeTsv path = CSV.writeTsv path . thaw
