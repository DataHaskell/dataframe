{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Typed Parquet reading.

The reader validates the file against a type-level schema as it loads, supplied
by type application:

@
type Trips = '[Column \"id\" Int, Column \"fare\" Double]

trips <- readParquet \@Trips \"trips.parquet\"   -- IO (TypedDataFrame Trips)
@

'readParquet' (and 'readParquetWithOpts'\/'readParquetFiles') throw a
'DataFrameException' on schema mismatch; 'readParquetWithError' returns the
mismatch as an 'Either' instead.
-}
module DataFrame.Typed.IO.Parquet (
    readParquet,
    readParquetWithError,
    readParquetWithOpts,
    readParquetFiles,
) where

import Control.Applicative ((<|>))
import Control.Exception (SomeException, try)
import qualified Data.Text as T

import DataFrame.IO.Parquet (ParquetReadOptions (..), defaultParquetReadOptions)
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError)
import DataFrame.Typed.Schema (KnownSchema, schemaColumnNames)
import DataFrame.Typed.Types (TypedDataFrame)

{- | Read only the columns @cols@ names, unless the caller selected columns
themselves. Parquet supplies the element types, so the schema contributes the
projection and the freeze checks the types.
-}
schemaOptions ::
    forall cols. (KnownSchema cols) => ParquetReadOptions -> ParquetReadOptions
schemaOptions opts =
    opts
        { selectedColumns = selectedColumns opts <|> Just (schemaColumnNames @cols)
        }

{- | Read a Parquet file into a typed DataFrame, throwing on schema mismatch.
Reads only the columns @cols@ names.

==== __Example__
@
ghci> trips <- readParquet \@Trips \"trips.parquet\"
@
-}
readParquet ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquet = readParquetWithOpts @cols defaultParquetReadOptions

{- | Read a Parquet file, returning a descriptive error on schema mismatch or
a missing column instead of throwing.

==== __Example__
@
ghci> readParquetWithError \@Trips \"trips.parquet\"
Right (TDF ...)
@
-}
readParquetWithError ::
    forall cols.
    (KnownSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readParquetWithError path = do
    r <-
        try
            (Parquet.readParquetWithOpts (schemaOptions @cols defaultParquetReadOptions) path)
    pure $ case r of
        Left (e :: SomeException) -> Left (T.pack (show e))
        Right df -> freezeWithError df

{- | Read a Parquet file with custom options, throwing on schema mismatch. The
schema still supplies the column selection; an explicit 'selectedColumns'
takes precedence.

==== __Example__
@
ghci> trips <- readParquetWithOpts \@Trips defaultParquetReadOptions{rowRange = Just (0, 10)} \"trips.parquet\"
@
-}
readParquetWithOpts ::
    forall cols.
    (KnownSchema cols) =>
    ParquetReadOptions -> FilePath -> IO (TypedDataFrame cols)
readParquetWithOpts opts path =
    Parquet.readParquetWithOpts (schemaOptions @cols opts) path
        >>= freezeOrThrow @cols

{- | Read a directory\/glob of Parquet files into a typed DataFrame.

==== __Example__
@
ghci> trips <- readParquetFiles \@Trips \".\/data\/trips\/*.parquet\"
@
-}
readParquetFiles ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquetFiles path =
    Parquet.readParquetFilesWithOpts
        (schemaOptions @cols defaultParquetReadOptions)
        path
        >>= freezeOrThrow @cols
