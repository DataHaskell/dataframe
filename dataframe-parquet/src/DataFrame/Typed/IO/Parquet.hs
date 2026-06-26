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

import qualified Data.Text as T

import DataFrame.IO.Parquet (ParquetReadOptions)
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

-- | Read a Parquet file into a typed DataFrame, throwing on schema mismatch.
readParquet ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquet path = Parquet.readParquet path >>= freezeOrThrow @cols

-- | Read a Parquet file, returning a descriptive error on schema mismatch.
readParquetWithError ::
    forall cols.
    (KnownSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readParquetWithError path = freezeWithError <$> Parquet.readParquet path

-- | Read a Parquet file with custom options, throwing on schema mismatch.
readParquetWithOpts ::
    forall cols.
    (KnownSchema cols) =>
    ParquetReadOptions -> FilePath -> IO (TypedDataFrame cols)
readParquetWithOpts opts path =
    Parquet.readParquetWithOpts opts path >>= freezeOrThrow @cols

-- | Read a directory\/glob of Parquet files into a typed DataFrame.
readParquetFiles ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquetFiles path = Parquet.readParquetFiles path >>= freezeOrThrow @cols
