{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Typed.IO.CSV (
    readCsv,
    readCsvWithError,
    readTsv,
    readCsvWithOpts,
    writeCsv,
    writeTsv,
) where

import qualified Data.Text as T

import DataFrame.IO.CSV (ReadOptions)
import qualified DataFrame.IO.CSV as CSV
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError, thaw)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

-- | Read a CSV file into a typed DataFrame, throwing on schema mismatch.
readCsv ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readCsv path = CSV.readCsv path >>= freezeOrThrow @cols

-- | Read a CSV file, returning a descriptive error on schema mismatch.
readCsvWithError ::
    forall cols.
    (KnownSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readCsvWithError path = freezeWithError <$> CSV.readCsv path

-- | Read a tab-separated file into a typed DataFrame, throwing on mismatch.
readTsv ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readTsv path = CSV.readTsv path >>= freezeOrThrow @cols

-- | Read a CSV file with custom options, throwing on schema mismatch.
readCsvWithOpts ::
    forall cols.
    (KnownSchema cols) =>
    ReadOptions -> FilePath -> IO (TypedDataFrame cols)
readCsvWithOpts opts path = CSV.readCsvWithOpts opts path >>= freezeOrThrow @cols

-- | Write a typed DataFrame to a CSV file.
writeCsv :: FilePath -> TypedDataFrame cols -> IO ()
writeCsv path = CSV.writeCsv path . thaw

-- | Write a typed DataFrame to a tab-separated file.
writeTsv :: FilePath -> TypedDataFrame cols -> IO ()
writeTsv path = CSV.writeTsv path . thaw
