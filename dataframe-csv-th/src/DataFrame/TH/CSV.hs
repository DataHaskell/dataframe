{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : DataFrame.TH.CSV
License     : MIT

CSV-file-based 'DataFrame.TH' splices. Splits out the CSV ingest path so
@dataframe-th@ stays IO-agnostic.
-}
module DataFrame.TH.CSV (
    declareColumnsFromCsvFile,
    declareColumnsFromCsvWithOpts,
    deriveSchemaValuesFromCsvFile,
    deriveSchemaValuesFromCsvWithOpts,
) where

import qualified Data.ByteString as BS

import Control.Monad.IO.Class (liftIO)
import Data.Maybe (fromMaybe)
import Language.Haskell.TH
import System.IO (IOMode (ReadMode), withFile)

import qualified DataFrame.IO.CSV as CSV
import DataFrame.TH.Records (declareColumns, declareSchemaValues)

{- | Splice a binding for every column of the 'DataFrame' read from a CSV
file. Each binding has type @Expr T@ where @T@ is the inferred column
type.
-}
declareColumnsFromCsvFile :: String -> DecsQ
declareColumnsFromCsvFile path = do
    df <-
        liftIO
            ( CSV.readSeparated
                (CSV.defaultReadOptions{CSV.numRowsToRead = Just 100})
                path
            )
    declareColumns df

-- | Like 'declareColumnsFromCsvFile' but with custom 'CSV.ReadOptions'.
declareColumnsFromCsvWithOpts :: CSV.ReadOptions -> String -> DecsQ
declareColumnsFromCsvWithOpts opts path = do
    df <- liftIO (CSV.readSeparated opts path)
    declareColumns df

deriveSchemaValuesFromCsvFile :: String -> String -> DecsQ
deriveSchemaValuesFromCsvFile =
    deriveSchemaValuesFromCsvWithOpts
        CSV.defaultReadOptions{CSV.numRowsToRead = Just 100}

deriveSchemaValuesFromCsvWithOpts ::
    CSV.ReadOptions -> String -> String -> DecsQ
deriveSchemaValuesFromCsvWithOpts opts prefix path = do
    df <- liftIO $ do
        sample <- readCsvSample sampleWindowBytes path
        CSV.decodeSeparatedStrict opts sample
    declareSchemaValues prefix df

sampleWindowBytes :: Int
sampleWindowBytes = 1024 * 1024

readCsvSample :: Int -> FilePath -> IO BS.ByteString
readCsvSample n path = do
    chunk <- withFile path ReadMode (\h -> BS.hGet h (n + 1))
    let stripBom b = fromMaybe b (BS.stripPrefix "\xEF\xBB\xBF" b)
    pure . stripBom $
        if BS.length chunk <= n
            then chunk
            else
                let window = BS.take n chunk
                 in maybe window (\i -> BS.take (i + 1) window) $
                        BS.elemIndexEnd 0x0a window
