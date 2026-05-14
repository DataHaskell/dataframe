{-# LANGUAGE FlexibleContexts #-}

{- |
Module      : DataFrame.TH.CSV
License     : MIT

CSV-file-based 'DataFrame.TH' splices. Splits out the CSV ingest path so
@dataframe-th@ stays IO-agnostic.
-}
module DataFrame.TH.CSV (
    declareColumnsFromCsvFile,
    declareColumnsFromCsvWithOpts,
) where

import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH

import qualified DataFrame.IO.CSV as CSV
import DataFrame.TH.Records (declareColumns)

{- | Splice a binding for every column of the 'DataFrame' read from a CSV
file. Each binding has type @Expr T@ where @T@ is the inferred column
type.
-}
declareColumnsFromCsvFile :: String -> DecsQ
declareColumnsFromCsvFile path = do
    df <-
        liftIO
            ( CSV.readSeparated
                (CSV.defaultReadOptions{CSV.numColumns = Just 100})
                path
            )
    declareColumns df

-- | Like 'declareColumnsFromCsvFile' but with custom 'CSV.ReadOptions'.
declareColumnsFromCsvWithOpts :: CSV.ReadOptions -> String -> DecsQ
declareColumnsFromCsvWithOpts opts path = do
    df <- liftIO (CSV.readSeparated opts path)
    declareColumns df
