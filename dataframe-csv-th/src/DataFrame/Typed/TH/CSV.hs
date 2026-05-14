{- |
Module      : DataFrame.Typed.TH.CSV
License     : MIT

CSV-file-based typed-schema derivation. Splits the CSV path out of
@DataFrame.Typed.TH@ so it can live in @dataframe-csv-th@.
-}
module DataFrame.Typed.TH.CSV (
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
) where

import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH

import qualified DataFrame.IO.CSV as D
import DataFrame.Typed.TH.Records (deriveSchema)

deriveSchemaFromCsvFile :: String -> String -> DecsQ
deriveSchemaFromCsvFile = deriveSchemaFromCsvFileWith D.defaultReadOptions

deriveSchemaFromCsvFileWith :: D.ReadOptions -> String -> String -> DecsQ
deriveSchemaFromCsvFileWith opts typeName path = do
    df <- liftIO (D.readSeparated opts path)
    deriveSchema typeName df
