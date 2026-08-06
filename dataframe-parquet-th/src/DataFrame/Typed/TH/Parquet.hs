{- |
Module      : DataFrame.Typed.TH.Parquet
License     : MIT

Parquet-file-based typed-schema derivation. The typed counterpart of
'DataFrame.TH.Parquet.declareColumnsFromParquetFile': instead of binding one
@Expr@ per column, it generates a type synonym describing the file's schema,
suitable for 'DataFrame.Typed.freeze' / @readParquet \@Schema@.

@
deriveSchemaFromParquetFile \"Trips\" \"trips.parquet\"
-- generates:  type Trips = '[ '(\"id\", Int), '(\"fare\", Double), ...]
@
-}
module DataFrame.Typed.TH.Parquet (
    deriveSchemaFromParquetFile,
) where

import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH

import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Typed.TH.Records (deriveSchema)

-- | Derive a typed schema synonym from a Parquet file (or directory\/glob).
deriveSchemaFromParquetFile :: String -> String -> DecsQ
deriveSchemaFromParquetFile typeName path = do
    df <- liftIO (Parquet.readParquetFiles path)
    deriveSchema typeName df
