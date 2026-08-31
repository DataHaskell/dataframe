module DataFrame.IO.Parquet.Writer.Options (
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
) where

import DataFrame.IO.Parquet.Thrift
import Pinch (enum)

data WriterStrategy = InMemory | TwoPass
    deriving (Eq, Show)

data ParquetWriteOptions = ParquetWriteOptions
    { pageSize :: !Int
    , rowGroupSize :: !Int
    , batchRows :: !Int
    , subBatchRows :: !Int
    , compressionCodec :: !CompressionCodec
    , strategy :: !WriterStrategy
    , maxRowsPerFile :: !(Maybe Int)
    }
    deriving (Eq, Show)

defaultParquetWriteOptions :: ParquetWriteOptions
defaultParquetWriteOptions =
    ParquetWriteOptions
        { pageSize = 1048576
        , rowGroupSize = 134217728
        , batchRows = 8192
        , subBatchRows = 2048
        , compressionCodec = SNAPPY enum
        , strategy = InMemory
        , maxRowsPerFile = Nothing
        }
