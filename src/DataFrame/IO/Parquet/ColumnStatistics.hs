module DataFrame.IO.Parquet.ColumnStatistics where

import qualified Data.ByteString as BS
import Data.Int (Int64)

data ColumnStatistics = ColumnStatistics
    { columnMin :: BS.ByteString
    , columnMax :: BS.ByteString
    , columnNullCount :: Int64
    , columnDistictCount :: Int64
    , columnMinValue :: BS.ByteString
    , columnMaxValue :: BS.ByteString
    , isColumnMaxValueExact :: Bool
    , isColumnMinValueExact :: Bool
    }
    deriving (Show, Eq)

emptyColumnStatistics :: ColumnStatistics
emptyColumnStatistics = ColumnStatistics BS.empty BS.empty 0 0 BS.empty BS.empty False False
