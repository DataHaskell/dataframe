
module Main (main) where

import Control.Exception (evaluate)
import Control.Monad (unless)
import DataFrame.IO.Parquet (readParquet)
import DataFrame.IO.Parquet.Writer (writeParquet)
import DataFrame.Internal.DataFrame (forceDataFrame)
import DataFrame10GB (
    stressColumns,
    stressDataFrame,
    stressResidentBytesLowerBound,
    stressRows,
 )
import System.Exit (exitFailure)
import System.FilePath ((</>))
import System.IO (hPutStrLn, stderr)
import System.IO.Temp (withSystemTempDirectory)

main :: IO ()
main = withSystemTempDirectory "dataframe-parquet-10gb-stress" $ \directory -> do
    expected <- evaluate (forceDataFrame stressDataFrame)
    let output = directory </> "roundtrip.parquet"
    putStrLn
        ( "writing "
            <> show stressRows
            <> " rows x "
            <> show stressColumns
            <> " columns (at least "
            <> show stressResidentBytesLowerBound
            <> " resident payload bytes)"
        )
    writeParquet output expected
    putStrLn "reading the stress dataframe"
    actual <- readParquet output
    putStrLn "checking dataframe equivalence"
    unless (expected == actual) $ do
        hPutStrLn stderr "10 GiB Parquet roundtrip mismatch"
        exitFailure
