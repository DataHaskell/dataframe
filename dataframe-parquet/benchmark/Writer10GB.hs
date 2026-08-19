module Main (main) where

import Control.DeepSeq (NFData (rnf))
import Criterion.Main (bench, defaultMain, envWithCleanup, whnfIO)
import DataFrame.IO.Parquet.Writer (writeParquet)
import DataFrame.Internal.DataFrame (DataFrame, forceDataFrame)
import DataFrame10GB (stressDataFrame)
import System.Directory (removeDirectoryRecursive)
import System.FilePath ((</>))
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)

data BenchmarkEnvironment = BenchmarkEnvironment
    { benchmarkDataFrame :: DataFrame
    , benchmarkDirectory :: FilePath
    , benchmarkOutput :: FilePath
    }

instance NFData BenchmarkEnvironment where
    rnf environment =
        forceDataFrame (benchmarkDataFrame environment) `seq`
            rnf (benchmarkDirectory environment) `seq`
                rnf (benchmarkOutput environment)

prepareEnvironment :: IO BenchmarkEnvironment
prepareEnvironment = do
    temporary <- getCanonicalTemporaryDirectory
    directory <- createTempDirectory temporary "dataframe-parquet-writer-10gb"
    pure
        BenchmarkEnvironment
            { benchmarkDataFrame = stressDataFrame
            , benchmarkDirectory = directory
            , benchmarkOutput = directory </> "benchmark.parquet"
            }

cleanupEnvironment :: BenchmarkEnvironment -> IO ()
cleanupEnvironment = removeDirectoryRecursive . benchmarkDirectory

main :: IO ()
main =
    defaultMain
        [ envWithCleanup prepareEnvironment cleanupEnvironment $ \environment ->
            -- Memory usage for this benchmark will be north of 20 GB.
            bench "write 10 GiB dataframe" $
                whnfIO
                    ( writeParquet
                        (benchmarkOutput environment)
                        (benchmarkDataFrame environment)
                    )
        ]
