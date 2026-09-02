module Main where

import qualified CaliforniaHousing
import qualified Chipotle
import qualified Iris
import qualified OneBillionRowChallenge
import System.Environment (getArgs)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["chipotle"] -> Chipotle.run
        ["california_housing"] -> CaliforniaHousing.run
        ["one_billion_row_challenge"] -> OneBillionRowChallenge.run
        ["one_billion_row_challenge", path] ->
            OneBillionRowChallenge.runCalculation path
        ["iris"] -> Iris.run
        _ ->
            putStrLn
                "Usage: examples <chipotle|california_housing|one_billion_row_challenge [csv-path]|iris>"
