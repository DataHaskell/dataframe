module Main where

import qualified Chipotle
import qualified OneBillionRowChallenge
import qualified TypedHousingMini
import System.Environment (getArgs)

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["chipotle"] -> Chipotle.run
        ["california_housing"] ->
            putStrLn "california_housing example requires hasktorch (disabled on Windows)."
        ["one_billion_row_challenge"] -> OneBillionRowChallenge.run
        ["iris"] ->
            putStrLn "iris example requires hasktorch (disabled on Windows)."
        ["typed_housing_mini"] -> TypedHousingMini.run
        _ ->
            putStrLn
                "Usage: examples <chipotle|california_housing|one_billion_row_challenge|iris|typed_housing_mini>"
