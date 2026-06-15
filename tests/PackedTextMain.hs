module Main (main) where

import qualified Internal.PackedText as PackedText
import qualified System.Exit as Exit
import Test.HUnit

main :: IO ()
main = do
    result <- runTestTT (TestList PackedText.tests)
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
