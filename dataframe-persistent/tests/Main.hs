module Main where

import qualified System.Exit as Exit

import PersistentTests
import SchemaTHTests

import Test.HUnit

main :: IO ()
main = do
    result <- runTestTT (TestList [persistentTests, schemaThTests])
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
