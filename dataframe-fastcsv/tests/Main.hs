module Main where

import qualified System.Exit as Exit

import Test.HUnit

import qualified Operations.ReadCsv

tests :: Test
tests = TestList Operations.ReadCsv.tests

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
