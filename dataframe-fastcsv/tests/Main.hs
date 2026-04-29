{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified System.Exit as Exit

import Test.HUnit
import Test.QuickCheck

import qualified Operations.ReadCsv
import qualified Properties.Csv

tests :: Test
tests = TestList Operations.ReadCsv.tests

isSuccessful :: Result -> Bool
isSuccessful (Success{}) = True
isSuccessful _ = False

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else do
            propsRes <- mapM (quickCheckWithResult stdArgs) Properties.Csv.tests
            if not (all isSuccessful propsRes)
                then Exit.exitFailure
                else Exit.exitSuccess
