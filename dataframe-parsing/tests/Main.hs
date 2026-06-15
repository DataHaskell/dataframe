module Main where

import qualified System.Exit as Exit

import Test.HUnit
import Test.QuickCheck

import qualified Properties.FastParsing
import qualified Unit.FastParsing

isSuccessful :: Result -> Bool
isSuccessful (Success{}) = True
isSuccessful _ = False

runProp :: (String, Property) -> IO Result
runProp (name, prop) = do
    putStrLn ("=== prop " ++ name)
    quickCheckWithResult stdArgs{maxSuccess = 20000} prop

main :: IO ()
main = do
    result <- runTestTT (TestList Unit.FastParsing.tests)
    propsRes <- mapM runProp Properties.FastParsing.tests
    if failures result > 0 || errors result > 0 || not (all isSuccessful propsRes)
        then Exit.exitFailure
        else Exit.exitSuccess
