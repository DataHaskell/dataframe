{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified System.Exit as Exit

import GenDataFrame ()
import Test.HUnit
import Test.QuickCheck

import qualified DecisionTree
import qualified Functions
import qualified IO.CSV
import qualified IO.JSON
import qualified Internal.Parsing
import qualified LazyParquet
import qualified Monad
import qualified Operations.Aggregations
import qualified Operations.Apply
import qualified Operations.Core
import qualified Operations.Derive
import qualified Operations.Filter
import qualified Operations.GroupBy
import qualified Operations.InsertColumn
import qualified Operations.Join
import qualified Operations.Merge
import qualified Operations.Nullable
import qualified Operations.Provenance
import qualified Operations.ReadCsv
import qualified Operations.Shuffle
import qualified Operations.Sort
import qualified Operations.Statistics
import qualified Operations.Subset
import qualified Operations.Take
import qualified Operations.Typing
import qualified Operations.Window
import qualified Operations.WriteCsv
import qualified Parquet
import qualified Properties

tests :: Test
tests =
    TestList $
        DecisionTree.tests
            ++ Internal.Parsing.tests
            ++ Operations.Aggregations.tests
            ++ Operations.Apply.tests
            ++ Operations.Core.tests
            ++ Operations.Derive.tests
            ++ Operations.Filter.tests
            ++ Operations.GroupBy.tests
            ++ Operations.InsertColumn.tests
            ++ Operations.Join.tests
            ++ Operations.Merge.tests
            ++ Operations.Nullable.tests
            ++ Operations.Provenance.tests
            ++ Operations.ReadCsv.tests
            ++ Operations.WriteCsv.tests
            ++ Operations.Shuffle.tests
            ++ Operations.Sort.tests
            ++ Operations.Statistics.tests
            ++ Operations.Subset.hunitTests
            ++ Operations.Take.tests
            ++ Operations.Typing.tests
            ++ Operations.Window.tests
            ++ Functions.tests
            ++ IO.CSV.tests
            ++ IO.JSON.tests
            ++ Parquet.tests
            ++ LazyParquet.tests

isSuccessful :: Result -> Bool
isSuccessful (Success{}) = True
isSuccessful _ = False

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else do
            -- Property tests
            propRes <-
                mapM
                    (quickCheckWithResult stdArgs)
                    Operations.Subset.tests
            monadRes <- mapM (quickCheckWithResult stdArgs) Monad.tests
            propsRes <- mapM (quickCheckWithResult stdArgs) Properties.tests
            if not (all isSuccessful propRes)
                || not (all isSuccessful monadRes)
                || not (all isSuccessful propsRes)
                then Exit.exitFailure
                else Exit.exitSuccess
