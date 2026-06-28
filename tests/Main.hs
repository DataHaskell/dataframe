{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified System.Exit as Exit

import GenDataFrame ()
import Test.HUnit
import Test.QuickCheck

import qualified Cart
import qualified DecisionTree
import qualified Functions
import qualified IO.CSV
import qualified IO.CsvGolden
import qualified IO.JSON
import qualified Internal.ColumnBuilder
import qualified Internal.DictEncode
import qualified Internal.Markdown
import qualified Internal.PackedText
import qualified Internal.Parsing
import qualified LazyParity
import qualified LazyParquet
import qualified Learn.Denotation
import qualified Learn.EdgeCases
import qualified Learn.Ensembles
import qualified Learn.Metamorphic
import qualified Learn.MetricsTests
import qualified Learn.Models
import qualified Learn.NumericalRigor
import qualified Learn.Numerics
import qualified Learn.SklearnParity
import qualified Learn.Symbolic
import qualified Learn.Synthesis
import qualified LinearSolver
import qualified Monad
import qualified Operations.Aggregations
import qualified Operations.Apply
import qualified Operations.Core
import qualified Operations.Derive
import qualified Operations.Filter
import qualified Operations.GroupBy
import qualified Operations.Inference
import qualified Operations.InsertColumn
import qualified Operations.Join
import qualified Operations.Merge
import qualified Operations.Nullable
import qualified Operations.NullableHashing
import qualified Operations.ParallelGroupBy
import qualified Operations.ParallelJoin
import qualified Operations.Provenance
import qualified Operations.ReadCsv
import qualified Operations.Record
import qualified Operations.SetOps
import qualified Operations.Shuffle
import qualified Operations.Sort
import qualified Operations.Statistics
import qualified Operations.Subset
import qualified Operations.Take
import qualified Operations.Typing
import qualified Operations.VectorKernel
import qualified Operations.Window
import qualified Operations.WriteCsv
import qualified PackedTextMigration
import qualified Parquet
import qualified Plotting
import qualified PrettyPrint
import qualified Properties
import qualified Properties.Categorical
import qualified Properties.Simplify
import qualified Simplify
import qualified TreePruning
import qualified Typed.IOReaders
import qualified Typed.Parity
import qualified Worklist

tests :: Test
tests =
    TestList $
        DecisionTree.tests
            ++ Internal.ColumnBuilder.tests
            ++ Internal.DictEncode.tests
            ++ Internal.Markdown.tests
            ++ Internal.PackedText.tests
            ++ Internal.Parsing.tests
            ++ Learn.Numerics.tests
            ++ Learn.Denotation.tests
            ++ Learn.Models.tests
            ++ Learn.Ensembles.tests
            ++ Learn.Symbolic.tests
            ++ Learn.SklearnParity.tests
            ++ Learn.Synthesis.tests
            ++ Learn.MetricsTests.tests
            ++ Learn.Metamorphic.tests
            ++ Learn.EdgeCases.tests
            ++ Learn.NumericalRigor.tests
            ++ Operations.Aggregations.tests
            ++ Operations.Apply.tests
            ++ Operations.Core.tests
            ++ Operations.Derive.tests
            ++ Operations.Filter.tests
            ++ Operations.GroupBy.tests
            ++ Operations.ParallelGroupBy.tests
            ++ Operations.ParallelJoin.tests
            ++ Operations.Inference.tests
            ++ Operations.InsertColumn.tests
            ++ Operations.Join.tests
            ++ Operations.Merge.tests
            ++ Operations.Nullable.tests
            ++ Operations.NullableHashing.tests
            ++ Operations.Provenance.tests
            ++ Operations.ReadCsv.tests
            ++ Operations.Record.tests
            ++ Operations.WriteCsv.tests
            ++ Operations.SetOps.tests
            ++ Operations.Shuffle.tests
            ++ Operations.Sort.tests
            ++ Operations.Statistics.tests
            ++ Operations.Subset.hunitTests
            ++ Operations.Take.tests
            ++ Operations.Typing.tests
            ++ Operations.VectorKernel.tests
            ++ Operations.Window.tests
            ++ Functions.tests
            ++ IO.CSV.tests
            ++ IO.CsvGolden.tests
            ++ IO.JSON.tests
            ++ Parquet.tests
            ++ LazyParquet.tests
            ++ LazyParity.tests
            ++ Plotting.tests
            ++ LinearSolver.tests
            ++ Simplify.tests
            ++ TreePruning.tests
            ++ Worklist.tests
            ++ Cart.tests
            ++ PackedTextMigration.tests
            ++ PrettyPrint.tests
            ++ Typed.Parity.tests
            ++ Typed.IOReaders.tests

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
            cbRes <-
                mapM
                    (quickCheckWithResult stdArgs)
                    Internal.ColumnBuilder.props
            propsRes <- mapM (quickCheckWithResult stdArgs) Properties.tests
            catRes <- mapM (quickCheckWithResult stdArgs) Properties.Categorical.tests
            simpRes <- mapM (quickCheckWithResult stdArgs) Properties.Simplify.tests
            wlRes <- mapM (quickCheckWithResult stdArgs) Worklist.props
            if not (all isSuccessful propRes)
                || not (all isSuccessful cbRes)
                || not (all isSuccessful monadRes)
                || not (all isSuccessful propsRes)
                || not (all isSuccessful catRes)
                || not (all isSuccessful simpRes)
                || not (all isSuccessful wlRes)
                then Exit.exitFailure
                else Exit.exitSuccess
