{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified System.Exit as Exit

import Test.HUnit
import Test.QuickCheck

import qualified Cart
import qualified DecisionTree
import qualified Learn.EdgeCases
import qualified Learn.NumericalRigor
import qualified Learn.Numerics
import qualified Learn.Symbolic
import qualified LinearSolver
import qualified Properties.Simplify
import qualified TreePruning
import qualified Worklist

tests :: Test
tests =
    TestList $
        Cart.tests
            ++ DecisionTree.tests
            ++ LinearSolver.tests
            ++ TreePruning.tests
            ++ Worklist.tests
            ++ Learn.Numerics.tests
            ++ Learn.Symbolic.tests
            ++ Learn.EdgeCases.tests
            ++ Learn.NumericalRigor.tests

isSuccessful :: Result -> Bool
isSuccessful (Success{}) = True
isSuccessful _ = False

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else do
            simpRes <- mapM (quickCheckWithResult stdArgs) Properties.Simplify.tests
            wlRes <- mapM (quickCheckWithResult stdArgs) Worklist.props
            if not (all isSuccessful simpRes) || not (all isSuccessful wlRes)
                then Exit.exitFailure
                else Exit.exitSuccess
