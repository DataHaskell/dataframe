{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Shuffle where

import qualified DataFrame as D

import Data.List (permutations)
import qualified Data.Map.Strict as M
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as VU
import DataFrame.Operations.Permutation (shuffle, shuffledIndices)
import System.Random (mkStdGen)
import Test.HUnit (Test (..), assertBool, assertEqual)

testDataFrame :: D.DataFrame
testDataFrame =
    D.fromNamedColumns
        [ ("numbers", D.fromList @Int [1 .. 26])
        ]

-- Test that shuffling does anything at all
shuffleShuffles :: Test
shuffleShuffles =
    let gen = mkStdGen 1234
        shuffled = shuffle gen testDataFrame
        initialNumbers = D.extractNumericColumn "numbers" testDataFrame
        shuffledNumbers = D.extractNumericColumn "numbers" shuffled
     in TestCase
            ( assertEqual
                "Shuffled column unequal to initial column"
                False
                (initialNumbers == shuffledNumbers)
            )

shufflePreservesColumnNames :: Test
shufflePreservesColumnNames =
    let gen = mkStdGen 837
        shuffled = shuffle gen testDataFrame
     in TestCase
            ( assertEqual
                "Column names are unchanged"
                (D.columnNames shuffled)
                (D.columnNames testDataFrame)
            )

-- Test that un-shuffling restores the original dataframe
-- which is known to be sorted in this case
shufflePreservesData :: Test
shufflePreservesData =
    let gen = mkStdGen 1234
        shuffled = shuffle gen testDataFrame
        sortedShuffled = D.sortBy [D.Asc (D.col @Int "numbers")] shuffled
     in TestCase
            (assertEqual "sort recovers initial numbers" testDataFrame sortedShuffled)

-- Test that shuffling isn't doing anything sneaky with summoning
-- random numbers somehow
shuffleSameSeedIsSameShuffle :: Test
shuffleSameSeedIsSameShuffle =
    let gen = mkStdGen 1234
        shuffled1 = shuffle gen testDataFrame
        shuffled2 = shuffle gen testDataFrame
     in TestCase
            (assertEqual "shuffle with same seed gives same result" shuffled1 shuffled2)

-- Test that different seeds give different results
shuffleDifferentSeedIsDifferent :: Test
shuffleDifferentSeedIsDifferent =
    let gen1 = mkStdGen 1234
        gen2 = mkStdGen 4321
        shuffled1 = shuffle gen1 testDataFrame
        shuffled2 = shuffle gen2 testDataFrame
     in TestCase
            ( assertEqual
                "shuffle with different seeds gives different results"
                False
                (shuffled1 == shuffled2)
            )

-- Test that ShuffleIndeces does not dorp, add, or repeat any index
shuffleDoesNotAddOrDropIndices :: Test
shuffleDoesNotAddOrDropIndices =
    let
        gen = mkStdGen 42
        actual = Set.fromList [0 .. 10]
        computedVector = shuffledIndices gen 11
        computed = (Set.fromList $ VU.toList $ shuffledIndices gen 11)
     in
        TestList
            [ TestCase
                (assertEqual "Indecis are not dropped or added" (VU.length computedVector) 11)
            , TestCase (assertEqual "There are no repeated indecis" computed actual)
            ]

-- A one-row frame has exactly one permutation.
shuffleSingleRow :: Test
shuffleSingleRow =
    TestCase
        ( assertEqual
            "shuffling one index yields that index"
            (VU.fromList [0 :: Int])
            (shuffledIndices (mkStdGen 7) 1)
        )

{- | Chi-squared statistic of observed counts against a flat expectation:
sum over cells of (observed - expected)^2 / expected.
-}
chiSquared :: [Int] -> Double
chiSquared counts =
    let expected = fromIntegral (sum counts) / fromIntegral (length counts)
     in sum [(fromIntegral o - expected) ^ (2 :: Int) / expected | o <- counts]

{- | Every permutation of n items is equally likely under a uniform shuffle,
so the counts over all n! outcomes are chi-squared with n! - 1 degrees of
freedom. Testing the whole permutation, rather than one position at a time,
also catches a shuffle whose positions are individually uniform but
correlated. Seeds are fixed, so the sample -- and the verdict -- is
deterministic.

n = 5 gives 120 outcomes; 12000 draws puts 100 in each on average. The bound
is the 0.999 quantile of chi-squared with 119 degrees of freedom.
-}
shufflePermutationsAreUniform :: Test
shufflePermutationsAreUniform =
    let n = 5
        trials = 12000
        observed =
            M.fromListWith
                (+)
                [(VU.toList (shuffledIndices (mkStdGen s) n), 1 :: Int) | s <- [1 .. trials]]
        counts = [M.findWithDefault 0 p observed | p <- permutations [0 .. n - 1]]
        stat = chiSquared counts
     in TestCase
            ( assertBool
                ("chi-squared over all permutations is " ++ show stat ++ ", above 172.4")
                (stat < 172.4)
            )

{- | The frequency test from Knuth 3.3.2: each item lands in each position with
probability 1/n, so the n x n position-by-item table is chi-squared with
(n - 1)^2 degrees of freedom. A larger n than the permutation test can afford,
to catch bias that only shows at scale, such as a shuffle that leaves a
suffix untouched or never leaves an item in place.

n = 10 and 5000 draws put 500 in each cell. The bound is the 0.999 quantile of
chi-squared with 81 degrees of freedom.
-}
shufflePositionsAreUniform :: Test
shufflePositionsAreUniform =
    let n = 10
        trials = 5000
        samples = [VU.toList (shuffledIndices (mkStdGen s) n) | s <- [1 .. trials]]
        cell p i = length [() | xs <- samples, xs !! p == i]
        stat = chiSquared [cell p i | p <- [0 .. n - 1], i <- [0 .. n - 1]]
     in TestCase
            ( assertBool
                ( "chi-squared over the position-by-item table is "
                    ++ show stat
                    ++ ", above 126.1"
                )
                (stat < 126.1)
            )

tests :: [Test]
tests =
    [ TestLabel "shuffleSingleRow" shuffleSingleRow
    , TestLabel "shufflePermutationsAreUniform" shufflePermutationsAreUniform
    , TestLabel "shufflePositionsAreUniform" shufflePositionsAreUniform
    , TestLabel "shuffleShuffles" shuffleShuffles
    , TestLabel "shufflePreservesData" shufflePreservesData
    , TestLabel "shufflePreservesColumnNames" shufflePreservesColumnNames
    , TestLabel "shuffleSameSeedIsSameShuffle" shuffleSameSeedIsSameShuffle
    , TestLabel "shuffleDifferentSeedIsDifferent" shuffleDifferentSeedIsDifferent
    , TestLabel "shuffleDoesNotAddOrDropIndices" shuffleDoesNotAddOrDropIndices
    ]
