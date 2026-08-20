{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Shuffle where

import qualified DataFrame as D

import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as VU
import DataFrame.Operations.Permutation (shuffle, shuffledIndices)
import System.Random (mkStdGen)
import Test.HUnit (Test (..), assertEqual)

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

-- Each position keeps its own index with probability 1/n under a uniform
-- shuffle. Bounds are wide enough that sampling noise cannot trip them, but
-- narrow enough to catch a shuffle that only permutes part of the vector or
-- that never leaves an element in place.
shuffleDoesNotFavourAnyPosition :: Test
shuffleDoesNotFavourAnyPosition =
    let n = 10
        trials = 400
        samples =
            [VU.toList (shuffledIndices (mkStdGen s) n) | s <- [1 .. trials]]
        fixedAt p = length [() | xs <- samples, xs !! p == p]
        lo = trials `div` (4 * n)
        hi = 3 * trials `div` n
        outliers = [p | p <- [0 .. n - 1], fixedAt p < lo || fixedAt p > hi]
     in TestCase
            ( assertEqual
                "every position keeps its index at roughly the same rate"
                []
                outliers
            )

tests :: [Test]
tests =
    [ TestLabel "shuffleSingleRow" shuffleSingleRow
    , TestLabel
        "shuffleDoesNotFavourAnyPosition"
        shuffleDoesNotFavourAnyPosition
    , TestLabel "shuffleShuffles" shuffleShuffles
    , TestLabel "shufflePreservesData" shufflePreservesData
    , TestLabel "shufflePreservesColumnNames" shufflePreservesColumnNames
    , TestLabel "shuffleSameSeedIsSameShuffle" shuffleSameSeedIsSameShuffle
    , TestLabel "shuffleDifferentSeedIsDifferent" shuffleDifferentSeedIsDifferent
    , TestLabel "shuffleDoesNotAddOrDropIndices" shuffleDoesNotAddOrDropIndices
    ]
