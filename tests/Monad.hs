{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Monad where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame
import DataFrame.Monad
import GenDataFrame ()
import System.Random
import qualified Test.HUnit as H
import Test.QuickCheck
import Test.QuickCheck.Monadic

roundToTwoPlaces :: Double -> Double
roundToTwoPlaces x = fromIntegral (round (x * 100) :: Int) / 100.0

prop_sampleM :: DataFrame -> Gen (Gen Property)
prop_sampleM df = monadic' $ do
    p <- run $ choose (0.0 :: Double, 1.0 :: Double)
    let expectedRate = roundToTwoPlaces p
    seed <- run $ choose (0, 1000)
    let rowCount = D.nRows df
    let colCount = D.nColumns df
    pre (colCount > 1 && rowCount > 100)
    let finalDf = execFrameM df (sampleM (mkStdGen seed) expectedRate)
    let finalRowCount = D.nRows finalDf
    let realRate = roundToTwoPlaces $ fromIntegral finalRowCount / fromIntegral rowCount
    let diff = abs $ expectedRate - realRate
    -- calculates the 99.99% confidence interval (quickcheck runs 100 tests, aim for 1/10000)
    let tolerance' = 3.89 * sqrt (expectedRate * (1 - expectedRate) / fromIntegral rowCount)
    assert (diff <= tolerance')

tests :: [DataFrame -> Gen (Gen Property)]
tests = [prop_sampleM]

-- Column-shaped verbs: 'dropM' drops rows, so these had no monadic spelling.

verbFixture :: DataFrame
verbFixture =
    D.fromNamedColumns
        [ ("A", DI.fromList ([3, 1, 2] :: [Int]))
        , ("B", DI.fromList (["x", "y", "z"] :: [T.Text]))
        , ("C", DI.fromList ([1.0, 2.0, 3.0] :: [Double]))
        ]

selectMKeepsColumns :: H.Test
selectMKeepsColumns =
    H.TestCase
        ( H.assertEqual
            "selectM keeps only the named columns"
            ["A", "B"]
            (D.columnNames (execFrameM verbFixture (selectM ["A", "B"])))
        )

excludeMDropsColumns :: H.Test
excludeMDropsColumns =
    H.TestCase
        ( H.assertEqual
            "excludeM drops the named columns"
            ["A", "C"]
            (D.columnNames (execFrameM verbFixture (excludeM ["B"])))
        )

sortByMOrdersRows :: H.Test
sortByMOrdersRows =
    H.TestCase
        ( H.assertEqual
            "sortByM sorts ascending on A"
            [1, 2, 3]
            ( D.columnAsList @Int
                (F.col @Int "A")
                (execFrameM verbFixture (sortByM [Asc (F.col @Int "A")]))
            )
        )

hunitTests :: [H.Test]
hunitTests =
    [ H.TestLabel "selectMKeepsColumns" selectMKeepsColumns
    , H.TestLabel "excludeMDropsColumns" excludeMDropsColumns
    , H.TestLabel "sortByMOrdersRows" sortByMOrdersRows
    ]
