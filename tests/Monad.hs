module Monad where

import qualified DataFrame as D
import DataFrame.Internal.DataFrame
import DataFrame.Monad
import GenDataFrame ()
import System.Random
import Test.QuickCheck
import Test.QuickCheck.Monadic

roundToTwoPlaces x = fromIntegral (round (x * 100)) / 100.0

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
    assert (diff <= 0.11)

tests = [prop_sampleM]
