module Monad where

import qualified DataFrame as D
import DataFrame.Internal.DataFrame
import DataFrame.Monad
import Debug.Trace
import GenDataFrame ()
import System.Random
import Test.QuickCheck
import Test.QuickCheck.Monadic

roundToTwoPlaces x = fromIntegral (round (x * 100)) / 100.0

prop_sampleM :: DataFrame -> Property
prop_sampleM df = monadicIO $ do
    p <- run $ generate (choose (0.0 :: Double, 1.0 :: Double))
    let expectedRate = roundToTwoPlaces p
    seed <- run $ generate (choose (0, 1000))
    let rowCount = D.nRows df
    pre (rowCount > 100 && expectedRate > 0.1 && expectedRate < 0.8)
    traceM $ "Rows in initialDf: " ++ show rowCount
    -- traceM $ show df
    let finalDf = execFrameM df (sampleM (mkStdGen seed) expectedRate)
    let finalRowCount = D.nRows finalDf
    let realRate = roundToTwoPlaces $ fromIntegral finalRowCount / fromIntegral rowCount
    traceM $
        "Rows in finalDf: "
            ++ show finalRowCount
            ++ "; expectedRate is "
            ++ show realRate
            ++ " where "
            ++ show expectedRate
            ++ " was expected"
    let diff = abs $ expectedRate - realRate
    traceM $ "Diff is " ++ show diff
    assert (diff <= 0.1)

tests = [prop_sampleM]
