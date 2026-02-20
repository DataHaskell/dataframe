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
    let proportion = roundToTwoPlaces p
    seed <- run $ generate (choose (0, 1000))
    let rowCount = D.nRows df
    pre (rowCount > 100 && proportion > 0.1 && proportion < 0.8)
    traceM $ "Rows in initialDf: " ++ show rowCount
    -- traceM $ show df
    let finalDf = execFrameM df (sampleM (mkStdGen seed) proportion)
    let finalRowCount = D.nRows finalDf
    let realProportion = roundToTwoPlaces $ fromIntegral finalRowCount / fromIntegral rowCount
    traceM $
        "Rows in finalDf: "
            ++ show finalRowCount
            ++ "; proportion is "
            ++ show realProportion
            ++ " where "
            ++ show proportion
            ++ " was expected"
    let diff = abs $ proportion - realProportion
    traceM $ "Diff is " ++ show diff
    assert (diff <= 0.1)

tests = [prop_sampleM]
