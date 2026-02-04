module Monad where

import qualified DataFrame                    as D
import           DataFrame.Internal.DataFrame
import           DataFrame.Monad
import           GenDataFrame                 ()
import           System.Random
import           Test.QuickCheck
import           Test.QuickCheck.Monadic

prop_sampleM :: DataFrame -> Property
prop_sampleM df = monadic $ do
    let rowCount = D.nRows df
    pre (rowCount >= 10)
    let finalDf = execFrameM df (sampleM (mkStdGen 142) 0.1)
    let finalRowCount = D.nRows finalDf
    assert (rowCount > finalRowCount)


tests = [prop_sampleM]
