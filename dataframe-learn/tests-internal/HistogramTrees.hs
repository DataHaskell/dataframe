{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Histogram trees (the gradient-boosting learner): the compiled tree must
route rows exactly as fitting did, and with no more distinct values than bins
it must partition the rows like the exact regression tree.
-}
module HistogramTrees (tests) where

import Data.Either (fromRight)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Test.HUnit

import DataFrame.DecisionTree.Cart (cartFeatures)
import DataFrame.DecisionTree.Fit (treeToExpr)
import DataFrame.DecisionTree.Histogram (TreeLimits (..), binFeatures, fitBinnedTree)
import DataFrame.DecisionTree.Regression (
    RegFit (..),
    RegTreeConfig (..),
    defaultRegTreeConfig,
    fitRegTree,
 )
import DataFrame.DecisionTree.Types (Tree)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Interpreter (interpret)
import qualified DataFrameApi as D

tests :: [Test]
tests =
    [ TestLabel "histogram tree: in-sample == interpreted (mixed nullable frame)" inSampleMatchesInterpret
    , TestLabel "histogram tree: in-sample == interpreted (more values than bins)" coarseBinsMatchInterpret
    , TestLabel "histogram tree: same partition as the exact tree" matchesExactTree
    ]

limits :: Int -> TreeLimits
limits depth = TreeLimits depth 2 1 0

fitBinned :: Int -> Int -> D.DataFrame -> VU.Vector Double -> (Tree Double, VU.Vector Double)
fitBinned bins depth df y =
    fitBinnedTree (limits depth) (binFeatures bins (V.fromList (cartFeatures "y" df))) y Nothing

interpreted :: D.DataFrame -> Tree Double -> VU.Vector Double
interpreted df t = case interpret @Double df (treeToExpr t) of
    Right (DI.TColumn c) -> fromRight VU.empty (DI.toVector @Double @VU.Vector c)
    Left e -> error (show e)

maybeCol :: (DI.Columnable a) => [Maybe a] -> DI.Column
maybeCol = DI.fromVector . V.fromList

-- Nullable Double, Int and Text columns plus a plain one, as in NullSplits.
mixedFrame :: Int -> ([Double], D.DataFrame)
mixedFrame n = (y, D.fromColumns (("y", DI.fromList y) : cols))
  where
    rows = [0 .. n - 1]
    md = [if i `mod` 7 == 0 then Nothing else Just (fromIntegral ((i * 37) `mod` 23) :: Double) | i <- rows]
    mi = [if i `mod` 5 == 2 then Nothing else Just ((i * 11) `mod` 9 :: Int) | i <- rows]
    mt = [if i `mod` 6 == 4 then Nothing else Just (["a", "b", "c"] !! (i `mod` 3) :: T.Text) | i <- rows]
    z = [fromIntegral ((i * 13) `mod` 17) :: Double | i <- rows]
    y = [fromIntegral ((i * 29) `mod` 31) / 7 :: Double | i <- rows]
    cols = [("md", maybeCol md), ("mi", maybeCol mi), ("mt", maybeCol mt), ("z", DI.fromList z)]

inSampleMatchesInterpret :: Test
inSampleMatchesInterpret = TestCase $ do
    let (y, df) = mixedFrame 60
        (t, inSample) = fitBinned 1024 6 df (VU.fromList y)
    assertEqual "in-sample predictions equal the interpreted tree" (interpreted df t) inSample

-- 23 distinct values of md squeezed into 4 bins: thresholds come from the
-- equal-count cuts, and routing must still agree.
coarseBinsMatchInterpret :: Test
coarseBinsMatchInterpret = TestCase $ do
    let (y, df) = mixedFrame 200
        (t, inSample) = fitBinned 4 5 df (VU.fromList y)
    assertEqual "in-sample predictions equal the interpreted tree" (interpreted df t) inSample

matchesExactTree :: Test
matchesExactTree = TestCase $ do
    let (y, df) = mixedFrame 120
        yv = VU.fromList y
        (_, binned) = fitBinned 1024 4 df yv
        exact =
            rfFitted
                ( fitRegTree
                    defaultRegTreeConfig{rtMaxDepth = 4}
                    (V.fromList (cartFeatures "y" df))
                    yv
                    Nothing
                )
        gap = VU.maximum (VU.zipWith (\a b -> abs (a - b)) binned exact)
    assertBool ("same leaves up to rounding, max gap " ++ show gap) (gap < 1e-9)
