{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Tree learners on nullable columns: the compiled tree must route null rows
as fitting did, and the regression tree must send them to the side that fits.
-}
module NullSplits (tests) where

import Data.Either (fromRight)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Test.HUnit

import DataFrame.Boosting.AdaBoost (AdaBoostConfig (..), defaultAdaBoostConfig)
import DataFrame.DecisionTree.Cart (buildCartTree, cartFeatures)
import DataFrame.DecisionTree.Fit (treeToExpr)
import DataFrame.DecisionTree.Regression (
    RegFit (..),
    RegTreeConfig (..),
    defaultRegTreeConfig,
    fitRegTree,
 )
import DataFrame.DecisionTree.Types (Tree, TreeConfig (..), defaultTreeConfig)
import DataFrame.Internal.Expression (Expr (..))
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Model (Fit (..), Predict (..))
import qualified DataFrameApi as D

tests :: [Test]
tests =
    [ TestLabel "null splits: in-sample == interpreted (mixed nullable frame)" inSampleMatchesInterpret
    , TestLabel "null splits: nulls join the side they fit" learnedDirection
    , TestLabel "null splits: null-indicator split" nullIndicator
    , TestLabel "null splits: unseen nulls still route" unseenNullsRoute
    , TestLabel "null splits: CART null indicator" cartNullIndicator
    , TestLabel "null splits: AdaBoost null indicator" adaBoostNullIndicator
    ]

fitAll :: Int -> D.DataFrame -> VU.Vector Double -> (Tree Double, VU.Vector Double)
fitAll depth df y = (rfTree fitted, rfFitted fitted)
  where
    fitted =
        fitRegTree
            defaultRegTreeConfig{rtMaxDepth = depth}
            (V.fromList (cartFeatures "y" df))
            y
            Nothing

interpreted :: D.DataFrame -> Tree Double -> VU.Vector Double
interpreted df = interpretedExpr df . treeToExpr

interpretedExpr :: D.DataFrame -> Expr Double -> VU.Vector Double
interpretedExpr df e = case interpret @Double df e of
    Right (DI.TColumn c) -> fromRight VU.empty (DI.toVector @Double @VU.Vector c)
    Left e -> error (show e)

withTarget :: [Double] -> [(T.Text, DI.Column)] -> D.DataFrame
withTarget y cols = D.fromColumns (("y", DI.fromList y) : cols)

maybeCol :: (DI.Columnable a) => [Maybe a] -> DI.Column
maybeCol = DI.fromVector . V.fromList

inSampleMatchesInterpret :: Test
inSampleMatchesInterpret = TestCase $ do
    let n = 60 :: Int
        rows = [0 .. n - 1]
        md = [if i `mod` 7 == 0 then Nothing else Just (fromIntegral ((i * 37) `mod` 23) :: Double) | i <- rows]
        mi = [if i `mod` 5 == 2 then Nothing else Just ((i * 11) `mod` 9 :: Int) | i <- rows]
        mt = [if i `mod` 6 == 4 then Nothing else Just (["a", "b", "c"] !! (i `mod` 3) :: T.Text) | i <- rows]
        z = [fromIntegral ((i * 13) `mod` 17) :: Double | i <- rows]
        y = [fromIntegral ((i * 29) `mod` 31) / 7 :: Double | i <- rows]
        df =
            withTarget
                y
                [("md", maybeCol md), ("mi", maybeCol mi), ("mt", maybeCol mt), ("z", DI.fromList z)]
        (t, inSample) = fitAll 6 df (VU.fromList y)
    assertEqual "in-sample predictions equal the interpreted tree" (interpreted df t) inSample

-- The null rows have the same target as the low values of x, so one split
-- fits exactly only if it sends the nulls left.
learnedDirection :: Test
learnedDirection = TestCase $ do
    let y = [0, 0, 0, 1, 1, 1, 0, 0]
        x = map Just [1, 2, 3, 4, 5, 6] ++ [Nothing, Nothing] :: [Maybe Double]
        df = withTarget y [("x", maybeCol x)]
        (t, inSample) = fitAll 1 df (VU.fromList y)
    assertEqual "fits exactly" (VU.fromList y) inSample
    assertEqual "interpreted agrees" (VU.fromList y) (interpreted df t)

nullIndicator :: Test
nullIndicator = TestCase $ do
    let y = [0, 0, 0, 1, 1]
        x = [Just 1, Just 2, Just 3, Nothing, Nothing] :: [Maybe Int]
        df = withTarget y [("x", maybeCol x)]
        (t, inSample) = fitAll 1 df (VU.fromList y)
    assertEqual "fits exactly" (VU.fromList y) inSample
    assertEqual "interpreted agrees" (VU.fromList y) (interpreted df t)

unseenNullsRoute :: Test
unseenNullsRoute = TestCase $ do
    let y = [0, 0, 0, 1, 1, 1]
        x = map Just [1, 2, 3, 4, 5, 6] :: [Maybe Int]
        (t, inSample) = fitAll 1 (withTarget y [("x", maybeCol x)]) (VU.fromList y)
        testDf = withTarget y [("x", maybeCol (take 4 x ++ [Nothing, Nothing]))]
        scored = interpreted testDf t
    assertEqual "fits exactly" (VU.fromList y) inSample
    assertEqual "present rows unchanged" (VU.take 4 inSample) (VU.take 4 scored)
    assertBool "null rows land in a leaf" (VU.all (`elem` [0, 1]) (VU.drop 4 scored))

-- The target depends only on whether x is null. If the split's threshold were
-- +Infinity instead of 3, CART and AdaBoost would send the null rows left
-- while fitting but right in the compiled tree, and the fit would break.
indicatorFrame :: ([Double], D.DataFrame)
indicatorFrame = (y, withTarget y [("x", maybeCol x)])
  where
    y = [0, 0, 0, 1, 1]
    x = [Just 1, Just 2, Just 3, Nothing, Nothing] :: [Maybe Int]

cartNullIndicator :: Test
cartNullIndicator = TestCase $ do
    let (y, df) = indicatorFrame
        t = buildCartTree @Double defaultTreeConfig{maxTreeDepth = 1, minLeafSize = 1} "y" df
    assertEqual "fits exactly" (VU.fromList y) (interpreted df t)

adaBoostNullIndicator :: Test
adaBoostNullIndicator = TestCase $ do
    let (y, df) = indicatorFrame
        cfg = defaultAdaBoostConfig{abNEstimators = 1, abMaxDepth = 1}
        model = fit cfg (Col @Double "y") df
    assertEqual "fits exactly" (VU.fromList y) (interpretedExpr df (predict model))
