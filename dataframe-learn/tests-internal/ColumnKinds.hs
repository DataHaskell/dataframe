{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Every tree learner splits on a column by its element type, never by its
storage: numbers (Double, Float, integral) by threshold, categories (Text,
String, Bool, Char) by level. Before 'DataFrame.DecisionTree.ColumnKind', an
unboxed Bool, Char or Float column was silently dropped.
-}
module ColumnKinds (tests) where

import Data.Either (fromRight)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Test.HUnit

import DataFrame.DecisionTree.Cart (cartFeaturesByColumn)
import DataFrame.DecisionTree.Fit (fitDecisionTree, treeToExpr)
import DataFrame.DecisionTree.Regression (
    RegFit (..),
    RegTreeConfig (..),
    defaultRegTreeConfig,
    fitRegTree,
 )
import DataFrame.DecisionTree.Types (TreeConfig (..), defaultTreeConfig)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr (..), getColumns)
import DataFrame.Internal.Interpreter (interpret)
import qualified DataFrameApi as D

tests :: [Test]
tests =
    [ TestLabel "column kinds: CART builds features for every element type" cartFeatureCounts
    , TestLabel "column kinds: regression tree splits on a Bool" (regressionSplits boolColumn)
    , TestLabel "column kinds: regression tree splits on a Char" (regressionSplits charColumn)
    , TestLabel "column kinds: regression tree splits on a Float" (regressionSplits floatColumn)
    , TestLabel "column kinds: regression tree splits on a String" (regressionSplits stringColumn)
    , TestLabel "column kinds: classifier splits on a Bool" (classifierSplits boolColumn)
    , TestLabel "column kinds: classifier splits on a Char" (classifierSplits charColumn)
    , TestLabel "column kinds: classifier splits on a Float" (classifierSplits floatColumn)
    , TestLabel "column kinds: classifier splits on a String" (classifierSplits stringColumn)
    ]

-- | Six rows; the second, fourth and fifth are the positive class in every fixture.
positives :: [Bool]
positives = [False, True, False, True, True, False]

boolColumn, charColumn, floatColumn, stringColumn :: DI.Column
boolColumn = DI.fromList positives
charColumn = DI.fromList [if p then 'y' else 'n' | p <- positives]
floatColumn = DI.fromList [if p then 2.5 else 0.5 :: Float | p <- positives]
stringColumn = DI.fromList [if p then "yes" else "no" :: String | p <- positives]

-- | Numbers give one threshold feature, categories one feature per level present.
cartFeatureCounts :: Test
cartFeatureCounts = TestCase $ do
    let df =
            D.fromColumns
                [ ("y", DI.fromList [0, 1, 0, 1 :: Double])
                , ("d", DI.fromList [0.5, 1.5, 2.5, 3.5 :: Double])
                , ("f", DI.fromList [0.5, 1.5, 2.5, 3.5 :: Float])
                , ("i", DI.fromList [1, 2, 3, 4 :: Int])
                , ("b", DI.fromList [False, True, False, True])
                , ("c", DI.fromList "abac")
                , ("t", DI.fromList ["p", "q", "p", "q" :: T.Text])
                , ("s", DI.fromList ["p", "q", "r", "q" :: String])
                , ("mb", DI.fromVector (V.fromList [Just True, Nothing, Just False, Just True]))
                ]
        counts = M.fromListWith (+) [(c, 1 :: Int) | (c, _) <- cartFeaturesByColumn "y" df]
    assertEqual
        "features per column"
        (M.fromList [("d", 1), ("f", 1), ("i", 1), ("b", 2), ("c", 3), ("t", 2), ("s", 3), ("mb", 2)])
        counts

-- | A depth-1 regression tree on the column alone fits the 0/1 target exactly.
regressionSplits :: DI.Column -> Test
regressionSplits col = TestCase $ do
    let y = [if p then 1 else 0 | p <- positives] :: [Double]
        df = D.fromColumns [("y", DI.fromList y), ("x", col)]
        fitted =
            fitRegTree
                defaultRegTreeConfig{rtMaxDepth = 1}
                (V.fromList (map snd (cartFeaturesByColumn "y" df)))
                (VU.fromList y)
                Nothing
        scored = case interpret @Double df (treeToExpr (rfTree fitted)) of
            Right (DI.TColumn c) -> fromRight VU.empty (DI.toVector @Double @VU.Vector c)
            Left e -> error (show e)
    assertEqual "fits exactly" (VU.fromList y) (rfFitted fitted)
    assertEqual "interpreted agrees" (VU.fromList y) scored

-- | A depth-1 classification tree on the column alone recovers the labels.
classifierSplits :: DI.Column -> Test
classifierSplits col = TestCase $ do
    let labels = [if p then "fraud" else "ok" | p <- positives] :: [T.Text]
        df = D.fromColumns [("label", DI.fromList labels), ("x", col)]
        e = fitDecisionTree @T.Text defaultTreeConfig{maxTreeDepth = 1} (Col "label") df
        predicted = case interpret @T.Text df e of
            Right (DI.TColumn c) -> fromRight V.empty (DI.toVector @T.Text @V.Vector c)
            Left err -> error (show err)
    assertEqual "splits on x" ["x"] (getColumns e)
    assertEqual "recovers the labels" (V.fromList labels) predicted
