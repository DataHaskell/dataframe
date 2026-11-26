{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Decision-tree splits follow element kind, never physical column storage.
module ColumnKinds (tests) where

import Data.Either (fromRight)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Test.HUnit

import DataFrame.DecisionTree.Cart (cartFeaturesByColumn, cartFeaturesWith)
import DataFrame.DecisionTree.Fit (fitDecisionTree, treeToExpr)
import DataFrame.DecisionTree.Numeric (numericCols)
import DataFrame.DecisionTree.Regression (
    RegFit (..),
    RegTreeConfig (..),
    defaultRegTreeConfig,
    fitRegTree,
 )
import DataFrame.DecisionTree.Types (
    TreeConfig (..),
    defaultColumnOrdering,
    defaultTreeConfig,
    orderable,
 )
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
    , TestLabel "column kinds: custom ColumnOrdering reaches CART and classifier" customOrderingSplits
    , TestLabel "column kinds: MergedColumn is consistent across learners" mergedColumnSplits
    ]

-- | Six rows; the 2nd, 4th and 5th are positive in every fixture.
positives :: [Bool]
positives = [False, True, False, True, True, False]

boolColumn, charColumn, floatColumn, stringColumn :: DI.Column
boolColumn = DI.fromList positives
charColumn = DI.fromList [if p then 'y' else 'n' | p <- positives]
floatColumn = DI.fromList [if p then 2.5 else 0.5 :: Float | p <- positives]
stringColumn = DI.fromList [if p then "yes" else "no" :: String | p <- positives]

-- | Numbers and non-null pairs give 1 feature; k>=3 levels give k.
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
        (M.fromList [("d", 1), ("f", 1), ("i", 1), ("b", 1), ("c", 3), ("t", 1), ("s", 3), ("mb", 2)])
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

-- | Custom category type, unsplittable until registered via 'orderable'.
data Tier = Free | Pro | Enterprise
    deriving (Eq, Ord, Show)

customOrderingSplits :: Test
customOrderingSplits = TestCase $ do
    let tiers = [if p then Enterprise else Free | p <- positives]
        labels = [if p then "paid" else "free" :: T.Text | p <- positives]
        y = [if p then 1 else 0 :: Double | p <- positives]
        dfCls = D.fromColumns [("label", DI.fromList labels), ("tier", DI.fromList tiers)]
        dfReg = D.fromColumns [("y", DI.fromList y), ("tier", DI.fromList tiers)]
        ords = defaultColumnOrdering <> orderable @Tier
        unregistered = fitDecisionTree @T.Text defaultTreeConfig{maxTreeDepth = 1} (Col "label") dfCls
        registered =
            fitDecisionTree @T.Text
                defaultTreeConfig{maxTreeDepth = 1, columnOrdering = ords, taoIterations = 0}
                (Col "label")
                dfCls
        predicted = case interpret @T.Text dfCls registered of
            Right (DI.TColumn c) -> fromRight V.empty (DI.toVector @T.Text @V.Vector c)
            Left err -> error (show err)
        regFit =
            fitRegTree
                defaultRegTreeConfig{rtMaxDepth = 1}
                (V.fromList (cartFeaturesWith ords "y" dfReg))
                (VU.fromList y)
                Nothing
    assertEqual "ignored when unregistered" [] (getColumns unregistered)
    assertEqual "CART seed splits on tier when registered" ["tier"] (getColumns registered)
    assertEqual "classifier recovers labels" (V.fromList labels) predicted
    assertEqual "regression fits with custom ordering" (VU.fromList y) (rfFitted regFit)

-- | MergedColumn (These Int Int) splits consistently across learners.
mergedColumnSplits :: Test
mergedColumnSplits = TestCase $ do
    let leftCol = DI.fromList [if p then 1 else 0 :: Int | p <- positives]
        rightCol = DI.fromList [if p then 9 else 2 :: Int | p <- positives]
        merged = DI.mkMergedColumns leftCol rightCol
        labels = [if p then "pos" else "neg" :: T.Text | p <- positives]
        df = D.fromColumns [("label", DI.fromList labels), ("m", merged)]
        ords = defaultColumnOrdering <> orderable @(DI.These Int Int)
        e =
            fitDecisionTree @T.Text
                defaultTreeConfig{maxTreeDepth = 1, columnOrdering = ords}
                (Col "label")
                df
        predicted = case interpret @T.Text df e of
            Right (DI.TColumn c) -> fromRight V.empty (DI.toVector @T.Text @V.Vector c)
            Left err -> error (show err)
    assertEqual "merged column is not numeric" 0 (length (numericCols df))
    assertEqual "default ordering skips merged column" 0 (length (cartFeaturesWith defaultColumnOrdering "label" df))
    assertEqual "registered ordering gives 1 binary feature" 1 (length (cartFeaturesWith ords "label" df))
    assertEqual "classifier recovers labels on merged column" (V.fromList labels) predicted
