{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Specification for the fitted-tree pruning pass ('pruneDead'): path-condition
entailment, the false-edge NaN gate, and same-branch collapse.
-}
module TreePruning (tests) where

import DataFrame.DecisionTree (Tree (..), pruneDead)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (eqExpr)
import DataFrame.Operators

import qualified Data.Text as T
import Test.HUnit

treeEq :: (Eq a) => Tree a -> Tree a -> Bool
treeEq (Leaf x) (Leaf y) = x == y
treeEq (Branch c1 l1 r1) (Branch c2 l2 r2) = eqExpr c1 c2 && treeEq l1 l2 && treeEq r1 r2
treeEq _ _ = False

prunesTo :: String -> Tree T.Text -> Tree T.Text -> Test
prunesTo label input want =
    TestLabel label . TestCase $
        assertBool
            (label ++ ": got " ++ show (pruneDead input) ++ " want " ++ show want)
            (treeEq (pruneDead input) want)

preserved :: String -> Tree T.Text -> Test
preserved label t = prunesTo label t t

pathEntailment :: [Test]
pathEntailment =
    [ prunesTo
        "ancestor entails child keeps true subtree"
        ( Branch
            (F.col Double "age" .> F.lit (50 :: Double))
            (Branch (F.col Double "age" .> F.lit (30 :: Double)) (Leaf "a") (Leaf "b"))
            (Leaf "c")
        )
        (Branch (F.col Double "age" .> F.lit (50 :: Double)) (Leaf "a") (Leaf "c"))
    , prunesTo
        "ancestor refutes child keeps false subtree"
        ( Branch
            (F.col Double "age" .> F.lit (50 :: Double))
            (Branch (F.col Double "age" .< F.lit (40 :: Double)) (Leaf "a") (Leaf "b"))
            (Leaf "c")
        )
        (Branch (F.col Double "age" .> F.lit (50 :: Double)) (Leaf "b") (Leaf "c"))
    ]

falseEdgeGate :: [Test]
falseEdgeGate =
    [ prunesTo
        "integral false edge entails child"
        ( Branch
            (F.toDouble (F.col Int "ai") .> F.lit (50 :: Double))
            (Leaf "c")
            ( Branch
                (F.toDouble (F.col Int "ai") .< F.lit (60 :: Double))
                (Leaf "a")
                (Leaf "b")
            )
        )
        ( Branch
            (F.toDouble (F.col Int "ai") .> F.lit (50 :: Double))
            (Leaf "c")
            (Leaf "a")
        )
    ]

sameBranchCollapse :: [Test]
sameBranchCollapse =
    [ prunesTo
        "equal leaves collapse the branch"
        (Branch (F.col Double "age" .> F.lit (50 :: Double)) (Leaf "a") (Leaf "a"))
        (Leaf "a")
    , prunesTo
        "collapse cascades upward"
        ( Branch
            (F.col Double "age" .> F.lit (50 :: Double))
            (Branch (F.col Double "hours" .> F.lit (40 :: Double)) (Leaf "a") (Leaf "a"))
            (Leaf "a")
        )
        (Leaf "a")
    ]

preservedTrees :: [Test]
preservedTrees =
    [ preserved
        "child not tight enough is kept"
        ( Branch
            (F.col Double "age" .> F.lit (50 :: Double))
            (Branch (F.col Double "age" .> F.lit (60 :: Double)) (Leaf "a") (Leaf "b"))
            (Leaf "c")
        )
    , preserved
        "double false edge is kept (NaN)"
        ( Branch
            (F.col Double "weight" .> F.lit (50 :: Double))
            (Leaf "c")
            (Branch (F.col Double "weight" .< F.lit (60 :: Double)) (Leaf "a") (Leaf "b"))
        )
    , preserved
        "cross-column descendant is kept"
        ( Branch
            (F.col Double "age" .> F.lit (50 :: Double))
            (Branch (F.col Double "income" .> F.lit (30000 :: Double)) (Leaf "a") (Leaf "b"))
            (Leaf "c")
        )
    ]

tests :: [Test]
tests = concat [pathEntailment, falseEdgeGate, sameBranchCollapse, preservedTrees]
