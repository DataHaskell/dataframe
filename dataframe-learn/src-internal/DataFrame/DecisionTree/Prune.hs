{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Post-convergence simplification of a fitted tree and its expression form:
drop branches forced by path-condition entailment, collapse identical
siblings, and fold redundant nested conditionals.
-}
module DataFrame.DecisionTree.Prune (
    pruneDead,
    treeEq,
    pruneExpr,
) where

import DataFrame.DecisionTree.Types (Tree (..))
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (..), eqExpr)
import DataFrame.Internal.Expression.Simplify (
    PredFact,
    entails,
    factFalse,
    factTrue,
 )

{- | Drop branches whose test is forced by the path conditions reaching them,
and collapse @Branch c t t@ to @t@. Sound for the decidable threshold subset;
other tests are left untouched.
-}
pruneDead :: forall a. (Columnable a) => Tree a -> Tree a
pruneDead = go []
  where
    go :: [PredFact] -> Tree a -> Tree a
    go _ (Leaf v) = Leaf v
    go facts (Branch cond left right) = case entails facts cond of
        Just True -> go facts left
        Just False -> go facts right
        Nothing ->
            reconcile
                cond
                (go (addFact (factTrue cond) facts) left)
                (go (addFact (factFalse cond) facts) right)

reconcile :: (Columnable a) => Expr Bool -> Tree a -> Tree a -> Tree a
reconcile cond left right
    | treeEq left right = left
    | otherwise = Branch cond left right

addFact :: Maybe PredFact -> [PredFact] -> [PredFact]
addFact (Just f) fs = f : fs
addFact Nothing fs = fs

treeEq :: (Columnable a) => Tree a -> Tree a -> Bool
treeEq (Leaf x) (Leaf y) = x == y
treeEq (Branch c1 l1 r1) (Branch c2 l2 r2) = eqExpr c1 c2 && treeEq l1 l2 && treeEq r1 r2
treeEq _ _ = False

{- | Recursively fold @If@ expressions whose branches coincide or nest the same
condition; leave other expressions structurally unchanged.
-}
pruneExpr :: forall a. (Columnable a) => Expr a -> Expr a
pruneExpr (If cond t0 f0) = collapseIf cond (pruneExpr t0) (pruneExpr f0)
pruneExpr (Unary op e) = Unary op (pruneExpr e)
pruneExpr (Binary op l r) = Binary op (pruneExpr l) (pruneExpr r)
pruneExpr e = e

collapseIf :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
collapseIf cond t f
    | eqExpr t f = t
    | If ci ti _ <- t, eqExpr cond ci = If cond ti f
    | If ci _ fi <- f, eqExpr cond ci = If cond t fi
    | otherwise = If cond t f
