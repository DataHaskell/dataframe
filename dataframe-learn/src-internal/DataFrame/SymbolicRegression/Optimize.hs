{-# LANGUAGE BangPatterns #-}

{- | Constant optimization for symbolic-regression candidates: finite-difference
gradient descent with backtracking line search on the embedded constants. Pure
and dependency-free; effective at the one-to-few constants a tree carries.
-}
module DataFrame.SymbolicRegression.Optimize (
    optimizeConstants,
    meanSquaredError,
) where

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.SymbolicRegression.Expr (
    SRExpr,
    constants,
    evalSR,
    setConstants,
 )

-- | Mean squared error of an expression's predictions against the target.
meanSquaredError ::
    V.Vector (VU.Vector Double) -> Int -> VU.Vector Double -> SRExpr -> Double
meanSquaredError feats n target e =
    let pred = evalSR feats n e
        diff = VU.zipWith (-) pred target
     in VU.sum (VU.map (\x -> x * x) diff) / fromIntegral (max 1 n)

-- | Refine an expression's constants to reduce MSE (no-op when constant-free).
optimizeConstants ::
    V.Vector (VU.Vector Double) ->
    Int ->
    VU.Vector Double ->
    Int ->
    SRExpr ->
    SRExpr
optimizeConstants feats n target iters expr
    | null theta0 = expr
    | otherwise = setConstants (descend iters theta0) expr
  where
    theta0 = constants expr
    eps = 1e-6
    mseAt theta = meanSquaredError feats n target (setConstants theta expr)
    descend 0 theta = theta
    descend k theta =
        let f0 = mseAt theta
            g = numGrad theta
            gn = sqrt (sum (map (\x -> x * x) g))
         in if gn < 1e-10
                then theta
                else
                    let theta' = lineSearch theta g f0
                     in if theta' == theta then theta else descend (k - 1) theta'
    numGrad theta =
        [ (mseAt (bump i eps theta) - mseAt (bump i (negate eps) theta)) / (2 * eps)
        | i <- [0 .. length theta - 1]
        ]
    bump i delta theta =
        [if j == i then t + delta else t | (j, t) <- zip [0 ..] theta]
    lineSearch theta g f0 = go (1.0 :: Double)
      where
        go !step
            | step < 1e-8 = theta
            | otherwise =
                let theta' = zipWith (\t gi -> t - step * gi) theta g
                 in if mseAt theta' < f0 then theta' else go (step / 2)
