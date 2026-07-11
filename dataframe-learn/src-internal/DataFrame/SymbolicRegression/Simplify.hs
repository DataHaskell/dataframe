{- | A fuel-bounded, deterministic algebraic simplifier — the dependency-light
stand-in for equality saturation. Used as a canonical dedup key for the Pareto
archive and to tidy reported expressions; total and evaluation-preserving.
-}
module DataFrame.SymbolicRegression.Simplify (
    simplify,
) where

import DataFrame.SymbolicRegression.Expr (BinOp (..), SRExpr (..), UnOp (..))

-- | Simplify to a fixed point (bounded by a fuel counter).
simplify :: SRExpr -> SRExpr
simplify = go (10 :: Int)
  where
    go 0 e = e
    go fuel e =
        let e' = step e
         in if e' == e then e else go (fuel - 1) e'

step :: SRExpr -> SRExpr
step (SUn op e) = simplifyUn op (step e)
step (SBin op a b) = simplifyBin op (step a) (step b)
step e = e

simplifyUn :: UnOp -> SRExpr -> SRExpr
simplifyUn SNeg (SUn SNeg e) = e
simplifyUn op (SConst c) = SConst (foldUn op c)
simplifyUn op e = SUn op e

simplifyBin :: BinOp -> SRExpr -> SRExpr -> SRExpr
simplifyBin op (SConst a) (SConst b) = SConst (foldBin op a b)
simplifyBin SAdd a (SConst 0) = a
simplifyBin SAdd (SConst 0) b = b
simplifyBin SSub a (SConst 0) = a
simplifyBin SSub a b | a == b = SConst 0
simplifyBin SMul _ (SConst 0) = SConst 0
simplifyBin SMul (SConst 0) _ = SConst 0
simplifyBin SMul a (SConst 1) = a
simplifyBin SMul (SConst 1) b = b
simplifyBin SDiv a (SConst 1) = a
simplifyBin SDiv a b | a == b = SConst 1
simplifyBin op a b
    | commutative op && a > b = SBin op b a
    | otherwise = SBin op a b

commutative :: BinOp -> Bool
commutative SAdd = True
commutative SMul = True
commutative _ = False

foldBin :: BinOp -> Double -> Double -> Double
foldBin SAdd a b = a + b
foldBin SSub a b = a - b
foldBin SMul a b = a * b
foldBin SDiv a b = if abs b < 1e-9 then 1 else a / b

foldUn :: UnOp -> Double -> Double
foldUn SNeg = negate
foldUn SSin = sin
foldUn SCos = cos
foldUn SExp = exp . min 50
foldUn SLog = \x -> log (abs x + 1e-9)
foldUn SSqrt = sqrt . abs
