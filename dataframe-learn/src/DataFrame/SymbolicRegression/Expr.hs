{-# LANGUAGE FlexibleContexts #-}

{- | The symbolic-regression expression tree: a small first-order ADT (no hegg,
no 'Fix'). Vectorized evaluation over a feature matrix and a total translation
to a dataframe 'Expr Double' (the SR result IS a dataframe expression). Division,
log, and sqrt are protected so evaluation never produces @NaN@.
-}
module DataFrame.SymbolicRegression.Expr (
    SRExpr (..),
    BinOp (..),
    UnOp (..),
    evalSR,
    toDataFrameExpr,
    srSize,
    constants,
    setConstants,
    allBinOps,
    allUnOps,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Operators ((.*.), (.+.), (.-.), (./.))

data BinOp = SAdd | SSub | SMul | SDiv
    deriving (Eq, Ord, Show, Enum, Bounded)

data UnOp = SNeg | SSin | SCos | SExp | SLog | SSqrt
    deriving (Eq, Ord, Show, Enum, Bounded)

-- | A symbolic-regression expression over feature variables and constants.
data SRExpr
    = SVar !Int
    | SConst !Double
    | SUn !UnOp SRExpr
    | SBin !BinOp SRExpr SRExpr
    deriving (Eq, Ord, Show)

allBinOps :: [BinOp]
allBinOps = [minBound .. maxBound]

allUnOps :: [UnOp]
allUnOps = [minBound .. maxBound]

{- | Evaluate over a feature matrix given column-major (@feats ! j@ is feature
@j@ across all rows). Protected operators keep results finite.
-}
evalSR :: V.Vector (VU.Vector Double) -> Int -> SRExpr -> VU.Vector Double
evalSR feats n = go
  where
    go (SVar j)
        | j < V.length feats = feats V.! j
        | otherwise = VU.replicate n 0
    go (SConst c) = VU.replicate n c
    go (SUn op e) = VU.map (unFn op) (go e)
    go (SBin op a b) = VU.zipWith (binFn op) (go a) (go b)

binFn :: BinOp -> Double -> Double -> Double
binFn SAdd a b = a + b
binFn SSub a b = a - b
binFn SMul a b = a * b
binFn SDiv a b = if abs b < 1e-9 then 1 else a / b

unFn :: UnOp -> Double -> Double
unFn SNeg = negate
unFn SSin = sin
unFn SCos = cos
unFn SExp = exp . min 50
unFn SLog = \x -> log (abs x + 1e-9)
unFn SSqrt = sqrt . abs

-- | Translate to a dataframe expression over the named feature columns.
toDataFrameExpr :: V.Vector T.Text -> SRExpr -> Expr Double
toDataFrameExpr names = go
  where
    go (SVar j)
        | j < V.length names = Col (names V.! j)
        | otherwise = F.lit 0
    go (SConst c) = F.lit c
    go (SUn op e) = unExpr op (go e)
    go (SBin op a b) = binExpr op (go a) (go b)
    unExpr SNeg = negate
    unExpr SSin = sin
    unExpr SCos = cos
    unExpr SExp = exp
    unExpr SLog = log
    unExpr SSqrt = sqrt
    binExpr SAdd = (.+.)
    binExpr SSub = (.-.)
    binExpr SMul = (.*.)
    binExpr SDiv = (./.)

srSize :: SRExpr -> Int
srSize (SVar _) = 1
srSize (SConst _) = 1
srSize (SUn _ e) = 1 + srSize e
srSize (SBin _ a b) = 1 + srSize a + srSize b

-- | The constant values in left-to-right traversal order.
constants :: SRExpr -> [Double]
constants (SConst c) = [c]
constants (SVar _) = []
constants (SUn _ e) = constants e
constants (SBin _ a b) = constants a ++ constants b

-- | Replace the constants in traversal order; extra values are ignored.
setConstants :: [Double] -> SRExpr -> SRExpr
setConstants vals e = fst (go vals e)
  where
    go vs (SConst _) = case vs of
        (v : rest) -> (SConst v, rest)
        [] -> (SConst 0, [])
    go vs (SVar j) = (SVar j, vs)
    go vs (SUn op a) = let (a', vs') = go vs a in (SUn op a', vs')
    go vs (SBin op a b) =
        let (a', vs') = go vs a
            (b', vs'') = go vs' b
         in (SBin op a' b', vs'')
