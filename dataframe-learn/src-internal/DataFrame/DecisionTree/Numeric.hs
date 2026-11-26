{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Numeric split candidates: expressions, arithmetic expansion, thresholds.
module DataFrame.DecisionTree.Numeric (
    NumExpr (..),
    numExprCols,
    numExprEq,
    combineNumExprs,
    numericConditions,
    generateNumericConds,
    missingnessConditions,
    percentilesOf,
    numericCondVecs,
    numericExprsWithTerms,
    numericCols,
) where

import DataFrame.DecisionTree.ColumnKind (
    AsDouble (..),
    ColumnKind (..),
    columnKind,
    withColumnType,
 )
import DataFrame.DecisionTree.CondVec (CondVec (..), materializeCondVec)
import DataFrame.DecisionTree.Types (SynthConfig (..), TreeConfig (..))
import DataFrame.Expression.Operators
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame, columnNames, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), eqExpr, getColumns, normalize)
import DataFrame.Internal.Interpreter (interpret)

import Data.List (sort)
import Data.Maybe (catMaybes, mapMaybe)
import Data.Proxy (Proxy (..))
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

-- | A numeric feature expression, non-nullable or nullable.
data NumExpr
    = NDouble !(Expr Double)
    | NMaybeDouble !(Expr (Maybe Double))

numExprCols :: NumExpr -> [T.Text]
numExprCols (NDouble e) = getColumns e
numExprCols (NMaybeDouble e) = getColumns e

numExprEq :: NumExpr -> NumExpr -> Bool
numExprEq (NDouble e1) (NDouble e2) = eqExpr e1 e2
numExprEq (NMaybeDouble e1) (NMaybeDouble e2) = eqExpr e1 e2
numExprEq _ _ = False

-- | Safe division: @0@ (or @Nothing@) where the divisor is zero.
safeDivD :: Expr Double -> Expr Double -> Expr Double
safeDivD a b = F.ifThenElse (b ./= F.lit (0 :: Double)) (a ./ b) (F.lit (0 :: Double))

safeDivMaybe :: Expr Bool -> Expr (Maybe Double) -> Expr (Maybe Double)
safeDivMaybe nonZero q = F.ifThenElse nonZero q (F.lit (Nothing :: Maybe Double))

-- | Arithmetic combinations (@+@, @-@, @*@, safe @/@) of two numeric exprs.
combineNumExprs :: NumExpr -> NumExpr -> [NumExpr]
combineNumExprs (NDouble e1) (NDouble e2) =
    map NDouble [e1 .+ e2, e1 .- e2, e1 .* e2, safeDivD e1 e2]
combineNumExprs (NDouble e1) (NMaybeDouble e2) =
    map
        NMaybeDouble
        [ e1 .+ e2
        , e1 .- e2
        , e1 .* e2
        , safeDivMaybe (F.fromMaybe False (e2 ./= F.lit (0 :: Double))) (e1 ./ e2)
        ]
combineNumExprs (NMaybeDouble e1) (NDouble e2) =
    map
        NMaybeDouble
        [ e1 .+ e2
        , e1 .- e2
        , e1 .* e2
        , safeDivMaybe (e2 ./= F.lit (0 :: Double)) (e1 ./ e2)
        ]
combineNumExprs (NMaybeDouble e1) (NMaybeDouble e2) =
    map
        NMaybeDouble
        [ e1 .+ e2
        , e1 .- e2
        , e1 .* e2
        , safeDivMaybe (F.fromMaybe False (e2 ./= F.lit (0 :: Double))) (e1 ./ e2)
        ]

numericConditions :: TreeConfig -> DataFrame -> [Expr Bool]
numericConditions = generateNumericConds

generateNumericConds :: TreeConfig -> DataFrame -> [Expr Bool]
generateNumericConds cfg df = thresholdConds ++ missingnessConditions df
  where
    thresholdConds = do
        expr <- numericExprsWithTerms (synthConfig cfg) df
        threshold <- numericThresholds cfg df expr
        condsFromExpr expr threshold

missingnessConditions :: DataFrame -> [Expr Bool]
missingnessConditions df = concatMap missingCond (columnNames df)
  where
    missingCond name = withColumnType (unsafeGetColumn name df) $ \bm (_ :: Proxy b) ->
        maybe [] (const [F.isNothing (Col @(Maybe b) name)]) bm

-- | Thresholds for nullable expressions come from the observed values only.
numericThresholds :: TreeConfig -> DataFrame -> NumExpr -> [Double]
numericThresholds cfg df (NDouble e) = thresholdsForExpr cfg df e
numericThresholds cfg df (NMaybeDouble e) =
    maybe
        []
        (percentilesOf (percentiles cfg) . catMaybes . V.toList)
        (interpretMaybeDoubleCol df e)

thresholdsForExpr :: TreeConfig -> DataFrame -> Expr Double -> [Double]
thresholdsForExpr cfg df e =
    maybe [] (percentilesOf (percentiles cfg) . V.toList) (interpretDoubleCol df e)

condsFromExpr :: NumExpr -> Double -> [Expr Bool]
condsFromExpr (NDouble e) t = [e .<= F.lit t, e .>= F.lit t, e .< F.lit t, e .> F.lit t]
condsFromExpr (NMaybeDouble e) t =
    map
        (F.fromMaybe False)
        [e .<= F.lit t, e .>= F.lit t, e .< F.lit t, e .> F.lit t]

-- | Percentile thresholds for a value list: sort once, index each percentile.
percentilesOf :: [Int] -> [Double] -> [Double]
percentilesOf ps valsList
    | n == 0 = []
    | otherwise = map (\p -> sortedV V.! min (n - 1) (max 0 (p * n `div` 100))) ps
  where
    !sortedV = V.fromList (sort valsList)
    !n = V.length sortedV

interpretDoubleCol :: DataFrame -> Expr Double -> Maybe (V.Vector Double)
interpretDoubleCol df e = case interpret @Double df e of
    Right (TColumn column) -> either (const Nothing) Just (toVector @Double column)
    _ -> Nothing

interpretMaybeDoubleCol ::
    DataFrame -> Expr (Maybe Double) -> Maybe (V.Vector (Maybe Double))
interpretMaybeDoubleCol df e = case interpret @(Maybe Double) df e of
    Right (TColumn column) -> either (const Nothing) Just (toVector @(Maybe Double) column)
    _ -> Nothing

-- | Materialize the numeric pool with one interpret per distinct expression.
numericCondVecs :: TreeConfig -> DataFrame -> DataFrame -> [CondVec]
numericCondVecs cfg dfGen df =
    concatMap forExpr (numericExprsWithTerms (synthConfig cfg) dfGen)
        ++ mapMaybe (materializeCondVec df) (missingnessConditions dfGen)
  where
    forExpr (NDouble e) = maybe [] (condsForDouble cfg e) (interpretDoubleCol df e)
    forExpr (NMaybeDouble e) = maybe [] (condsForMaybe cfg e) (interpretMaybeDoubleCol df e)

condsForDouble :: TreeConfig -> Expr Double -> V.Vector Double -> [CondVec]
condsForDouble cfg e vals = concatMap (doubleCondsAt e vals (V.length vals)) ts
  where
    ts = percentilesOf (percentiles cfg) (V.toList vals)

doubleCondsAt :: Expr Double -> V.Vector Double -> Int -> Double -> [CondVec]
doubleCondsAt e vals n t =
    [ CondVec (e .<= F.lit t) (gen (<= t))
    , CondVec (e .>= F.lit t) (gen (>= t))
    , CondVec (e .< F.lit t) (gen (< t))
    , CondVec (e .> F.lit t) (gen (> t))
    ]
  where
    gen p = VU.generate n (\i -> p (vals V.! i))

condsForMaybe ::
    TreeConfig -> Expr (Maybe Double) -> V.Vector (Maybe Double) -> [CondVec]
condsForMaybe cfg e mvals = concatMap (maybeCondsAt e mvals (V.length mvals)) ts
  where
    ts = percentilesOf (percentiles cfg) (catMaybes (V.toList mvals))

maybeCondsAt ::
    Expr (Maybe Double) -> V.Vector (Maybe Double) -> Int -> Double -> [CondVec]
maybeCondsAt e mvals n t =
    [ CondVec (F.fromMaybe False (e .<= F.lit t)) (gen (<= t))
    , CondVec (F.fromMaybe False (e .>= F.lit t)) (gen (>= t))
    , CondVec (F.fromMaybe False (e .< F.lit t)) (gen (< t))
    , CondVec (F.fromMaybe False (e .> F.lit t)) (gen (> t))
    ]
  where
    gen p = VU.generate n (\i -> maybe False p (mvals V.! i))

-- | Arithmetic candidate expansion, generated already-deduped.
numericExprsWithTerms :: SynthConfig -> DataFrame -> [NumExpr]
numericExprsWithTerms cfg df
    | not (enableArithOps cfg) = base
    | otherwise =
        base ++ expandRounds cfg base (max 0 (maxExprDepth cfg - 1)) base seen0
  where
    base = numericCols df
    seen0 = Set.fromList (map keyNum base)

keyNum :: NumExpr -> String
keyNum (NDouble e) = show (normalize e)
keyNum (NMaybeDouble e) = show (normalize e)

isDisallowed :: SynthConfig -> NumExpr -> NumExpr -> Bool
isDisallowed cfg e1 e2 =
    any (\(l, r) -> l `elem` cols && r `elem` cols) (disallowedCombinations cfg)
  where
    cols = numExprCols e1 <> numExprCols e2

roundProducts :: SynthConfig -> [NumExpr] -> [NumExpr] -> [NumExpr]
roundProducts cfg frontier base =
    [ c
    | e1 <- frontier
    , e2 <- base
    , not (numExprEq e1 e2)
    , not (isDisallowed cfg e1 e2)
    , c <- combineNumExprs e1 e2
    ]

expandRounds ::
    SynthConfig -> [NumExpr] -> Int -> [NumExpr] -> Set.Set String -> [NumExpr]
expandRounds _ _ 0 _ _ = []
expandRounds cfg base d frontier seen
    | null admitted = []
    | otherwise = admitted ++ expandRounds cfg base (d - 1) admitted seen'
  where
    (admitted, seen') = admitNovel seen (roundProducts cfg frontier base)

admitNovel :: Set.Set String -> [NumExpr] -> ([NumExpr], Set.Set String)
admitNovel seen0 = go seen0 []
  where
    go seen acc [] = (reverse acc, seen)
    go seen acc (c : cs)
        | keyNum c `Set.member` seen = go seen acc cs
        | otherwise = go (Set.insert (keyNum c) seen) (c : acc) cs

numericCols :: DataFrame -> [NumExpr]
numericCols df = concatMap (numExprsOfColumn df) (columnNames df)

-- | A column's numeric expression when 'columnKind' is 'Ordered'.
numExprsOfColumn :: DataFrame -> T.Text -> [NumExpr]
numExprsOfColumn df colName =
    withColumnType (unsafeGetColumn colName df) $ \bm (_ :: Proxy b) ->
        numExprOf @b colName bm

-- | Numbers need no ordering registry; categories return 'Unsplittable'.
numExprOf :: forall b. (Columnable b) => T.Text -> Maybe Bitmap -> [NumExpr]
numExprOf c bm = case columnKind @b mempty of
    Ordered num -> [maybe (NDouble (columnAsDouble num c)) (const (NMaybeDouble (nullableAsDouble num c))) bm]
    _ -> []
