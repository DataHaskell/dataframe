{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Synthesis where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
 )
import DataFrame.Internal.Expression (
    Expr (..),
    eSize,
 )
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Statistics
import DataFrame.Operations.Core (columnAsDoubleVector)
import qualified DataFrame.Operations.Statistics as Stats
import DataFrame.Operations.Subset (exclude)

import Control.Exception (throw)
import Data.Function
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe (listToMaybe)
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.Operations.Core as D
import DataFrame.Operators
import Debug.Trace (trace)
import Type.Reflection (typeRep)

generateConditions ::
    TypedColumn Double -> [Expr Bool] -> [Expr Double] -> DataFrame -> [Expr Bool]
generateConditions labels conds ps df =
    let
        newConds =
            [ p .<= q
            | p <- filter (not . isLiteral) ps
            , q <- ps
            , p /= q
            ]
                ++ [ F.not p
                   | p <- conds
                   ]
        expandedConds =
            conds
                ++ newConds
                ++ [p .&& q | p <- newConds, q <- conds, p /= q]
                ++ [p .|| q | p <- newConds, q <- conds, p /= q]
     in
        pickTopNBool df labels (deduplicate df expandedConds)

generatePrograms ::
    Bool ->
    [Expr Bool] ->
    [Expr Double] ->
    [Expr Double] ->
    [Expr Double] ->
    [Expr Double]
generatePrograms _ _ vars' constants [] = vars' ++ constants
generatePrograms includeConds conds vars constants ps =
    let
        existingPrograms = ps ++ vars ++ constants
     in
        existingPrograms
            ++ [ transform p
               | p <- ps ++ vars
               , Prelude.not (isConditional p)
               , transform <-
                    [ sqrt
                    , abs
                    , log . (+ Lit 1)
                    , exp
                    , sin
                    , cos
                    , F.relu
                    , signum
                    ]
               ]
            ++ [ F.pow p i
               | p <- existingPrograms
               , Prelude.not (isConditional p)
               , i <- [2 .. 6]
               ]
            ++ [ p + q
               | (i, p) <- zip [(0 :: Int) ..] existingPrograms
               , (j, q) <- zip [(0 :: Int) ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , Prelude.not (isConditional p || isConditional q)
               , i >= j
               ]
            ++ [ p - q
               | (i, p) <- zip [(0 :: Int) ..] existingPrograms
               , (j, q) <- zip [(0 :: Int) ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , Prelude.not (isConditional p || isConditional q)
               , i /= j
               ]
            ++ ( if includeConds
                    then
                        [ F.min p q
                        | (i, p) <- zip [(0 :: Int) ..] existingPrograms
                        , (j, q) <- zip [(0 :: Int) ..] existingPrograms
                        , Prelude.not (isLiteral p && isLiteral q)
                        , Prelude.not (isConditional p || isConditional q)
                        , p /= q
                        , i > j
                        ]
                            ++ [ F.max p q
                               | (i, p) <- zip [(0 :: Int) ..] existingPrograms
                               , (j, q) <- zip [(0 :: Int) ..] existingPrograms
                               , Prelude.not (isLiteral p && isLiteral q)
                               , Prelude.not (isConditional p || isConditional q)
                               , p /= q
                               , i > j
                               ]
                            ++ [ F.ifThenElse cond r s
                               | cond <- conds
                               , r <- existingPrograms
                               , s <- existingPrograms
                               , Prelude.not (isConditional r || isConditional s)
                               , r /= s
                               ]
                    else []
               )
            ++ [ p * q
               | (i, p) <- zip [(0 :: Int) ..] existingPrograms
               , (j, q) <- zip [(0 :: Int) ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , Prelude.not (isConditional p || isConditional q)
               , i >= j
               ]
            ++ [ p / q
               | p <- existingPrograms
               , q <- existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , Prelude.not (isConditional p || isConditional q)
               , p /= q
               ]

isLiteral :: Expr a -> Bool
isLiteral (Lit _) = True
isLiteral _ = False

isConditional :: Expr a -> Bool
isConditional (If{}) = True
isConditional _ = False

deduplicate ::
    forall a.
    (Columnable a) =>
    DataFrame ->
    [Expr a] ->
    [(Expr a, TypedColumn a)]
deduplicate df = go [] . L.nub . L.sortBy (\e1 e2 -> compare (eSize e1) (eSize e2))
  where
    go _ [] = []
    go seen (x : xs)
        | hasInvalid = go seen xs
        | res `elem` seen = go seen xs
        | otherwise = (x, res) : go (res : seen) xs
      where
        res = case interpret @a df x of
            Left e -> throw e
            Right v -> v
        hasInvalid = case res of
            (TColumn (UnboxedColumn _ (column :: VU.Vector b))) -> case testEquality (typeRep @Double) (typeRep @b) of
                Just Refl -> VU.any (\n -> isNaN n || isInfinite n) column
                Nothing -> False
            _ -> False

-- | Checks if two programs generate the same outputs given all the same inputs.
equivalent :: DataFrame -> Expr Double -> Expr Double -> Bool
equivalent df p1 p2 = case (==) <$> interpret df p1 <*> interpret df p2 of
    Left e -> throw e
    Right v -> v

synthesizeFeatureExpr ::
    -- | Target expression
    T.Text ->
    BeamConfig ->
    DataFrame ->
    Either String (Expr Double)
synthesizeFeatureExpr target cfg df =
    let
        df' = exclude [target] df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            cfg
            t
            (percentiles df')
            []
            [] of
            Nothing -> Left "No programs found"
            Just p -> Right p

f1FromBinary :: VU.Vector Double -> VU.Vector Double -> Maybe Double
f1FromBinary trues preds =
    let (!tp, !fp, !fn) =
            VU.foldl' step (0 :: Int, 0 :: Int, 0 :: Int) $
                VU.zip (VU.map (> 0) preds) (VU.map (> 0) trues)
     in f1FromCounts tp fp fn
  where
    step (!tp, !fp, !fn) (!p, !t) =
        case (p, t) of
            (True, True) -> (tp + 1, fp, fn)
            (True, False) -> (tp, fp + 1, fn)
            (False, True) -> (tp, fp, fn + 1)
            (False, False) -> (tp, fp, fn)

f1FromCounts :: Int -> Int -> Int -> Maybe Double
f1FromCounts tp fp fn =
    let tp' = fromIntegral tp
        fp' = fromIntegral fp
        fn' = fromIntegral fn
        precision = if tp' + fp' == 0 then 0 else tp' / (tp' + fp')
        recall = if tp' + fn' == 0 then 0 else tp' / (tp' + fn')
     in if precision + recall == 0
            then Nothing
            else Just (2 * precision * recall / (precision + recall))

fitClassifier ::
    -- | Target expression
    T.Text ->
    -- | Depth of search (Roughly, how many terms in the final expression)
    Int ->
    -- | Beam size - the number of candidate expressions to consider at a time.
    Int ->
    DataFrame ->
    Either String (Expr Int)
fitClassifier target d b df =
    let
        df' = exclude [target] df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            (BeamConfig d b F1 True)
            t
            (percentiles df' ++ [Lit 1, Lit 0, Lit (-1)])
            []
            [] of
            Nothing -> Left "No programs found"
            Just p -> Right (F.ifThenElse (p .> (0 :: Expr Double)) 1 0)

percentiles :: DataFrame -> [Expr Double]
percentiles df =
    let
        doubleColumns =
            map
                (either throw id . ((`columnAsDoubleVector` df) . Col @Double))
                (D.columnNames df)
     in
        concatMap
            (\c -> map (Lit . roundTo2SigDigits . (`percentile'` c)) [1, 25, 75, 99])
            doubleColumns
            ++ map (Lit . roundTo2SigDigits . variance') doubleColumns
            ++ map (Lit . roundTo2SigDigits . sqrt . variance') doubleColumns

roundToSigDigits :: Int -> Double -> Double
roundToSigDigits n x
    | x == 0 = 0
    | otherwise =
        let magnitude = floor (logBase 10 (abs x))
            scale = 10 ** fromIntegral (n - 1 - magnitude)
         in fromIntegral (round (x * scale) :: Int) / scale

roundTo2SigDigits :: Double -> Double
roundTo2SigDigits = roundToSigDigits 2

fitRegression ::
    -- | Target expression
    T.Text ->
    -- | Depth of search (Roughly, how many terms in the final expression)
    Int ->
    -- | Beam size - the number of candidate expressions to consider at a time.
    Int ->
    DataFrame ->
    Either String (Expr Double)
fitRegression target d b df =
    let
        df' = exclude [target] df
        targetMean = Stats.mean (Col @Double target) df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
        cfg = BeamConfig d b MeanSquaredError True
        constants =
            percentiles df'
                ++ [Lit targetMean]
                ++ [ F.pow p i
                   | i <- [1 .. 6]
                   , p <- [Lit 10, Lit 1, Lit 0.1]
                   ]
     in
        case beamSearch df' cfg t constants [] [] of
            Nothing -> Left "No programs found"
            Just p -> Right p

data LossFunction
    = PearsonCorrelation
    | MutualInformation
    | MeanSquaredError
    | F1

getLossFunction ::
    LossFunction -> (VU.Vector Double -> VU.Vector Double -> Maybe Double)
getLossFunction f = case f of
    MutualInformation ->
        ( \l r ->
            mutualInformationBinned
                (Prelude.max 10 (ceiling (sqrt (fromIntegral (VU.length l) :: Double))))
                l
                r
        )
    PearsonCorrelation -> (\l r -> (^ (2 :: Int)) <$> correlation' l r)
    MeanSquaredError -> (\l r -> fmap negate (meanSquaredError l r))
    F1 -> f1FromBinary

data BeamConfig = BeamConfig
    { searchDepth :: Int
    , beamLength :: Int
    , lossFunction :: LossFunction
    , includeConditionals :: Bool
    }

defaultBeamConfig :: BeamConfig
defaultBeamConfig = BeamConfig 2 100 PearsonCorrelation False

beamSearch ::
    DataFrame ->
    -- | Parameters of the beam search.
    BeamConfig ->
    -- | Examples
    TypedColumn Double ->
    -- | Constants
    [Expr Double] ->
    -- | Conditions
    [Expr Bool] ->
    -- | Programs
    [Expr Double] ->
    Maybe (Expr Double)
beamSearch df cfg outputs constants conds programs
    | searchDepth cfg == 0 = case ps of
        [] -> Nothing
        (x : _) -> Just x
    | otherwise =
        beamSearch
            df
            (cfg{searchDepth = searchDepth cfg - 1})
            outputs
            constants
            conditions
            (generatePrograms (includeConditionals cfg) conditions vars constants ps)
  where
    vars = map Col names
    conditions = generateConditions outputs conds (vars ++ constants) df
    ps = pickTopN df outputs cfg $ deduplicate df programs
    names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df

pickTopN ::
    DataFrame ->
    TypedColumn Double ->
    BeamConfig ->
    [(Expr Double, TypedColumn a)] ->
    [Expr Double]
pickTopN _ _ _ [] = []
pickTopN df (TColumn column) cfg ps =
    let
        l = case toVector @Double @VU.Vector column of
            Left e -> throw e
            Right v -> v
        ordered =
            Prelude.take
                (beamLength cfg)
                ( map fst $
                    L.sortBy
                        ( \(_, c2) (_, c1) ->
                            if maybe False isInfinite c1
                                || maybe False isInfinite c2
                                || maybe False isNaN c1
                                || maybe False isNaN c2
                                then LT
                                else compare c1 c2
                        )
                        ( map
                            (\(e, res) -> (e, getLossFunction (lossFunction cfg) l (asDoubleVector res)))
                            ps
                        )
                )
        asDoubleVector c =
            let
                (TColumn col') = c
             in
                case toVector @Double @VU.Vector col' of
                    Left e -> throw e
                    Right v -> VU.convert v
        interpretDoubleVector e' =
            let
                (TColumn col') = case interpret df e' of
                    Left err -> throw err
                    Right v -> v
             in
                case toVector @Double @VU.Vector col' of
                    Left err -> throw err
                    Right v -> VU.convert v
     in
        trace
            ( "Best loss: "
                ++ show
                    ( getLossFunction (lossFunction cfg) l . interpretDoubleVector
                        <$> listToMaybe ordered
                    )
                ++ " "
                ++ (if null ordered then "empty" else show (listToMaybe ordered))
            )
            ordered

pickTopNBool ::
    DataFrame ->
    TypedColumn Double ->
    [(Expr Bool, TypedColumn Bool)] ->
    [Expr Bool]
pickTopNBool _ _ [] = []
pickTopNBool _df (TColumn column) ps =
    let
        l = case toVector @Double @VU.Vector column of
            Left e -> throw e
            Right v -> v
        ordered =
            Prelude.take
                10
                ( map fst $
                    L.sortBy
                        ( \(_, c2) (_, c1) ->
                            if maybe False isInfinite c1
                                || maybe False isInfinite c2
                                || maybe False isNaN c1
                                || maybe False isNaN c2
                                then LT
                                else compare c1 c2
                        )
                        ( map
                            (\(e, res) -> (e, getLossFunction MutualInformation l (asDoubleVector res)))
                            ps
                        )
                )
        asDoubleVector c =
            let
                (TColumn col') = c
             in
                case toVector @Bool @VU.Vector col' of
                    Left e -> throw e
                    Right v -> VU.map (fromIntegral @Int @Double . fromEnum) v
     in
        ordered

satisfiesExamples :: DataFrame -> TypedColumn Double -> Expr Double -> Bool
satisfiesExamples df column expr =
    let
        result = case interpret df expr of
            Left e -> throw e
            Right v -> v
     in
        result == column
