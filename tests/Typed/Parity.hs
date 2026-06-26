{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Typed.Parity (tests) where

import Data.Either (fromRight)
import qualified Data.Text as T
import System.Random (mkStdGen)

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.Statistics as TS

import Test.HUnit

type S =
    '[ DT.Column "x" Int
     , DT.Column "y" Double
     , DT.Column "g" T.Text
     ]

baseDF :: D.DataFrame
baseDF =
    D.fromNamedColumns
        [ ("x", DI.fromList [1, 2, 3, 4, 5, 6 :: Int])
        , ("y", DI.fromList [10.0, 25.0, 30.0, 5.0, 50.0, 12.0 :: Double])
        , ("g", DI.fromList ["a", "b", "a", "b", "a", "b" :: T.Text])
        ]

tdf :: DT.TypedDataFrame S
tdf = either (error . show) id (DT.freezeWithError baseDF)

-- Statistics reducers should match the untyped ones exactly (same code path).
statParity :: Test
statParity =
    TestList
        [ eq "mean" (TS.mean (DT.col @"y") tdf) (D.mean (F.col @Double "y") baseDF)
        , eq "median" (TS.median (DT.col @"y") tdf) (D.median (F.col @Double "y") baseDF)
        , eq
            "variance"
            (TS.variance (DT.col @"y") tdf)
            (D.variance (F.col @Double "y") baseDF)
        , eq
            "stddev"
            (TS.standardDeviation (DT.col @"y") tdf)
            (D.standardDeviation (F.col @Double "y") baseDF)
        , eq "sum" (TS.sum (DT.col @"x") tdf) (D.sum (F.col @Int "x") baseDF)
        , eq
            "percentile"
            (TS.percentile 50 (DT.col @"y") tdf)
            (D.percentile 50 (F.col @Double "y") baseDF)
        , eq
            "iqr"
            (TS.interQuartileRange (DT.col @"y") tdf)
            (D.interQuartileRange (F.col @Double "y") baseDF)
        , eq
            "skewness"
            (TS.skewness (DT.col @"y") tdf)
            (D.skewness (F.col @Double "y") baseDF)
        , eq
            "correlation"
            (TS.correlation @"x" @"y" tdf)
            (D.correlation "x" "y" baseDF)
        ]
  where
    eq :: (Eq a, Show a) => String -> a -> a -> Test
    eq lbl typed untyped = TestCase (assertEqual ("typed stat " ++ lbl) untyped typed)

-- Expression combinators ported in Expr.Extra: derive a column both ways.
exprParity :: Test
exprParity =
    TestList
        [ deriveEq "pow" (DT.pow (DT.col @"x") 2) (F.pow (F.col @Int "x") 2)
        , deriveEq "relu" (DT.relu (DT.col @"x")) (F.relu (F.col @Int "x"))
        , deriveEq "toMaybe" (DT.toMaybe (DT.col @"x")) (F.toMaybe (F.col @Int "x"))
        , deriveEq
            "min"
            (DT.min (DT.col @"x") (DT.lit 3))
            (F.min (F.col @Int "x") (F.lit @Int 3))
        , deriveEq
            "splitOn"
            (DT.splitOn "a" (DT.col @"g"))
            (F.splitOn "a" (F.col @T.Text "g"))
        , deriveEq
            "recodeWithDefault"
            (DT.recodeWithDefault 0 [(1 :: Int, 100 :: Int)] (DT.col @"x"))
            (F.recodeWithDefault 0 [(1 :: Int, 100 :: Int)] (F.col @Int "x"))
        ]
  where
    deriveEq ::
        (DI.Columnable a) =>
        String -> DT.TExpr S a -> D.Expr a -> Test
    deriveEq lbl te ue =
        TestCase
            ( assertEqual
                ("typed derive " ++ lbl)
                (D.derive "r" ue baseDF)
                (DT.thaw (DT.derive @"r" te tdf))
            )

-- apply / valueCounts / horizontal merge.
applyParity :: Test
applyParity =
    TestList
        [ TestCase
            ( assertEqual
                "applyColumn type-change"
                (D.apply (show :: Int -> String) "x" baseDF)
                (DT.thaw (DT.applyColumn @"x" (show :: Int -> String) tdf))
            )
        , TestCase
            ( assertEqual
                "applyMany type-preserving"
                (D.applyMany (+ (1 :: Int)) ["x"] baseDF)
                (DT.thaw (DT.applyMany @'["x"] (+ (1 :: Int)) tdf))
            )
        , TestCase
            ( assertEqual
                "valueCounts"
                (D.valueCounts (F.col @T.Text "g") baseDF)
                (DT.valueCounts (DT.col @"g") tdf)
            )
        , TestCase
            ( assertEqual
                "horizontal merge (|||)"
                (leftDF D.||| rightDF)
                (DT.thaw ((DT.|||) leftT rightT))
            )
        ]

-- Sampling/splitting determinism: same seed => same rows as the untyped form.
samplingParity :: Test
samplingParity =
    TestList
        [ TestCase
            ( assertEqual
                "randomSplit"
                (D.randomSplit (mkStdGen 7) 0.5 baseDF)
                (let (a, b) = DT.randomSplit (mkStdGen 7) 0.5 tdf in (DT.thaw a, DT.thaw b))
            )
        , TestCase
            ( assertEqual
                "kFolds"
                (D.kFolds (mkStdGen 7) 3 baseDF)
                (map DT.thaw (DT.kFolds (mkStdGen 7) 3 tdf))
            )
        ]

-- Matrix / numeric-vector extraction.
accessParity :: Test
accessParity =
    TestList
        [ TestCase
            ( assertEqual
                "columnAsDoubleVector"
                (extract (D.columnAsDoubleVector (F.col @Double "y") baseDF))
                (DT.columnAsDoubleVector @"y" tdf)
            )
        , TestCase
            ( assertEqual
                "toDoubleMatrix"
                (extract (D.toDoubleMatrix numericDF))
                (DT.toDoubleMatrix numericT)
            )
        ]
  where
    extract :: Either e a -> a
    extract = fromRight (error "extraction failed")

-- Disjoint frames for the (|||) test.
leftDF :: D.DataFrame
leftDF = D.fromNamedColumns [("x", DI.fromList [1, 2 :: Int])]

rightDF :: D.DataFrame
rightDF = D.fromNamedColumns [("y", DI.fromList [1.0, 2.0 :: Double])]

leftT :: DT.TypedDataFrame '[DT.Column "x" Int]
leftT = either (error . show) id (DT.freezeWithError leftDF)

rightT :: DT.TypedDataFrame '[DT.Column "y" Double]
rightT = either (error . show) id (DT.freezeWithError rightDF)

-- Numeric-only frame for the matrix test.
numericDF :: D.DataFrame
numericDF =
    D.fromNamedColumns
        [ ("x", DI.fromList [1, 2, 3 :: Int])
        , ("y", DI.fromList [1.5, 2.5, 3.5 :: Double])
        ]

numericT :: DT.TypedDataFrame '[DT.Column "x" Int, DT.Column "y" Double]
numericT = either (error . show) id (DT.freezeWithError numericDF)

tests :: [Test]
tests =
    [ statParity
    , exprParity
    , applyParity
    , samplingParity
    , accessParity
    ]
