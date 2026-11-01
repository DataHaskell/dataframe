{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Typed.Parity (tests) where

import Data.Either (fromRight)
import qualified Data.Text as T
import System.Random (mkStdGen)

import qualified DataFrame as D
import DataFrame.Expression.Operators ((./=.))
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (eqExpr)
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.Functions as DTF
import qualified DataFrame.Typed.Statistics as TS

import Test.HUnit

type S =
    '[ '("x", Int)
     , '("y", Double)
     , '("g", T.Text)
     ]

baseDF :: D.DataFrame
baseDF =
    D.fromColumns
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
        [ eq "mean" (TS.mean (DT.col @"y") tdf) (D.mean (F.double "y") baseDF)
        , eq "median" (TS.median (DT.col @"y") tdf) (D.median (F.double "y") baseDF)
        , eq
            "variance"
            (TS.variance (DT.col @"y") tdf)
            (D.variance (F.double "y") baseDF)
        , eq
            "stddev"
            (TS.standardDeviation (DT.col @"y") tdf)
            (D.standardDeviation (F.double "y") baseDF)
        , eq "sum" (TS.sum (DT.col @"x") tdf) (D.sum (F.int "x") baseDF)
        , eq
            "percentile"
            (TS.percentile 50 (DT.col @"y") tdf)
            (D.percentile 50 (F.double "y") baseDF)
        , eq
            "iqr"
            (TS.interQuartileRange (DT.col @"y") tdf)
            (D.interQuartileRange (F.double "y") baseDF)
        , eq
            "skewness"
            (TS.skewness (DT.col @"y") tdf)
            (D.skewness (F.double "y") baseDF)
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
        [ deriveEq "pow" (DTF.pow (DT.col @"x") 2) (F.pow (F.int "x") 2)
        , deriveEq "relu" (DTF.relu (DT.col @"x")) (F.relu (F.int "x"))
        , deriveEq "toMaybe" (DTF.toMaybe (DT.col @"x")) (F.toMaybe (F.int "x"))
        , deriveEq
            "min"
            (DTF.min (DT.col @"x") (DT.lit 3))
            (F.min (F.int "x") (F.lit @Int 3))
        , deriveEq
            "splitOn"
            (DTF.splitOn "a" (DT.col @"g"))
            (F.splitOn "a" (F.text "g"))
        , deriveEq
            "recodeWithDefault"
            (DTF.recodeWithDefault 0 [(1 :: Int, 100 :: Int)] (DT.col @"x"))
            (F.recodeWithDefault 0 [(1 :: Int, 100 :: Int)] (F.int "x"))
        , deriveEq
            "eq"
            (DTF.eq (DT.col @"x") (DT.lit 3))
            (F.eq (F.int "x") (F.lit @Int 3))
        , deriveEq
            "and"
            (DTF.and (DTF.lt (DT.col @"x") (DT.lit 5)) (DTF.geq (DT.col @"x") (DT.lit 2)))
            (F.and (F.lt x (F.lit @Int 5)) (F.geq x (F.lit @Int 2)))
        , deriveEq "contains" (DTF.contains "a" (DT.col @"g")) (F.contains "a" g)
        , deriveEq "length" (DTF.length (DT.col @"g")) (F.length g)
        , deriveEq
            "isNull"
            (DTF.isNull (DTF.toMaybe (DT.col @"x")))
            (F.isNull (F.toMaybe x))
        , deriveEq "cast" (DTF.cast @"x" @Int) (F.cast @Int "x")
        , deriveEq
            "castWithDefault"
            (DTF.castWithDefault @"g" (0 :: Int))
            (F.castWithDefault (0 :: Int) "g")
        ]
  where
    x = F.int "x"
    g = F.text "g"
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

-- Typed aggregations build the same expression as their untyped forms.
aggParity :: Test
aggParity =
    TestList
        [ same "countDistinct" (DTF.countDistinct (DT.col @"x")) (F.countDistinct x)
        , same "topK" (DTF.topK 2 (DT.col @"x")) (F.topK 2 x)
        , same "bottomK" (DTF.bottomK 2 (DT.col @"x")) (F.bottomK 2 x)
        , same "not" (DTF.not (DTF.eq (DT.col @"x") 1)) (F.not (F.eq x 1))
        ]
  where
    x = F.int "x"
    same :: (DI.Columnable a) => String -> DT.TExpr S a -> D.Expr a -> Test
    same lbl te ue = TestCase (assertBool lbl (eqExpr (DT.unTExpr te) ue))

-- apply / valueCounts / horizontal merge.
applyParity :: Test
applyParity =
    TestList
        [ TestCase
            ( assertEqual
                "apply type-change"
                (D.apply (show :: Int -> String) "x" baseDF)
                (DT.thaw (DT.apply @"x" (show :: Int -> String) tdf))
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
                (D.valueCounts (F.text "g") baseDF)
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
                (extract (D.columnAsDoubleVector (F.double "y") baseDF))
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
leftDF = D.fromColumns [("x", DI.fromList [1, 2 :: Int])]

rightDF :: D.DataFrame
rightDF = D.fromColumns [("y", DI.fromList [1.0, 2.0 :: Double])]

leftT :: DT.TypedDataFrame '[ '("x", Int)]
leftT = either (error . show) id (DT.freezeWithError leftDF)

rightT :: DT.TypedDataFrame '[ '("y", Double)]
rightT = either (error . show) id (DT.freezeWithError rightDF)

-- Numeric-only frame for the matrix test.
numericDF :: D.DataFrame
numericDF =
    D.fromColumns
        [ ("x", DI.fromList [1, 2, 3 :: Int])
        , ("y", DI.fromList [1.5, 2.5, 3.5 :: Double])
        ]

numericT :: DT.TypedDataFrame '[ '("x", Int), '("y", Double)]
numericT = either (error . show) id (DT.freezeWithError numericDF)

-- Ops added for import-swap parity: same result through either API.
newOpsParity :: Test
newOpsParity =
    TestList
        [ same
            "topBy"
            (D.topBy 1 2 [D.Desc (F.double "y")] baseDF)
            (DT.topBy 1 2 [DT.Desc (DT.col @"y")] tdf)
        , same
            "deriveMany"
            ( D.deriveMany
                [ "x2" D..= (F.int "x" * 2)
                , "y2" D..= (F.double "y" + 1)
                ]
                baseDF
            )
            ( DT.deriveMany
                (DT.as @"y2" (DT.col @"y" + 1) . DT.as @"x2" (DT.col @"x" * 2))
                tdf
            )
        , same
            "join INNER"
            (D.join D.INNER ["x"] leftDF numericDF)
            (DT.join @'DT.INNER @'["x"] leftT numericT)
        , same
            "join LEFT"
            (D.join D.LEFT ["x"] numericDF leftDF)
            (DT.join @'DT.LEFT @'["x"] numericT leftT)
        , same
            "applyInt"
            (D.applyInt (* 10) "x" baseDF)
            (DT.applyInt @"x" (* 10) tdf)
        , same
            "applyDouble"
            (D.applyDouble (> 20) "y" baseDF)
            (DT.applyDouble @"y" (> 20) tdf)
        , same
            "fold"
            (D.fold (\n -> D.filter (F.int "x" ./=. F.lit n)) [2, 4] baseDF)
            (DT.fold (\n -> DT.filter (DT.col @"x" DT../=. DT.lit n)) [2, 4] tdf)
        , TestCase (assertBool "empty is null" (DT.null DT.empty))
        , TestCase (assertEqual "null" (D.null baseDF) (DT.null tdf))
        , TestCase (assertEqual "toCsv" (D.toCsv baseDF) (DT.toCsv tdf))
        , TestCase
            (assertEqual "toMarkdown" (D.toMarkdown baseDF) (DT.toMarkdown tdf))
        , TestCase
            ( assertEqual
                "frame stats via DataFrame.Typed"
                (D.mean (F.double "y") baseDF)
                (DT.mean (DT.col @"y") tdf)
            )
        , TestCase
            ( assertEqual
                "readJSONEither"
                (D.toCsv <$> D.readJSONEither jsonBytes)
                (DT.toCsv <$> DT.readJSONEither @'[ '("a", Maybe Double)] jsonBytes)
            )
        ]
  where
    same :: String -> D.DataFrame -> DT.TypedDataFrame cols -> Test
    same lbl untyped typed = TestCase (assertEqual lbl untyped (DT.thaw typed))
    jsonBytes = "[{\"a\": 1}, {\"a\": 2}]"

tests :: [Test]
tests =
    [ newOpsParity
    , statParity
    , exprParity
    , aggParity
    , applyParity
    , samplingParity
    , accessParity
    ]
