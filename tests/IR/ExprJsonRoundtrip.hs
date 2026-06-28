{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Round-trip, pipeline, end-to-end, and wire-shape contract tests for the
-- expression/pipeline serializer.
module IR.ExprJsonRoundtrip (tests) where

import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Char8 as BS8
import Data.Either (isLeft)
import Test.HUnit

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable, TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr, UExpr (..))
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Operators (ifThenElse, (.>.))
import qualified Data.Vector.Unboxed as VU

import DataFrame.LinearModel (defaultLinearConfig)
import DataFrame.Model (fit, predict)
import DataFrame.Transform (Transform (..), applyTransform)
import DataFrame.Transform.Serialize (loadTransformFromFile, saveTransformToFile)

import DataFrame.Expr.Serialize (
    SomeExpr (..),
    decodeExprAny,
    decodeExprAt,
    decodeExprFromBytes,
    encodeExpr,
    loadExprAtFromFile,
    loadPipelineFromFile,
    saveExprToFile,
    savePipelineToFile,
 )
import System.IO.Temp (withSystemTempDirectory)

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

close :: [Double] -> [Double] -> Bool
close a b = length a == length b && and (zipWith (\x y -> abs (x - y) <= 1e-9) a b)

df3 :: D.DataFrame
df3 = D.fromNamedColumns [("x", DI.fromList [1, 2, 3 :: Double])]

regDF :: D.DataFrame
regDF =
    D.fromNamedColumns
        [ ("x1", DI.fromList xs1)
        , ("x2", DI.fromList xs2)
        , ("y", DI.fromList [2 * a - 3 * b + 1 | (a, b) <- zip xs1 xs2])
        ]
  where
    xs1 = [1, 2, 3, 4, 5, 6, 7, 8] :: [Double]
    xs2 = [2, 1, 4, 3, 6, 5, 8, 7] :: [Double]

{- | An expression round-trips when encode → decode → re-encode yields the same
JSON. Avoids needing an 'Eq' instance on the GADT.
-}
roundtrips :: (Columnable a) => Expr a -> Bool
roundtrips e = either (const False) id $ do
    v1 <- encodeExpr e
    SomeExpr _ e' <- decodeExprAny v1
    v2 <- encodeExpr e'
    Right (v1 == v2)

testCase :: String -> Bool -> Test
testCase name ok = TestCase (assertBool name ok)

-- Row-wise expression node kinds.
exprRoundtripTests :: [Test]
exprRoundtripTests =
    [ testCase "col" (roundtrips (F.col @Double "x"))
    , testCase "lit" (roundtrips (F.lit @Int 42))
    , testCase "arithmetic" (roundtrips (F.col @Double "x" * F.lit 2 + F.lit 1))
    , testCase "division" (roundtrips (F.col @Double "x" / F.lit 2))
    , testCase "comparison" (roundtrips (F.col @Double "x" .>. F.lit 0))
    , testCase "unary sqrt" (roundtrips (sqrt (F.col @Double "x")))
    , testCase
        "if/then/else"
        (roundtrips (ifThenElse (F.col @Double "x" .>. F.lit 0) (F.lit 1.0) (F.lit (-1.0)) :: Expr Double))
    ]

-- Aggregation and window node kinds (the decoder cases added for full parity).
aggRoundtripTests :: [Test]
aggRoundtripTests =
    [ testCase "sum" (roundtrips (F.sum (F.col @Double "x")))
    , testCase "count" (roundtrips (F.count (F.col @Double "x")))
    , testCase "minimum" (roundtrips (F.minimum (F.col @Double "x")))
    , testCase "maximum" (roundtrips (F.maximum (F.col @Int "x")))
    , testCase "mean" (roundtrips (F.mean (F.col @Double "x")))
    , testCase "variance" (roundtrips (F.variance (F.col @Double "x")))
    , testCase "median" (roundtrips (F.median (F.col @Double "x")))
    , testCase "over" (roundtrips (F.over ["g"] (F.sum (F.col @Double "x"))))
    ]

-- Documented limitation: collect's list-typed output has no type tag, so it
-- cannot be encoded.
limitationTests :: [Test]
limitationTests =
    [ testCase
        "collect cannot encode"
        (isLeft (encodeExpr (F.collect (F.col @Double "x"))))
    ]

-- Pin the wire shape Python decodes against: (x * 2) + 1 over doubles.
contractTests :: [Test]
contractTests =
    [ TestCase $ case Aeson.eitherDecodeStrict (BS8.pack contractJson) >>= decodeExprAt @Double of
        Left err -> assertFailure ("contract decode failed: " <> err)
        Right e -> assertBool "contract interprets to [3,5,7]" (close (interpD df3 e) [3, 5, 7])
    , testCase
        "collect agg node fails to decode"
        (isLeft (decodeExprFromBytes (BS8.pack collectJson)))
    ]

contractJson :: String
contractJson =
    "{\"node\":\"binary\",\"out_type\":\"double\",\"op\":\"add\",\"arg_type\":\"double\","
        <> "\"lhs\":{\"node\":\"binary\",\"out_type\":\"double\",\"op\":\"mult\",\"arg_type\":\"double\","
        <> "\"lhs\":{\"node\":\"col\",\"out_type\":\"double\",\"name\":\"x\"},"
        <> "\"rhs\":{\"node\":\"lit\",\"out_type\":\"double\",\"value\":2.0}},"
        <> "\"rhs\":{\"node\":\"lit\",\"out_type\":\"double\",\"value\":1.0}}"

collectJson :: String
collectJson =
    "{\"node\":\"agg\",\"out_type\":\"double\",\"agg\":\"collect\",\"arg_type\":\"double\","
        <> "\"arg\":{\"node\":\"col\",\"out_type\":\"double\",\"name\":\"x\"}}"

-- File round-trips for a single expression, a pipeline, and a fitted model.
ioTests :: [Test]
ioTests =
    [ TestCase $ withSystemTempDirectory "expr-ser" $ \dir -> do
        let fp = dir ++ "/expr.json"
            e = F.col @Double "x" * F.lit 2 + F.lit 1
        sr <- saveExprToFile fp e
        assertEqual "save expr ok" (Right ()) sr
        loaded <- loadExprAtFromFile @Double fp
        case loaded of
            Left err -> assertFailure ("load expr failed: " <> err)
            Right e' -> assertBool "loaded expr interprets equal" (close (interpD df3 e') (interpD df3 e))
    , TestCase $ withSystemTempDirectory "expr-ser" $ \dir -> do
        let fp = dir ++ "/pipeline.json"
            nes = [("z", UExpr (F.col @Double "x" * F.lit 2)), ("w", UExpr (F.col @Double "x" + F.lit 10))]
        sr <- savePipelineToFile fp nes
        assertEqual "save pipeline ok" (Right ()) sr
        loaded <- loadPipelineFromFile fp
        case loaded of
            Left err -> assertFailure ("load pipeline failed: " <> err)
            Right nes' -> do
                let df' = applyTransform (Transform nes') df3
                assertBool "z = 2x" (close (interpD df' (F.col @Double "z")) [2, 4, 6])
                assertBool "w = x+10" (close (interpD df' (F.col @Double "w")) [11, 12, 13])
    , TestCase $ withSystemTempDirectory "expr-ser" $ \dir -> do
        let fp = dir ++ "/transform.json"
            t = Transform [("z", UExpr (F.col @Double "x" * F.lit 2))]
        sr <- saveTransformToFile fp t
        assertEqual "save transform ok" (Right ()) sr
        loaded <- loadTransformFromFile fp
        case loaded of
            Left err -> assertFailure ("load transform failed: " <> err)
            Right t' ->
                assertBool "transform z = 2x" (close (interpD (applyTransform t' df3) (F.col @Double "z")) [2, 4, 6])
    , TestCase $ withSystemTempDirectory "expr-ser" $ \dir -> do
        -- End-to-end: fit OLS, persist its prediction, reload, compare outputs.
        let fp = dir ++ "/model.json"
            m = fit defaultLinearConfig (F.col @Double "y") regDF
        sr <- saveExprToFile fp (predict m)
        assertEqual "save model ok" (Right ()) sr
        loaded <- loadExprAtFromFile @Double fp
        case loaded of
            Left err -> assertFailure ("load model failed: " <> err)
            Right e' ->
                assertBool
                    "reloaded prediction matches original"
                    (close (interpD regDF e') (interpD regDF (predict m)))
    ]

tests :: [Test]
tests =
    exprRoundtripTests
        ++ aggRoundtripTests
        ++ limitationTests
        ++ contractTests
        ++ ioTests
