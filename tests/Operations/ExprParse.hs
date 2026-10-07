{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Reloading expressions from their two printed forms.
module Operations.ExprParse (tests) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import DataFrame.Expression.Parse
import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr, eqExpr, prettyPrint)
import DataFrame.Internal.Expression.Operators ((./=.), (.<=.))
import Test.HUnit

frame :: D.DataFrame
frame =
    D.fromColumns
        [ ("Age", D.fromList [25, 41, 63, 18 :: Int])
        , ("Departure/Arrival time convenient", D.fromList [0, 3, 5, 2 :: Int])
        , ("Class", D.fromList ["Business", "Eco", "Eco Plus", "Eco" :: T.Text])
        , ("Arrival Delay in Minutes", D.fromList [0, 12.5, 3, 0 :: Double])
        ]

-- A fitted-tree shape: a cast Int threshold, a Text inequality, a column whose
-- name contains an operator character, a negative leaf, and a scaled sum.
model :: Expr Double
model =
    F.lit 0.1
        * F.ifThenElse
            (F.toDouble (F.col @Int "Age") .<=. F.lit 40.5)
            ( F.ifThenElse
                (F.col @T.Text "Class" ./=. F.lit "Business")
                (F.lit (-0.859))
                (F.lit 0.31)
            )
            ( F.ifThenElse
                (F.toDouble (F.col @Int "Departure/Arrival time convenient") .<=. F.lit 2.5)
                (F.lit 1.2)
                (F.col @Double "Arrival Delay in Minutes" / F.lit 10)
            )
        + F.lit 0.1
            * F.ifThenElse
                (F.col @T.Text "Class" ./=. F.lit "Eco")
                (F.lit 0.5)
                (F.lit (-0.25))

scores :: Expr Double -> VU.Vector Double
scores e =
    either
        (error . show)
        id
        (D.columnAsUnboxedVector (F.col @Double "s") (D.derive "s" e frame))

testShowRoundTrip :: Test
testShowRoundTrip = TestCase $ case parseShownAs @Double (show model) of
    Left err -> assertFailure ("parseShown failed: " ++ err)
    Right e -> do
        assertBool "structurally equal" (eqExpr e model)
        assertEqual "same scores" (scores model) (scores e)

testPrettyRoundTrip :: Test
testPrettyRoundTrip = TestCase $ case parsePrettyAs @Double (schemaOf frame) (prettyPrint model) of
    Left err -> assertFailure ("parsePretty failed: " ++ err)
    Right e -> do
        assertEqual "reprint identical" (prettyPrint model) (prettyPrint e)
        let gap = VU.maximum (VU.zipWith (\a b -> abs (a - b)) (scores model) (scores e))
        assertBool ("scores within 1e-12, got " ++ show gap) (gap < 1e-12)

testSchema :: Test
testSchema = TestCase $ do
    let s = schemaOf frame
    assertEqual "Int column" (Just TInt) (M.lookup "Age" s)
    assertEqual "Text column" (Just TText) (M.lookup "Class" s)
    assertEqual
        "Double column"
        (Just TDouble)
        (M.lookup "Arrival Delay in Minutes" s)

tests :: [Test]
tests =
    [ TestLabel "expr parse: show form round-trips" testShowRoundTrip
    , TestLabel "expr parse: pretty form round-trips" testPrettyRoundTrip
    , TestLabel "expr parse: schema types" testSchema
    ]
