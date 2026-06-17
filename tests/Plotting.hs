{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- |
Tests for the Vega-Lite web plotting backend: field-type inference, the
box-plot mark fix, NaN handling, escaping, computed-expression encodings, and
typed/untyped spec parity.
-}
module Plotting (tests) where

import Data.Aeson (Value (Array, Null, Object, String), toJSON)
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import Data.Function ((&))
import qualified Data.List as L
import Data.Maybe (fromMaybe, isJust)
import qualified Data.Text as T
import qualified Data.Vector as V
import Test.HUnit

import qualified DataFrame as D
import DataFrame.Functions (col)
import qualified DataFrame.Typed as DT

import qualified DataFrame.Display.Web.Chart as C
import qualified DataFrame.Display.Web.Chart.Typed as CT
import qualified DataFrame.Display.Web.Plot as P

-- Unqualified so the record update below has a single 'includeZero' field
-- label in scope; GHC < 9.8 can't disambiguate the qualified duplicate field.
import DataFrame.Display.Web.Plot (Scatter (includeZero))

-- ---------------------------------------------------------------------------
-- Fixtures + JSON helpers
-- ---------------------------------------------------------------------------

numFrame :: D.DataFrame
numFrame =
    D.fromNamedColumns
        [ ("a", D.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
        , ("b", D.fromList ([10.0, 20.0, 30.0, 40.0] :: [Double]))
        ]

mixedFrame :: D.DataFrame
mixedFrame =
    D.fromNamedColumns
        [ ("a", D.fromList ([1.0, 2.0, 3.0] :: [Double]))
        , ("g", D.fromList (["x", "y", "x"] :: [T.Text]))
        ]

lookupKey :: T.Text -> Value -> Maybe Value
lookupKey k (Object o) = KM.lookup (K.fromText k) o
lookupKey _ _ = Nothing

jpath :: [T.Text] -> Value -> Maybe Value
jpath ks v = foldl (\mv k -> mv >>= lookupKey k) (Just v) ks

dataValues :: Value -> V.Vector Value
dataValues spec = case jpath ["data", "values"] spec of
    Just (Array xs) -> xs
    _ -> V.empty

-- ---------------------------------------------------------------------------
-- Test cases
-- ---------------------------------------------------------------------------

fieldTypeInference :: Test
fieldTypeInference = TestCase $ do
    let spec =
            C.toVegaSpec
                ( C.chart mixedFrame
                    & C.mark C.Point
                    & C.enc C.X (col @Double "a")
                    & C.enc C.Y (col @T.Text "g")
                )
    assertEqual
        "numeric column -> quantitative"
        (Just (String "quantitative"))
        (jpath ["encoding", "x", "type"] spec)
    assertEqual
        "text column -> nominal"
        (Just (String "nominal"))
        (jpath ["encoding", "y", "type"] spec)

boxIsBoxplot :: Test
boxIsBoxplot = TestCase $ do
    let spec =
            C.toVegaSpec (C.chart numFrame & C.mark C.Boxplot & C.enc C.Y (col @Double "a"))
    assertEqual
        "Chart box uses boxplot mark"
        (Just (String "boxplot"))
        (jpath ["mark", "type"] spec)

legacyBoxIsBoxplot :: Test
legacyBoxIsBoxplot = TestCase $ do
    html <- P.box (P.mkBox ["a", "b"]) numFrame
    assertBool
        "legacy box HTML mentions the boxplot mark"
        ("boxplot" `L.isInfixOf` html)
    assertBool
        "legacy box HTML no longer claims 'showing medians'"
        (not ("showing medians" `L.isInfixOf` html))

nanBecomesNull :: Test
nanBecomesNull = TestCase $ do
    let df = D.fromNamedColumns [("a", D.fromList ([0 / 0, 1.0] :: [Double]))]
        spec = C.toVegaSpec (C.chart df & C.enc C.Y (col @Double "a"))
        firstA = lookupKey "a" (fromMaybe Null (dataValues spec V.!? 0))
    assertEqual "NaN inlines as null" (Just Null) firstA

escapingSafe :: Test
escapingSafe = TestCase $ do
    let weird = "we\"ir\\d"
        df = D.fromNamedColumns [(weird, D.fromList ([1.0, 2.0] :: [Double]))]
        spec = C.toVegaSpec (C.chart df & C.enc C.X (col @Double weird))
        row0 = fromMaybe Null (dataValues spec V.!? 0)
    assertBool
        "weird column name present as a data key"
        (Data.Maybe.isJust (lookupKey weird row0))
    assertEqual
        "encoding references the weird field name"
        (Just (String weird))
        (jpath ["encoding", "x", "field"] spec)

computedExpr :: Test
computedExpr = TestCase $ do
    let spec =
            C.toVegaSpec
                (C.chart numFrame & C.enc C.Y (col @Double "a" + col @Double "a"))
        row0 = fromMaybe Null (dataValues spec V.!? 0)
    assertEqual
        "computed field named after channel"
        (Just (String "y"))
        (jpath ["encoding", "y", "field"] spec)
    assertEqual
        "computed value is a + a = 2"
        (Just (toJSON (2.0 :: Double)))
        (lookupKey "y" row0)

includeZeroChart :: Test
includeZeroChart = TestCase $ do
    let spec =
            C.toVegaSpec
                ( C.chart numFrame
                    & C.mark C.Point
                    & C.enc C.X (col @Double "a")
                    & C.enc C.Y (col @Double "b")
                    & C.includeZero C.X False
                )
    assertEqual
        "x scale drops the zero anchor"
        (Just (toJSON False))
        (jpath ["encoding", "x", "scale", "zero"] spec)
    assertEqual
        "y scale untouched"
        Nothing
        (jpath ["encoding", "y", "scale"] spec)

includeZeroMergesWithLog :: Test
includeZeroMergesWithLog = TestCase $ do
    let spec =
            C.toVegaSpec
                ( C.chart numFrame
                    & C.enc C.Y (col @Double "a")
                    & C.logScale C.Y
                    & C.includeZero C.Y False
                )
    assertEqual
        "log scale survives alongside the zero flag"
        (Just (String "log"))
        (jpath ["encoding", "y", "scale", "type"] spec)
    assertEqual
        "zero flag lands on the same scale object"
        (Just (toJSON False))
        (jpath ["encoding", "y", "scale", "zero"] spec)

scatterFitsAxesByDefault :: Test
scatterFitsAxesByDefault = TestCase $ do
    html <- P.scatter (P.mkScatter "a" "b") numFrame
    assertBool
        "scatter axes fit the data by default"
        ("\"zero\":false" `L.isInfixOf` html)
    anchored <- P.scatter ((P.mkScatter "a" "b"){includeZero = True}) numFrame
    assertBool
        "includeZero = True anchors the axes at zero explicitly"
        ("\"zero\":true" `L.isInfixOf` anchored)

lineFitsAxesByDefault :: Test
lineFitsAxesByDefault = TestCase $ do
    html <- P.line (P.mkLine "a" ["b"]) numFrame
    assertBool
        "line axes fit the data by default"
        ("\"zero\":false" `L.isInfixOf` html)

typedParity :: Test
typedParity = TestCase $ do
    let tdf =
            DT.unsafeFreeze numFrame ::
                DT.TypedDataFrame '[DT.Column "a" Double, DT.Column "b" Double]
        specU =
            C.toVegaSpec
                ( C.chart numFrame
                    & C.mark C.Point
                    & C.enc C.X (col @Double "a")
                    & C.enc C.Y (col @Double "b")
                )
        specT =
            CT.toVegaSpec
                ( CT.chart tdf
                    & CT.mark CT.Point
                    & CT.enc CT.X (DT.col @"a")
                    & CT.enc CT.Y (DT.col @"b")
                )
    assertEqual "typed spec equals untyped spec" specU specT

tests :: [Test]
tests =
    [ TestLabel "Plotting.fieldTypeInference" fieldTypeInference
    , TestLabel "Plotting.boxIsBoxplot" boxIsBoxplot
    , TestLabel "Plotting.legacyBoxIsBoxplot" legacyBoxIsBoxplot
    , TestLabel "Plotting.nanBecomesNull" nanBecomesNull
    , TestLabel "Plotting.escapingSafe" escapingSafe
    , TestLabel "Plotting.computedExpr" computedExpr
    , TestLabel "Plotting.includeZeroChart" includeZeroChart
    , TestLabel "Plotting.includeZeroMergesWithLog" includeZeroMergesWithLog
    , TestLabel "Plotting.scatterFitsAxesByDefault" scatterFitsAxesByDefault
    , TestLabel "Plotting.lineFitsAxesByDefault" lineFitsAxesByDefault
    , TestLabel "Plotting.typedParity" typedParity
    ]
