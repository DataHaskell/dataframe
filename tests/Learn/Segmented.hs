{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Learn.Segmented (tests) where

import Control.Exception (SomeException, evaluate, try)
import Data.List (isInfixOf)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.LinearModel.Logistic (defaultLogisticConfig)
import DataFrame.LinearModel.Regression (
    LinearRegressor (..),
    defaultLinearConfig,
 )
import DataFrame.Metrics (rmse)
import DataFrame.ModelSelection (crossValidate)
import DataFrame.Segmented
import DataFrame.SymbolicRegression (SRConfig (..), SRModel, defaultSRConfig)

import Test.HUnit

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

interpT :: D.DataFrame -> Expr T.Text -> [T.Text]
interpT df e = case interpret @T.Text df e of
    Right (TColumn c) -> either (const []) V.toList (toVector @T.Text @V.Vector c)
    Left err -> error (show err)

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

-- | A two-segment frame: y = 2x+1 on group "a", y = -3x+5 on group "b".
twoSegDF :: D.DataFrame
twoSegDF =
    D.fromNamedColumns
        [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
        , ("x", DI.fromList xs)
        ,
            ( "y"
            , DI.fromList ([2 * x + 1 | x <- take 4 xs] ++ [(-3) * x + 5 | x <- drop 4 xs])
            )
        ]
  where
    xs = [1, 2, 3, 4, 1, 2, 3, 4] :: [Double]

segByKey ::
    [T.Text] -> SegmentedModel a LinearRegressor -> Segment LinearRegressor
segByKey k m = head [s | s <- smSegments m, segKey s == k]

lowFloor :: Segmented cfg -> Segmented cfg
lowFloor s = s{segMinRows = 3}

recoveryTests :: [Test]
recoveryTests =
    [ "per-segment OLS recovered" ~: do
        let m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") twoSegDF
            a = segModel (segByKey ["a"] m)
            b = segModel (segByKey ["b"] m)
        assertBool "two segments" (length (smSegments m) == 2)
        assertBool "a slope ~ 2" (close 1e-9 (regCoef a VU.! 0) 2)
        assertBool "a intercept ~ 1" (close 1e-9 (regIntercept a) 1)
        assertBool "b slope ~ -3" (close 1e-9 (regCoef b VU.! 0) (-3))
        assertBool "b intercept ~ 5" (close 1e-9 (regIntercept b) 5)
    , "prediction reproduces y" ~: do
        let m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") twoSegDF
            preds = interpD twoSegDF (predict m)
            truth = interpD twoSegDF (F.col @Double "y")
        assertBool "routed predictions match y" (and (zipWith (close 1e-6) preds truth))
    ]

routingTests :: [Test]
routingTests =
    [ "unseen category routes to fallback" ~: do
        let m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") twoSegDF
            fb = smFallback m
            unseen =
                D.fromNamedColumns
                    [ ("g", DI.fromList (["c"] :: [T.Text]))
                    , ("x", DI.fromList ([2.0] :: [Double]))
                    , ("y", DI.fromList ([0.0] :: [Double]))
                    ]
            [p] = interpD unseen (predict m)
            expected = regIntercept fb + regCoef fb VU.! 0 * 2.0
        assertBool "unseen -> fallback affine" (close 1e-9 p expected)
    ]

genericityTests :: [Test]
genericityTests =
    [ "logistic base composes" ~: do
        let labels = replicate 4 ("pos" :: T.Text) ++ replicate 4 "neg"
            df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    , ("x", DI.fromList ([1, 2, 3, 4, 1, 2, 3, 4] :: [Double]))
                    , ("label", DI.fromList labels)
                    ]
            m = fit (lowFloor (segmented defaultLogisticConfig)) (F.col @T.Text "label") df
            preds = interpT df (predict m)
        assertBool "logistic predicts one label per row" (length preds == 8)
    , "symbolic base composes" ~: do
        let cfg = defaultSRConfig{srGenerations = 2, srPopSize = 12}
            m = fit (lowFloor (segmented cfg)) (F.col @Double "y") twoSegDF
            preds = interpD twoSegDF (predict m)
        assertBool "symbolic predicts one value per row" (length preds == 8)
    ]

selectionTests :: [Test]
selectionTests =
    [ "segmentOn picks only the named column" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    , ("h", DI.fromList (cycle3 8))
                    , ("x", DI.fromList ([1, 2, 3, 4, 1, 2, 3, 4] :: [Double]))
                    , ("y", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    ]
            m =
                fit
                    (segmentOn (lowFloor (segmented defaultLinearConfig)) ["g"])
                    (F.col @Double "y")
                    df
        assertEqual "only g" ["g"] (smCatCols m)
    , "cardinality cap skips high-cardinality text" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    , ("k", DI.fromList (map (T.pack . show) [1 .. 8 :: Int]))
                    , ("x", DI.fromList ([1, 2, 3, 4, 1, 2, 3, 4] :: [Double]))
                    , ("y", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    ]
            cfg = (lowFloor (segmented defaultLinearConfig)){segMaxCard = 2}
            m = fit cfg (F.col @Double "y") df
        assertEqual "k (8 distinct) skipped, g kept" ["g"] (smCatCols m)
    ]
  where
    cycle3 n = take n (cycle (["p", "q", "r"] :: [T.Text]))

nullTests :: [Test]
nullTests =
    [ "null categorical routes to fallback" ~: do
        let gcol =
                [Just "a", Just "a", Just "a", Nothing, Just "b", Just "b", Just "b", Nothing] ::
                    [Maybe T.Text]
            df =
                D.fromNamedColumns
                    [ ("g", DI.fromList gcol)
                    , ("x", DI.fromList ([1, 2, 3, 9, 1, 2, 3, 9] :: [Double]))
                    , ("y", DI.fromList ([3, 5, 7, 0, 2, -1, -4, 0] :: [Double]))
                    ]
            m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") df
        -- Only "a" and "b" appear as segments; "null" is never a key.
        assertBool
            "no null-keyed segment"
            (["null"] `notElem` map segKey (smSegments m))
        assertBool "two real segments" (length (smSegments m) == 2)
    ]

-- | Fit a segmented linear model, capturing any fit-time error message.
fitErr ::
    D.DataFrame -> IO (Either SomeException (SegmentedModel Double LinearRegressor))
fitErr df =
    try
        (evaluate (fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") df))

guardTests :: [Test]
guardTests =
    [ "int feature error points at toDouble" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    , ("n", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Int]))
                    , ("y", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    ]
        r <- fitErr df
        case r of
            Left e ->
                assertBool
                    "names column n and toDouble"
                    ("n" `isInfixOf` show e && "toDouble" `isInfixOf` show e)
            Right _ -> assertFailure "expected an actionable error for the Int feature"
    , "nullable feature error points at dropping" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    ,
                        ( "z"
                        , DI.fromList
                            ( [Just 1, Nothing, Just 3, Just 4, Just 5, Just 6, Just 7, Just 8] ::
                                [Maybe Double]
                            )
                        )
                    , ("y", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    ]
        r <- fitErr df
        case r of
            Left e ->
                assertBool
                    "names column z and filterJust"
                    ("z" `isInfixOf` show e && "filterJust" `isInfixOf` show e)
            Right _ -> assertFailure "expected an actionable error for the nullable feature"
    , "well-typed frame fits after casting/dropping" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ replicate 4 "b"))
                    , ("n", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    , ("y", DI.fromList ([1, 2, 3, 4, 5, 6, 7, 8] :: [Double]))
                    ]
            m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") df
        assertEqual "two segments" 2 (length (smSegments m))
    ]

diagnosticTests :: [Test]
diagnosticTests =
    [ "undersized segment falls back, diagnostics correct" ~: do
        let df =
                D.fromNamedColumns
                    [ ("g", DI.fromList (replicate 4 ("a" :: T.Text) ++ ["b"]))
                    , ("x", DI.fromList ([1, 2, 3, 4, 9] :: [Double]))
                    , ("y", DI.fromList ([3, 5, 7, 9, 0] :: [Double]))
                    ]
            m = fit (lowFloor (segmented defaultLinearConfig)) (F.col @Double "y") df
        assertEqual "one local segment" 1 (length (smSegments m))
        assertEqual "segment a has 4 rows" 4 (segN (segByKey ["a"] m))
        assertEqual "b fell back with 1 row" [(["b"], 1)] (smFellBack m)
    ]

cvTests :: [Test]
cvTests =
    [ "composes with crossValidate" ~: do
        let scores =
                crossValidate
                    2
                    0
                    rmse
                    (F.col @Double "y")
                    ( predict
                        . fit ((segmented defaultLinearConfig){segMinRows = 2}) (F.col @Double "y")
                    )
                    twoSegDF
        assertBool "two fold scores" (length scores == 2)
        assertBool
            "scores are finite"
            (all (\s -> not (isNaN s || isInfinite s)) scores)
    ]

poolingTests :: [Test]
poolingTests =
    [ "lambda limits and interior monotonicity" ~: do
        let fitL lam =
                fit
                    (pooled (lowFloor (segmented defaultLinearConfig)) lam)
                    (F.col @Double "y")
                    twoSegDF
            m0 = fitL 0
            mInf = fitL 1e9
            mMid = fitL 5
            slope k mm = regCoef (segModel (segByKey k mm)) VU.! 0
            inter k mm = regIntercept (segModel (segByKey k mm))
        -- lambda = 0: independent OLS (matches recovery test).
        assertBool "lambda0 a slope 2" (close 1e-9 (slope ["a"] m0) 2)
        assertBool "lambda0 b slope -3" (close 1e-9 (slope ["b"] m0) (-3))
        -- lambda -> inf: both segments converge to the n_g-weighted reference
        -- (raw slope -0.5, raw intercept 3.0 for this balanced fixture).
        assertBool "lambdaInf a slope ~ -0.5" (close 1e-3 (slope ["a"] mInf) (-0.5))
        assertBool "lambdaInf b slope ~ -0.5" (close 1e-3 (slope ["b"] mInf) (-0.5))
        assertBool "lambdaInf a intercept ~ 3" (close 1e-3 (inter ["a"] mInf) 3.0)
        assertBool
            "lambdaInf segments coincide"
            (close 1e-3 (slope ["a"] mInf) (slope ["b"] mInf))
        -- interior: a's slope sits strictly between its OLS (2) and the ref (-0.5).
        assertBool
            "interior between OLS and ref"
            (slope ["a"] mMid < 2 && slope ["a"] mMid > (-0.5))
    , "pooling unsupported for symbolic base" ~: do
        r <-
            try
                ( evaluate
                    ( fit
                        (pooled (lowFloor (segmented defaultSRConfig)) 1.0)
                        (F.col @Double "y")
                        twoSegDF
                    )
                ) ::
                IO (Either SomeException (SegmentedModel Double SRModel))
        case r of
            Left _ -> return ()
            Right _ -> assertFailure "expected a fit-time error for pooling on a symbolic base"
    ]

tests :: [Test]
tests =
    concat
        [ recoveryTests
        , routingTests
        , genericityTests
        , selectionTests
        , nullTests
        , guardTests
        , diagnosticTests
        , cvTests
        , poolingTests
        ]
