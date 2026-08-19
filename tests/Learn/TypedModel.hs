{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | 'fit'/'predict' are frame-aware. Training on a 'TypedDataFrame' with a typed
target ('DT.col') returns a 'Fitted' carrying the schema, and 'predict' on it
yields a typed 'TExpr'; training on an untyped 'DataFrame' with an untyped target
('F.col') returns the bare model and an 'Expr'. Both paths must agree numerically.
-}
module Learn.TypedModel (tests) where

import Data.Maybe (fromJust)
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.LinearModel.Regression (
    LinearConfig (..),
    LinearRegressor (..),
    Penalty (..),
    defaultLinearConfig,
 )
import DataFrame.Model (Fitted (..), fit, predict)
import qualified DataFrame.Typed as DT

import Test.HUnit

-- | All-Double schema: two features and a Double target.
type Houses =
    '[ '("x1", Double)
     , '("x2", Double)
     , '("y", Double)
     ]

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

housesTDF :: DT.TypedDataFrame Houses
housesTDF = fromJust (DT.freeze @Houses regDF)

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

close :: Double -> Double -> Double -> Bool
close tol a b = abs (a - b) <= tol

sameModel :: LinearRegressor -> LinearRegressor -> Bool
sameModel a b =
    and (zipWith (close 1e-12) (VU.toList (regCoef a)) (VU.toList (regCoef b)))
        && close 1e-12 (regIntercept a) (regIntercept b)

{- | 'fit' on a 'TypedDataFrame' returns a 'Fitted' whose model matches the
bare model from 'fit' on the equivalent 'DataFrame'.
-}
testTypedFitParity :: Test
testTypedFitParity = TestCase $ do
    let onTyped = fit defaultLinearConfig (DT.col @"y") housesTDF
        onUntyped = fit defaultLinearConfig (F.col @Double "y") regDF
    assertBool
        "fittedModel of typed fit == untyped fit"
        (sameModel (fittedModel onTyped) onUntyped)

{- | 'predict' on the typed fit yields a 'TExpr Houses Double'; unwrapped and
interpreted it matches 'predict' on the untyped fit.
-}
testTypedPredict :: Test
testTypedPredict = TestCase $ do
    let onTyped = fit defaultLinearConfig (DT.col @"y") housesTDF
        onUntyped = fit defaultLinearConfig (F.col @Double "y") regDF
        typedExpr = DT.unTExpr (predict onTyped) -- :: Expr Double via TExpr Houses Double
        untypedExpr = predict onUntyped -- :: Expr Double
        truth = interpD regDF (F.col @Double "y")
    assertBool
        "typed predict matches untyped predict"
        ( and
            (zipWith (close 1e-12) (interpD regDF typedExpr) (interpD regDF untypedExpr))
        )
    assertBool
        "typed predict recovers the target"
        (and (zipWith (close 1e-6) (interpD regDF typedExpr) truth))

-- | The frame-awareness holds for a regularized fit too.
testTypedRidge :: Test
testTypedRidge = TestCase $ do
    let cfg = defaultLinearConfig{lcPenalty = Ridge 0.01}
        onTyped = fit cfg (DT.col @"y") housesTDF
        onUntyped = fit cfg (F.col @Double "y") regDF
    assertBool
        "ridge: fittedModel of typed fit == untyped fit"
        (sameModel (fittedModel onTyped) onUntyped)

tests :: [Test]
tests =
    [ TestLabel "typed fit returns Fitted matching untyped" testTypedFitParity
    , TestLabel "typed predict yields TExpr matching untyped" testTypedPredict
    , TestLabel "typed ridge fit parity" testTypedRidge
    ]
