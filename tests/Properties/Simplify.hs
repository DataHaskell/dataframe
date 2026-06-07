{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Property tests for the simplifier and tree-pruning pass: the durable
guarantees the hand-written examples only sample.

  * @simplify@ preserves denotation (@interpret e ≡ interpret (simplify e)@),
    over Bool and Maybe Bool, on a DataFrame that includes NaN, null, and
    exact-boundary rows.
  * @simplify@ is idempotent (reaches a normal form within the fixpoint cap).
  * @pruneDead@ preserves the function the tree computes, on every row.
-}
module Properties.Simplify (tests) where

import qualified DataFrame as D
import DataFrame.DecisionTree (Tree (..), predictWithTree, pruneDead)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (TColumn), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr, eqExpr)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Simplify (simplify)
import DataFrame.Operators

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Test.QuickCheck

-- A fixture spanning the interesting rows: exact thresholds, gaps, NaN, null.
fixtureDF :: D.DataFrame
fixtureDF =
    D.fromNamedColumns
        [
            ( "x"
            , DI.fromList ([10, 20, 25, 30, 35, 40, 50, 0 / 0, -(1 / 0), 1 / 0] :: [Double])
            )
        , ("n", DI.fromList ([10, 20, 25, 30, 35, 40, 50, 0, 100, -5] :: [Int]))
        ,
            ( "m"
            , DI.fromList
                ( [ Just 10
                  , Nothing
                  , Just 30
                  , Just 35
                  , Nothing
                  , Just 50
                  , Just 0
                  , Just 30
                  , Nothing
                  , Just 40
                  ] ::
                    [Maybe Double]
                )
            )
        ]

thresholds :: [Double]
thresholds = [20, 25, 30, 35, 40]

-- ---- generators ----

-- strict-Bool comparison atoms over the Double column "x" and Int column "n"
genAtomBool :: Gen (Expr Bool)
genAtomBool = do
    t <- elements thresholds
    oneof
        [ elements
            [ F.col Double "x" .< F.lit t
            , F.col Double "x" .<= F.lit t
            , F.col Double "x" .> F.lit t
            , F.col Double "x" .>= F.lit t
            , F.col Double "x" .== F.lit t
            , F.col Double "x" ./= F.lit t
            ]
        , elements
            [ F.toDouble (F.col Int "n") .< F.lit t
            , F.toDouble (F.col Int "n") .<= F.lit t
            , F.toDouble (F.col Int "n") .> F.lit t
            , F.toDouble (F.col Int "n") .>= F.lit t
            ]
        ]

genBoolExpr :: Int -> Gen (Expr Bool)
genBoolExpr d
    | d <= 0 = genAtomBool
    | otherwise =
        oneof
            [ genAtomBool
            , F.and <$> genBoolExpr (d - 1) <*> genBoolExpr (d - 1)
            , F.or <$> genBoolExpr (d - 1) <*> genBoolExpr (d - 1)
            , F.not <$> genBoolExpr (d - 1)
            ]

-- nullable comparison atoms over the Maybe Double column "m"
genAtomMaybe :: Gen (Expr (Maybe Bool))
genAtomMaybe = do
    t <- elements thresholds
    elements
        [ F.col (Maybe Double) "m" .< F.lit t
        , F.col (Maybe Double) "m" .<= F.lit t
        , F.col (Maybe Double) "m" .> F.lit t
        , F.col (Maybe Double) "m" .>= F.lit t
        , F.col (Maybe Double) "m" .== F.lit t
        , F.col (Maybe Double) "m" ./= F.lit t
        ]

genMaybeExpr :: Int -> Gen (Expr (Maybe Bool))
genMaybeExpr d
    | d <= 0 = genAtomMaybe
    | otherwise =
        oneof
            [ genAtomMaybe
            , (.&&) <$> genMaybeExpr (d - 1) <*> genMaybeExpr (d - 1)
            , (.||) <$> genMaybeExpr (d - 1) <*> genMaybeExpr (d - 1)
            ]

genTree :: Int -> Gen (Tree T.Text)
genTree d
    | d <= 0 = Leaf <$> elements ["A", "B", "C"]
    | otherwise =
        oneof
            [ Leaf <$> elements ["A", "B", "C"]
            , do
                cond <- genAtomBool
                Branch cond <$> genTree (d - 1) <*> genTree (d - 1)
            ]

-- ---- evaluation helpers ----

evalBool :: D.DataFrame -> Expr Bool -> Maybe (VU.Vector Bool)
evalBool df e = case interpret @Bool df e of
    Right (TColumn tcol) -> either (const Nothing) Just (toVector @Bool @VU.Vector tcol)
    Left _ -> Nothing

evalMaybe :: D.DataFrame -> Expr (Maybe Bool) -> Maybe (V.Vector (Maybe Bool))
evalMaybe df e = case interpret @(Maybe Bool) df e of
    Right (TColumn tcol) -> either (const Nothing) Just (toVector @(Maybe Bool) @V.Vector tcol)
    Left _ -> Nothing

-- ---- properties ----

prop_simplifyPreservesBool :: Property
prop_simplifyPreservesBool =
    forAll (genBoolExpr 4) $ \e ->
        evalBool fixtureDF e === evalBool fixtureDF (simplify e)

prop_simplifyPreservesMaybe :: Property
prop_simplifyPreservesMaybe =
    forAll (genMaybeExpr 3) $ \e ->
        evalMaybe fixtureDF e === evalMaybe fixtureDF (simplify e)

prop_simplifyIdempotent :: Property
prop_simplifyIdempotent =
    forAll (genBoolExpr 4) $ \e ->
        let s = simplify e in property (eqExpr (simplify s) s)

prop_pruneDeadPreserves :: Property
prop_pruneDeadPreserves =
    forAll (genTree 4) $ \t ->
        let n = D.nRows fixtureDF
            predAll tr = [predictWithTree @T.Text "x" fixtureDF i tr | i <- [0 .. n - 1]]
         in predAll (pruneDead t) === predAll t

tests :: [Property]
tests =
    [ prop_simplifyPreservesBool
    , prop_simplifyPreservesMaybe
    , prop_simplifyIdempotent
    , prop_pruneDeadPreserves
    ]
