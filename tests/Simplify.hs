{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Specification for simplify: each case compares the full expression.
module Simplify (tests) where

import DataFrame.Expression.Operators
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr, eqExpr)
import DataFrame.Internal.Expression.Simplify (simplify)

import Test.HUnit

simplifiesTo :: (Columnable a) => String -> Expr a -> Expr a -> Test
simplifiesTo label input want =
    TestLabel label . TestCase $
        assertBool
            (label ++ ": got " ++ show (simplify input) ++ " want " ++ show want)
            (eqExpr (simplify input) want)

unchanged :: (Columnable a) => String -> Expr a -> Test
unchanged label e = simplifiesTo label e e

sameDirection :: [Test]
sameDirection =
    [ simplifiesTo
        "and lower bounds keeps max"
        ( F.and
            (F.double "age" .> F.lit (20 :: Double))
            (F.double "age" .> F.lit (25 :: Double))
        )
        (F.double "age" .> F.lit (25 :: Double))
    , simplifiesTo
        "and upper bounds keeps min"
        ( F.and
            (F.double "age" .< F.lit (50 :: Double))
            (F.double "age" .< F.lit (40 :: Double))
        )
        (F.double "age" .< F.lit (40 :: Double))
    , simplifiesTo
        "or lower bounds keeps min"
        ( F.or
            (F.double "age" .> F.lit (20 :: Double))
            (F.double "age" .> F.lit (25 :: Double))
        )
        (F.double "age" .> F.lit (20 :: Double))
    , simplifiesTo
        "or upper bounds keeps max"
        ( F.or
            (F.double "age" .< F.lit (50 :: Double))
            (F.double "age" .< F.lit (40 :: Double))
        )
        (F.double "age" .< F.lit (50 :: Double))
    ]

mixedDirection :: [Test]
mixedDirection =
    [ simplifiesTo
        "closed interval at a point becomes equality"
        ( F.and
            (F.double "age" .>= F.lit (30 :: Double))
            (F.double "age" .<= F.lit (30 :: Double))
        )
        (F.double "age" .== F.lit (30 :: Double))
    , simplifiesTo
        "open contradiction becomes False"
        ( F.and
            (F.double "age" .> F.lit (30 :: Double))
            (F.double "age" .< F.lit (30 :: Double))
        )
        (F.lit False)
    , simplifiesTo
        "disjoint bounds become False"
        ( F.and
            (F.double "age" .> F.lit (30 :: Double))
            (F.double "age" .< F.lit (20 :: Double))
        )
        (F.lit False)
    , simplifiesTo
        "distinct points conjoined become False"
        ( F.and
            (F.double "age" .== F.lit (30 :: Double))
            (F.double "age" .== F.lit (40 :: Double))
        )
        (F.lit False)
    , simplifiesTo
        "point inside half-space becomes the point"
        ( F.and
            (F.double "age" .== F.lit (30 :: Double))
            (F.double "age" .> F.lit (25 :: Double))
        )
        (F.double "age" .== F.lit (30 :: Double))
    , simplifiesTo
        "point outside half-space becomes False"
        ( F.and
            (F.double "age" .== F.lit (30 :: Double))
            (F.double "age" .> F.lit (40 :: Double))
        )
        (F.lit False)
    , simplifiesTo
        "negation redundant under bound drops"
        ( F.and
            (F.double "age" ./= F.lit (30 :: Double))
            (F.double "age" .> F.lit (40 :: Double))
        )
        (F.double "age" .> F.lit (40 :: Double))
    ]

tautologies :: [Test]
tautologies =
    [ simplifiesTo
        "integral exhaustive cover becomes True"
        ( F.or
            (F.toDouble (F.int "ai") .<= F.lit (30 :: Double))
            (F.toDouble (F.int "ai") .> F.lit (30 :: Double))
        )
        (F.lit True)
    , simplifiesTo
        "distinct inequalities cover everything"
        ( F.or
            (F.double "age" ./= F.lit (30 :: Double))
            (F.double "age" ./= F.lit (40 :: Double))
        )
        (F.lit True)
    , simplifiesTo
        "inequality or equality at same point"
        ( F.or
            (F.double "age" ./= F.lit (30 :: Double))
            (F.double "age" .== F.lit (30 :: Double))
        )
        (F.lit True)
    ]

booleanAlgebra :: [Test]
booleanAlgebra =
    [ simplifiesTo
        "idempotent and"
        ( F.and
            (F.double "age" .> F.lit (20 :: Double))
            (F.double "age" .> F.lit (20 :: Double))
        )
        (F.double "age" .> F.lit (20 :: Double))
    , simplifiesTo
        "absorption and over or"
        ( F.and
            (F.double "age" .> F.lit (20 :: Double))
            ( F.or
                (F.double "age" .> F.lit (20 :: Double))
                (F.double "hours" .> F.lit (40 :: Double))
            )
        )
        (F.double "age" .> F.lit (20 :: Double))
    , simplifiesTo
        "true and unit"
        (F.and (F.lit True) (F.double "hours" .> F.lit (40 :: Double)))
        (F.double "hours" .> F.lit (40 :: Double))
    , simplifiesTo
        "false and annihilates"
        (F.and (F.lit False) (F.double "hours" .> F.lit (40 :: Double)))
        (F.lit False)
    , simplifiesTo
        "double negation"
        (F.not (F.not (F.double "age" .> F.lit (20 :: Double))))
        (F.double "age" .> F.lit (20 :: Double))
    ]

ifCollapse :: [Test]
ifCollapse =
    [ simplifiesTo
        "boolean if becomes its condition"
        ( F.ifThenElse
            (F.double "age" .> F.lit (20 :: Double))
            (F.lit True)
            (F.lit False)
        )
        (F.double "age" .> F.lit (20 :: Double))
    , simplifiesTo
        "if with equal branches collapses"
        ( F.ifThenElse
            (F.double "hours" .> F.lit (40 :: Double))
            (F.double "age" .> F.lit (20 :: Double))
            (F.double "age" .> F.lit (20 :: Double))
        )
        (F.double "age" .> F.lit (20 :: Double))
    ]

multiPass :: [Test]
multiPass =
    [ simplifiesTo
        "long and chain keeps tightest"
        ( F.and
            ( F.and
                ( F.and
                    (F.double "age" .> F.lit (10 :: Double))
                    (F.double "age" .> F.lit (20 :: Double))
                )
                (F.double "age" .> F.lit (30 :: Double))
            )
            (F.double "age" .> F.lit (40 :: Double))
        )
        (F.double "age" .> F.lit (40 :: Double))
    , simplifiesTo
        "consolidate then contradiction"
        ( F.and
            ( F.and
                (F.double "age" .>= F.lit (30 :: Double))
                (F.double "age" .>= F.lit (40 :: Double))
            )
            (F.double "age" .<= F.lit (35 :: Double))
        )
        (F.lit False)
    , simplifiesTo
        "cascade of contradictions"
        ( F.or
            ( F.and
                (F.double "age" .> F.lit (30 :: Double))
                (F.double "age" .< F.lit (20 :: Double))
            )
            ( F.and
                (F.double "hours" .> F.lit (200 :: Double))
                (F.double "hours" .< F.lit (10 :: Double))
            )
        )
        (F.lit False)
    , simplifiesTo
        "consolidate enabling idempotence"
        ( F.and
            ( F.or
                (F.double "age" .> F.lit (20 :: Double))
                (F.double "age" .> F.lit (25 :: Double))
            )
            ( F.or
                (F.double "age" .> F.lit (20 :: Double))
                (F.double "age" .> F.lit (30 :: Double))
            )
        )
        (F.double "age" .> F.lit (20 :: Double))
    , simplifiesTo
        "de morgan over contradiction"
        ( F.not
            ( F.and
                (F.double "age" .> F.lit (30 :: Double))
                (F.double "age" .< F.lit (20 :: Double))
            )
        )
        (F.lit True)
    , simplifiesTo
        "interior contradiction collapses the conjunction"
        ( F.and
            ( F.and
                (F.double "age" .> F.lit (10 :: Double))
                (F.double "hours" .> F.lit (40 :: Double))
            )
            ( F.and
                (F.double "age" .> F.lit (30 :: Double))
                (F.double "age" .< F.lit (25 :: Double))
            )
        )
        (F.lit False)
    ]

nullAware :: [Test]
nullAware =
    [ simplifiesTo
        "just-literal lower bounds keep max"
        ( (F.int "age" .> F.lit (Just (30 :: Int)))
            .&& (F.int "age" .> F.lit (Just (35 :: Int)))
        )
        (F.int "age" .> F.lit (Just (35 :: Int)))
    , simplifiesTo
        "just-literal contradiction over non-null column becomes Just False"
        ( (F.int "age" .> F.lit (Just (30 :: Int)))
            .&& (F.int "age" .< F.lit (Just (20 :: Int)))
        )
        (F.lit (Just False))
    , unchanged
        "nullable column contradiction stays unknown"
        ( (F.maybeInt "w" .> F.lit (Just (30 :: Int)))
            .&& (F.maybeInt "w" .< F.lit (Just (20 :: Int)))
        )
    , unchanged
        "nullable column tautology stays unknown"
        ( (F.maybeInt "w" .<= F.lit (Just (30 :: Int)))
            .|| (F.maybeInt "w" .> F.lit (Just (30 :: Int)))
        )
    , simplifiesTo
        "fromMaybe consolidation keeps tighter"
        ( F.and
            (F.fromMaybe False (F.maybeDouble "w" .<= F.lit (5 :: Double)))
            (F.fromMaybe False (F.maybeDouble "w" .<= F.lit (3 :: Double)))
        )
        (F.fromMaybe False (F.maybeDouble "w" .<= F.lit (3 :: Double)))
    , simplifiesTo
        "fromMaybe contradiction becomes False"
        ( F.and
            (F.fromMaybe False (F.maybeDouble "w" .> F.lit (30 :: Double)))
            (F.fromMaybe False (F.maybeDouble "w" .< F.lit (20 :: Double)))
        )
        (F.lit False)
    , unchanged
        "fromMaybe tautology stays unsimplified"
        ( F.or
            (F.fromMaybe False (F.maybeDouble "w" .<= F.lit (30 :: Double)))
            (F.fromMaybe False (F.maybeDouble "w" .> F.lit (30 :: Double)))
        )
    ]

bailing :: [Test]
bailing =
    [ unchanged
        "proper interval is not collapsed"
        ( F.and
            (F.double "age" .>= F.lit (20 :: Double))
            (F.double "age" .<= F.lit (65 :: Double))
        )
    , unchanged
        "or with a gap is not a tautology"
        ( F.or
            (F.double "age" .<= F.lit (30 :: Double))
            (F.double "age" .> F.lit (40 :: Double))
        )
    , unchanged
        "two inequalities are not an interval"
        ( F.and
            (F.double "age" ./= F.lit (30 :: Double))
            (F.double "age" ./= F.lit (40 :: Double))
        )
    , unchanged
        "cross-column conjunction is left alone"
        ( F.and
            (F.double "age" .> F.lit (50 :: Double))
            (F.double "hours" .> F.lit (40 :: Double))
        )
    , unchanged
        "double exhaustive cover bails (NaN)"
        ( F.or
            (F.double "age" .<= F.lit (30 :: Double))
            (F.double "age" .> F.lit (30 :: Double))
        )
    , unchanged
        "punctured interval is not a single atom"
        ( F.and
            (F.double "age" ./= F.lit (30 :: Double))
            (F.double "age" .> F.lit (20 :: Double))
        )
    ]

tests :: [Test]
tests =
    concat
        [ sameDirection
        , mixedDirection
        , tautologies
        , booleanAlgebra
        , ifCollapse
        , multiPass
        , nullAware
        , bailing
        ]
