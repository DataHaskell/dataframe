{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Operators are identified by tag, never by display name.
module Operations.OperatorIdentity (tests) where

import qualified Data.Text as T
import Data.Time (Day)
import qualified DataFrame as D
import DataFrame.Expression.Operators
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr, eqExpr, structuralKey)
import DataFrame.Internal.Expression.Simplify (simplify)

import Test.HUnit

xs :: D.DataFrame
xs = D.fromColumns [("x", D.fromList [1, 2, 3 :: Int])]

derived :: Expr Int -> [Int]
derived e = D.columnAsList (F.int "y") (D.derive "y" e xs)

weird :: T.Text -> Expr Int -> Expr Int -> Expr Int
weird opName = lift2Decorated (\a b -> a * 100 + b) opName Nothing True 6

udfNamedLikeBuiltin :: Test
udfNamedLikeBuiltin =
    TestLabel "udf named add keeps its own function" . TestCase $
        assertEqual "" [101, 202, 303] (derived (weird "add" x x))
  where
    x = F.int "x"

udfOtherName :: Test
udfOtherName =
    TestLabel "udf with fresh name" . TestCase $
        assertEqual "" [101, 202, 303] (derived (weird "myadd" x x))
  where
    x = F.int "x"

anonymousNotEqual :: Test
anonymousNotEqual =
    TestLabel "anonymous udfs are not equal" . TestCase $
        assertBool "" (not (eqExpr (F.lift2 (+) x x) (F.lift2 (*) x x)))
  where
    x = F.int "x"

namedEqual :: Test
namedEqual =
    TestLabel "same named op is equal" . TestCase $
        assertBool "" (eqExpr (weird "f" x x) (weird "f" x x))
  where
    x = F.int "x"

namedVsBuiltin :: Test
namedVsBuiltin =
    TestLabel "named add differs from builtin add" . TestCase $
        assertBool "" (not (eqExpr (weird "add" x x) (x + x)))
  where
    x = F.int "x"

fromMaybeDefaults :: Test
fromMaybeDefaults =
    TestLabel "fromMaybe defaults are part of identity" . TestCase $
        assertBool "" (not (eqExpr (F.fromMaybe 0 m) (F.fromMaybe 1 m)))
  where
    m = F.maybeInt "m"

powVariants :: Test
powVariants =
    TestLabel "^ and ^^ are different ops" . TestCase $
        assertBool "" (not (eqExpr (d .^. two) (d .^^. two)))
  where
    d = F.double "d"
    two = F.lit (2 :: Int)

rowsKept :: D.DataFrame -> Expr Bool -> Int
rowsKept df p = D.nRows (D.filter p df)

-- Both sides must keep the same rows before and after simplification.
sameRows :: String -> D.DataFrame -> Expr Bool -> Test
sameRows label df p =
    TestLabel label . TestCase $
        assertEqual label (rowsKept df p) (rowsKept df (simplify p))

anonymousAtomsNotMerged :: Test
anonymousAtomsNotMerged =
    sameRows "distinct udfs over one column are separate atoms" ds $
        F.and
            (F.lift (* 10) d .> F.lit (3 :: Double))
            (F.lift id d .< F.lit (2 :: Double))
  where
    ds = D.fromColumns [("d", D.fromList [1, 2, 3 :: Double])]
    d = F.double "d"

fromMaybeTrueNotFalseOnNull :: Test
fromMaybeTrueNotFalseOnNull =
    sameRows "fromMaybe True keeps null rows" ms $
        F.and
            (F.fromMaybe True (m .< F.lit (5 :: Double)))
            (F.fromMaybe True (m .> F.lit (10 :: Double)))
  where
    ms =
        D.fromColumns
            [("m", D.fromList [Just 1, Nothing, Just 20 :: Maybe Double])]
    m = F.maybeDouble "m"

-- The six-sum moment shape must not see through a user function.
momentsRespectUdf :: Test
momentsRespectUdf =
    TestLabel "moment fast path keeps udf in sum" . TestCase $
        assertEqual "" [60, 150] (D.columnAsList (F.double "sx") out)
  where
    df =
        D.fromColumns
            [ ("k", D.fromList [1, 1, 1, 2, 2, 2 :: Int])
            , ("x", D.fromList [1, 2, 3, 4, 5, 6 :: Double])
            , ("y", D.fromList [6, 5, 4, 3, 2, 1 :: Double])
            ]
    x = F.double "x"
    y = F.double "y"
    out =
        D.sortBy [D.Asc (F.int "k")] $
            D.aggregate
                [ F.count x `as` "n"
                , F.sum (F.lift (* 10) x) `as` "sx"
                , F.sum y `as` "sy"
                , F.sum (x * x) `as` "sxx"
                , F.sum (y * y) `as` "syy"
                , F.sum (x * y) `as` "sxy"
                ]
                (D.groupBy ["k"] df)

-- A function is builtin iff its expression has a provable structural key.
hasKey :: (Columnable a) => Expr a -> Bool
hasKey = (/= Nothing) . structuralKey

-- Every non-higher-order function in DataFrame.Functions is a builtin.
everyFunctionIsBuiltin :: Test
everyFunctionIsBuiltin =
    TestList
        [ TestLabel fn (TestCase (assertBool fn ok))
        | (fn, ok) <- builtins
        ]
  where
    x = F.int "x"
    d = F.double "d"
    m = F.maybeInt "m"
    t = F.text "t"
    b = F.bool "b"
    l = F.col @[Int] "l"
    day = F.col @Day "day"
    builtins =
        [ ("cast", hasKey (F.cast @Int "x"))
        , ("castWithDefault", hasKey (F.castWithDefault (0 :: Int) "x"))
        , ("castEither", hasKey (F.castEither @Int "x"))
        , ("unsafeCast", hasKey (F.unsafeCast @Int "x"))
        , ("castExpr", hasKey (F.castExpr @Int t))
        , ("castExprWithDefault", hasKey (F.castExprWithDefault (0 :: Int) t))
        , ("castExprEither", hasKey (F.castExprEither @Int t))
        , ("unsafeCastExpr", hasKey (F.unsafeCastExpr @Int t))
        , ("toDouble", hasKey (F.toDouble x))
        , ("div", hasKey (F.div x x))
        , ("mod", hasKey (F.mod x x))
        , ("eq", hasKey (F.eq x x))
        , ("lt", hasKey (F.lt x x))
        , ("gt", hasKey (F.gt x x))
        , ("leq", hasKey (F.leq x x))
        , ("geq", hasKey (F.geq x x))
        , ("and", hasKey (F.and b b))
        , ("or", hasKey (F.or b b))
        , ("not", hasKey (F.not b))
        , ("count", hasKey (F.count x))
        , ("countDistinct", hasKey (F.countDistinct x))
        , ("countAll", hasKey F.countAll)
        , ("collect", hasKey (F.collect x))
        , ("mode", hasKey (F.mode x))
        , ("minimum", hasKey (F.minimum x))
        , ("maximum", hasKey (F.maximum x))
        , ("sum", hasKey (F.sum x))
        , ("sumMaybe", hasKey (F.sumMaybe m))
        , ("mean", hasKey (F.mean x))
        , ("topK", hasKey (F.topK 2 x))
        , ("bottomK", hasKey (F.bottomK 2 x))
        , ("meanMaybe", hasKey (F.meanMaybe m))
        , ("variance", hasKey (F.variance x))
        , ("median", hasKey (F.median x))
        , ("medianMaybe", hasKey (F.medianMaybe m))
        , ("percentile", hasKey (F.percentile 50 d))
        , ("stddev", hasKey (F.stddev x))
        , ("stddevMaybe", hasKey (F.stddevMaybe m))
        , ("zScore", hasKey (F.zScore d))
        , ("pow", hasKey (F.pow x 2))
        , ("relu", hasKey (F.relu x))
        , ("min", hasKey (F.min x x))
        , ("max", hasKey (F.max x x))
        , ("toMaybe", hasKey (F.toMaybe x))
        , ("fromMaybe", hasKey (F.fromMaybe 0 m))
        , ("isJust", hasKey (F.isJust m))
        , ("isNothing", hasKey (F.isNothing m))
        , ("isNull", hasKey (F.isNull m))
        , ("isNotNull", hasKey (F.isNotNull m))
        , ("fromJust", hasKey (F.fromJust m))
        , ("recode", hasKey (F.recode [(1 :: Int, 'a')] x))
        , ("recodeWithDefault", hasKey (F.recodeWithDefault 'z' [(1 :: Int, 'a')] x))
        ,
            ( "recodeWithCondition"
            , hasKey (F.recodeWithCondition (F.lit (0 :: Int)) [((.> F.lit @Int 1), 1)] x)
            )
        , ("firstOrNothing", hasKey (F.firstOrNothing l))
        , ("lastOrNothing", hasKey (F.lastOrNothing l))
        , ("contains", hasKey (F.contains "a" t))
        , ("length", hasKey (F.length t))
        , ("splitOn", hasKey (F.splitOn "," t))
        , ("match", hasKey (F.match "a+" t))
        , ("matchAll", hasKey (F.matchAll "a+" t))
        , ("parseDate", hasKey (F.parseDate @Day "%Y" t))
        , ("daysBetween", hasKey (F.daysBetween day day))
        , ("over", hasKey (F.over ["k"] (F.sum x)))
        ]

-- Functions taking arbitrary Haskell functions can only be anonymous.
higherOrderAreAnonymous :: Test
higherOrderAreAnonymous =
    TestList
        [ TestLabel fn (TestCase (assertBool fn (Prelude.not ok)))
        | (fn, ok) <- anonymous
        ]
  where
    x = F.int "x"
    m = F.maybeInt "m"
    anonymous =
        [ ("lift", hasKey (F.lift (+ 1) x))
        , ("lift2", hasKey (F.lift2 (+) x x))
        , ("nullLift", hasKey (F.nullLift (+ 1) m))
        , ("nullLift2", hasKey (F.nullLift2 (+) m x))
        , ("reduce", hasKey (F.reduce x (0 :: Int) (+)))
        , ("whenPresent", hasKey (F.whenPresent (+ 1) m))
        , ("whenBothPresent", hasKey (F.whenBothPresent (+) m m))
        , ("bind", hasKey (F.bind (Just . (+ 1)) m))
        ]

tests :: [Test]
tests =
    [ udfNamedLikeBuiltin
    , udfOtherName
    , anonymousNotEqual
    , namedEqual
    , namedVsBuiltin
    , fromMaybeDefaults
    , powVariants
    , anonymousAtomsNotMerged
    , fromMaybeTrueNotFalseOnNull
    , momentsRespectUdf
    , everyFunctionIsBuiltin
    , higherOrderAreAnonymous
    ]
