{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | An expression means what it computes for one row at a time. @If@ takes
each row's value from the branch that row chooses and nothing from the other;
a repeated subexpression may be computed once but never changes a result.
-}
module Operations.RowWise (tests) where

import Control.Exception (throw)
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified DataFrame as D
import DataFrame.Expression.Operators ((./=.), (.<=.), (.>.))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable, TypedColumn (..), toVector)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Interpreter (interpret)

import Test.HUnit

-- | The second size is above the 200k threshold where loops run in parallel.
sizes :: [Int]
sizes = [60, 200003]

xs, ys :: Int -> [Int]
xs n = take n (cycle [7, -3, 0, 12, 5, -8, 1])
ys n = take n (cycle [2, 0, -1, 0, 3])

ds :: Int -> [Double]
ds n = take n (cycle [0.5, -1.5, 2.25, 100, -0.0, 3])

names :: Int -> [T.Text]
names n = take n (cycle ["Eco", "Business", "", "Eco Plus"])

maybes :: Int -> [Maybe Int]
maybes n = take n (cycle [Just 1, Nothing, Just 3, Nothing, Just 5, Just 6])

frame :: Int -> D.DataFrame
frame n =
    D.fromColumns
        [ "x" D.=: xs n
        , "y" D.=: ys n
        , "d" D.=: ds n
        , "name" D.=: names n
        , "m" D.=: maybes n
        , "flag" D.=: map (fmap (> 2)) (maybes n)
        ]

eval :: forall a. (Columnable a) => Int -> Expr a -> Either String [a]
eval n e = case interpret @a (frame n) e of
    Left err -> Left (show err)
    Right (TColumn col) -> Right (V.toList (either throw id (toVector @a @V.Vector col)))

x, y :: Expr Int
x = F.col "x"
y = F.col "y"

d :: Expr Double
d = F.col "d"

-- | The expression's rows equal the reference rows, at every size.
rows :: (Columnable a) => String -> Expr a -> (Int -> [a]) -> [Test]
rows label e expected =
    [ TestCase
        (assertEqual (label ++ " n=" ++ show n) (Right (expected n)) (eval n e))
    | n <- sizes
    ]

tests :: [Test]
tests =
    concat
        [ rows
            "a guard protects its branch"
            (If (y ./=. F.lit 0) (F.div x y) (F.lit 0))
            (\n -> [if b /= 0 then a `div` b else 0 | (a, b) <- zip (xs n) (ys n)])
        , rows
            "a guarded branch may repeat a failing operation"
            (If (y ./=. F.lit 0) (F.div x y + F.div x y) (F.lit 0))
            (\n -> [if b /= 0 then 2 * (a `div` b) else 0 | (a, b) <- zip (xs n) (ys n)])
        , rows
            "a null in the branch not chosen is ignored"
            (If (x .>. F.lit 0) (F.col @(Maybe Int) "m") (F.lit (Just 0)))
            (\n -> [if a > 0 then m else Just 0 | (a, m) <- zip (xs n) (maybes n)])
        , rows
            "nested conditions"
            ( If
                (x .>. F.lit 0)
                (If (y .>. F.lit 0) (x + y) (x - y))
                (If (y .>. F.lit 1) (x * y) x)
            )
            ( \n ->
                [ if a > 0 then (if b > 0 then a + b else a - b) else (if b > 1 then a * b else a)
                | (a, b) <- zip (xs n) (ys n)
                ]
            )
        , rows
            "an aggregate inside a branch sees every row"
            (If (d .<=. F.lit 1) (d - F.mean d) (F.lit 0))
            ( \n ->
                let mean = sum (ds n) / fromIntegral n
                 in [if v <= 1 then v - mean else 0 | v <- ds n]
            )
        , rows
            "text branches"
            (If (x .>. F.lit 0) (F.lift T.toUpper (F.col @T.Text "name")) (F.lit "none"))
            (\n -> [if a > 0 then T.toUpper t else "none" | (a, t) <- zip (xs n) (names n)])
        , rows
            "nullable branches"
            ( If
                (x .>. F.lit 0)
                (F.lift (fmap (+ 1)) (F.col @(Maybe Int) "m"))
                (F.col @(Maybe Int) "m")
            )
            (\n -> [if a > 0 then fmap (+ 1) m else m | (a, m) <- zip (xs n) (maybes n)])
        , rows
            "a repeated subexpression"
            ((x + y) * (x + y) - (x + y))
            (\n -> [(a + b) * (a + b) - (a + b) | (a, b) <- zip (xs n) (ys n)])
        , rows
            "a subexpression repeated across branches"
            (If (y .>. F.lit 0) ((x + y) * F.lit 2) ((x + y) * F.lit 3))
            (\n -> [if b > 0 then (a + b) * 2 else (a + b) * 3 | (a, b) <- zip (xs n) (ys n)])
        , rows
            "different functions under one name are not merged"
            (F.lift (+ 1) x + F.lift (* 2) x)
            (\n -> [(a + 1) + (a * 2) | a <- xs n])
        ,
            [ TestCase
                ( assertBool
                    "a missing column is reported even in a branch never chosen"
                    ( either
                        (const True)
                        (const False)
                        (eval 60 (If (F.lit True) x (F.col @Int "absent")))
                    )
                )
            , TestCase
                ( assertBool
                    "a null condition is an error"
                    ( either
                        (const True)
                        (const False)
                        (eval 60 (If (F.col @Bool "flag") x y))
                    )
                )
            ]
        ]
