{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Comparisons, arithmetic, if-then-else and toDouble evaluate to their
Haskell functions applied row by row, for every mix of column and literal
operands. Floating results compare by bit pattern.
-}
module Operations.InterpreterKernels where

import Control.Exception (throw)
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified DataFrame as D
import DataFrame.Expression.Operators (
    (./=.),
    (.<.),
    (.<=.),
    (.==.),
    (.>.),
    (.>=.),
 )
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (
    Column,
    Columnable,
    TypedColumn (..),
    toVector,
 )
import DataFrame.Internal.Expression (Expr (..), prettyPrint)
import DataFrame.Internal.Interpreter (interpret)

import Assertions (BitEq, assertBitEq)
import Test.HUnit

-- | One side of an expression: a column with known rows, or a literal.
data Operand e = Column T.Text [e] | Literal e

operandExpr :: (Columnable e) => Operand e -> Expr e
operandExpr (Column name _) = Col name
operandExpr (Literal v) = Lit v

-- | The operand's value in each of @n@ rows.
operandRows :: Int -> Operand e -> [e]
operandRows _ (Column _ rows) = rows
operandRows n (Literal v) = replicate n v

operandColumns :: (Columnable e) => Operand e -> [(T.Text, Column)]
operandColumns (Column name rows) = [name D.=: rows]
operandColumns (Literal _) = []

-- | An operator paired with what it computes for one row.
data BinOp e r = BinOp (Expr e -> Expr e -> Expr r) (e -> e -> r)

comparisons :: (Columnable e, Ord e) => [BinOp e Bool]
comparisons =
    [ BinOp (.<.) (<)
    , BinOp (.<=.) (<=)
    , BinOp (.>.) (>)
    , BinOp (.>=.) (>=)
    , BinOp (.==.) (==)
    , BinOp (./=.) (/=)
    ]

arithmetic :: (Columnable e, Num e) => [BinOp e e]
arithmetic = [BinOp (+) (+), BinOp (-) (-), BinOp (*) (*)]

doubles :: [Double]
doubles = [0 / 0, -(1 / 0), -1.5, -0.0, 0.0, 5.0e-324, 0.5, 1, 2.5, 1.0e308, 1 / 0]

ints :: [Int]
ints = [minBound, -3, -1, 0, 1, 7, maxBound]

-- | The second size is above the 200k threshold where loops run in parallel.
sizes :: [Int]
sizes = [length doubles * length doubles, 200003]

-- | Two columns holding every pair of the given values, cycled to @n@ rows.
columnPair :: Int -> [e] -> (Operand e, Operand e)
columnPair n vals = (Column "x" xs, Column "y" ys)
  where
    (xs, ys) = unzip (take n (cycle [(a, b) | a <- vals, b <- vals]))

{- | Column-column, column-literal and literal-column operands. The large size
uses fewer literals.
-}
operandPairs :: Int -> [e] -> [(Operand e, Operand e)]
operandPairs n vals = (x, y) : concat [[(x, Literal t), (Literal t, x)] | t <- lits]
  where
    (x, y) = columnPair n vals
    lits = if n > 1000 then take 3 vals else vals

run :: forall a. (Columnable a) => [(T.Text, Column)] -> Expr a -> [a]
run columns e = case interpret @a (D.fromColumns columns) e of
    Left err -> throw err
    Right (TColumn col) -> V.toList (either throw id (toVector @a @V.Vector col))

binaryCases ::
    (Columnable e, Columnable r, BitEq r, Show r) => [e] -> [BinOp e r] -> [Test]
binaryCases vals ops =
    [ TestCase
        (assertBitEq (prettyPrint e ++ " n=" ++ show n) expected (run columns e))
    | n <- sizes
    , (l, r) <- operandPairs n vals
    , let columns = operandColumns l ++ operandColumns r
    , BinOp op f <- ops
    , let e = op (operandExpr l) (operandExpr r)
          expected = zipWith f (operandRows n l) (operandRows n r)
    ]

-- | If-then-else over a Bool column, with branches of every operand mix.
selectCases :: (Columnable e, BitEq e, Show e) => [e] -> [Test]
selectCases vals =
    [ TestCase
        (assertBitEq (prettyPrint e ++ " n=" ++ show n) expected (run columns e))
    | n <- sizes
    , let cond = Column "c" (take n (cycle [True, False, False]))
          lits = take 3 vals
    , (l, r) <-
        operandPairs n vals ++ zip (map Literal lits) (map Literal (drop 1 lits))
    , let columns = operandColumns cond ++ operandColumns l ++ operandColumns r
          e = If (operandExpr cond) (operandExpr l) (operandExpr r)
          expected =
            zipWith3
                (\c a b -> if c then a else b)
                (operandRows n cond)
                (operandRows n l)
                (operandRows n r)
    ]

caseWhenCases :: [Test]
caseWhenCases =
    [ TestCase (assertBitEq (prettyPrint e ++ " n=" ++ show n) expected (run columns e))
    | n <- sizes
    , let x = fst (columnPair n ints)
          columns = operandColumns x
          xe = operandExpr x
          e =
            F.caseWhen (xe .>. Lit 5) (Lit 1)
                $ F.caseWhen (xe .>. Lit 0) (Lit 2)
                $ F.orElse (Lit (3 :: Int))
          expected = [if v > 5 then 1 else if v > 0 then 2 else 3 | v <- operandRows n x]
    ]

toDoubleCases :: [Test]
toDoubleCases =
    [ TestCase
        ( assertBitEq
            (prettyPrint e ++ " n=" ++ show n)
            (map fromIntegral (operandRows n x))
            (run (operandColumns x) e)
        )
    | n <- sizes
    , let x = fst (columnPair n ints)
          e = F.toDouble (operandExpr x)
    ]

tests :: [Test]
tests =
    binaryCases doubles comparisons
        ++ binaryCases ints comparisons
        ++ binaryCases doubles arithmetic
        ++ binaryCases ints arithmetic
        ++ selectCases doubles
        ++ selectCases ints
        ++ caseWhenCases
        ++ toDoubleCases
