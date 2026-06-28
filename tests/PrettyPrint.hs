{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Golden tests pinning the format of 'prettyPrint'/'prettyPrintWidth': flat
@else if@ ladders, operator-precedence parenthesization, width-aware wrapping of
commutative chains, and short expressions staying on one line.
-}
module PrettyPrint (tests) where

import qualified Data.Text as T
import DataFrame.Internal.Expression (Expr, prettyPrint, prettyPrintWidth)
import DataFrame.Operators
import Test.HUnit

a, b, c :: Expr Double
a = col "a"
b = col "b"
c = col "c"

golden :: String -> String -> String -> Test
golden label want got =
    TestLabel label . TestCase $ assertEqual label want got

tests :: [Test]
tests =
    [ -- A short conditional stays compact; then/else still break onto own lines.
      golden
        "fits on one line"
        "if x .>=. 0.0\nthen \"pos\"\nelse \"neg\""
        ( prettyPrint
            (ifThenElse (col @Double "x" .>=. lit 0.0) (lit @T.Text "pos") (lit "neg"))
        )
    , -- Nested else-if forms a flat ladder (no staircase indentation).
      golden
        "flat else-if ladder"
        "if a .>. 1.0\nthen \"x\"\nelse if b .>. 2.0\nthen \"y\"\nelse \"z\""
        ( prettyPrint
            ( ifThenElse
                (col @Double "a" .>. lit 1.0)
                (lit @T.Text "x")
                (ifThenElse (col @Double "b" .>. lit 2.0) (lit "y") (lit "z"))
            )
        )
    , -- A lower-precedence operand is parenthesized under a higher one.
      golden "precedence: (a + b) * c" "(a + b) * c" (prettyPrint ((a + b) * c))
    , -- No spurious parens when precedence already disambiguates.
      golden "precedence: a + b * c" "a + b * c" (prettyPrint (a + b * c))
    , -- Non-commutative operators are not flattened/reordered.
      golden "left-assoc subtraction" "a - b - c" (prettyPrint (a - b - c))
    , -- A commutative chain stays on one line when it fits the width.
      golden
        "commutative chain fits"
        "alpha + beta + gamma"
        (prettyPrintWidth 80 (col @Double "alpha" + col "beta" + col "gamma"))
    , -- The same chain wraps at the operator with a hanging indent when narrow.
      golden
        "commutative chain wraps"
        "alpha\n  + beta\n  + gamma"
        (prettyPrintWidth 12 (col @Double "alpha" + col "beta" + col "gamma"))
    , -- A comparison/logical chain that fits stays free of grouping parens.
      golden
        "boolean chain fits: no paren noise"
        "a + b .>=. c .&&. a .>. b"
        (prettyPrintWidth 80 ((a + b .>=. c) .&&. (a .>. b)))
    , -- When a nested operand wraps, parens delimit it; flat operands stay bare.
      golden
        "wrapped operand gets parens"
        "(a + b + c\n  .>=. 0.0)\n  .&&. a .>. b"
        (prettyPrintWidth 16 ((a + b + c .>=. lit 0.0) .&&. (a .>. b)))
    ]
