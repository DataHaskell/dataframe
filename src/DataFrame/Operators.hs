{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Operators where

import Data.Function ((&))
import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    BinaryOp (
        MkBinaryOp,
        binaryCommutative,
        binaryFn,
        binaryName,
        binaryPrecedence,
        binarySymbol
    ),
    Expr (Binary, Lit),
    NamedExpr,
    UExpr (UExpr),
 )

infix 8 .^^
infix 4 .==, .<, .<=, .>=, .>, ./=
infixr 3 .&&
infixr 2 .||
infixr 0 .=

(|>) :: a -> (a -> b) -> b
(|>) = (&)

as :: (Columnable a) => Expr a -> T.Text -> NamedExpr
as expr name = (name, UExpr expr)

(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

(.==) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(.==) =
    Binary
        ( MkBinaryOp
            { binaryFn = (==)
            , binaryName = "eq"
            , binarySymbol = Just "=="
            , binaryCommutative = True
            , binaryPrecedence = 4
            }
        )

(./=) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(./=) =
    Binary
        ( MkBinaryOp
            { binaryFn = (/=)
            , binaryName = "neq"
            , binarySymbol = Just "/="
            , binaryCommutative = True
            , binaryPrecedence = 4
            }
        )

(.<) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.<) =
    Binary
        ( MkBinaryOp
            { binaryFn = (<)
            , binaryName = "lt"
            , binarySymbol = Just "<"
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

(.>) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.>) =
    Binary
        ( MkBinaryOp
            { binaryFn = (>)
            , binaryName = "gt"
            , binarySymbol = Just ">"
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

(.<=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.<=) =
    Binary
        ( MkBinaryOp
            { binaryFn = (<=)
            , binaryName = "leq"
            , binarySymbol = Just "<="
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

(.>=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.>=) =
    Binary
        ( MkBinaryOp
            { binaryFn = (>=)
            , binaryName = "geq"
            , binarySymbol = Just ">="
            , binaryCommutative = False
            , binaryPrecedence = 4
            }
        )

(.&&) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&) =
    Binary
        ( MkBinaryOp
            { binaryFn = (&&)
            , binaryName = "and"
            , binarySymbol = Just "&&"
            , binaryCommutative = True
            , binaryPrecedence = 3
            }
        )

(.||) :: Expr Bool -> Expr Bool -> Expr Bool
(.||) =
    Binary
        ( MkBinaryOp
            { binaryFn = (||)
            , binaryName = "or"
            , binarySymbol = Just "||"
            , binaryCommutative = True
            , binaryPrecedence = 2
            }
        )

(.^^) :: (Columnable a, Num a) => Expr a -> Int -> Expr a
(.^^) expr i =
    Binary
        ( MkBinaryOp
            { binaryFn = (^)
            , binaryName = "pow"
            , binarySymbol = Just "^"
            , binaryCommutative = False
            , binaryPrecedence = 8
            }
        )
        expr
        (Lit i)
