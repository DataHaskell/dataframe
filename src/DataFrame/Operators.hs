{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

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
    Expr (Binary, Col, If, Lit, Unary),
    NamedExpr,
    UExpr (UExpr),
    UnaryOp (MkUnaryOp, unaryFn, unaryName, unarySymbol),
 )
import DataFrame.Internal.Nullable (
    BaseType,
    DivWidenOp,
    NullCmpResult,
    NullLift2Op (applyNull2),
    NullableCmpOp (nullCmpOp),
    NumericWidenOp,
    WidenResult,
    WidenResultDiv,
    divArithOp,
    widenArithOp,
 )
import DataFrame.Internal.Types (Promote, PromoteDiv)

infixr 8 .^^, .^^., .^, .^.
infixl 7 .*, ./, .*., ./.
infixl 6 .+, .-, .+., .-.
infix 4 .==, .==., .<, .<., .<=, .<=., .>=, .>=., .>, .>., ./=, ./=.
infixr 3 .&&, .&&.
infixr 2 .||, .||.
infixr 0 .=

(|>) :: a -> (a -> b) -> b
(|>) = (&)

as :: (Columnable a) => Expr a -> T.Text -> NamedExpr
as expr colName = (colName, UExpr expr)

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

liftDecorated ::
    (Columnable a, Columnable b) =>
    (a -> b) -> T.Text -> Maybe T.Text -> Expr a -> Expr b
liftDecorated f opName rep = Unary (MkUnaryOp{unaryFn = f, unaryName = opName, unarySymbol = rep})

lift2Decorated ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) ->
    T.Text ->
    Maybe T.Text ->
    Bool ->
    Int ->
    Expr c ->
    Expr b ->
    Expr a
lift2Decorated f opName rep comm prec =
    Binary
        ( MkBinaryOp
            { binaryFn = f
            , binaryName = opName
            , binarySymbol = rep
            , binaryCommutative = comm
            , binaryPrecedence = prec
            }
        )

(.==.) ::
    (Columnable a, Eq a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.==.) = lift2Decorated (==) "eq" (Just ".==.") True 4

(./=.) ::
    (Columnable a, Eq a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(./=.) = lift2Decorated (/=) "neq" (Just "./=.") True 4

(.<.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.<.) = lift2Decorated (<) "lt" (Just ".<.") False 4

(.>.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.>.) = lift2Decorated (>) "gt" (Just ".>.") False 4

(.<=.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.<=.) = lift2Decorated (<=) "leq" (Just ".<=.") False 4

(.>=.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.>=.) = lift2Decorated (>=) "geq" (Just ".>=.") False 4

(.+.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.+.) = (+)

(.-.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.-.) = (-)

(.*.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.*.) = (*)

(./.) :: (Columnable a, Fractional a) => Expr a -> Expr a -> Expr a
(./.) = (/)

-- Nullable-aware arithmetic operators

{- | Nullable-aware addition. Works for all combinations of nullable\/non-nullable operands.
@col \@Int "x" .+ col \@(Maybe Int) "y"  -- :: Expr (Maybe Int)@
-}
(.+) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.+) = lift2Decorated (applyNull2 (widenArithOp (+))) "nulladd" (Just ".+") True 6

-- | Nullable-aware subtraction.
(.-) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.-) = lift2Decorated (applyNull2 (widenArithOp (-))) "nullsub" (Just ".-") False 6

-- | Nullable-aware multiplication.
(.*) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.*) = lift2Decorated (applyNull2 (widenArithOp (*))) "nullmul" (Just ".*") True 7

-- | Nullable-aware division. Integral operands are promoted to Double.
(./) ::
    ( DivWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (PromoteDiv (BaseType a) (BaseType b)) (WidenResultDiv a b)
    , Fractional (PromoteDiv (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResultDiv a b)
(./) = lift2Decorated (applyNull2 (divArithOp (/))) "nulldiv" (Just "./") False 7

-- Nullable-aware comparison operators (three-valued logic: Nothing if either operand is Nothing)

-- | Nullable-aware equality. Returns @Maybe Bool@ when either operand is nullable.
(.==) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.==) = lift2Decorated (nullCmpOp (==)) "eq" (Just ".==") True 4

-- | Nullable-aware inequality.
(./=) ::
    (NullableCmpOp a b (NullCmpResult a b), Eq (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(./=) = lift2Decorated (nullCmpOp (/=)) "neq" (Just "./=") True 4

-- | Nullable-aware less-than.
(.<) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<) = lift2Decorated (nullCmpOp (<)) "lt" (Just ".<") False 4

-- | Nullable-aware greater-than.
(.>) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>) = lift2Decorated (nullCmpOp (>)) "gt" (Just ".>") False 4

-- | Nullable-aware less-than-or-equal.
(.<=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<=) = lift2Decorated (nullCmpOp (<=)) "leq" (Just ".<=") False 4

-- | Nullable-aware greater-than-or-equal.
(.>=) ::
    (NullableCmpOp a b (NullCmpResult a b), Ord (BaseType a)) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>=) = lift2Decorated (nullCmpOp (>=)) "geq" (Just ".>=") False 4

(.&&.) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&.) = lift2Decorated (&&) "and" (Just ".&&.") True 3

(.||.) :: Expr Bool -> Expr Bool -> Expr Bool
(.||.) = lift2Decorated (||) "or" (Just ".||.") True 2

-- | Nullable-aware logical AND. Returns @Maybe Bool@ when either operand is nullable.
(.&&) ::
    (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.&&) = lift2Decorated (nullCmpOp (&&)) "nulland" (Just ".&&") True 3

-- | Nullable-aware logical OR. Returns @Maybe Bool@ when either operand is nullable.
(.||) ::
    (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.||) = lift2Decorated (nullCmpOp (||)) "nullor" (Just ".||") True 2

(.^^) ::
    ( Columnable (BaseType a)
    , Columnable (BaseType b)
    , Fractional (BaseType a)
    , Integral (BaseType b)
    , NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (BaseType a) a
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a -> Expr b -> Expr a
(.^^) = lift2Decorated (applyNull2 (^^)) "pow" (Just ".^^") False 8

(.^) ::
    ( Columnable (BaseType a)
    , Columnable (BaseType b)
    , Num (BaseType a)
    , Integral (BaseType b)
    , NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (BaseType a) a
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a -> Expr b -> Expr a
(.^) = lift2Decorated (applyNull2 (^)) "pow" (Just ".^") False 8

-- Same-type (non-nullable) exponentiation operators

(.^^.) ::
    (Columnable a, Columnable b, Fractional a, Integral b) =>
    Expr a -> Expr b -> Expr a
(.^^.) = lift2Decorated (^^) "pow" (Just ".^^.") False 8

(.^.) ::
    (Columnable a, Columnable b, Num a, Integral b) =>
    Expr a -> Expr b -> Expr a
(.^.) = lift2Decorated (^) "pow" (Just ".^.") False 8
