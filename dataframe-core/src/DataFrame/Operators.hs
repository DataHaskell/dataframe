{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Operators where

import Data.Function ((&))
import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    BinUDF (MkBinaryOp),
    BinaryOp (
        binaryCommutative,
        binaryFn,
        binaryName,
        binaryPrecedence,
        binarySymbol
    ),
    Expr (Binary, Col, If, Lit, Unary),
    NamedExpr,
    UExpr (UExpr),
    UnUDF (MkUnaryOp),
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
    widenCmpOp,
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

col :: forall a -> (Columnable a) => T.Text -> Expr a
col _ = Col

col' :: forall a. (Columnable a) => T.Text -> Expr a
col' = Col

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

liftDecorated ::
    (Columnable a, Columnable b) =>
    (a -> b) -> T.Text -> Maybe T.Text -> Expr a -> Expr b
liftDecorated f opName rep = Unary (MkUnaryOp f opName rep)

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
    Binary (MkBinaryOp f opName rep comm prec)

data NullEq a b c where
    NullEq ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Eq (Promote (BaseType a) (BaseType b))
        ) =>
        NullEq a b (NullCmpResult a b)

data NullNeq a b c where
    NullNeq ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Eq (Promote (BaseType a) (BaseType b))
        ) =>
        NullNeq a b (NullCmpResult a b)

data NullLt a b c where
    NullLt ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Ord (Promote (BaseType a) (BaseType b))
        ) =>
        NullLt a b (NullCmpResult a b)

data NullGt a b c where
    NullGt ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Ord (Promote (BaseType a) (BaseType b))
        ) =>
        NullGt a b (NullCmpResult a b)

data NullLeq a b c where
    NullLeq ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Ord (Promote (BaseType a) (BaseType b))
        ) =>
        NullLeq a b (NullCmpResult a b)

data NullGeq a b c where
    NullGeq ::
        ( NumericWidenOp (BaseType a) (BaseType b)
        , NullLift2Op a b Bool (NullCmpResult a b)
        , Ord (Promote (BaseType a) (BaseType b))
        ) =>
        NullGeq a b (NullCmpResult a b)

data NullAnd a b c where
    NullAnd ::
        (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
        NullAnd a b (NullCmpResult a b)

data NullOr a b c where
    NullOr ::
        (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
        NullOr a b (NullCmpResult a b)

instance BinaryOp NullEq where
    binaryFn NullEq = applyNull2 (widenCmpOp (==))
    binaryName NullEq = "eq"
    binarySymbol NullEq = Just ".=="
    binaryCommutative NullEq = True
    binaryPrecedence NullEq = 4
instance BinaryOp NullNeq where
    binaryFn NullNeq = applyNull2 (widenCmpOp (/=))
    binaryName NullNeq = "neq"
    binarySymbol NullNeq = Just "./="
    binaryCommutative NullNeq = True
    binaryPrecedence NullNeq = 4
instance BinaryOp NullLt where
    binaryFn NullLt = applyNull2 (widenCmpOp (<))
    binaryName NullLt = "lt"
    binarySymbol NullLt = Just ".<"
    binaryPrecedence NullLt = 4
instance BinaryOp NullGt where
    binaryFn NullGt = applyNull2 (widenCmpOp (>))
    binaryName NullGt = "gt"
    binarySymbol NullGt = Just ".>"
    binaryPrecedence NullGt = 4
instance BinaryOp NullLeq where
    binaryFn NullLeq = applyNull2 (widenCmpOp (<=))
    binaryName NullLeq = "leq"
    binarySymbol NullLeq = Just ".<="
    binaryPrecedence NullLeq = 4
instance BinaryOp NullGeq where
    binaryFn NullGeq = applyNull2 (widenCmpOp (>=))
    binaryName NullGeq = "geq"
    binarySymbol NullGeq = Just ".>="
    binaryPrecedence NullGeq = 4
instance BinaryOp NullAnd where
    binaryFn NullAnd = nullCmpOp (&&)
    binaryName NullAnd = "nulland"
    binarySymbol NullAnd = Just ".&&"
    binaryCommutative NullAnd = True
    binaryPrecedence NullAnd = 3
instance BinaryOp NullOr where
    binaryFn NullOr = nullCmpOp (||)
    binaryName NullOr = "nullor"
    binarySymbol NullOr = Just ".||"
    binaryCommutative NullOr = True
    binaryPrecedence NullOr = 2

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

{- | Nullable-aware equality. Widens numeric operands to their common type,
so @Expr Double .== Expr Int@ typechecks. Returns @Maybe Bool@ when either
operand is nullable.
-}
(.==) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Eq (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.==) = Binary NullEq

-- | Nullable-aware inequality. Widens numeric operands to their common type.
(./=) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Eq (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(./=) = Binary NullNeq

-- | Nullable-aware less-than. Widens numeric operands to their common type.
(.<) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Ord (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<) = Binary NullLt

-- | Nullable-aware greater-than. Widens numeric operands to their common type.
(.>) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Ord (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>) = Binary NullGt

{- | Nullable-aware less-than-or-equal. Widens numeric operands to their
common type, so @Expr Double .<= Expr Int@ typechecks.
-}
(.<=) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Ord (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<=) = Binary NullLeq

-- | Nullable-aware greater-than-or-equal. Widens numeric operands to their common type.
(.>=) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Ord (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.>=) = Binary NullGeq

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
(.&&) = Binary NullAnd

-- | Nullable-aware logical OR. Returns @Maybe Bool@ when either operand is nullable.
(.||) ::
    (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.||) = Binary NullOr

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
