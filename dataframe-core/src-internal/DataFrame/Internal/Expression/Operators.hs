{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Internal.Expression.Operators where

import Data.Function ((&))
import Data.Int (Int32, Int64)
import qualified Data.Text as T
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Column.Types (Promote, PromoteDiv)
import DataFrame.Internal.Expression (
    BinOpId (..),
    BinUDF (MkBinaryOp),
    BinaryOp (
        binaryCommutative,
        binaryFn,
        binaryId,
        binaryPrecedence,
        binarySymbol
    ),
    Expr (Binary, Col, If, Lit, Unary),
    NamedExpr,
    OpRef (..),
    SomeExpr (SomeExpr),
    UnOpId,
    UnUDF (MkUnaryOp),
 )
import DataFrame.Internal.Expression.Operators.Nullable (
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
as expr colName = (colName, SomeExpr expr)

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

-- | Column reference of type Int; same as `col @Int`.
int :: T.Text -> Expr Int
int = Col

-- | Column reference of type Integer; same as `col @Integer`.
integer :: T.Text -> Expr Integer
integer = Col

-- | Column reference of type Double; same as `col @Double`.
double :: T.Text -> Expr Double
double = Col

-- | Column reference of type Text; same as `col @T.Text`.
text :: T.Text -> Expr T.Text
text = Col

-- | Column reference of type Bool; same as `col @Bool`.
bool :: T.Text -> Expr Bool
bool = Col

-- | Nullable column reference of type Maybe Int.
maybeInt :: T.Text -> Expr (Maybe Int)
maybeInt = Col

-- | Nullable column reference of type Maybe Double.
maybeDouble :: T.Text -> Expr (Maybe Double)
maybeDouble = Col

-- | Nullable column reference of type Maybe Text.
maybeText :: T.Text -> Expr (Maybe T.Text)
maybeText = Col

-- | Nullable column reference of type Maybe Bool.
maybeBool :: T.Text -> Expr (Maybe Bool)
maybeBool = Col

-- | Column reference of type Int32.
int32 :: T.Text -> Expr Int32
int32 = Col

-- | Column reference of type Int64.
int64 :: T.Text -> Expr Int64
int64 = Col

-- | Column reference of type Float (32-bit).
float32 :: T.Text -> Expr Float
float32 = Col

-- | Column reference of type Double (64-bit).
float64 :: T.Text -> Expr Double
float64 = Col

-- | Nullable column reference of type Maybe Int32.
maybeInt32 :: T.Text -> Expr (Maybe Int32)
maybeInt32 = Col

-- | Nullable column reference of type Maybe Int64.
maybeInt64 :: T.Text -> Expr (Maybe Int64)
maybeInt64 = Col

-- | Nullable column reference of type Maybe Float (32-bit).
maybeFloat32 :: T.Text -> Expr (Maybe Float)
maybeFloat32 = Col

-- | Nullable column reference of type Maybe Double (64-bit).
maybeFloat64 :: T.Text -> Expr (Maybe Double)
maybeFloat64 = Col

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

caseWhen :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
caseWhen = If

-- | Closes a 'caseWhen' chain with its fallback value.
orElse :: Expr a -> Expr a
orElse = id

lit :: (Columnable a) => a -> Expr a
lit = Lit

(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

-- | A user op; equal names must mean equal functions. Never hits builtins.
liftDecorated ::
    (Columnable a, Columnable b) =>
    (a -> b) -> T.Text -> Maybe T.Text -> Expr a -> Expr b
liftDecorated f opName rep = Unary (MkUnaryOp f (Named opName) rep)

liftBuiltin ::
    (Columnable a, Columnable b) =>
    UnOpId -> (a -> b) -> Maybe T.Text -> Expr a -> Expr b
liftBuiltin op f rep = Unary (MkUnaryOp f (Builtin op) rep)

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
    Binary (MkBinaryOp f (Named opName) rep comm prec)

lift2Builtin ::
    (Columnable c, Columnable b, Columnable a) =>
    BinOpId ->
    (c -> b -> a) ->
    Maybe T.Text ->
    Bool ->
    Int ->
    Expr c ->
    Expr b ->
    Expr a
lift2Builtin op f rep comm prec = Binary (MkBinaryOp f (Builtin op) rep comm prec)

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
    binaryId NullEq = Builtin BEq
    binarySymbol NullEq = Just ".=="
    binaryCommutative NullEq = True
    binaryPrecedence NullEq = 4
instance BinaryOp NullNeq where
    binaryFn NullNeq = applyNull2 (widenCmpOp (/=))
    binaryId NullNeq = Builtin BNeq
    binarySymbol NullNeq = Just "./="
    binaryCommutative NullNeq = True
    binaryPrecedence NullNeq = 4
instance BinaryOp NullLt where
    binaryFn NullLt = applyNull2 (widenCmpOp (<))
    binaryId NullLt = Builtin BLt
    binarySymbol NullLt = Just ".<"
    binaryPrecedence NullLt = 4
instance BinaryOp NullGt where
    binaryFn NullGt = applyNull2 (widenCmpOp (>))
    binaryId NullGt = Builtin BGt
    binarySymbol NullGt = Just ".>"
    binaryPrecedence NullGt = 4
instance BinaryOp NullLeq where
    binaryFn NullLeq = applyNull2 (widenCmpOp (<=))
    binaryId NullLeq = Builtin BLeq
    binarySymbol NullLeq = Just ".<="
    binaryPrecedence NullLeq = 4
instance BinaryOp NullGeq where
    binaryFn NullGeq = applyNull2 (widenCmpOp (>=))
    binaryId NullGeq = Builtin BGeq
    binarySymbol NullGeq = Just ".>="
    binaryPrecedence NullGeq = 4
instance BinaryOp NullAnd where
    binaryFn NullAnd = nullCmpOp (&&)
    binaryId NullAnd = Builtin BNullAnd
    binarySymbol NullAnd = Just ".&&"
    binaryCommutative NullAnd = True
    binaryPrecedence NullAnd = 3
instance BinaryOp NullOr where
    binaryFn NullOr = nullCmpOp (||)
    binaryId NullOr = Builtin BNullOr
    binarySymbol NullOr = Just ".||"
    binaryCommutative NullOr = True
    binaryPrecedence NullOr = 2

(.==.) ::
    (Columnable a, Eq a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.==.) = lift2Builtin BEq (==) (Just ".==.") True 4

(./=.) ::
    (Columnable a, Eq a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(./=.) = lift2Builtin BNeq (/=) (Just "./=.") True 4

(.<.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.<.) = lift2Builtin BLt (<) (Just ".<.") False 4

(.>.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.>.) = lift2Builtin BGt (>) (Just ".>.") False 4

(.<=.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.<=.) = lift2Builtin BLeq (<=) (Just ".<=.") False 4

(.>=.) ::
    (Columnable a, Ord a) =>
    Expr a ->
    Expr a ->
    Expr Bool
(.>=.) = lift2Builtin BGeq (>=) (Just ".>=.") False 4

(.+.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.+.) = (+)

(.-.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.-.) = (-)

(.*.) :: (Columnable a, Num a) => Expr a -> Expr a -> Expr a
(.*.) = (*)

(./.) :: (Columnable a, Fractional a) => Expr a -> Expr a -> Expr a
(./.) = (/)

-- Nullable-aware arithmetic operators

-- | Nullable-aware addition for nullable and non-nullable operands.
(.+) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.+) = lift2Builtin BNullAdd (applyNull2 (widenArithOp (+))) (Just ".+") True 6

-- | Nullable-aware subtraction.
(.-) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.-) = lift2Builtin BNullSub (applyNull2 (widenArithOp (-))) (Just ".-") False 6

-- | Nullable-aware multiplication.
(.*) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (Promote (BaseType a) (BaseType b)) (WidenResult a b)
    , Num (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResult a b)
(.*) = lift2Builtin BNullMul (applyNull2 (widenArithOp (*))) (Just ".*") True 7

-- | Nullable-aware division. Integral operands are promoted to Double.
(./) ::
    ( DivWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b (PromoteDiv (BaseType a) (BaseType b)) (WidenResultDiv a b)
    , Fractional (PromoteDiv (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (WidenResultDiv a b)
(./) = lift2Builtin BNullDiv (applyNull2 (divArithOp (/))) (Just "./") False 7

-- Three-valued comparisons: Nothing if either operand is Nothing.

-- | Nullable-aware equality that widens numeric operands to a common type.
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

-- | Nullable-aware less-than-or-equal that widens numeric operands.
(.<=) ::
    ( NumericWidenOp (BaseType a) (BaseType b)
    , NullLift2Op a b Bool (NullCmpResult a b)
    , Ord (Promote (BaseType a) (BaseType b))
    ) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.<=) = Binary NullLeq

-- | Nullable-aware greater-than-or-equal that widens numeric operands.
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
(.&&.) = lift2Builtin BAnd (&&) (Just ".&&.") True 3

(.||.) :: Expr Bool -> Expr Bool -> Expr Bool
(.||.) = lift2Builtin BOr (||) (Just ".||.") True 2

-- | Nullable-aware logical AND, giving @Maybe Bool@ if either side is nullable.
(.&&) ::
    (NullableCmpOp a b (NullCmpResult a b), BaseType a ~ Bool) =>
    Expr a ->
    Expr b ->
    Expr (NullCmpResult a b)
(.&&) = Binary NullAnd

-- | Nullable-aware logical OR, giving @Maybe Bool@ if either side is nullable.
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
(.^^) = lift2Builtin BPowFrac (applyNull2 (^^)) (Just ".^^") False 8

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
(.^) = lift2Builtin BPow (applyNull2 (^)) (Just ".^") False 8

-- Same-type (non-nullable) exponentiation operators

(.^^.) ::
    (Columnable a, Columnable b, Fractional a, Integral b) =>
    Expr a -> Expr b -> Expr a
(.^^.) = lift2Builtin BPowFrac (^^) (Just ".^^.") False 8

(.^.) ::
    (Columnable a, Columnable b, Num a, Integral b) =>
    Expr a -> Expr b -> Expr a
(.^.) = lift2Builtin BPow (^) (Just ".^.") False 8
