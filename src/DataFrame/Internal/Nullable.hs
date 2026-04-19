{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

{- | Nullable-aware binary operations for expressions.

This module provides two type classes, 'NullableArithOp' and 'NullableCmpOp',
which enable operators like '.+', '.-', '.*', './', '.==' etc. to work
transparently across combinations of nullable (@Maybe a@) and non-nullable
(@a@) column types.

The partial functional dependencies uniquely determine the result type from
the operand types, so GHC infers it without annotations.

The four combinations covered for each class:

* @(a, a)@               — non-nullable × non-nullable
* @(Maybe a, a)@         — nullable × non-nullable
* @(a, Maybe a)@         — non-nullable × nullable
* @(Maybe a, Maybe a)@   — both nullable

== Usage

@
-- Mixing nullable and non-nullable columns:
F.col \@Int \"x\" '.+' F.col \@(Maybe Int) \"y\"  -- :: Expr (Maybe Int)

-- Both non-nullable (existing behaviour preserved):
F.col \@Int \"x\" '.+' F.col \@Int \"y\"           -- :: Expr Int

-- Comparison with three-valued logic:
F.col \@(Maybe Int) \"x\" '.==' F.col \@Int \"y\"  -- :: Expr (Maybe Bool)
@
-}
module DataFrame.Internal.Nullable (
    -- * Type family
    BaseType,

    -- * Arithmetic class
    NullableArithOp (..),

    -- * Comparison class
    NullableCmpOp (..),

    -- * Generalized nullable lift classes
    NullLift1Op (..),
    NullLift2Op (..),

    -- * Result-type type families (drive inference in nullLift / nullLift2)
    NullLift1Result,
    NullLift2Result,

    -- * Result-type type family for comparison operators
    NullCmpResult,

    -- * Numeric widening
    NumericWidenOp (..),
    widenArithOp,
    WidenResult,

    -- * Division widening (integral × integral → Double)
    DivWidenOp (..),
    divArithOp,
    WidenResultDiv,
) where

import Data.Int (Int32, Int64)
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Types (Promote, PromoteDiv)

{- | Strip one layer of 'Maybe'.

@
BaseType (Maybe a) = a
BaseType a         = a   -- for any non-Maybe type
@
-}
type family BaseType a where
    BaseType (Maybe a) = a
    BaseType a = a

{- | Class for arithmetic binary operations that work transparently over
nullable and non-nullable column types.

The functional dependency @a b -> c@ ensures GHC can infer the result type @c@
from the operand types. The 'OVERLAPPABLE' pragma on the non-nullable instance
ensures the more specific @(Maybe a, Maybe a)@ instance wins when both operands
are nullable.
-}
class
    ( Columnable a
    , Columnable b
    , Columnable c
    ) =>
    NullableArithOp a b c
        | a b -> c
    where
    {- | Lift an arithmetic function over the inner (non-Maybe) values.
    'Nothing' short-circuits: any 'Nothing' operand produces 'Nothing'.
    -}
    nullArithOp ::
        (BaseType a -> BaseType a -> BaseType a) ->
        a ->
        b ->
        c

{- | Compute the result type of a nullable comparison.

@
NullCmpResult (Maybe a) b = Maybe Bool
NullCmpResult a (Maybe b) = Maybe Bool   -- when a is apart from Maybe
NullCmpResult a b         = Bool
@

Used by the comparison operators ('.==', '.<', etc.) so GHC infers the
return type without an explicit annotation.
-}
type family NullCmpResult a b where
    NullCmpResult (Maybe a) b = Maybe Bool
    NullCmpResult a (Maybe b) = Maybe Bool
    NullCmpResult a b = Bool

{- | Class for comparison binary operations that work transparently over
nullable and non-nullable column types.

No functional dependency on @e@: the 'OVERLAPPING'\/'OVERLAPPABLE' pragmas on
instances disambiguate at call sites without a FundDep (which would conflict
when both operands are @Maybe@). GHC selects the unique most-specific instance
from the concrete operand types.
-}
class
    ( Columnable a
    , Columnable b
    , Columnable e
    ) =>
    NullableCmpOp a b e
    where
    {- | Lift a comparison function over the inner values (three-valued logic).
    Returns 'Nothing' when either operand is 'Nothing'.
    -}
    nullCmpOp ::
        (BaseType a -> BaseType a -> Bool) ->
        a ->
        b ->
        e

{- | Non-nullable × Non-nullable: apply directly, no wrapping.
Arithmetic result is @a@; comparison result is @Bool@.
-}
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, a ~ BaseType a) =>
    NullableArithOp a a a
    where
    nullArithOp f = f

instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable Bool, a ~ BaseType a) =>
    NullableCmpOp a a Bool
    where
    nullCmpOp f = f

-- | Nullable × Non-nullable: 'Nothing' short-circuits.
instance
    (Columnable a, Columnable (Maybe a)) =>
    NullableArithOp (Maybe a) a (Maybe a)
    where
    nullArithOp _f Nothing _ = Nothing
    nullArithOp f (Just x) y = Just (f x y)

instance
    (Columnable a, Columnable (Maybe a), Columnable (Maybe Bool)) =>
    NullableCmpOp (Maybe a) a (Maybe Bool)
    where
    nullCmpOp _f Nothing _ = Nothing
    nullCmpOp f (Just x) y = Just (f x y)

-- | Non-nullable × Nullable: 'Nothing' short-circuits.
instance
    ( Columnable a
    , Columnable (Maybe a)
    , a ~ BaseType a
    ) =>
    NullableArithOp a (Maybe a) (Maybe a)
    where
    nullArithOp _f _ Nothing = Nothing
    nullArithOp f x (Just y) = Just (f x y)

instance
    ( Columnable a
    , Columnable (Maybe a)
    , Columnable (Maybe Bool)
    , a ~ BaseType a
    ) =>
    NullableCmpOp a (Maybe a) (Maybe Bool)
    where
    nullCmpOp _f _ Nothing = Nothing
    nullCmpOp f x (Just y) = Just (f x y)

-- | Nullable × Nullable: either 'Nothing' short-circuits.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable (Maybe a)) =>
    NullableArithOp (Maybe a) (Maybe a) (Maybe a)
    where
    nullArithOp _f Nothing _ = Nothing
    nullArithOp _f _ Nothing = Nothing
    nullArithOp f (Just x) (Just y) = Just (f x y)

instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable (Maybe a), Columnable (Maybe Bool)) =>
    NullableCmpOp (Maybe a) (Maybe a) (Maybe Bool)
    where
    nullCmpOp _f Nothing _ = Nothing
    nullCmpOp _f _ Nothing = Nothing
    nullCmpOp f (Just x) (Just y) = Just (f x y)

-- ---------------------------------------------------------------------------
-- Generalized nullable lift (unary)
-- ---------------------------------------------------------------------------

{- | Lift a unary function over a column expression, propagating 'Nothing'.

When @a@ is non-nullable the function is applied directly; when @a = Maybe x@
the function is applied under the 'Just' and 'Nothing' short-circuits.

Use via 'DataFrame.Functions.nullLift'.
-}

{- | Compute the result type of a nullable unary lift.

@
NullLift1Result (Maybe a) r = Maybe r
NullLift1Result a         r = r        -- for any non-Maybe a
@

Used by 'DataFrame.Functions.nullLift' so GHC can infer the return type
without an explicit annotation.
-}
type family NullLift1Result a r where
    NullLift1Result (Maybe a) r = Maybe r
    NullLift1Result a r = r

class
    ( Columnable a
    , Columnable r
    , Columnable c
    ) =>
    NullLift1Op a r c
    where
    applyNull1 :: (BaseType a -> r) -> a -> c

-- | Non-nullable: apply directly.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable r, a ~ BaseType a) =>
    NullLift1Op a r r
    where
    applyNull1 f = f

-- | Nullable: propagate 'Nothing'.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable r, Columnable (Maybe r)) =>
    NullLift1Op (Maybe a) r (Maybe r)
    where
    applyNull1 _ Nothing = Nothing
    applyNull1 f (Just x) = Just (f x)

-- ---------------------------------------------------------------------------
-- Generalized nullable lift (binary)
-- ---------------------------------------------------------------------------

{- | Lift a binary function over two column expressions, propagating 'Nothing'.

The four combinations:

* @(a, b)@               — both non-nullable: result is @r@
* @(Maybe a, b)@         — left nullable: result is @Maybe r@
* @(a, Maybe b)@         — right nullable: result is @Maybe r@
* @(Maybe a, Maybe b)@   — both nullable: result is @Maybe r@

Use via 'DataFrame.Functions.nullLift2'.
-}

{- | Compute the result type of a nullable binary lift.

@
NullLift2Result (Maybe a) b         r = Maybe r
NullLift2Result a         (Maybe b) r = Maybe r   -- when a is apart from Maybe
NullLift2Result a         b         r = r
@

Used by 'DataFrame.Functions.nullLift2' so GHC can infer the return type.
-}
type family NullLift2Result a b r where
    NullLift2Result (Maybe a) b r = Maybe r
    NullLift2Result a (Maybe b) r = Maybe r
    NullLift2Result a b r = r

class
    ( Columnable a
    , Columnable b
    , Columnable r
    , Columnable c
    ) =>
    NullLift2Op a b r c
    where
    applyNull2 :: (BaseType a -> BaseType b -> r) -> a -> b -> c

-- | Both non-nullable: apply directly.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, a ~ BaseType a, b ~ BaseType b) =>
    NullLift2Op a b r r
    where
    applyNull2 f = f

-- | Left nullable: 'Nothing' short-circuits.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r), b ~ BaseType b) =>
    NullLift2Op (Maybe a) b r (Maybe r)
    where
    applyNull2 _ Nothing _ = Nothing
    applyNull2 f (Just x) y = Just (f x y)

-- | Right nullable: 'Nothing' short-circuits.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r), a ~ BaseType a) =>
    NullLift2Op a (Maybe b) r (Maybe r)
    where
    applyNull2 _ _ Nothing = Nothing
    applyNull2 f x (Just y) = Just (f x y)

-- | Both nullable: either 'Nothing' short-circuits.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r)) =>
    NullLift2Op (Maybe a) (Maybe b) r (Maybe r)
    where
    applyNull2 _ Nothing _ = Nothing
    applyNull2 _ _ Nothing = Nothing
    applyNull2 f (Just x) (Just y) = Just (f x y)

-- ---------------------------------------------------------------------------
-- Numeric widening
-- ---------------------------------------------------------------------------

{- | Widen two numeric base types to their promoted common type.

When @a ~ b@ the coercions are identity; otherwise one operand is widened
(e.g. 'Int' → 'Double').
-}
class (Columnable (Promote a b)) => NumericWidenOp a b where
    widen1 :: a -> Promote a b
    widen2 :: b -> Promote a b

-- | Same type: identity coercions.
instance {-# OVERLAPPING #-} (Columnable a) => NumericWidenOp a a where
    widen1 = id
    widen2 = id

instance NumericWidenOp Int Double where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Double Int where
    widen1 = id
    widen2 = fromIntegral
instance NumericWidenOp Float Double where widen1 = realToFrac; widen2 = id
instance NumericWidenOp Double Float where
    widen1 = id
    widen2 = realToFrac
instance NumericWidenOp Int Float where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Float Int where
    widen1 = id
    widen2 = fromIntegral

-- | Apply an arithmetic function after widening both operands to their common type.
widenArithOp ::
    forall a b.
    (NumericWidenOp a b) =>
    (Promote a b -> Promote a b -> Promote a b) ->
    a ->
    b ->
    Promote a b
widenArithOp f x y = f (widen1 @a @b x) (widen2 @a @b y)

-- | Result type of a widening binary operator, accounting for nullable wrappers.
type WidenResult a b = NullLift2Result a b (Promote (BaseType a) (BaseType b))

-- ---------------------------------------------------------------------------
-- Division widening (integral × integral → Double)
-- ---------------------------------------------------------------------------

{- | Like 'NumericWidenOp' but uses 'PromoteDiv': integral×integral → Double.
Floating types still dominate (Double > Float), and any two integral types
(same or mixed) are both widened to Double.
-}
class (Columnable (PromoteDiv a b)) => DivWidenOp a b where
    divWiden1 :: a -> PromoteDiv a b
    divWiden2 :: b -> PromoteDiv a b

-- Floating same-type (identity)
instance DivWidenOp Double Double where divWiden1 = id; divWiden2 = id
instance DivWidenOp Float Float where divWiden1 = id; divWiden2 = id

-- Mixed Double/Float
instance DivWidenOp Double Float where divWiden1 = id; divWiden2 = realToFrac
instance DivWidenOp Float Double where divWiden1 = realToFrac; divWiden2 = id

-- Double beats integral
instance DivWidenOp Double Int where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int Double where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Double Int32 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int32 Double where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Double Int64 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int64 Double where divWiden1 = fromIntegral; divWiden2 = id

-- Float beats integral
instance DivWidenOp Float Int where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int Float where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Float Int32 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int32 Float where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Float Int64 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int64 Float where divWiden1 = fromIntegral; divWiden2 = id

-- Integral × integral → Double
instance DivWidenOp Int Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral

-- | Apply an arithmetic function after widening both operands via 'PromoteDiv'.
divArithOp ::
    forall a b.
    (DivWidenOp a b) =>
    (PromoteDiv a b -> PromoteDiv a b -> PromoteDiv a b) ->
    a ->
    b ->
    PromoteDiv a b
divArithOp f x y = f (divWiden1 @a @b x) (divWiden2 @a @b y)

-- | Result type of a division-widening binary operator, accounting for nullable wrappers.
type WidenResultDiv a b =
    NullLift2Result a b (PromoteDiv (BaseType a) (BaseType b))
