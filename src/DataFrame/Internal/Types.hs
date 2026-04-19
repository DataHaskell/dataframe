{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Types where

import Data.Int (Int16, Int32, Int64, Int8)
import Data.Kind (Constraint, Type)
import Data.Typeable (Typeable)
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)

type Columnable' a = (Typeable a, Show a, Eq a)

{- | A type with column representations used to select the
"right" representation when specializing the `toColumn` function.
-}
data Rep
    = RBoxed
    | RUnboxed
    | RNullableBoxed

-- | Type-level if statement.
type family If (cond :: Bool) (yes :: k) (no :: k) :: k where
    If 'True yes _ = yes
    If 'False _ no = no

-- | All unboxable types (according to the `vector` package).
type family Unboxable (a :: Type) :: Bool where
    Unboxable Int = 'True
    Unboxable Int8 = 'True
    Unboxable Int16 = 'True
    Unboxable Int32 = 'True
    Unboxable Int64 = 'True
    Unboxable Word = 'True
    Unboxable Word8 = 'True
    Unboxable Word16 = 'True
    Unboxable Word32 = 'True
    Unboxable Word64 = 'True
    Unboxable Char = 'True
    Unboxable Bool = 'True
    Unboxable Double = 'True
    Unboxable Float = 'True
    Unboxable _ = 'False

type family Numeric (a :: Type) :: Bool where
    Numeric Integer = 'True
    Numeric Int = 'True
    Numeric Int8 = 'True
    Numeric Int16 = 'True
    Numeric Int32 = 'True
    Numeric Int64 = 'True
    Numeric Word = 'True
    Numeric Word8 = 'True
    Numeric Word16 = 'True
    Numeric Word32 = 'True
    Numeric Word64 = 'True
    Numeric Double = 'True
    Numeric Float = 'True
    Numeric _ = 'False

-- | Compute the column representation tag for any 'a'.
type family KindOf a :: Rep where
    KindOf (Maybe a) = 'RNullableBoxed
    KindOf a = If (Unboxable a) 'RUnboxed 'RBoxed

-- | Type-level boolean for constraint/type comparison.
data SBool (b :: Bool) where
    STrue :: SBool 'True
    SFalse :: SBool 'False

-- | The runtime witness for our type-level branching.
class SBoolI (b :: Bool) where
    sbool :: SBool b

instance SBoolI 'True where sbool = STrue
instance SBoolI 'False where sbool = SFalse

-- | Type-level function to determine whether or not a type is unboxa
sUnbox :: forall a. (SBoolI (Unboxable a)) => SBool (Unboxable a)
sUnbox = sbool @(Unboxable a)

sNumeric :: forall a. (SBoolI (Numeric a)) => SBool (Numeric a)
sNumeric = sbool @(Numeric a)

type family When (flag :: Bool) (c :: Constraint) :: Constraint where
    When 'True c = c
    When 'False c = () -- empty constraint

type UnboxIf a = When (Unboxable a) (VU.Unbox a)

type family IntegralTypes (a :: Type) :: Bool where
    IntegralTypes Integer = 'True
    IntegralTypes Int = 'True
    IntegralTypes Int8 = 'True
    IntegralTypes Int16 = 'True
    IntegralTypes Int32 = 'True
    IntegralTypes Int64 = 'True
    IntegralTypes Word = 'True
    IntegralTypes Word8 = 'True
    IntegralTypes Word16 = 'True
    IntegralTypes Word32 = 'True
    IntegralTypes Word64 = 'True
    IntegralTypes _ = 'False

sIntegral :: forall a. (SBoolI (IntegralTypes a)) => SBool (IntegralTypes a)
sIntegral = sbool @(IntegralTypes a)

type IntegralIf a = When (IntegralTypes a) (Integral a)

type family FloatingTypes (a :: Type) :: Bool where
    FloatingTypes Float = 'True
    FloatingTypes Double = 'True
    FloatingTypes _ = 'False

sFloating :: forall a. (SBoolI (FloatingTypes a)) => SBool (FloatingTypes a)
sFloating = sbool @(FloatingTypes a)

type FloatingIf a = When (FloatingTypes a) (Real a, Fractional a)

{- | Numeric type promotion: resolves the common type for mixed arithmetic.
Double dominates over Float/Int; Float dominates over Int; same types stay unchanged.
-}
type family Promote (a :: Type) (b :: Type) :: Type where
    Promote a a = a
    Promote Double _ = Double
    Promote _ Double = Double
    Promote Float _ = Float
    Promote _ Float = Float
    Promote Int64 _ = Int64
    Promote _ Int64 = Int64
    Promote Int32 _ = Int32
    Promote _ Int32 = Int32
    Promote a _ = a

{- | Like 'Promote', but integral × integral → Double for use with './' .
Double\/Float still dominate; any two integral types (same or mixed) become Double.
-}
type family PromoteDiv (a :: Type) (b :: Type) :: Type where
    PromoteDiv Double _ = Double
    PromoteDiv _ Double = Double
    PromoteDiv Float _ = Float
    PromoteDiv _ Float = Float
    PromoteDiv _ _ = Double -- Int/Int32/Int64 in any combination
