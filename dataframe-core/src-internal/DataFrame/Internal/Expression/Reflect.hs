{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Class evidence for the column types the library supports, recovered from
'Typeable'. A 'SomeExpr' carries only 'Columnable' (which includes 'Typeable' and
'Eq'); code that builds new expressions from one (decoders, parsers) asks here for
the 'Num', 'Ord', ... dictionaries an operator needs.
-}
module DataFrame.Internal.Expression.Reflect (
    Dict (..),
    RealUnbox,
    numDict,
    ordDict,
    fracDict,
    floatingDict,
    realDict,
    integralDict,
    realUnboxDict,
) where

import Data.Foldable (asum)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Kind (Constraint, Type)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import Type.Reflection (Typeable, typeRep)

-- | A captured class dictionary.
data Dict (c :: Constraint) where
    Dict :: (c) => Dict c

-- | 'Real' and 'VU.Unbox' together (variance, median and similar unboxed reductions).
class (Real a, VU.Unbox a) => RealUnbox a

instance (Real a, VU.Unbox a) => RealUnbox a

-- | Evidence for @c a@ when @a@ is the concrete type @b@.
via ::
    forall b (c :: Type -> Constraint) a.
    (Typeable a, Typeable b, c b) => Maybe (Dict (c a))
via = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Just Dict
    Nothing -> Nothing

-- | The fixed-width integral types.
ints ::
    forall (c :: Type -> Constraint) a.
    ( Typeable a
    , c Int
    , c Int8
    , c Int16
    , c Int32
    , c Int64
    , c Word
    , c Word8
    , c Word16
    , c Word32
    , c Word64
    ) =>
    [Maybe (Dict (c a))]
ints =
    [ via @Int @c @a
    , via @Int8 @c @a
    , via @Int16 @c @a
    , via @Int32 @c @a
    , via @Int64 @c @a
    , via @Word @c @a
    , via @Word8 @c @a
    , via @Word16 @c @a
    , via @Word32 @c @a
    , via @Word64 @c @a
    ]

floats ::
    forall (c :: Type -> Constraint) a.
    (Typeable a, c Double, c Float) => [Maybe (Dict (c a))]
floats = [via @Double @c @a, via @Float @c @a]

numDict :: forall a. (Typeable a) => Maybe (Dict (Num a))
numDict = asum (ints @Num @a ++ [via @Integer @Num @a] ++ floats @Num @a)

ordDict :: forall a. (Typeable a) => Maybe (Dict (Ord a))
ordDict =
    asum
        ( ints @Ord @a
            ++ [via @Integer @Ord @a]
            ++ floats @Ord @a
            ++ [via @Bool @Ord @a, via @Char @Ord @a, via @T.Text @Ord @a, via @String @Ord @a]
        )

fracDict :: forall a. (Typeable a) => Maybe (Dict (Fractional a))
fracDict = asum (floats @Fractional @a)

floatingDict :: forall a. (Typeable a) => Maybe (Dict (Floating a))
floatingDict = asum (floats @Floating @a)

realDict :: forall a. (Typeable a) => Maybe (Dict (Real a))
realDict = asum (ints @Real @a ++ [via @Integer @Real @a] ++ floats @Real @a)

integralDict :: forall a. (Typeable a) => Maybe (Dict (Integral a))
integralDict = asum (ints @Integral @a ++ [via @Integer @Integral @a])

-- | 'Integer' is deliberately absent: it is 'Real' but not 'VU.Unbox'.
realUnboxDict :: forall a. (Typeable a) => Maybe (Dict (RealUnbox a))
realUnboxDict = asum (ints @RealUnbox @a ++ floats @RealUnbox @a)
