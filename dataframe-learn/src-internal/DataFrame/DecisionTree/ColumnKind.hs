{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Dispatch decision-tree splits by element type rather than storage.
module DataFrame.DecisionTree.ColumnKind (
    ColumnKind (..),
    AsDouble (..),
    columnKind,
    withColumnType,
    withColumnVector,
) where

import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

import DataFrame.DecisionTree.Types (ColumnOrdering, withOrdFrom)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.Expression (Expr (..))

-- | What a column of element type @a@ offers a split.
data ColumnKind a where
    -- | Floating or integral: threshold splits on its 'Double' value.
    Ordered :: AsDouble a -> ColumnKind a
    -- | Registered non-numeric 'Ord' type: equality splits on levels.
    Discrete :: (Ord a) => ColumnKind a
    -- | Neither a number nor a registered category: no splits.
    Unsplittable :: ColumnKind a

-- | Classify @a@ as 'Ordered', 'Discrete' (if in @ords@), or 'Unsplittable'.
columnKind :: forall a. (Columnable a) => ColumnOrdering -> ColumnKind a
columnKind ords = case sFloating @a of
    STrue -> Ordered asDouble
    SFalse -> case sIntegral @a of
        STrue -> Ordered asDouble
        SFalse -> fromMaybe Unsplittable (withOrdFrom @a ords Discrete)

-- | How to read a number column as a 'Double' in values and split expressions.
data AsDouble a = AsDouble
    { valueAsDouble :: a -> Double
    , columnAsDouble :: T.Text -> Expr Double
    -- ^ Plain 'Col' for 'Double', or lifted via 'F.toDouble'.
    , nullableAsDouble :: T.Text -> Expr (Maybe Double)
    -- ^ Nullable numeric column lifted to @Maybe Double@.
    }

asDouble :: forall a. (Columnable a, Real a) => AsDouble a
asDouble = case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> AsDouble id Col Col
    Nothing ->
        AsDouble
            realToFrac
            (F.toDouble . Col @a)
            (F.whenPresent (realToFrac @a @Double) . Col @(Maybe a))

-- | A column's null bitmap and element type, without materializing storage.
withColumnType ::
    Column -> (forall a. (Columnable a) => Maybe Bitmap -> Proxy a -> r) -> r
withColumnType col k = case col of
    BoxedColumn bm (_ :: V.Vector a) -> k bm (Proxy @a)
    UnboxedColumn bm (_ :: VU.Vector a) -> k bm (Proxy @a)
    PackedText bm _ -> k bm (Proxy @T.Text)
    MergedColumn l r ->
        withColumnType l $ \_ (_ :: Proxy a) ->
            withColumnType r $ \_ (_ :: Proxy b) ->
                k Nothing (Proxy @(These a b))

-- | A column's null bitmap and boxed elements (null slots are placeholders).
withColumnVector ::
    Column -> (forall a. (Columnable a) => Maybe Bitmap -> V.Vector a -> r) -> r
withColumnVector col k = case col of
    BoxedColumn bm v -> k bm v
    UnboxedColumn bm v -> k bm (VG.convert v)
    pt@(PackedText _ _) -> withColumnVector (materializePacked pt) k
    mc@(MergedColumn _ _) -> withColumnVector (materializeMerged mc) k
