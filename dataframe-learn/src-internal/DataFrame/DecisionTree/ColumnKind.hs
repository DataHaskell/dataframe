{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | How the tree learners split on a column.

The kind of a column is decided by its element type, never by how it is stored.
An unboxed @Bool@ and a boxed @Text@ are both categories; an unboxed @Float@ and
an unboxed @Int@ are both numbers. Every split-candidate generator (CART features,
the classifier's numeric pool and its categorical pool) dispatches through
'columnKind', so a column type is either split on by all of them in the same way,
or by none of them.
-}
module DataFrame.DecisionTree.ColumnKind (
    ColumnKind (..),
    AsDouble (..),
    columnKind,
    withColumnVector,
) where

import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import Type.Reflection (typeRep)

import DataFrame.DecisionTree.Types (ColumnOrdering, withOrdFrom)
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.Expression (Expr (..))

-- | What a column of element type @a@ offers a split.
data ColumnKind a where
    -- | A number (floating or integral): threshold splits on its value as a 'Double'.
    Ordered :: AsDouble a -> ColumnKind a
    -- | A category (any other type with a registered ordering): equality splits on its levels.
    Discrete :: (Ord a) => ColumnKind a
    -- | Neither a number nor a registered category: no splits.
    Unsplittable :: ColumnKind a

{- | The kind of element type @a@. Categories need an 'Ord' instance from the
registry (to collect their levels); 'DataFrame.DecisionTree.Types.defaultColumnOrdering'
covers @Bool@, @Char@, @Text@ and @String@.
-}
columnKind :: forall a. (Columnable a) => ColumnOrdering -> ColumnKind a
columnKind ords = case sFloating @a of
    STrue -> Ordered asDouble
    SFalse -> case sIntegral @a of
        STrue -> Ordered asDouble
        SFalse -> fromMaybe Unsplittable (withOrdFrom @a ords Discrete)

-- | How to read a number column as a 'Double': its values, and the expression a split prints.
data AsDouble a = AsDouble
    { valueAsDouble :: a -> Double
    , columnAsDouble :: T.Text -> Expr Double
    -- ^ The column as a 'Double' (a plain reference when it already is one).
    , nullableAsDouble :: T.Text -> Expr (Maybe Double)
    -- ^ The same for a nullable column.
    }

asDouble :: forall a. (Columnable a, Real a) => AsDouble a
asDouble = case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> AsDouble id Col Col
    Nothing ->
        AsDouble
            realToFrac
            (F.toDouble . Col @a)
            (F.whenPresent (realToFrac @a @Double) . Col @(Maybe a))

{- | A column's null bitmap and elements as a boxed vector, whatever its storage.
Elements at null positions are placeholders and must not be read.
-}
withColumnVector ::
    Column -> (forall a. (Columnable a) => Maybe Bitmap -> V.Vector a -> r) -> r
withColumnVector col k = case col of
    BoxedColumn bm v -> k bm v
    UnboxedColumn bm v -> k bm (VG.convert v)
    pt@(PackedText _ _) -> withColumnVector (materializePacked pt) k
    mc@(MergedColumn _ _) -> withColumnVector (materializeMerged mc) k
