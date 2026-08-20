{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
Predicates and introspection over a 'Column': representation tests, null/type
queries, and the human-readable type descriptions used in error messages.
-}
module DataFrame.Internal.Column.Properties where

import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU

import Data.Kind (Type)
import Data.Maybe (isJust)
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column.Base
import DataFrame.Internal.Column.Bitmap
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Data.PackedText (packedLength)
import Type.Reflection (
    TypeRep,
    Typeable,
    eqTypeRep,
    typeRep,
    pattern App,
    type (:~:) (Refl),
    type (:~~:) (HRefl),
 )

-- | Whether a column is a 'PackedText'.
isPackedText :: Column -> Bool
isPackedText (PackedText _ _) = True
isPackedText _ = False
{-# INLINE isPackedText #-}

-- | Whether a column is a 'MergedColumn'.
isMergedColumn :: Column -> Bool
isMergedColumn (MergedColumn _ _) = True
isMergedColumn _ = False
{-# INLINE isMergedColumn #-}

-- | Checks if a column contains missing values (has a bitmap).
hasMissing :: Column -> Bool
hasMissing (BoxedColumn (Just _) _) = True
hasMissing (UnboxedColumn (Just _) _) = True
hasMissing (PackedText (Just _) _) = True
hasMissing _ = False

-- | Checks if a column contains only missing values.
allMissing :: Column -> Bool
allMissing (BoxedColumn (Just bm) col) =
    not (VB.null col) && popCountUpTo (VB.length col) bm == 0
allMissing (UnboxedColumn (Just bm) col) =
    not (VU.null col) && popCountUpTo (VU.length col) bm == 0
allMissing (PackedText (Just bm) p) =
    packedLength p > 0 && popCountUpTo (packedLength p) bm == 0
allMissing _ = False

-- | Checks if a column contains numeric values.
isNumeric :: Column -> Bool
isNumeric c@(MergedColumn _ _) = isNumeric (mergedHead c)
isNumeric (UnboxedColumn _ (_vec :: VU.Vector a)) = case sNumeric @a of
    STrue -> True
    _ -> False
isNumeric (BoxedColumn _ (_vec :: VB.Vector a)) = case testEquality (typeRep @a) (typeRep @Integer) of
    Nothing -> False
    Just Refl -> True
isNumeric (PackedText _ _) = False

{- | Whether the column stores element type @a@. For nullable columns, also
'True' when @a = Maybe b@ and the column stores @b@ internally.
-}
hasElemType :: forall a. (Columnable a) => Column -> Bool
hasElemType = \case
    BoxedColumn bm (_column :: VB.Vector b) -> checkBoxed bm (typeRep @b)
    UnboxedColumn bm (_column :: VU.Vector b) -> checkUnboxed bm (typeRep @b)
    PackedText bm _ -> checkBoxed bm (typeRep @T.Text)
    c@(MergedColumn _ _) -> hasElemType @a (mergedHead c)
  where
    directMatch :: forall (b :: Type). TypeRep b -> Bool
    directMatch = isJust . testEquality (typeRep @a)
    checkMaybe :: forall (b :: Type). TypeRep b -> Bool
    checkMaybe tb = case typeRep @a of
        App tMaybe tInner -> case eqTypeRep tMaybe (typeRep @Maybe) of
            Just HRefl -> isJust (testEquality tInner tb)
            Nothing -> False
        _ -> False
    checkBoxed :: forall (b :: Type). Maybe Bitmap -> TypeRep b -> Bool
    checkBoxed bm tb = directMatch tb || (isJust bm && checkMaybe tb)
    checkUnboxed :: forall (b :: Type). Maybe Bitmap -> TypeRep b -> Bool
    checkUnboxed bm tb = directMatch tb || (isJust bm && checkMaybe tb)

-- | An internal/debugging function to get the column type of a column.
columnVersionString :: Column -> String
columnVersionString column = case column of
    BoxedColumn Nothing _ -> "Boxed"
    BoxedColumn (Just _) _ -> "NullableBoxed"
    UnboxedColumn Nothing _ -> "Unboxed"
    UnboxedColumn (Just _) _ -> "NullableUnboxed"
    PackedText Nothing _ -> "Boxed"
    PackedText (Just _) _ -> "NullableBoxed"
    MergedColumn _ _ -> columnVersionString (mergedHead column)

{- | An internal/debugging function to get the type stored in the outermost vector
of a column.
-}
columnTypeString :: Column -> String
columnTypeString column = case column of
    BoxedColumn Nothing (_ :: VB.Vector a) -> show (typeRep @a)
    BoxedColumn (Just _) (_ :: VB.Vector a) -> showMaybeType @a
    UnboxedColumn Nothing (_ :: VU.Vector a) -> show (typeRep @a)
    UnboxedColumn (Just _) (_ :: VU.Vector a) -> showMaybeType @a
    PackedText Nothing _ -> show (typeRep @T.Text)
    PackedText (Just _) _ -> showMaybeType @T.Text
    MergedColumn _ _ -> columnTypeString (mergedHead column)
  where
    showMaybeType :: forall a. (Typeable a) => String
    showMaybeType =
        let s = show (typeRep @a)
         in "Maybe " ++ if ' ' `elem` s then "(" ++ s ++ ")" else s

-- | Whether row @i@ is null, respecting the bitmap.
columnElemIsNull :: Column -> Int -> Bool
columnElemIsNull (BoxedColumn (Just bm) _) i = not (bitmapTestBit bm i)
columnElemIsNull (UnboxedColumn (Just bm) _) i = not (bitmapTestBit bm i)
columnElemIsNull (PackedText (Just bm) _) i = not (bitmapTestBit bm i)
columnElemIsNull _ _ = False

-- | O(n) Gets the number of non-null elements in the column.
numElements :: Column -> Int
numElements (MergedColumn a b) = min (columnLength a) (columnLength b)
numElements (BoxedColumn Nothing xs) = VB.length xs
numElements (BoxedColumn (Just bm) xs) = popCountUpTo (VB.length xs) bm
numElements (UnboxedColumn Nothing xs) = VU.length xs
numElements (UnboxedColumn (Just bm) xs) = popCountUpTo (VU.length xs) bm
numElements (PackedText Nothing p) = packedLength p
numElements (PackedText (Just bm) p) = popCountUpTo (packedLength p) bm
{-# INLINE numElements #-}
