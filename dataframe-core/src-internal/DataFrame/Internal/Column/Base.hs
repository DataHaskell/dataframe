{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
Core column definitions: the type-erased 'Column' GADT and its mutable/typed
companions, the 'Columnable' constraint, and the representation-level
primitives ('materializePacked', 'materializeMerged') that the non-orphan
'Show'/'Eq' instances depend on.

Predicates live in "DataFrame.Internal.Column.Properties", vector/list
conversions in "DataFrame.Internal.Column.Conversion", and bulk transformations
in "DataFrame.Internal.Column.Operations".
-}
module DataFrame.Internal.Column.Base where

import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (forM_)
import Control.Monad.ST (runST)
import Data.Maybe (fromMaybe, isNothing)
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column.Bitmap
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    packedIndexText,
    packedLength,
    packedSlice,
    packedTake,
    sliceEqBytes,
 )
import Type.Reflection (typeRep, type (:~:) (Refl))

-- | Constraint synonym for what we can put into columns.
type Columnable a =
    ( Columnable' a
    , ColumnifyRep (KindOf a) a
    , UnboxIf a
    , IntegralIf a
    , FloatingIf a
    , SBoolI (Unboxable a)
    , SBoolI (Numeric a)
    , SBoolI (IntegralTypes a)
    , SBoolI (FloatingTypes a)
    )

{- | Type-erased column GADT. Pattern-matching on the constructor recovers the
representation; nullability is an optional bit-packed 'Bitmap' (@Nothing@ = no
nulls, @Just bm@ = bit @i@ set iff row @i@ is valid).
-}
data Column where
    BoxedColumn :: (Columnable a) => Maybe Bitmap -> VB.Vector a -> Column
    UnboxedColumn ::
        (Columnable a, VU.Unbox a) => Maybe Bitmap -> VU.Vector a -> Column
    -- TODO: mchavinda - investigate splitting this into a separate intermediate
    --                   representation.
    -- Efficient intermediate formats.z
    -- Bit-packed Text: shared UTF-8 byte buffer + row offsets + optional bitmap;
    -- Text is materialized on demand. Only CSV ingest emits this; user-built
    -- Text columns stay 'BoxedColumn'.
    PackedText :: Maybe Bitmap -> {-# UNPACK #-} !PackedTextData -> Column
    -- A join's same-named non-key column pair ('mkMergedColumns'): both sides
    -- keep their native (packed/dict/unboxed) representation; per-row 'These'
    -- values only materialize on element access ('materializeMerged').
    MergedColumn :: !Column -> !Column -> Column

instance Show Column where
    show :: Column -> String
    show c@(MergedColumn _ _) = show (materializeMerged c)
    show (BoxedColumn Nothing column) = show column
    show (BoxedColumn (Just bm) column) =
        let n = VB.length column
            elems =
                [ if bitmapTestBit bm i then show (VB.unsafeIndex column i) else "null"
                | i <- [0 .. n - 1]
                ]
         in "[" ++ foldl (\acc e -> if null acc then e else acc ++ "," ++ e) "" elems ++ "]"
    show (UnboxedColumn Nothing column) = show column
    show (UnboxedColumn (Just bm) column) =
        let n = VU.length column
            elems =
                [ if bitmapTestBit bm i then show (VU.unsafeIndex column i) else "null"
                | i <- [0 .. n - 1]
                ]
         in "[" ++ foldl (\acc e -> if null acc then e else acc ++ "," ++ e) "" elems ++ "]"
    show c@(PackedText _ _) = show (materializePacked c)

{- | Compare two nullable boxed columns element by element, skipping null slots.
Uses a manual loop to avoid stream fusion forcing null-slot error thunks.
-}
eqBoxedCols ::
    (Eq a) => Maybe Bitmap -> VB.Vector a -> Maybe Bitmap -> VB.Vector a -> Bool
eqBoxedCols bm1 a bm2 b
    | VB.length a /= VB.length b = False
    | otherwise = go 0
  where
    !n = VB.length a
    go !i
        | i >= n = True
        | nullA || nullB = (nullA == nullB) && go (i + 1)
        | VB.unsafeIndex a i == VB.unsafeIndex b i = go (i + 1)
        | otherwise = False
      where
        nullA = maybe False (\bm -> not (bitmapTestBit bm i)) bm1
        nullB = maybe False (\bm -> not (bitmapTestBit bm i)) bm2
{-# INLINE eqBoxedCols #-}

instance Eq Column where
    (==) :: Column -> Column -> Bool
    (==) (BoxedColumn bm1 (a :: VB.Vector t1)) (BoxedColumn bm2 (b :: VB.Vector t2)) =
        case testEquality (typeRep @t1) (typeRep @t2) of
            Nothing -> False
            Just Refl -> eqBoxedCols bm1 a bm2 b
    (==) (UnboxedColumn bm1 (a :: VU.Vector t1)) (UnboxedColumn bm2 (b :: VU.Vector t2)) =
        case testEquality (typeRep @t1) (typeRep @t2) of
            Nothing -> False
            Just Refl ->
                VU.length a == VU.length b
                    && VU.and
                        ( VU.imap
                            ( \i x ->
                                let nullA = maybe False (\bm -> not (bitmapTestBit bm i)) bm1
                                    nullB = maybe False (\bm -> not (bitmapTestBit bm i)) bm2
                                 in if nullA || nullB then nullA == nullB else x == VU.unsafeIndex b i
                            )
                            a
                        )
    (==) lhs@(MergedColumn _ _) rhs = materializeMerged lhs == rhs
    (==) lhs rhs@(MergedColumn _ _) = lhs == materializeMerged rhs
    (==) (PackedText bm1 p1) (PackedText bm2 p2) = eqPackedCols bm1 p1 bm2 p2
    (==) lhs@(PackedText _ _) rhs = materializePacked lhs == rhs
    (==) lhs rhs@(PackedText _ _) = lhs == materializePacked rhs
    (==) _ _ = False

{- | Byte-slice equality of two packed-text columns, skipping null slots
(a null compares equal only to a null), mirroring 'eqBoxedCols'.
-}
eqPackedCols ::
    Maybe Bitmap -> PackedTextData -> Maybe Bitmap -> PackedTextData -> Bool
eqPackedCols bm1 p1 bm2 p2
    | packedLength p1 /= packedLength p2 = False
    | otherwise = go 0
  where
    !n = packedLength p1
    go !i
        | i >= n = True
        | nullA || nullB = (nullA == nullB) && go (i + 1)
        | otherwise =
            let (a1, o1, l1) = packedSlice p1 i
                (a2, o2, l2) = packedSlice p2 i
             in sliceEqBytes a1 o1 l1 a2 o2 l2 && go (i + 1)
      where
        nullA = maybe False (\bm -> not (bitmapTestBit bm i)) bm1
        nullB = maybe False (\bm -> not (bitmapTestBit bm i)) bm2
{-# INLINE eqPackedCols #-}

{- | A mutable companion struct to dataframe columns.

Used mostly as an intermediate structure for I/O.
-}
data MutableColumn where
    MBoxedColumn :: (Columnable a) => VBM.IOVector a -> MutableColumn
    MUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> MutableColumn

{- | A wrapper around the type-erased 'Column' carrying a phantom element type,
used to type-check expressions. The phantom is not guaranteed to match the
underlying vector's type.
-}
data TypedColumn a where
    TColumn :: (Columnable a) => Column -> TypedColumn a

instance (Eq a) => Eq (TypedColumn a) where
    (==) :: (Eq a) => TypedColumn a -> TypedColumn a -> Bool
    (==) (TColumn a) (TColumn b) = a == b

instance (Show a) => Show (TypedColumn a) where
    show :: (Show a) => TypedColumn a -> String
    show (TColumn col) = show col

-- | Unwrap a 'TypedColumn' back to its type-erased 'Column'.
unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

{- | Decode a 'PackedText' into a @BoxedColumn Text@ (bit-identical to
materializing at freeze). Identity on every other column.
-}
materializePacked :: Column -> Column
materializePacked (PackedText bm p) =
    BoxedColumn bm (VB.generate (packedLength p) (packedIndexText p))
materializePacked c = c
{-# INLINE materializePacked #-}

-- | Return the 'Maybe Bitmap' from a column.
columnBitmap :: Column -> Maybe Bitmap
columnBitmap (BoxedColumn bm _) = bm
columnBitmap (UnboxedColumn bm _) = bm
columnBitmap (PackedText bm _) = bm
columnBitmap (MergedColumn _ _) = Nothing

{- | A class for converting a vector to a column of the appropriate type.
Given each Rep we tell the `toColumnRep` function which Column type to pick.
-}
class ColumnifyRep (r :: Rep) a where
    toColumnRep :: VB.Vector a -> Column

instance
    (Columnable a, VU.Unbox a) =>
    ColumnifyRep 'RUnboxed a
    where
    toColumnRep :: (Columnable a, VUM.Unbox a) => VB.Vector a -> Column
    toColumnRep v = UnboxedColumn Nothing (VU.convert v)

instance
    (Columnable a) =>
    ColumnifyRep 'RBoxed a
    where
    toColumnRep :: (Columnable a) => VB.Vector a -> Column
    toColumnRep = BoxedColumn Nothing

instance
    (Columnable a) =>
    ColumnifyRep 'RNullableBoxed (Maybe a)
    where
    toColumnRep :: (Columnable a) => VB.Vector (Maybe a) -> Column
    toColumnRep v =
        let
            n = VB.length v
            nullIdxs = VU.filter (isNothing . VB.unsafeIndex v) (VU.enumFromN 0 n)
            bm =
                if VU.null nullIdxs then allValidBitmap n else buildBitmapFromNulls' n nullIdxs
         in
            case sUnbox @a of
                STrue -> UnboxedColumn (Just bm) $ runST $ do
                    mv <- VUM.new n
                    VG.iforM_ v $ \i mx -> forM_ mx (VUM.unsafeWrite mv i)
                    VU.unsafeFreeze mv
                SFalse ->
                    BoxedColumn
                        (Just bm)
                        (VB.map (fromMaybe (errorWithoutStackTrace "toColumnRep: Nothing slot")) v)

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (MergedColumn a b) = min (columnLength a) (columnLength b)
columnLength (BoxedColumn _ xs) = VB.length xs
columnLength (UnboxedColumn _ xs) = VU.length xs
columnLength (PackedText _ p) = packedLength p
{-# INLINE columnLength #-}

-- | O(n) Takes the first n values of a column.
takeColumn :: Int -> Column -> Column
takeColumn n (MergedColumn a b) = MergedColumn (takeColumn n a) (takeColumn n b)
takeColumn n (BoxedColumn bm xs) =
    BoxedColumn (fmap (bitmapSlice 0 n) bm) (VG.take n xs)
takeColumn n (UnboxedColumn bm xs) =
    UnboxedColumn (fmap (bitmapSlice 0 n) bm) (VG.take n xs)
takeColumn n (PackedText bm p) =
    PackedText (fmap (bitmapSlice 0 n) bm) (packedTake n p)
{-# INLINE takeColumn #-}

{- | Merge two columns using `These`. O(1): the sides are kept in their
native representation and 'These' values materialize on element access.
-}
mkMergedColumns :: Column -> Column -> Column
mkMergedColumns = MergedColumn
{-# INLINE mkMergedColumns #-}

-- | Decode a 'MergedColumn' into the eager @BoxedColumn (These a b)@ form.
materializeMerged :: Column -> Column
materializeMerged (MergedColumn colA colB) =
    mergeEager (materializeMerged colA) (materializeMerged colB)
materializeMerged c = c

mergedHead :: Column -> Column
mergedHead (MergedColumn a b) =
    materializeMerged (MergedColumn (takeColumn 1 a) (takeColumn 1 b))
mergedHead c = c

{- | The eager element-wise merge ('These' per row, boxed). Bitmaps are
honored for every representation pair: a null side yields 'This'/'That',
both-null is an error (the join kernels never produce such a row).
-}
mergeEager :: Column -> Column -> Column
mergeEager colA colB = case (colA, colB) of
    (MergedColumn a b, _) -> mergeEager (mergeEager a b) colB
    (_, MergedColumn a b) -> mergeEager colA (mergeEager a b)
    (PackedText _ _, _) -> mergeEager (materializePacked colA) colB
    (_, PackedText _ _) -> mergeEager colA (materializePacked colB)
    (BoxedColumn bmA c1, BoxedColumn bmB c2) ->
        merged bmA bmB (VG.length c1) (VG.length c2) (c1 VG.!) (c2 VG.!)
    (BoxedColumn bmA c1, UnboxedColumn bmB c2) ->
        merged bmA bmB (VG.length c1) (VG.length c2) (c1 VG.!) (c2 VG.!)
    (UnboxedColumn bmA c1, BoxedColumn bmB c2) ->
        merged bmA bmB (VG.length c1) (VG.length c2) (c1 VG.!) (c2 VG.!)
    (UnboxedColumn bmA c1, UnboxedColumn bmB c2) ->
        merged bmA bmB (VG.length c1) (VG.length c2) (c1 VG.!) (c2 VG.!)
  where
    merged ::
        (Columnable a, Columnable b) =>
        Maybe Bitmap ->
        Maybe Bitmap ->
        Int ->
        Int ->
        (Int -> a) ->
        (Int -> b) ->
        Column
    merged bmA bmB lenA lenB atA atB =
        BoxedColumn Nothing $ VB.generate (min lenA lenB) $ \i ->
            case (validAt bmA i, validAt bmB i) of
                (True, True) -> These (atA i) (atB i)
                (True, False) -> This (atA i)
                (False, True) -> That (atB i)
                (False, False) -> error "mkMergedColumns: both null"
    validAt mbm i = maybe True (`bitmapTestBit` i) mbm
    {-# INLINE validAt #-}
