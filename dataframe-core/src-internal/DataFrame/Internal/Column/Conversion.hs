{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
Conversions between 'Column' and ordinary vectors\/lists, plus the typed
extraction functions ('toVector', 'toDoubleVector', ...) that recover a
column's element type.
-}
module DataFrame.Internal.Column.Conversion where

import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (ST)
import Data.Kind (Type)
import Data.Type.Equality (TestEquality (..))
import Data.Word (Word8)
import DataFrame.Errors (
    DataFrameException (ExpectedNonNullableException, TypeMismatchException),
    TypeErrorContext (
        MkTypeErrorContext,
        callingFunctionName,
        errorColumnName,
        expectedType,
        userType
    ),
 )
import DataFrame.Internal.Column.Base
import DataFrame.Internal.Column.Bitmap
import DataFrame.Internal.Column.Properties
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Data.PackedText (packedIndexText, packedLength)
import System.Random (RandomGen, UniformRange, uniformR)
import Type.Reflection (TypeRep, Typeable, typeRep, type (:~:) (Refl))

{- | O(n) Convert a vector to a column. Automatically picks the best representation of a vector to store the underlying data in.

__Examples:__

@
> import qualified Data.Vector as V
> fromVector (VB.fromList [(1 :: Int), 2, 3, 4])
[1,2,3,4]
@
-}
fromVector ::
    forall a.
    (Columnable a, ColumnifyRep (KindOf a) a) =>
    VB.Vector a -> Column
fromVector = toColumnRep @(KindOf a)

{- | O(n) Convert an unboxed vector to a column. This avoids the extra conversion if you already have the data in an unboxed vector.

__Examples:__

@
> import qualified Data.Vector.Unboxed as V
> fromUnboxedVector (VB.fromList [(1 :: Int), 2, 3, 4])
[1,2,3,4]
@
-}
fromUnboxedVector ::
    forall a. (Columnable a, VU.Unbox a) => VU.Vector a -> Column
fromUnboxedVector = UnboxedColumn Nothing

{- | O(n) Convert a list to a column. Automatically picks the best representation of a vector to store the underlying data in.

__Examples:__

@
> fromList [(1 :: Int), 2, 3, 4]
[1,2,3,4]
@
-}
fromList ::
    forall a.
    (Columnable a, ColumnifyRep (KindOf a) a) =>
    [a] -> Column
fromList = toColumnRep @(KindOf a) . VB.fromList

{- | O(n) Create a column of random elements within a range.

Takes a random number generator, a length, and a lower and upper bound for the random values.

__Examples:__

@
> import System.Random (mkStdGen)
> mkRandom (mkStdGen 42) 4 0 10
[4,2,6,5]
@
-}
mkRandom ::
    (RandomGen g, Columnable a, ColumnifyRep (KindOf a) a, UniformRange a) =>
    g -> Int -> a -> a -> Column
mkRandom pureGen k lo hi = fromList $ go pureGen k
  where
    go _g 0 = []
    go g n =
        let
            (!v, !g') = uniformR (lo, hi) g
         in
            v : go g' (n - 1)

{- | O(n) Converts a column to a list. Throws an exception if the wrong type is specified.

__Examples:__

@
> column = fromList [(1 :: Int), 2, 3, 4]
> toList @Int column
[1,2,3,4]
> toList @Double column
exception: ...
@
-}
toList :: forall a. (Columnable a) => Column -> [a]
toList xs = case toVector @a xs of
    Left err -> throw err
    Right val -> VB.toList val

{- | Type-safe conversion of a column to a vector of element type @a@ (specify via
type application); 'Left' 'TypeMismatchException' when the column's type differs.

>>> toVector @Int @VU.Vector column
Right (unboxed vector of Ints)

>>> toVector @Text @VB.Vector column
Right (boxed vector of Text)
-}
toVector ::
    forall a v.
    (VG.Vector v a, Columnable a) => Column -> Either DataFrameException (v a)
toVector col = case col of
    PackedText _ _ -> toVector (materializePacked col)
    MergedColumn _ _ -> toVector (materializeMerged col)
    BoxedColumn bm (inner :: VB.Vector c) ->
        -- Check if user wants Maybe c (nullable) or c directly
        case testEquality (typeRep @a) (typeRep @c) of
            Just Refl -> Right $ VG.convert inner
            Nothing ->
                -- Try: a = Maybe c
                case testEquality (typeRep @a) (typeRep @(Maybe c)) of
                    Just Refl ->
                        -- Use VB.generate to avoid fusion forcing null slots
                        let !n = VB.length inner
                            maybeVec = case bm of
                                Nothing -> VB.generate n (Just . VB.unsafeIndex inner)
                                Just bitmap -> VB.generate n $ \i ->
                                    if bitmapTestBit bitmap i then Just (VB.unsafeIndex inner i) else Nothing
                         in Right $ VG.convert maybeVec
                    Nothing ->
                        Left $
                            TypeMismatchException
                                ( MkTypeErrorContext
                                    { userType = Right (typeRep @a)
                                    , expectedType = Right (typeRep @c)
                                    , callingFunctionName = Just "toVector"
                                    , errorColumnName = Nothing
                                    }
                                )
    UnboxedColumn bm (inner :: VU.Vector c) ->
        case testEquality (typeRep @a) (typeRep @c) of
            Just Refl -> Right $ VG.convert inner
            Nothing ->
                case testEquality (typeRep @a) (typeRep @(Maybe c)) of
                    Just Refl ->
                        let maybeVec = case bm of
                                Nothing -> VB.generate (VU.length inner) (Just . VU.unsafeIndex inner)
                                Just bitmap -> VB.generate (VU.length inner) $ \i ->
                                    if bitmapTestBit bitmap i then Just (VU.unsafeIndex inner i) else Nothing
                         in Right $ VG.convert maybeVec
                    Nothing ->
                        Left $
                            TypeMismatchException
                                ( MkTypeErrorContext
                                    { userType = Right (typeRep @a)
                                    , expectedType = Right (typeRep @c)
                                    , callingFunctionName = Just "toVector"
                                    , errorColumnName = Nothing
                                    }
                                )

{-# INLINEABLE toVector #-}

-- Some common types we will use for numerical computing.

{- | Convert a column to an unboxed 'Double' vector, coercing numeric types
('realToFrac' for floats, 'fromIntegral' for integrals; nulls become @NaN@).
'Left' 'TypeMismatchException' when the column is not numeric.
-}
toDoubleVector :: Column -> Either DataFrameException (VU.Vector Double)
toDoubleVector column =
    case column of
        PackedText _ _ -> toDoubleVector (materializePacked column)
        MergedColumn _ _ -> toDoubleVector (materializeMerged column)
        UnboxedColumn (Just _) _ -> Left ExpectedNonNullableException
        UnboxedColumn Nothing (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> Right f
            Nothing -> case sFloating @a of
                STrue -> Right (VU.map realToFrac f)
                SFalse -> case sIntegral @a of
                    STrue -> Right (VU.map fromIntegral f)
                    SFalse ->
                        Left $
                            TypeMismatchException
                                ( MkTypeErrorContext
                                    { userType = Right (typeRep @Double)
                                    , expectedType = Right (typeRep @a)
                                    , callingFunctionName = Just "toDoubleVector"
                                    , errorColumnName = Nothing
                                    }
                                )
        BoxedColumn (Just _) (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
            Just Refl -> Left ExpectedNonNullableException
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Double)
                            , expectedType = Left (columnTypeString column) :: Either String (TypeRep ())
                            , callingFunctionName = Just "toDoubleVector"
                            , errorColumnName = Nothing
                            }
                        )
        BoxedColumn Nothing (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
            Just Refl -> Right (VB.convert $ VB.map fromIntegral f)
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Double)
                            , expectedType = Left (columnTypeString column) :: Either String (TypeRep ())
                            , callingFunctionName = Just "toDoubleVector"
                            , errorColumnName = Nothing
                            }
                        )

{- | Convert a column to an unboxed 'Float' vector, coercing numeric types (nulls
become @NaN@); 'Left' 'TypeMismatchException' when not numeric. Converting from
'Double' may lose precision.
-}
toFloatVector :: Column -> Either DataFrameException (VU.Vector Float)
toFloatVector column =
    case column of
        PackedText _ _ -> toFloatVector (materializePacked column)
        MergedColumn _ _ -> toFloatVector (materializeMerged column)
        UnboxedColumn bm (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Float) of
            Just Refl -> case bm of
                Nothing -> Right f
                Just bitmap -> Right $ VU.imap (\i x -> if bitmapTestBit bitmap i then x else read "NaN") f
            Nothing -> case sFloating @a of
                STrue ->
                    Right
                        ( VU.imap
                            ( \i x -> case bm of
                                Just bitmap | not (bitmapTestBit bitmap i) -> read "NaN"
                                _ -> realToFrac x
                            )
                            f
                        )
                SFalse -> case sIntegral @a of
                    STrue ->
                        Right
                            ( VU.imap
                                ( \i x -> case bm of
                                    Just bitmap | not (bitmapTestBit bitmap i) -> read "NaN"
                                    _ -> fromIntegral x
                                )
                                f
                            )
                    SFalse ->
                        Left $
                            TypeMismatchException
                                ( MkTypeErrorContext
                                    { userType = Right (typeRep @Float)
                                    , expectedType = Right (typeRep @a)
                                    , callingFunctionName = Just "toFloatVector"
                                    , errorColumnName = Nothing
                                    }
                                )
        BoxedColumn bm (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
            Just Refl ->
                Right
                    ( VB.convert $
                        VB.imap
                            ( \i x -> case bm of
                                Just bitmap | not (bitmapTestBit bitmap i) -> read "NaN"
                                _ -> fromIntegral x
                            )
                            f
                    )
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Float)
                            , expectedType = Left (columnTypeString column) :: Either String (TypeRep ())
                            , callingFunctionName = Just "toFloatVector"
                            , errorColumnName = Nothing
                            }
                        )

{- | Convert a column to an unboxed 'Int' vector, coercing numeric types
(floats are 'round'ed via banker's rounding); 'Left' 'TypeMismatchException'
when the column is not numeric. Does not support nullable columns.
-}
toIntVector :: Column -> Either DataFrameException (VU.Vector Int)
toIntVector column =
    case column of
        PackedText _ _ -> toIntVector (materializePacked column)
        MergedColumn _ _ -> toIntVector (materializeMerged column)
        UnboxedColumn _ (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> Right f
            Nothing -> case sFloating @a of
                STrue -> Right (VU.map (round . (realToFrac :: a -> Double)) f)
                SFalse -> case sIntegral @a of
                    STrue -> Right (VU.map fromIntegral f)
                    SFalse ->
                        Left $
                            TypeMismatchException
                                ( MkTypeErrorContext
                                    { userType = Right (typeRep @Int)
                                    , expectedType = Right (typeRep @a)
                                    , callingFunctionName = Just "toIntVector"
                                    , errorColumnName = Nothing
                                    }
                                )
        BoxedColumn _ (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
            Just Refl -> Right (VB.convert $ VB.map fromIntegral f)
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Int)
                            , expectedType = Left (columnTypeString column) :: Either String (TypeRep ())
                            , callingFunctionName = Just "toIntVector"
                            , errorColumnName = Nothing
                            }
                        )

toUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) => Column -> Either DataFrameException (VU.Vector a)
toUnboxedVector column =
    case column of
        UnboxedColumn _ (f :: VU.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> Right f
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @a)
                            , expectedType = Right (typeRep @b)
                            , callingFunctionName = Just "toUnboxedVector"
                            , errorColumnName = Nothing
                            }
                        )
        _ ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Left (columnTypeString column) :: Either String (TypeRep ())
                        , callingFunctionName = Just "toUnboxedVector"
                        , errorColumnName = Nothing
                        }
                    )
{-# INLINE toUnboxedVector #-}

-- Shared finaliser for the two parseUnboxedColumn* helpers.  Freezes
-- the mutable data vector, and only materialises the bitmap when the
-- column actually had nulls.
{-# INLINE finalizeParseResult #-}
finalizeParseResult ::
    (VU.Unbox a) =>
    VUM.STVector s a ->
    VUM.STVector s Word8 ->
    Bool ->
    ST s (Maybe (Maybe Bitmap, VU.Vector a))
finalizeParseResult values vmask anyNull
    | anyNull = do
        vs <- VU.unsafeFreeze values
        vm <- VU.unsafeFreeze vmask
        return (Just (Just (buildBitmapFromValid vm), vs))
    | otherwise = do
        vs <- VU.unsafeFreeze values
        return (Just (Nothing, vs))

-- | Convert any Column to a vector of Text labels (one per row).
columnToTextVec :: Column -> VB.Vector T.Text
columnToTextVec c@(MergedColumn _ _) = columnToTextVec (materializeMerged c)
columnToTextVec (BoxedColumn bm (col' :: VB.Vector a)) =
    case bm of
        Nothing -> case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> col'
            Nothing -> VB.map (T.pack . show) col'
        Just bitmap ->
            VB.imap
                (\i x -> if bitmapTestBit bitmap i then T.pack (show x) else "null")
                col'
columnToTextVec (UnboxedColumn bm col') =
    case bm of
        Nothing -> VB.map (T.pack . show) (VB.convert col')
        Just bitmap ->
            VB.generate (VU.length col') $ \i ->
                if bitmapTestBit bitmap i then T.pack (show (col' VU.! i)) else "null"
columnToTextVec (PackedText bm p) =
    VB.generate (packedLength p) $ \i -> case bm of
        Just bitmap | not (bitmapTestBit bitmap i) -> "null"
        _ -> packedIndexText p i

-- An internal helper for type errors
throwTypeMismatch ::
    forall (a :: Type) (b :: Type).
    (Typeable a, Typeable b) => Either DataFrameException Column
throwTypeMismatch =
    Left $
        TypeMismatchException
            MkTypeErrorContext
                { userType = Right (typeRep @b)
                , expectedType = Right (typeRep @a)
                , callingFunctionName = Nothing
                , errorColumnName = Nothing
                }
