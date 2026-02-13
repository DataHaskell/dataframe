{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Column where

import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (runST)
import Data.Kind (Type)
import Data.Maybe
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import Type.Reflection

{- | Our representation of a column is a GADT that can store data based on the underlying data.

This allows us to pattern match on data kinds and limit some operations to only some
kinds of vectors. E.g. operations for missing data only happen in an OptionalColumn.
-}
data Column where
    BoxedColumn :: (Columnable a) => VB.Vector a -> Column
    UnboxedColumn :: (Columnable a, VU.Unbox a) => VU.Vector a -> Column
    OptionalColumn :: (Columnable a) => VB.Vector (Maybe a) -> Column

data MutableColumn where
    MBoxedColumn :: (Columnable a) => VBM.IOVector a -> MutableColumn
    MUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> MutableColumn

{- | A TypedColumn is a wrapper around our type-erased column.
It is used to type check expressions on columns.
-}
data TypedColumn a where
    TColumn :: (Columnable a) => Column -> TypedColumn a

instance (Eq a) => Eq (TypedColumn a) where
    (==) :: (Eq a) => TypedColumn a -> TypedColumn a -> Bool
    (==) (TColumn a) (TColumn b) = a == b

instance (Ord a) => Ord (TypedColumn a) where
    compare :: (Ord a) => TypedColumn a -> TypedColumn a -> Ordering
    compare (TColumn a) (TColumn b) = compare a b

-- | Gets the underlying value from a TypedColumn.
unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

-- | Gets the underlying vector from a TypedColumn.
vectorFromTypedColumn :: TypedColumn a -> VB.Vector a
vectorFromTypedColumn (TColumn value) = either throw id (toVector value)

-- | Checks if a column contains missing values.
hasMissing :: Column -> Bool
hasMissing (OptionalColumn column) = True
hasMissing _ = False

-- | Checks if a column contains only missing values.
allMissing :: Column -> Bool
allMissing (OptionalColumn column) = VB.length (VB.filter isNothing column) == VB.length column
allMissing _ = False

-- | Checks if a column contains numeric values.
isNumeric :: Column -> Bool
isNumeric (UnboxedColumn (vec :: VU.Vector a)) = case sNumeric @a of
    STrue -> True
    _ -> False
isNumeric (BoxedColumn (vec :: VB.Vector a)) = case testEquality (typeRep @a) (typeRep @Integer) of
    Nothing -> False
    Just Refl -> True
isNumeric _ = False

-- | Checks if a column is of a given type values.
hasElemType :: forall a. (Columnable a) => Column -> Bool
hasElemType (BoxedColumn (column :: VB.Vector b)) = fromMaybe False $ do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure True
hasElemType (UnboxedColumn (column :: VU.Vector b)) = fromMaybe False $ do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure True
hasElemType (OptionalColumn (column :: VB.Vector b)) = fromMaybe False $ do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure True

-- | An internal/debugging function to get the column type of a column.
columnVersionString :: Column -> String
columnVersionString column = case column of
    BoxedColumn _ -> "Boxed"
    UnboxedColumn _ -> "Unboxed"
    OptionalColumn _ -> "Optional"

{- | An internal/debugging function to get the type stored in the outermost vector
of a column.
-}
columnTypeString :: Column -> String
columnTypeString column = case column of
    BoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
    UnboxedColumn (column :: VU.Vector a) -> show (typeRep @a)
    OptionalColumn (column :: VB.Vector a) -> show (typeRep @a)

instance (Show a) => Show (TypedColumn a) where
    show :: (Show a) => TypedColumn a -> String
    show (TColumn col) = show col

instance Show Column where
    show :: Column -> String
    show (BoxedColumn column) = show column
    show (UnboxedColumn column) = show column
    show (OptionalColumn column) = show column

instance Eq Column where
    (==) :: Column -> Column -> Bool
    (==) (BoxedColumn (a :: VB.Vector t1)) (BoxedColumn (b :: VB.Vector t2)) =
        case testEquality (typeRep @t1) (typeRep @t2) of
            Nothing -> False
            Just Refl -> a == b
    (==) (OptionalColumn (a :: VB.Vector t1)) (OptionalColumn (b :: VB.Vector t2)) =
        case testEquality (typeRep @t1) (typeRep @t2) of
            Nothing -> False
            Just Refl -> a == b
    (==) (UnboxedColumn (a :: VU.Vector t1)) (UnboxedColumn (b :: VU.Vector t2)) =
        case testEquality (typeRep @t1) (typeRep @t2) of
            Nothing -> False
            Just Refl -> a == b
    (==) _ _ = False

-- Generalised LEQ that does reflection.
generalLEQ ::
    forall a b. (Typeable a, Typeable b, Ord a, Ord b) => a -> b -> Bool
generalLEQ x y = case testEquality (typeRep @a) (typeRep @b) of
    Nothing -> False
    Just Refl -> x <= y

instance Ord Column where
    (<=) :: Column -> Column -> Bool
    (<=) (BoxedColumn (a :: VB.Vector t1)) (BoxedColumn (b :: VB.Vector t2)) = generalLEQ a b
    (<=) (OptionalColumn (a :: VB.Vector t1)) (OptionalColumn (b :: VB.Vector t2)) = generalLEQ a b
    (<=) (UnboxedColumn (a :: VU.Vector t1)) (UnboxedColumn (b :: VU.Vector t2)) = generalLEQ a b
    (<=) _ _ = False

{- | A class for converting a vector to a column of the appropriate type.
Given each Rep we tell the `toColumnRep` function which Column type to pick.
-}
class ColumnifyRep (r :: Rep) a where
    toColumnRep :: VB.Vector a -> Column

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

instance
    (Columnable a, VU.Unbox a) =>
    ColumnifyRep 'RUnboxed a
    where
    toColumnRep :: (Columnable a, VUM.Unbox a) => VB.Vector a -> Column
    toColumnRep = UnboxedColumn . VU.convert

instance
    (Columnable a) =>
    ColumnifyRep 'RBoxed a
    where
    toColumnRep :: (Columnable a) => VB.Vector a -> Column
    toColumnRep = BoxedColumn

instance
    (Columnable a) =>
    ColumnifyRep 'ROptional (Maybe a)
    where
    toColumnRep = OptionalColumn

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
fromUnboxedVector = UnboxedColumn

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

throwTypeMismatch ::
    forall (a :: Type) (b :: Type).
    (Typeable a, Typeable b) => Either DataFrameException Column
throwTypeMismatch =
    Left $
        TypeMismatchException
            MkTypeErrorContext
                { userType = Right (typeRep @b)
                , expectedType = Right (typeRep @a)
                , callingFunctionName = Just "mapColumn"
                , errorColumnName = Nothing
                }

-- | An internal function to map a function over the values of a column.
mapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (b -> c) -> Column -> Either DataFrameException Column
mapColumn f = \case
    BoxedColumn (col :: VB.Vector a) -> run col
    OptionalColumn (col :: VB.Vector a) -> run col
    UnboxedColumn (col :: VU.Vector a) -> runUnboxed col
  where
    run :: forall a. (Typeable a) => VB.Vector a -> Either DataFrameException Column
    run col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right (fromVector @c (VB.map f col))
        Nothing -> throwTypeMismatch @a @b

    runUnboxed ::
        forall a.
        (Typeable a, VU.Unbox a) => VU.Vector a -> Either DataFrameException Column
    runUnboxed col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ case sUnbox @c of
            STrue -> UnboxedColumn (VU.map f col)
            SFalse -> fromVector @c (VB.generate (VU.length col) (f . VU.unsafeIndex col))
        Nothing -> throwTypeMismatch @a @b
{-# SPECIALIZE mapColumn ::
    (Double -> Double) -> Column -> Either DataFrameException Column
    #-}
{-# INLINEABLE mapColumn #-}

-- | Applies a function that returns an unboxed result to an unboxed vector, storing the result in a column.
imapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (Int -> b -> c) -> Column -> Either DataFrameException Column
imapColumn f = \case
    BoxedColumn (col :: VB.Vector a) -> run col
    OptionalColumn (col :: VB.Vector a) -> run col
    UnboxedColumn (col :: VU.Vector a) -> runUnboxed col
  where
    run :: forall a. (Typeable a) => VB.Vector a -> Either DataFrameException Column
    run col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right (fromVector @c (VB.imap f col))
        Nothing -> throwTypeMismatch @a @b

    runUnboxed ::
        forall a.
        (Typeable a, VU.Unbox a) => VU.Vector a -> Either DataFrameException Column
    runUnboxed col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ case sUnbox @c of
            STrue -> UnboxedColumn (VU.imap f col)
            SFalse -> BoxedColumn (VB.imap f (VG.convert col))
        Nothing -> throwTypeMismatch @a @b

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs
columnLength (OptionalColumn xs) = VG.length xs
{-# INLINE columnLength #-}

-- | O(n) Gets the number of elements in the column.
numElements :: Column -> Int
numElements (BoxedColumn xs) = VG.length xs
numElements (UnboxedColumn xs) = VG.length xs
numElements (OptionalColumn xs) = VG.foldl' (\acc x -> acc + fromEnum (isJust x)) 0 xs
{-# INLINE numElements #-}

-- | O(n) Takes the first n values of a column.
takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (OptionalColumn xs) = OptionalColumn $ VG.take n xs
{-# INLINE takeColumn #-}

-- | O(n) Takes the last n values of a column.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

-- | O(n) Takes n values after a given column index.
sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (BoxedColumn xs) = BoxedColumn $ VG.slice start n xs
sliceColumn start n (UnboxedColumn xs) = UnboxedColumn $ VG.slice start n xs
sliceColumn start n (OptionalColumn xs) = OptionalColumn $ VG.slice start n xs
{-# INLINE sliceColumn #-}

-- | O(n) Selects the elements at a given set of indices. May change the order.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (OptionalColumn column) = OptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VU.ifilter (\i _ -> i `S.member` indexes) column
{-# INLINE atIndices #-}

-- | O(n) Selects the elements at a given set of indices. Does not change the order.
atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ VG.unsafeBackpermute column (VG.convert indexes)
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ VU.unsafeBackpermute column indexes
atIndicesStable indexes (OptionalColumn column) = OptionalColumn $ VG.unsafeBackpermute column (VG.convert indexes)
{-# INLINE atIndicesStable #-}

atIndicesWithNulls :: VB.Vector (Maybe Int) -> Column -> Column
atIndicesWithNulls indices column
    | VB.all isJust indices =
        atIndicesStable
            (VU.fromList (catMaybes (VB.toList indices)))
            column
    | otherwise = case column of
        BoxedColumn col ->
            OptionalColumn $ VB.map (fmap (col VB.!)) indices
        UnboxedColumn col ->
            OptionalColumn $ VB.map (fmap (col VU.!)) indices
        OptionalColumn col ->
            OptionalColumn $ VB.map (\ix -> ix >>= (col VB.!)) indices

-- | Internal helper to get indices in a boxed vector.
getIndices :: VU.Vector Int -> VB.Vector a -> VB.Vector a
getIndices indices xs = VB.generate (VU.length indices) (\i -> xs VB.! (indices VU.! i))
{-# INLINE getIndices #-}

-- | Internal helper to get indices in an unboxed vector.
getIndicesUnboxed :: (VU.Unbox a) => VU.Vector Int -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = VU.generate (VU.length indices) (\i -> xs VU.! (indices VU.! i))
{-# INLINE getIndicesUnboxed #-}

findIndices ::
    forall a.
    (Columnable a) =>
    (a -> Bool) ->
    Column ->
    Either DataFrameException (VU.Vector Int)
findIndices pred = \case
    BoxedColumn (v :: VB.Vector b) -> run v VG.convert
    OptionalColumn (v :: VB.Vector b) -> run v VG.convert
    UnboxedColumn (v :: VU.Vector b) -> run v id
  where
    run ::
        forall b v.
        (Typeable b, VG.Vector v b, VG.Vector v Int) =>
        v b ->
        (v Int -> VU.Vector Int) ->
        Either DataFrameException (VU.Vector Int)
    run column finalize = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right . finalize $ VG.findIndices pred column
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "findIndices"
                        , errorColumnName = Nothing
                        }

-- | An internal function that returns a vector of how indexes change after a column is sorted.
sortedIndexes :: Bool -> Column -> VU.Vector Int
sortedIndexes asc = \case
    BoxedColumn column -> sortWorker column
    UnboxedColumn column -> sortWorker column
    OptionalColumn column -> sortWorker column
  where
    sortWorker ::
        (VG.Vector v a, Ord a, VG.Vector v (Int, a), VG.Vector v Int) =>
        v a -> VU.Vector Int
    sortWorker column = runST $ do
        withIndexes <- VG.thaw $ VG.indexed column
        let cmp = if asc then compare else flip compare
        VA.sortBy (\(_, b) (_, b') -> cmp b b') withIndexes
        sorted <- VG.unsafeFreeze withIndexes
        return $ VG.convert $ VG.map fst sorted
{-# INLINE sortedIndexes #-}

-- | Fold (right) column with index.
ifoldrColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (Int -> a -> b -> b) -> b -> Column -> Either DataFrameException b
ifoldrColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.ifoldr f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "ifoldrColumn"
                    , errorColumnName = Nothing
                    }
                )
ifoldrColumn f acc c@(OptionalColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.ifoldr f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "ifoldrColumn"
                    , errorColumnName = Nothing
                    }
                )
ifoldrColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.ifoldr f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "ifoldrColumn"
                    , errorColumnName = Nothing
                    }
                )

foldlColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (b -> a -> b) -> b -> Column -> Either DataFrameException b
foldlColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl' f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldlColumn"
                    , errorColumnName = Nothing
                    }
                )
foldlColumn f acc c@(OptionalColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl' f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldlColumn"
                    , errorColumnName = Nothing
                    }
                )
foldlColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl' f acc column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldlColumn"
                    , errorColumnName = Nothing
                    }
                )

foldl1Column ::
    forall a.
    (Columnable a) =>
    (a -> a -> a) -> Column -> Either DataFrameException a
foldl1Column f c@(BoxedColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl1' f column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldl1Column"
                    , errorColumnName = Nothing
                    }
                )
foldl1Column f c@(OptionalColumn (column :: VB.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl1' f column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldl1Column"
                    , errorColumnName = Nothing
                    }
                )
foldl1Column f c@(UnboxedColumn (column :: VU.Vector d)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> pure $ VG.foldl1' f column
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "foldl1Column"
                    , errorColumnName = Nothing
                    }
                )

headColumn :: forall a. (Columnable a) => Column -> Either DataFrameException a
headColumn (BoxedColumn (col :: VB.Vector b)) = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl ->
        if VG.null col
            then Left (EmptyDataSetException "headColumn")
            else pure (VG.head col)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @b)
                    , callingFunctionName = Just "headColumn"
                    , errorColumnName = Nothing
                    }
                )
headColumn (UnboxedColumn (col :: VU.Vector b)) = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl ->
        if VG.null col
            then Left (EmptyDataSetException "headColumn")
            else pure (VG.head col)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @b)
                    , callingFunctionName = Just "headColumn"
                    , errorColumnName = Nothing
                    }
                )
headColumn (OptionalColumn (col :: VB.Vector b)) = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl ->
        if VG.null col
            then Left (EmptyDataSetException "headColumn")
            else pure (VG.head col)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @b)
                    , callingFunctionName = Just "headColumn"
                    , errorColumnName = Nothing
                    }
                )

-- | An internal, column version of zip.
zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VG.zip column other)
zipColumns (BoxedColumn column) (UnboxedColumn other) =
    BoxedColumn
        ( VB.generate
            (min (VG.length column) (VG.length other))
            (\i -> (column VG.! i, other VG.! i))
        )
zipColumns (BoxedColumn column) (OptionalColumn optcolumn) = BoxedColumn (VG.zip (VB.convert column) optcolumn)
zipColumns (UnboxedColumn column) (BoxedColumn other) =
    BoxedColumn
        ( VB.generate
            (min (VG.length column) (VG.length other))
            (\i -> (column VG.! i, other VG.! i))
        )
zipColumns (UnboxedColumn column) (UnboxedColumn other) = UnboxedColumn (VG.zip column other)
zipColumns (UnboxedColumn column) (OptionalColumn optcolumn) = BoxedColumn (VG.zip (VB.convert column) optcolumn)
zipColumns (OptionalColumn optcolumn) (BoxedColumn column) = BoxedColumn (VG.zip optcolumn (VB.convert column))
zipColumns (OptionalColumn optcolumn) (UnboxedColumn column) = BoxedColumn (VG.zip optcolumn (VB.convert column))
zipColumns (OptionalColumn optcolumn) (OptionalColumn optother) = BoxedColumn (VG.zip optcolumn optother)
{-# INLINE zipColumns #-}

-- | An internal, column version of zipWith.
zipWithColumns ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Column -> Column -> Either DataFrameException Column
zipWithColumns f (UnboxedColumn (column :: VU.Vector d)) (UnboxedColumn (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
        Just Refl -> pure $ case sUnbox @c of
            STrue -> fromUnboxedVector (VU.zipWith f column other)
            SFalse -> fromVector $ VB.zipWith f (VG.convert column) (VG.convert other)
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @b)
                        , expectedType = Right (typeRep @e)
                        , callingFunctionName = Just "zipWithColumns"
                        , errorColumnName = Nothing
                        }
                    )
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @a)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "zipWithColumns"
                    , errorColumnName = Nothing
                    }
                )
zipWithColumns f left right = case toVector @a left of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException (context{callingFunctionName = Just "zipWithColumns"})
    Left e -> Left e
    Right left' -> case toVector @b right of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException (context{callingFunctionName = Just "zipWithColumns"})
        Left e -> Left e
        Right right' -> pure $ fromVector $ VB.zipWith f left' right'
{-# INLINE zipWithColumns #-}

-- Functions for mutable columns (intended for IO).
writeColumn :: Int -> T.Text -> MutableColumn -> IO (Either T.Text Bool)
writeColumn i value (MBoxedColumn (col :: VBM.IOVector a)) =
    let
     in case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl ->
                ( if isNullish value
                    then VBM.unsafeWrite col i "" >> return (Left $! value)
                    else VBM.unsafeWrite col i value >> return (Right True)
                )
            Nothing -> return (Left value)
writeColumn i value (MUnboxedColumn (col :: VUM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> case readInt value of
            Just v -> VUM.unsafeWrite col i v >> return (Right True)
            Nothing -> VUM.unsafeWrite col i 0 >> return (Left value)
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Nothing -> return (Left $! value)
            Just Refl -> case readDouble value of
                Just v -> VUM.unsafeWrite col i v >> return (Right True)
                Nothing -> VUM.unsafeWrite col i 0 >> return (Left $! value)
{-# INLINE writeColumn #-}

freezeColumn' :: [(Int, T.Text)] -> MutableColumn -> IO Column
freezeColumn' nulls (MBoxedColumn col)
    | null nulls = BoxedColumn <$> VB.unsafeFreeze col
    | all (isNullish . snd) nulls =
        OptionalColumn
            . VB.imap (\i v -> if i `elem` map fst nulls then Nothing else Just v)
            <$> VB.unsafeFreeze col
    | otherwise =
        BoxedColumn
            . VB.imap
                ( \i v ->
                    if i `elem` map fst nulls
                        then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                        else Right v
                )
            <$> VB.unsafeFreeze col
freezeColumn' nulls (MUnboxedColumn col)
    | null nulls = UnboxedColumn <$> VU.unsafeFreeze col
    | all (isNullish . snd) nulls =
        VU.unsafeFreeze col >>= \c ->
            return $
                OptionalColumn $
                    VB.generate
                        (VU.length c)
                        (\i -> if i `elem` map fst nulls then Nothing else Just (c VU.! i))
    | otherwise =
        VU.unsafeFreeze col >>= \c ->
            return $
                BoxedColumn $
                    VB.generate
                        (VU.length c)
                        ( \i ->
                            if i `elem` map fst nulls
                                then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                                else Right (c VU.! i)
                        )
{-# INLINE freezeColumn' #-}

-- | Fills the end of a column, up to n, with Nothing. Does nothing if column has length greater than n.
expandColumn :: Int -> Column -> Column
expandColumn n (OptionalColumn col) = OptionalColumn $ col <> VB.replicate (n - VG.length col) Nothing
expandColumn n column@(BoxedColumn col)
    | n > VG.length col =
        OptionalColumn $ VB.map Just col <> VB.replicate (n - VG.length col) Nothing
    | otherwise = column
expandColumn n column@(UnboxedColumn col)
    | n > VG.length col =
        OptionalColumn $
            VB.map Just (VU.convert col) <> VB.replicate (n - VG.length col) Nothing
    | otherwise = column

-- | Fills the beginning of a column, up to n, with Nothing. Does nothing if column has length greater than n.
leftExpandColumn :: Int -> Column -> Column
leftExpandColumn n column@(OptionalColumn col)
    | n > VG.length col =
        OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> col
    | otherwise = column
leftExpandColumn n column@(BoxedColumn col)
    | n > VG.length col =
        OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> VG.map Just col
    | otherwise = column
leftExpandColumn n column@(UnboxedColumn col)
    | n > VG.length col =
        OptionalColumn $
            VG.replicate (n - VG.length col) Nothing <> VG.map Just (VU.convert col)
    | otherwise = column

{- | Concatenates two columns.
Returns Nothing if the columns are of different types.
-}
concatColumns :: Column -> Column -> Either DataFrameException Column
concatColumns (OptionalColumn left) (OptionalColumn right) = case testEquality (typeOf left) (typeOf right) of
    Just Refl -> pure (OptionalColumn $ left <> right)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeOf right)
                    , expectedType = Right (typeOf left)
                    , callingFunctionName = Just "concatColumns"
                    , errorColumnName = Nothing
                    }
                )
concatColumns (BoxedColumn left) (BoxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Just Refl -> pure (BoxedColumn $ left <> right)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeOf right)
                    , expectedType = Right (typeOf left)
                    , callingFunctionName = Just "concatColumns"
                    , errorColumnName = Nothing
                    }
                )
concatColumns (UnboxedColumn left) (UnboxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Just Refl -> pure (UnboxedColumn $ left <> right)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeOf right)
                    , expectedType = Right (typeOf left)
                    , callingFunctionName = Just "concatColumns"
                    , errorColumnName = Nothing
                    }
                )
concatColumns left right =
    Left $
        TypeMismatchException
            ( MkTypeErrorContext
                { userType = Right (typeOf right)
                , expectedType = Right (typeOf left)
                , callingFunctionName = Just "concatColumns"
                , errorColumnName = Nothing
                }
            )

{- | Concatenates two columns.

Works similar to 'concatColumns', but unlike that function, it will also combine columns of different types
by wrapping the values in an Either.

E.g. combining Column containing [1,2] with Column containing ["a","b"]
will result in a Column containing [Left 1, Left 2, Right "a", Right "b"].
-}
concatColumnsEither :: Column -> Column -> Column
concatColumnsEither (OptionalColumn left) (OptionalColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing ->
        OptionalColumn $ fmap (fmap Left) left <> fmap (fmap Right) right
    Just Refl ->
        OptionalColumn $ left <> right
concatColumnsEither (BoxedColumn left) (BoxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing ->
        BoxedColumn $ fmap Left left <> fmap Right right
    Just Refl ->
        BoxedColumn $ left <> right
concatColumnsEither (UnboxedColumn left) (UnboxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing ->
        BoxedColumn $ fmap Left (VG.convert left) <> fmap Right (VG.convert right)
    Just Refl ->
        UnboxedColumn $ left <> right
concatColumnsEither (BoxedColumn left) (UnboxedColumn right) =
    BoxedColumn $ fmap Left left <> fmap Right (VG.convert right)
concatColumnsEither (UnboxedColumn left) (BoxedColumn right) =
    BoxedColumn $ fmap Left (VG.convert left) <> fmap Right right
concatColumnsEither (OptionalColumn (left :: VB.Vector (Maybe a))) (BoxedColumn (right :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> OptionalColumn $ left <> fmap Just right
        Nothing -> OptionalColumn $ fmap (fmap Left) left <> fmap (Just . Right) right
concatColumnsEither (BoxedColumn (left :: VB.Vector a)) (OptionalColumn (right :: VB.Vector (Maybe b))) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> OptionalColumn $ fmap Just left <> right
        Nothing -> OptionalColumn $ fmap (Just . Left) left <> fmap (fmap Right) right
concatColumnsEither (OptionalColumn (left :: VB.Vector (Maybe a))) (UnboxedColumn (right :: VU.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> OptionalColumn $ left <> fmap Just (VG.convert right)
        Nothing ->
            OptionalColumn $ fmap (fmap Left) left <> fmap (Just . Right) (VG.convert right)
concatColumnsEither (UnboxedColumn (left :: VU.Vector a)) (OptionalColumn (right :: VB.Vector (Maybe b))) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> OptionalColumn $ fmap Just (VG.convert left) <> right
        Nothing ->
            OptionalColumn $ fmap (Just . Left) (VG.convert left) <> fmap (fmap Right) right

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

{- | Converts a column to a vector of a specific type.

This is a type-safe conversion that requires the column's element type
to exactly match the requested type. You must specify the desired type
via type applications.

==== __Type Parameters__

[@a@] The element type to convert to
[@v@] The vector type (e.g., 'VU.Vector', 'VB.Vector')

==== __Examples__

>>> toVector @Int @VU.Vector column
Right (unboxed vector of Ints)

>>> toVector @Text @VB.Vector column
Right (boxed vector of Text)

==== __Returns__

* 'Right' - The converted vector if types match
* 'Left' 'TypeMismatchException' - If the column's type doesn't match the requested type

==== __See also__

For numeric conversions with automatic type coercion, see 'toDoubleVector',
'toFloatVector', and 'toIntVector'.
-}
toVector ::
    forall a v.
    (VG.Vector v a, Columnable a) => Column -> Either DataFrameException (v a)
toVector column@(OptionalColumn (col :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVector"
                        , errorColumnName = Nothing
                        }
                    )
toVector (BoxedColumn (col :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVector"
                        , errorColumnName = Nothing
                        }
                    )
toVector (UnboxedColumn (col :: VU.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVector"
                        , errorColumnName = Nothing
                        }
                    )

-- Some common types we will use for numerical computing.

{- | Converts a column to an unboxed vector of 'Double' values.

This function performs intelligent type coercion for numeric types:

* If the column is already 'Double', returns it directly
* If the column contains other floating-point types, converts via 'realToFrac'
* If the column contains integral types, converts via 'fromIntegral' (beware of overflow if the type is `Integer`).

==== __Optional column handling__

For 'OptionalColumn' types, 'Nothing' values are converted to @NaN@ (Not a Number).
This allows optional numeric data to be represented in the resulting vector.

==== __Returns__

* 'Right' - The converted 'Double' vector
* 'Left' 'TypeMismatchException' - If the column is not numeric
-}
toDoubleVector :: Column -> Either DataFrameException (VU.Vector Double)
toDoubleVector column =
    case column of
        UnboxedColumn (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Double) of
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
        OptionalColumn (f :: VB.Vector (Maybe a)) -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> Right (VB.convert $ VB.map (fromMaybe (read @Double "NaN")) f)
            Nothing -> case sFloating @a of
                STrue ->
                    Right
                        (VB.convert $ VB.map (maybe (read @Double "NaN") realToFrac) f)
                SFalse -> case sIntegral @a of
                    STrue ->
                        Right
                            (VB.convert $ VB.map (maybe (read @Double "NaN") fromIntegral) f)
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
        BoxedColumn (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
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

{- | Converts a column to an unboxed vector of 'Float' values.

This function performs intelligent type coercion for numeric types:

* If the column is already 'Float', returns it directly
* If the column contains other floating-point types, converts via 'realToFrac'
* If the column contains integral types, converts via 'fromIntegral'
* If the column is boxed 'Integer', converts via 'fromIntegral' (beware of overflow for 64-bit integers and `Integer`)

==== __Optional column handling__

For 'OptionalColumn' types, 'Nothing' values are converted to @NaN@ (Not a Number).
This allows optional numeric data to be represented in the resulting vector.

==== __Returns__

* 'Right' - The converted 'Float' vector
* 'Left' 'TypeMismatchException' - If the column is not numeric

==== __Precision warning__

Converting from 'Double' to 'Float' may result in loss of precision.
-}
toFloatVector :: Column -> Either DataFrameException (VU.Vector Float)
toFloatVector column =
    case column of
        UnboxedColumn (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Float) of
            Just Refl -> Right f
            Nothing -> case sFloating @a of
                STrue -> Right (VU.map realToFrac f)
                SFalse -> case sIntegral @a of
                    STrue -> Right (VU.map fromIntegral f)
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
        OptionalColumn (f :: VB.Vector (Maybe a)) -> case testEquality (typeRep @a) (typeRep @Float) of
            Just Refl -> Right (VB.convert $ VB.map (fromMaybe (read @Float "NaN")) f)
            Nothing -> case sFloating @a of
                STrue ->
                    Right
                        (VB.convert $ VB.map (maybe (read @Float "NaN") realToFrac) f)
                SFalse -> case sIntegral @a of
                    STrue ->
                        Right
                            (VB.convert $ VB.map (maybe (read @Float "NaN") fromIntegral) f)
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
        BoxedColumn (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
            Just Refl -> Right (VB.convert $ VB.map fromIntegral f)
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

{- | Converts a column to an unboxed vector of 'Int' values.

This function performs intelligent type coercion for numeric types:

* If the column is already 'Int', returns it directly
* If the column contains floating-point types, rounds via 'round' and converts
* If the column contains other integral types, converts via 'fromIntegral'
* If the column is boxed 'Integer', converts via 'fromIntegral'

==== __Returns__

* 'Right' - The converted 'Int' vector
* 'Left' 'TypeMismatchException' - If the column is not numeric

==== __Note__

Unlike 'toDoubleVector' and 'toFloatVector', this function does NOT support
'OptionalColumn'. Optional columns must be handled separately.

==== __Rounding behavior__

Floating-point values are rounded to the nearest integer using 'round'.
For example: 2.5 rounds to 2, 3.5 rounds to 4 (banker's rounding).
-}
toIntVector :: Column -> Either DataFrameException (VU.Vector Int)
toIntVector column =
    case column of
        UnboxedColumn (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> Right f
            Nothing -> case sFloating @a of
                STrue -> Right (VU.map (round . realToFrac) f)
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
        BoxedColumn (f :: VB.Vector a) -> case testEquality (typeRep @a) (typeRep @Integer) of
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
        _ ->
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
        UnboxedColumn (f :: VU.Vector b) -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> Right f
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Int)
                            , expectedType = Right (typeRep @a)
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
{-# SPECIALIZE toUnboxedVector ::
    Column -> Either DataFrameException (VU.Vector Double)
    #-}
{-# INLINE toUnboxedVector #-}
