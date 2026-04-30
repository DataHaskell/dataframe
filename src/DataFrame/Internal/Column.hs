{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
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

import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.DeepSeq (NFData (..), rnf)
import Control.Exception (throw)
import Control.Monad (forM_, when)
import Control.Monad.ST (ST, runST)
import Data.Bits (
    complement,
    popCount,
    setBit,
    shiftL,
    shiftR,
    testBit,
    (.&.),
 )
import Data.Kind (Type)
import Data.Maybe
import Data.These
import Data.Type.Equality (TestEquality (..))
import Data.Word (Word8)
import DataFrame.Errors
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import System.IO.Unsafe (unsafePerformIO)
import System.Random
import Type.Reflection

-- | A bit-packed validity bitmap. Bit @i@ = 1 means row @i@ is valid (not null).
type Bitmap = VU.Vector Word8

{- | Our representation of a column is a GADT that can store data based on the underlying data.

This allows us to pattern match on data kinds and limit some operations to only some
kinds of vectors. Nullability is represented via an optional bit-packed 'Bitmap':
@Nothing@ = no nulls; @Just bm@ = bit @i@ of @bm@ is 1 iff row @i@ is valid.
-}
data Column where
    BoxedColumn :: (Columnable a) => Maybe Bitmap -> VB.Vector a -> Column
    UnboxedColumn ::
        (Columnable a, VU.Unbox a) => Maybe Bitmap -> VU.Vector a -> Column

{- | A mutable companion struct to dataframe columns.

Used mostly as an intermediate structure for I/O.
-}
data MutableColumn where
    MBoxedColumn :: (Columnable a) => VBM.IOVector a -> MutableColumn
    MUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> MutableColumn

-- ---------------------------------------------------------------------------
-- Bitmap helpers
-- ---------------------------------------------------------------------------

-- | Test whether row @i@ is valid (not null) in a bitmap.
bitmapTestBit :: Bitmap -> Int -> Bool
bitmapTestBit bm i = testBit (VU.unsafeIndex bm (i `shiftR` 3)) (i .&. 7)
{-# INLINE bitmapTestBit #-}

-- | Build a fully-valid bitmap for @n@ rows (all bits set).
allValidBitmap :: Int -> Bitmap
allValidBitmap n =
    let bytes = (n + 7) `shiftR` 3
        lastBits = n .&. 7
        full = VU.replicate (bytes - 1) 0xFF
        lastByte = if lastBits == 0 then 0xFF else (1 `shiftL` lastBits) - 1
     in if bytes == 0 then VU.empty else VU.snoc full lastByte
{-# INLINE allValidBitmap #-}

{- | Build a bitmap from a @VU.Vector Word8@ validity vector
(1 = valid, 0 = null), as produced by Arrow / Parquet decoders.
-}
buildBitmapFromValid :: VU.Vector Word8 -> Bitmap
buildBitmapFromValid valid =
    let n = VU.length valid
        bytes = (n + 7) `shiftR` 3
     in VU.generate bytes $ \b ->
            let base = b `shiftL` 3
                setBitIf acc bit =
                    let idx = base + bit
                     in if idx < n && VU.unsafeIndex valid idx /= 0
                            then setBit acc bit
                            else acc
             in foldl setBitIf (0 :: Word8) [0 .. 7]

{- | Build a bitmap from a list of null-row indices.
@nullIdxs@ are the positions that are NULL.
-}
buildBitmapFromNulls :: Int -> [Int] -> Bitmap
buildBitmapFromNulls n nullIdxs =
    let base = allValidBitmap n
     in VU.modify
            ( \mv ->
                forM_ nullIdxs $ \i -> do
                    let byteIdx = i `shiftR` 3
                        bitIdx = i .&. 7
                    v <- VUM.unsafeRead mv byteIdx
                    VUM.unsafeWrite mv byteIdx (clearBit8 v bitIdx)
            )
            base
  where
    clearBit8 :: Word8 -> Int -> Word8
    clearBit8 b bit = b .&. complement (1 `shiftL` bit)

-- | Slice a bitmap for rows @[start .. start+len-1]@.
bitmapSlice :: Int -> Int -> Bitmap -> Bitmap
bitmapSlice start len bm
    | start .&. 7 == 0 =
        -- byte-aligned: simple slice; clamp so we never ask for more bytes than exist
        let startByte = start `shiftR` 3
            bytes = min ((len + 7) `shiftR` 3) (VU.length bm - startByte)
         in VU.slice startByte bytes bm
    | otherwise =
        -- non-aligned: unpack bit-by-bit and repack
        let n = min len (VU.length bm `shiftL` 3 - start)
         in buildBitmapFromValid $
                VU.generate n $
                    \i -> if bitmapTestBit bm (start + i) then 1 else 0

-- | Concatenate two bitmaps covering @n1@ and @n2@ rows respectively.
bitmapConcat :: Int -> Bitmap -> Int -> Bitmap -> Bitmap
bitmapConcat n1 bm1 n2 bm2 =
    buildBitmapFromValid $
        VU.generate (n1 + n2) $ \i ->
            if i < n1
                then if bitmapTestBit bm1 i then 1 else 0
                else if bitmapTestBit bm2 (i - n1) then 1 else 0

-- | Combine two bitmaps with AND (both must be valid for result to be valid).
mergeBitmaps :: Bitmap -> Bitmap -> Bitmap
mergeBitmaps = VU.zipWith (.&.)

{- | Materialize a nullable column from @VB.Vector (Maybe a)@.
When @a@ is unboxable, creates an 'UnboxedColumn' (more compact).
Otherwise creates a 'BoxedColumn'.
Always attaches a bitmap so the column is recognized as nullable even when
no 'Nothing' values are present (preserves the Maybe type marker).
-}
fromMaybeVec :: forall a. (Columnable a) => VB.Vector (Maybe a) -> Column
fromMaybeVec v = case sUnbox @a of
    STrue -> fromMaybeVecUnboxed v
    SFalse ->
        let n = VB.length v
            nullIdxs = [i | i <- [0 .. n - 1], isNothing (VB.unsafeIndex v i)]
            bm = if null nullIdxs then allValidBitmap n else buildBitmapFromNulls n nullIdxs
            dat = VB.map (fromMaybe (errorWithoutStackTrace "fromMaybeVec: Nothing slot")) v
         in BoxedColumn (Just bm) dat

{- | Materialize a nullable 'UnboxedColumn' to @VB.Vector (Maybe a)@ using runST.
Always attaches a bitmap so the column is recognized as nullable even when
no 'Nothing' values are present (preserves the Maybe type marker).
-}
fromMaybeVecUnboxed ::
    forall a. (Columnable a, VU.Unbox a) => VB.Vector (Maybe a) -> Column
fromMaybeVecUnboxed v =
    let n = VB.length v
        nullIdxs = [i | i <- [0 .. n - 1], isNothing (VB.unsafeIndex v i)]
        bm = if null nullIdxs then allValidBitmap n else buildBitmapFromNulls n nullIdxs
        dat = runST $ do
            mv <- VUM.new n
            VG.iforM_ v $ \i mx -> forM_ mx (VUM.unsafeWrite mv i)
            VU.unsafeFreeze mv
     in UnboxedColumn (Just bm) dat

-- | Materialize an element from a column at index @i@, respecting the bitmap.
columnElemIsNull :: Column -> Int -> Bool
columnElemIsNull (BoxedColumn (Just bm) _) i = not (bitmapTestBit bm i)
columnElemIsNull (UnboxedColumn (Just bm) _) i = not (bitmapTestBit bm i)
columnElemIsNull _ _ = False

-- | Return the 'Maybe Bitmap' from a column.
columnBitmap :: Column -> Maybe Bitmap
columnBitmap (BoxedColumn bm _) = bm
columnBitmap (UnboxedColumn bm _) = bm

-- ---------------------------------------------------------------------------
-- End bitmap helpers
-- ---------------------------------------------------------------------------

{- | A TypedColumn is a wrapper around our type-erased column.
It is used to type check expressions on columns.

Note: there is no guarantee that the Phanton type is the
same as the underlying vector type.
-}
data TypedColumn a where
    TColumn :: (Columnable a) => Column -> TypedColumn a

instance (Eq a) => Eq (TypedColumn a) where
    (==) :: (Eq a) => TypedColumn a -> TypedColumn a -> Bool
    (==) (TColumn a) (TColumn b) = a == b

-- | Gets the underlying value from a TypedColumn.
unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

-- | Gets the underlying vector from a TypedColumn.
vectorFromTypedColumn :: TypedColumn a -> VB.Vector a
vectorFromTypedColumn (TColumn value) = either throw id (toVector value)

-- | Checks if a column contains missing values (has a bitmap).
hasMissing :: Column -> Bool
hasMissing (BoxedColumn (Just _) _) = True
hasMissing (UnboxedColumn (Just _) _) = True
hasMissing _ = False

-- | Checks if a column contains only missing values.
allMissing :: Column -> Bool
allMissing (BoxedColumn (Just bm) col) = VU.all (== 0) bm && not (VB.null col)
allMissing (UnboxedColumn (Just bm) col) = VU.all (== 0) bm && not (VU.null col)
allMissing _ = False

-- | Checks if a column contains numeric values.
isNumeric :: Column -> Bool
isNumeric (UnboxedColumn _ (_vec :: VU.Vector a)) = case sNumeric @a of
    STrue -> True
    _ -> False
isNumeric (BoxedColumn _ (_vec :: VB.Vector a)) = case testEquality (typeRep @a) (typeRep @Integer) of
    Nothing -> False
    Just Refl -> True

{- | Checks if a column is of a given type values.
For nullable columns (@BoxedColumn (Just _)@ or @UnboxedColumn (Just _)@),
also returns @True@ when @a = Maybe b@ and the column stores @b@ internally.
-}
hasElemType :: forall a. (Columnable a) => Column -> Bool
hasElemType = \case
    BoxedColumn bm (_column :: VB.Vector b) -> checkBoxed bm (typeRep @b)
    UnboxedColumn bm (_column :: VU.Vector b) -> checkUnboxed bm (typeRep @b)
  where
    -- Direct type match
    directMatch :: forall (b :: Type). TypeRep b -> Bool
    directMatch = isJust . testEquality (typeRep @a)
    -- For a nullable column (has bitmap), also accept a = Maybe b
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

{- | An internal/debugging function to get the type stored in the outermost vector
of a column.
-}
columnTypeString :: Column -> String
columnTypeString column = case column of
    BoxedColumn Nothing (_ :: VB.Vector a) -> show (typeRep @a)
    BoxedColumn (Just _) (_ :: VB.Vector a) -> showMaybeType @a
    UnboxedColumn Nothing (_ :: VU.Vector a) -> show (typeRep @a)
    UnboxedColumn (Just _) (_ :: VU.Vector a) -> showMaybeType @a
  where
    showMaybeType :: forall a. (Typeable a) => String
    showMaybeType =
        let s = show (typeRep @a)
         in "Maybe " ++ if ' ' `elem` s then "(" ++ s ++ ")" else s

instance (Show a) => Show (TypedColumn a) where
    show :: (Show a) => TypedColumn a -> String
    show (TColumn col) = show col

instance NFData Column where
    rnf (BoxedColumn Nothing (v :: VB.Vector a)) = VB.foldl' (const (`seq` ())) () v
    rnf (BoxedColumn (Just bm) (v :: VB.Vector a)) =
        let n = VB.length v
            go !i
                | i >= n = ()
                | bitmapTestBit bm i = VB.unsafeIndex v i `seq` go (i + 1)
                | otherwise = go (i + 1)
         in go 0
    rnf (UnboxedColumn _ v) = v `seq` ()

instance Show Column where
    show :: Column -> String
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
    (==) _ _ = False

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
    toColumnRep = fromMaybeVec

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

-- | An internal function to map a function over the values of a column.
mapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (b -> c) -> Column -> Either DataFrameException Column
mapColumn f = \case
    BoxedColumn bm (col :: VB.Vector a) -> runBoxed bm col
    UnboxedColumn bm (col :: VU.Vector a) -> runUnboxed bm col
  where
    runBoxed ::
        forall a.
        (Columnable a) =>
        Maybe Bitmap -> VB.Vector a -> Either DataFrameException Column
    runBoxed bm col = case testEquality (typeRep @b) (typeRep @(Maybe a)) of
        -- user maps over Maybe a (nullable column as Maybe)
        Just Refl ->
            let !n = VB.length col
             in -- Build result directly without intermediate Maybe vector to avoid
                -- fusion forcing null slots via VU.convert.
                Right $ case sUnbox @c of
                    STrue -> UnboxedColumn Nothing $
                        VU.generate n $ \i ->
                            f
                                ( if maybe True (`bitmapTestBit` i) bm
                                    then Just (VB.unsafeIndex col i)
                                    else Nothing
                                )
                    SFalse -> fromVector @c $
                        VB.generate n $ \i ->
                            f
                                ( if maybe True (`bitmapTestBit` i) bm
                                    then Just (VB.unsafeIndex col i)
                                    else Nothing
                                )
        Nothing -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl ->
                -- user maps over inner type a; preserve bitmap
                Right $ case sUnbox @c of
                    STrue -> UnboxedColumn bm (VU.generate (VB.length col) (f . VB.unsafeIndex col))
                    SFalse -> BoxedColumn bm (VB.map f col)
            Nothing -> throwTypeMismatch @a @b

    runUnboxed ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        Maybe Bitmap -> VU.Vector a -> Either DataFrameException Column
    runUnboxed bm col = case testEquality (typeRep @b) (typeRep @(Maybe a)) of
        Just Refl ->
            let !n = VU.length col
             in Right $ case sUnbox @c of
                    STrue -> UnboxedColumn Nothing $
                        VU.generate n $ \i ->
                            f
                                ( if maybe True (`bitmapTestBit` i) bm
                                    then Just (VU.unsafeIndex col i)
                                    else Nothing
                                )
                    SFalse -> fromVector @c $
                        VB.generate n $ \i ->
                            f
                                ( if maybe True (`bitmapTestBit` i) bm
                                    then Just (VU.unsafeIndex col i)
                                    else Nothing
                                )
        Nothing -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> Right $ case sUnbox @c of
                STrue -> UnboxedColumn bm (VU.map f col)
                SFalse -> BoxedColumn bm (VB.generate (VU.length col) (f . VU.unsafeIndex col))
            Nothing -> throwTypeMismatch @a @b
{-# INLINEABLE mapColumn #-}

-- | Applies a function that returns an unboxed result to an unboxed vector, storing the result in a column.
imapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (Int -> b -> c) -> Column -> Either DataFrameException Column
imapColumn f = \case
    BoxedColumn bm (col :: VB.Vector a) -> runBoxed bm col
    UnboxedColumn bm (col :: VU.Vector a) -> runUnboxed bm col
  where
    runBoxed ::
        forall a.
        (Columnable a) =>
        Maybe Bitmap -> VB.Vector a -> Either DataFrameException Column
    runBoxed bm col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ case sUnbox @c of
            STrue ->
                UnboxedColumn
                    bm
                    (VU.generate (VB.length col) (\i -> f i (VB.unsafeIndex col i)))
            SFalse -> BoxedColumn bm (VB.imap f col)
        Nothing -> throwTypeMismatch @a @b

    runUnboxed ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        Maybe Bitmap -> VU.Vector a -> Either DataFrameException Column
    runUnboxed bm col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ case sUnbox @c of
            STrue -> UnboxedColumn bm (VU.imap f col)
            SFalse -> BoxedColumn bm (VB.imap f (VG.convert col))
        Nothing -> throwTypeMismatch @a @b

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn _ xs) = VB.length xs
columnLength (UnboxedColumn _ xs) = VU.length xs
{-# INLINE columnLength #-}

-- | O(n) Gets the number of non-null elements in the column.
numElements :: Column -> Int
numElements (BoxedColumn Nothing xs) = VB.length xs
numElements (BoxedColumn (Just bm) _xs) = VU.foldl' (\acc b -> acc + popCount b) 0 bm
numElements (UnboxedColumn Nothing xs) = VU.length xs
numElements (UnboxedColumn (Just bm) _xs) = VU.foldl' (\acc b -> acc + popCount b) 0 bm
{-# INLINE numElements #-}

-- | O(n) Takes the first n values of a column.
takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn bm xs) =
    BoxedColumn (fmap (bitmapSlice 0 n) bm) (VG.take n xs)
takeColumn n (UnboxedColumn bm xs) =
    UnboxedColumn (fmap (bitmapSlice 0 n) bm) (VG.take n xs)
{-# INLINE takeColumn #-}

-- | O(n) Takes the last n values of a column.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

-- | O(n) Takes n values after a given column index.
sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (BoxedColumn bm xs) =
    BoxedColumn (fmap (bitmapSlice start n) bm) (VG.slice start n xs)
sliceColumn start n (UnboxedColumn bm xs) =
    UnboxedColumn (fmap (bitmapSlice start n) bm) (VG.slice start n xs)
{-# INLINE sliceColumn #-}

-- | O(n) Selects the elements at a given set of indices. Does not change the order.
atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn bm column) =
    BoxedColumn
        ( fmap
            ( \bm0 ->
                buildBitmapFromValid $
                    VU.map (\i -> if bitmapTestBit bm0 i then 1 else 0) indexes
            )
            bm
        )
        ( VB.generate
            (VU.length indexes)
            ((column `VB.unsafeIndex`) . (indexes `VU.unsafeIndex`))
        )
atIndicesStable indexes (UnboxedColumn bm column) =
    UnboxedColumn
        ( fmap
            ( \bm0 ->
                buildBitmapFromValid $
                    VU.map (\i -> if bitmapTestBit bm0 i then 1 else 0) indexes
            )
            bm
        )
        (VU.unsafeBackpermute column indexes)
{-# INLINE atIndicesStable #-}

{- | Like 'atIndicesStable' but treats negative indices as null.
Keeps the index vector fully unboxed (no @VB.Vector (Maybe Int)@).
-}
gatherWithSentinel :: VU.Vector Int -> Column -> Column
gatherWithSentinel indices col =
    let !n = VU.length indices
        newBm = buildBitmapFromValid $ VU.generate n $ \i ->
            if VU.unsafeIndex indices i < 0 then 0 else 1
     in case col of
            BoxedColumn srcBm v ->
                let dat = VB.generate n $ \i ->
                        let !idx = VU.unsafeIndex indices i
                         in if idx < 0 then VB.unsafeIndex v 0 else VB.unsafeIndex v idx
                    bm = case srcBm of
                        Nothing -> Just newBm
                        Just sb ->
                            Just
                                ( mergeBitmaps
                                    newBm
                                    ( buildBitmapFromValid $ VU.generate n $ \i ->
                                        let idx = VU.unsafeIndex indices i
                                         in if idx >= 0 && bitmapTestBit sb idx then 1 else 0
                                    )
                                )
                 in BoxedColumn bm dat
            UnboxedColumn srcBm v ->
                let dat = runST $ do
                        mv <- VUM.new n
                        VG.iforM_ indices $ \i idx ->
                            when (idx >= 0) $ VUM.unsafeWrite mv i (VU.unsafeIndex v idx)
                        VU.unsafeFreeze mv
                    bm = case srcBm of
                        Nothing -> Just newBm
                        Just sb ->
                            Just
                                ( mergeBitmaps
                                    newBm
                                    ( buildBitmapFromValid $ VU.generate n $ \i ->
                                        let idx = VU.unsafeIndex indices i
                                         in if idx >= 0 && bitmapTestBit sb idx then 1 else 0
                                    )
                                )
                 in UnboxedColumn bm dat
{-# INLINE gatherWithSentinel #-}

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
findIndices predicate = \case
    BoxedColumn _ (v :: VB.Vector b) -> run v VG.convert
    UnboxedColumn _ (v :: VU.Vector b) -> run v id
  where
    run ::
        forall b v.
        (Typeable b, VG.Vector v b, VG.Vector v Int) =>
        v b ->
        (v Int -> VU.Vector Int) ->
        Either DataFrameException (VU.Vector Int)
    run column finalize = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right . finalize $ VG.findIndices predicate column
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "findIndices"
                        , errorColumnName = Nothing
                        }

-- | Fold (right) column with index.
ifoldrColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (Int -> a -> b -> b) -> b -> Column -> Either DataFrameException b
ifoldrColumn f acc = \case
    BoxedColumn _ column -> foldrWorker column
    UnboxedColumn _ column -> foldrWorker column
  where
    foldrWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException b
    foldrWorker vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> pure $ VG.ifoldr f acc vec
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "ifoldrColumn"
                        , errorColumnName = Nothing
                        }
                    )

foldlColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (b -> a -> b) -> b -> Column -> Either DataFrameException b
foldlColumn f acc = \case
    BoxedColumn _ column -> foldlWorker column
    UnboxedColumn _ column -> foldlWorker column
  where
    foldlWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException b
    foldlWorker vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> pure $ VG.foldl' f acc vec
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "ifoldrColumn"
                        , errorColumnName = Nothing
                        }
                    )

foldl1Column ::
    forall a.
    (Columnable a) =>
    (a -> a -> a) -> Column -> Either DataFrameException a
foldl1Column f = \case
    BoxedColumn _ column -> foldl1Worker column
    UnboxedColumn _ column -> foldl1Worker column
  where
    foldl1Worker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException a
    foldl1Worker vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> pure $ VG.foldl1' f vec
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "foldl1Column"
                        , errorColumnName = Nothing
                        }
                    )

{- | O(n) Seedless fold over groups using the first element of each group as seed.
Like 'foldDirectGroups' but for the case where no initial accumulator is available.
-}
foldl1DirectGroups ::
    forall a.
    (Columnable a) =>
    (a -> a -> a) ->
    Column ->
    VU.Vector Int ->
    VU.Vector Int ->
    Either DataFrameException Column
foldl1DirectGroups f col valueIndices offsets
    | VU.length offsets <= 1 = pure $ fromVector @a VB.empty
    | otherwise = case col of
        UnboxedColumn _ (vec :: VU.Vector d) -> UnboxedColumn Nothing <$> foldl1Worker vec
        BoxedColumn _ (vec :: VB.Vector d) -> BoxedColumn Nothing <$> foldl1Worker vec
  where
    foldl1Worker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException (v c)
    foldl1Worker vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl ->
            Right $
                VG.generate (VU.length offsets - 1) foldGroup
          where
            foldGroup k =
                let !s = VU.unsafeIndex offsets k
                    !e = VU.unsafeIndex offsets (k + 1)
                    !seed = VG.unsafeIndex vec (VU.unsafeIndex valueIndices s)
                 in go (s + 1) e seed
            go !i !e !acc
                | i >= e = acc
                | otherwise =
                    go (i + 1) e $!
                        f acc (VG.unsafeIndex vec (VU.unsafeIndex valueIndices i))
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "foldl1DirectGroups"
                        , errorColumnName = Nothing
                        }
{-# INLINEABLE foldl1DirectGroups #-}

{- | O(n) fold over groups by scanning the column LINEARLY.
rowToGroup[i] = group index for row i.
Avoids random column reads; random writes go to the accumulator array which is
small (nGroups entries) and typically cache-resident.
When @acc@ is unboxable, uses an unboxed mutable vector for the accumulator
array, eliminating pointer indirection on every read/write.
-}
foldLinearGroups ::
    forall b acc.
    (Columnable b, Columnable acc) =>
    (acc -> b -> acc) ->
    acc ->
    Column ->
    VU.Vector Int -> -- rowToGroup (length n)
    Int -> -- nGroups
    Either DataFrameException Column
foldLinearGroups f seed col rowToGroup nGroups
    | nGroups == 0 = Right (fromVector @acc VB.empty)
    | otherwise = case col of
        UnboxedColumn _ (vec :: VU.Vector d) -> foldLinearWorker vec
        BoxedColumn _ (vec :: VB.Vector d) -> foldLinearWorker vec
  where
    foldLinearWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException Column
    foldLinearWorker vec = case testEquality (typeRep @b) (typeRep @c) of
        Just Refl ->
            Right $
                unsafePerformIO $
                    runWith
                        ( \readAt writeAt ->
                            VG.iforM_ vec $ \row x -> do
                                let !k = VG.unsafeIndex rowToGroup row
                                cur <- readAt k
                                writeAt k $! f cur x
                        )
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @b)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "foldLinearGroups"
                        , errorColumnName = Nothing
                        }

    -- \| Allocate accumulators, run the traversal, return a frozen Column.
    -- When @acc@ is unboxable, uses an unboxed mutable vector (no pointer
    -- indirection per read/write) and returns UnboxedColumn directly —
    -- avoiding a round-trip through VB.Vector.
    runWith :: ((Int -> IO acc) -> (Int -> acc -> IO ()) -> IO ()) -> IO Column
    runWith body = case sUnbox @acc of
        STrue -> do
            accs <- VUM.replicate nGroups seed
            body (VUM.unsafeRead accs) (VUM.unsafeWrite accs)
            UnboxedColumn Nothing <$> VU.unsafeFreeze accs
        SFalse -> do
            accs <- VBM.replicate nGroups seed
            body (VBM.unsafeRead accs) (VBM.unsafeWrite accs)
            fromVector @acc <$> VB.unsafeFreeze accs
    {-# INLINE runWith #-}
{-# INLINEABLE foldLinearGroups #-}

headColumn :: forall a. (Columnable a) => Column -> Either DataFrameException a
headColumn = \case
    BoxedColumn _ col -> headWorker col
    UnboxedColumn _ col -> headWorker col
  where
    headWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        v c ->
        Either DataFrameException a
    headWorker vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl ->
            if VG.null vec
                then Left (EmptyDataSetException "headColumn")
                else pure (VG.head vec)
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "headColumn"
                        , errorColumnName = Nothing
                        }
                    )

-- | An internal, column version of zip.
zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn _ column) (BoxedColumn _ other) = BoxedColumn Nothing (VG.zip column other)
zipColumns (BoxedColumn _ column) (UnboxedColumn _ other) =
    BoxedColumn
        Nothing
        ( VB.generate
            (min (VG.length column) (VG.length other))
            (\i -> (column VG.! i, other VG.! i))
        )
zipColumns (UnboxedColumn _ column) (BoxedColumn _ other) =
    BoxedColumn
        Nothing
        ( VB.generate
            (min (VG.length column) (VG.length other))
            (\i -> (column VG.! i, other VG.! i))
        )
zipColumns (UnboxedColumn _ column) (UnboxedColumn _ other) = UnboxedColumn Nothing (VG.zip column other)
{-# INLINE zipColumns #-}

-- | Merge two columns using `These`.
mergeColumns :: Column -> Column -> Column
mergeColumns colA colB = case (colA, colB) of
    (BoxedColumn bmA c1, BoxedColumn bmB c2) -> case (bmA, bmB) of
        (Just ba, Just bb) ->
            BoxedColumn Nothing $ mkVec c1 c2 $ \i v1 v2 ->
                let nullA = not (bitmapTestBit ba i)
                    nullB = not (bitmapTestBit bb i)
                 in case (nullA, nullB) of
                        (True, True) -> error "mergeColumns: both null"
                        (False, True) -> This v1
                        (True, False) -> That v2
                        (False, False) -> These v1 v2
        (Just ba, Nothing) ->
            BoxedColumn Nothing $ mkVec c1 c2 $ \i v1 v2 ->
                if not (bitmapTestBit ba i) then That v2 else These v1 v2
        (Nothing, Just bb) ->
            BoxedColumn Nothing $ mkVec c1 c2 $ \i v1 v2 ->
                if not (bitmapTestBit bb i) then This v1 else These v1 v2
        (Nothing, Nothing) ->
            BoxedColumn Nothing $ mkVecSimple c1 c2 These
    (BoxedColumn _ c1, UnboxedColumn _ c2) ->
        BoxedColumn Nothing $ mkVecSimple c1 c2 These
    (UnboxedColumn _ c1, BoxedColumn _ c2) ->
        BoxedColumn Nothing $ mkVecSimple c1 c2 These
    (UnboxedColumn _ c1, UnboxedColumn _ c2) ->
        BoxedColumn Nothing $ mkVecSimple c1 c2 These
  where
    mkVec c1 c2 combineElements =
        VB.generate
            (min (VG.length c1) (VG.length c2))
            (\i -> combineElements i (c1 VG.! i) (c2 VG.! i))
    {-# INLINE mkVec #-}

    mkVecSimple c1 c2 f =
        VB.generate
            (min (VG.length c1) (VG.length c2))
            (\i -> f (c1 VG.! i) (c2 VG.! i))
    {-# INLINE mkVecSimple #-}
{-# INLINE mergeColumns #-}

-- | An internal, column version of zipWith.
zipWithColumns ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Column -> Column -> Either DataFrameException Column
zipWithColumns f (UnboxedColumn bmL (column :: VU.Vector d)) (UnboxedColumn bmR (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
        Just Refl
            -- Fast path: both plain unboxed, no bitmaps involved in the output type
            | isNothing bmL
            , isNothing bmR ->
                pure $ case sUnbox @c of
                    STrue -> UnboxedColumn Nothing (VU.zipWith f column other)
                    SFalse -> fromVector $ VB.zipWith f (VG.convert column) (VG.convert other)
        -- Type mismatch or bitmap involvement: fall through to general toVector path
        _ -> zipWithColumnsGeneral f (UnboxedColumn bmL column) (UnboxedColumn bmR other)
    Nothing -> zipWithColumnsGeneral f (UnboxedColumn bmL column) (UnboxedColumn bmR other)
-- TODO: mchavinda - reuse pattern from interpret where we augment the
-- error at the end.
zipWithColumns f left right = zipWithColumnsGeneral f left right

zipWithColumnsGeneral ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Column -> Column -> Either DataFrameException Column
zipWithColumnsGeneral f left right = case toVector @a left of
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
{-# INLINE zipWithColumnsGeneral #-}
{-# INLINE zipWithColumns #-}

-- Functions for mutable columns (intended for IO).
writeColumn :: Int -> T.Text -> MutableColumn -> IO (Either T.Text Bool)
writeColumn i value (MBoxedColumn (col :: VBM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl ->
            if isNullish value
                then VBM.unsafeWrite col i "" >> return (Left $! value)
                else VBM.unsafeWrite col i value >> return (Right True)
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
    | null nulls = BoxedColumn Nothing <$> VB.unsafeFreeze col
    | all (isNullish . snd) nulls = do
        frozen <- VB.unsafeFreeze col
        let n = VB.length frozen
            bm = buildBitmapFromNulls n (map fst nulls)
        return $ BoxedColumn (Just bm) frozen
    | otherwise =
        BoxedColumn Nothing
            . VB.imap
                ( \i v ->
                    if i `elem` map fst nulls
                        then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                        else Right v
                )
            <$> VB.unsafeFreeze col
freezeColumn' nulls (MUnboxedColumn col)
    | null nulls = UnboxedColumn Nothing <$> VU.unsafeFreeze col
    | all (isNullish . snd) nulls = do
        c <- VU.unsafeFreeze col
        let n = VU.length c
            bm = buildBitmapFromNulls n (map fst nulls)
        return $ UnboxedColumn (Just bm) c
    | otherwise = do
        c <- VU.unsafeFreeze col
        return $
            BoxedColumn Nothing $
                VB.generate
                    (VU.length c)
                    ( \i ->
                        if i `elem` map fst nulls
                            then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                            else Right (c VU.! i)
                    )
{-# INLINE freezeColumn' #-}

{- | Freeze a mutable column into an @Either Text a@ column: every recorded
null position becomes @Left rawText@ (preserving the original input), every
other position becomes @Right v@. Used by CSV readers under 'EitherRead' mode.
-}
freezeColumnEither :: [(Int, T.Text)] -> MutableColumn -> IO Column
freezeColumnEither nulls (MBoxedColumn col) = do
    frozen <- VB.unsafeFreeze col
    let nullMap = nulls
    pure $
        BoxedColumn Nothing $
            VB.imap
                ( \i v -> case lookup i nullMap of
                    Just t -> Left t
                    Nothing -> Right v
                )
                frozen
freezeColumnEither nulls (MUnboxedColumn col) = do
    c <- VU.unsafeFreeze col
    let nullMap = nulls
    pure $
        BoxedColumn Nothing $
            VB.generate (VU.length c) $ \i ->
                case lookup i nullMap of
                    Just t -> Left t
                    Nothing -> Right (c VU.! i)
{-# INLINE freezeColumnEither #-}

{- | Promote a non-nullable column to a nullable one (add an all-valid bitmap).
No-op when already nullable.
-}
ensureOptional :: Column -> Column
ensureOptional c@(BoxedColumn (Just _) _) = c
ensureOptional (BoxedColumn Nothing col) =
    BoxedColumn (Just (allValidBitmap (VB.length col))) col
ensureOptional c@(UnboxedColumn (Just _) _) = c
ensureOptional (UnboxedColumn Nothing col) =
    UnboxedColumn (Just (allValidBitmap (VU.length col))) col

-- | Fills the end of a column, up to n, with null rows. Does nothing if column has length >= n.
expandColumn :: Int -> Column -> Column
expandColumn n column@(BoxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls n [VG.length col .. n - 1])
                Just b ->
                    Just
                        (bitmapConcat (VG.length col) b extra (VU.replicate ((extra + 7) `shiftR` 3) 0))
            -- pad data with default (undefined slot, protected by bitmap)
            newCol = col <> VB.replicate extra (errorWithoutStackTrace "expandColumn: null slot")
         in BoxedColumn newBm newCol
expandColumn n column@(UnboxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls n [VG.length col .. n - 1])
                Just b ->
                    Just
                        (bitmapConcat (VG.length col) b extra (VU.replicate ((extra + 7) `shiftR` 3) 0))
            newCol = runST $ do
                mv <- VUM.new n
                VU.imapM_ (VUM.unsafeWrite mv) col
                VU.unsafeFreeze mv
         in UnboxedColumn newBm newCol

-- | Fills the beginning of a column, up to n, with null rows. Does nothing if column has length >= n.
leftExpandColumn :: Int -> Column -> Column
leftExpandColumn n column@(BoxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            origLen = VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls n [0 .. extra - 1])
                Just b ->
                    let nullPart = VU.replicate ((extra + 7) `shiftR` 3) 0
                     in Just (bitmapConcat extra nullPart origLen b)
            newCol =
                VB.replicate extra (errorWithoutStackTrace "leftExpandColumn: null slot") <> col
         in BoxedColumn newBm newCol
leftExpandColumn n column@(UnboxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            origLen = VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls n [0 .. extra - 1])
                Just b ->
                    let nullPart = VU.replicate ((extra + 7) `shiftR` 3) 0
                     in Just (bitmapConcat extra nullPart origLen b)
            newCol = runST $ do
                mv <- VUM.new n
                VU.imapM_ (\i x -> VUM.unsafeWrite mv (extra + i) x) col
                VU.unsafeFreeze mv
         in UnboxedColumn newBm newCol

{- | Concatenates two columns.
Returns Nothing if the columns are of different types.
-}
concatColumns :: Column -> Column -> Either DataFrameException Column
concatColumns left right = case (left, right) of
    (BoxedColumn bmL l, BoxedColumn bmR r) -> case testEquality (typeOf l) (typeOf r) of
        Just Refl ->
            let newBm = case (bmL, bmR) of
                    (Nothing, Nothing) -> Nothing
                    (Just bl, Nothing) ->
                        Just
                            (bitmapConcat (VB.length l) bl (VB.length r) (allValidBitmap (VB.length r)))
                    (Nothing, Just br) ->
                        Just
                            (bitmapConcat (VB.length l) (allValidBitmap (VB.length l)) (VB.length r) br)
                    (Just bl, Just br) -> Just (bitmapConcat (VB.length l) bl (VB.length r) br)
             in pure (BoxedColumn newBm (l <> r))
        Nothing -> Left (mismatchErr (typeOf r) (typeOf l))
    (UnboxedColumn bmL l, UnboxedColumn bmR r) -> case testEquality (typeOf l) (typeOf r) of
        Just Refl ->
            let newBm = case (bmL, bmR) of
                    (Nothing, Nothing) -> Nothing
                    (Just bl, Nothing) ->
                        Just
                            (bitmapConcat (VU.length l) bl (VU.length r) (allValidBitmap (VU.length r)))
                    (Nothing, Just br) ->
                        Just
                            (bitmapConcat (VU.length l) (allValidBitmap (VU.length l)) (VU.length r) br)
                    (Just bl, Just br) -> Just (bitmapConcat (VU.length l) bl (VU.length r) br)
             in pure (UnboxedColumn newBm (l <> r))
        Nothing -> Left (mismatchErr (typeOf r) (typeOf l))
    _ -> Left (mismatchErr (typeOf right) (typeOf left))
  where
    mismatchErr ::
        forall (x :: Type) (y :: Type). TypeRep x -> TypeRep y -> DataFrameException
    mismatchErr ta tb =
        withTypeable ta $
            withTypeable tb $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right ta
                        , expectedType = Right tb
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

{- | O(n) Concatenate a list of same-type columns in a single allocation.
All columns must have the same constructor and element type (as they will
within a single Parquet column). Calls 'error' on mismatch.
-}
concatManyColumns :: [Column] -> Column
concatManyColumns [] = fromList ([] :: [Maybe Int])
concatManyColumns [c] = c
concatManyColumns (c0 : cs) = case c0 of
    BoxedColumn bm0 v0 ->
        let getCol (BoxedColumn bm v) = case testEquality (typeOf v0) (typeOf v) of
                Just Refl -> (bm, v)
                Nothing -> error "concatManyColumns: BoxedColumn type mismatch"
            getCol _ = error "concatManyColumns: column constructor mismatch"
            rest = map getCol cs
            allVecs = v0 : map snd rest
            allBms = bm0 : map fst rest
            newBm
                | all isNothing allBms = Nothing
                | otherwise =
                    let pairs = zip allVecs allBms
                        expandedBms = map (\(v, mb) -> fromMaybe (allValidBitmap (VB.length v)) mb) pairs
                        go b1 n1 b2 n2 = bitmapConcat n1 b1 n2 b2
                        concatBms [] = VU.empty
                        concatBms [(b, _v)] = b
                        concatBms ((b1, v1) : (b2, v2) : rest') =
                            let merged = go b1 (VB.length v1) b2 (VB.length v2)
                             in concatBms ((merged, v1 <> v2) : rest')
                     in Just $ concatBms (zip expandedBms allVecs)
         in BoxedColumn newBm (VB.concat allVecs)
    UnboxedColumn bm0 v0 ->
        let getCol (UnboxedColumn bm v) = case testEquality (typeOf v0) (typeOf v) of
                Just Refl -> (bm, v)
                Nothing -> error "concatManyColumns: UnboxedColumn type mismatch"
            getCol _ = error "concatManyColumns: column constructor mismatch"
            rest = map getCol cs
            allVecs = v0 : map snd rest
            allBms = bm0 : map fst rest
            newBm
                | all isNothing allBms = Nothing
                | otherwise =
                    let pairs = zip allVecs allBms
                        expandedBms = map (\(v, mb) -> fromMaybe (allValidBitmap (VU.length v)) mb) pairs
                        go b1 n1 b2 n2 = bitmapConcat n1 b1 n2 b2
                        concatBms [] = VU.empty
                        concatBms [(b, _)] = b
                        concatBms ((b1, v1) : (b2, v2) : rest') =
                            let merged = go b1 (VU.length v1) b2 (VU.length v2)
                             in concatBms ((merged, v1 <> v2) : rest')
                     in Just $ concatBms (zip expandedBms allVecs)
         in UnboxedColumn newBm (VU.concat allVecs)

concatColumnsEither :: Column -> Column -> Column
concatColumnsEither (BoxedColumn bmL left) (BoxedColumn bmR right) = case testEquality (typeOf left) (typeOf right) of
    Nothing ->
        BoxedColumn Nothing $ fmap Left left <> fmap Right right
    Just Refl ->
        let newBm = case (bmL, bmR) of
                (Nothing, Nothing) -> Nothing
                (Just bl, Nothing) ->
                    Just
                        ( bitmapConcat
                            (VB.length left)
                            bl
                            (VB.length right)
                            (allValidBitmap (VB.length right))
                        )
                (Nothing, Just br) ->
                    Just
                        ( bitmapConcat
                            (VB.length left)
                            (allValidBitmap (VB.length left))
                            (VB.length right)
                            br
                        )
                (Just bl, Just br) -> Just (bitmapConcat (VB.length left) bl (VB.length right) br)
         in BoxedColumn newBm $ left <> right
concatColumnsEither (UnboxedColumn bmL left) (UnboxedColumn bmR right) = case testEquality (typeOf left) (typeOf right) of
    Nothing ->
        BoxedColumn Nothing $
            fmap Left (VG.convert left) <> fmap Right (VG.convert right)
    Just Refl ->
        let newBm = case (bmL, bmR) of
                (Nothing, Nothing) -> Nothing
                (Just bl, Nothing) ->
                    Just
                        ( bitmapConcat
                            (VU.length left)
                            bl
                            (VU.length right)
                            (allValidBitmap (VU.length right))
                        )
                (Nothing, Just br) ->
                    Just
                        ( bitmapConcat
                            (VU.length left)
                            (allValidBitmap (VU.length left))
                            (VU.length right)
                            br
                        )
                (Just bl, Just br) -> Just (bitmapConcat (VU.length left) bl (VU.length right) br)
         in UnboxedColumn newBm $ left <> right
concatColumnsEither (BoxedColumn _ left) (UnboxedColumn _ right) =
    BoxedColumn Nothing $ fmap Left left <> fmap Right (VG.convert right)
concatColumnsEither (UnboxedColumn _ left) (BoxedColumn _ right) =
    BoxedColumn Nothing $ fmap Left (VG.convert left) <> fmap Right right

-- | Allocate a mutable column of size @n@ matching the constructor/type of the given column.
newMutableColumn :: Int -> Column -> IO MutableColumn
newMutableColumn n (BoxedColumn _ (_ :: VB.Vector a)) =
    MBoxedColumn <$> (VBM.new n :: IO (VBM.IOVector a))
newMutableColumn n (UnboxedColumn _ (_ :: VU.Vector a)) =
    MUnboxedColumn <$> (VUM.new n :: IO (VUM.IOVector a))

-- | Copy a column chunk into a mutable column starting at offset @off@.
copyIntoMutableColumn :: MutableColumn -> Int -> Column -> IO ()
copyIntoMutableColumn (MBoxedColumn (mv :: VBM.IOVector b)) off (BoxedColumn _ (v :: VB.Vector a)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> VG.imapM_ (\i x -> VBM.unsafeWrite mv (off + i) x) v
        Nothing -> error "copyIntoMutableColumn: Boxed type mismatch"
copyIntoMutableColumn (MUnboxedColumn (mv :: VUM.IOVector b)) off (UnboxedColumn _ (v :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> VG.imapM_ (\i x -> VUM.unsafeWrite mv (off + i) x) v
        Nothing -> error "copyIntoMutableColumn: Unboxed type mismatch"
copyIntoMutableColumn _ _ _ =
    error "copyIntoMutableColumn: constructor mismatch"

-- | Freeze a mutable column into an immutable column.
freezeMutableColumn :: MutableColumn -> IO Column
freezeMutableColumn (MBoxedColumn mv) = BoxedColumn Nothing <$> VB.unsafeFreeze mv
freezeMutableColumn (MUnboxedColumn mv) = UnboxedColumn Nothing <$> VU.unsafeFreeze mv

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
toVector col = case col of
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
        UnboxedColumn bm (f :: VU.Vector a) -> case testEquality (typeRep @a) (typeRep @Double) of
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
                                    { userType = Right (typeRep @Double)
                                    , expectedType = Right (typeRep @a)
                                    , callingFunctionName = Just "toDoubleVector"
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
