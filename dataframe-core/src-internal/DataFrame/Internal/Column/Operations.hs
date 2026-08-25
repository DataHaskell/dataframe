{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

{- |
Bulk operations over columns: mapping, folding, slicing, gathering, zipping,
appending, and the mutable-column IO helpers.
-}
module DataFrame.Internal.Column.Operations where

import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (when)
import Control.Monad.ST (runST)
import Data.Bits (shiftR)
import Data.Kind (Type)
import Data.Maybe (fromMaybe, isNothing)
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors (
    DataFrameException (EmptyDataSetException, TypeMismatchException),
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
import DataFrame.Internal.Column.Conversion
import DataFrame.Internal.Column.Properties
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    packedGather,
    packedLength,
 )
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (
    TypeRep,
    Typeable,
    typeOf,
    typeRep,
    withTypeable,
    type (:~:) (Refl),
 )

{- | Force evaluation of all elements in a column. Replacement for the removed
@instance NFData Column@; used by the IO and lazy-executor strict paths.
-}
forceColumn :: Column -> ()
forceColumn (BoxedColumn Nothing (v :: VB.Vector a)) = VB.foldl' (const (`seq` ())) () v
forceColumn (BoxedColumn (Just bm) (v :: VB.Vector a)) =
    let n = VB.length v
        go !i
            | i >= n = ()
            | bitmapTestBit bm i = VB.unsafeIndex v i `seq` go (i + 1)
            | otherwise = go (i + 1)
     in go 0
forceColumn (UnboxedColumn _ v) = v `seq` ()
forceColumn (PackedText _ (PackedTextData arr offs sel _)) = arr `seq` offs `seq` sel `seq` ()
forceColumn (MergedColumn a b) =
    forceColumn a `seq` forceColumn b `seq` checkMergedNoBothNull a b

{- | 'MergedColumn' defers element construction, so forcing must still surface
the one deferred error — a row null on both sides — inside strict IO/executor
boundaries. O(rows) bitmap walk, no allocation; both-null needs a bitmap on
each side, so anything else passes immediately.
-}
checkMergedNoBothNull :: Column -> Column -> ()
checkMergedNoBothNull a b = case (columnBitmap a, columnBitmap b) of
    (Just ba, Just bb) ->
        let !n = min (columnLength a) (columnLength b)
            go !i
                | i >= n = ()
                | bitmapTestBit ba i || bitmapTestBit bb i = go (i + 1)
                | otherwise = error "mkMergedColumns: both null"
         in go 0
    _ -> ()

-- | Allocate a mutable column of size @n@ matching the constructor/type of the given column.
newMutableColumn :: Int -> Column -> IO MutableColumn
newMutableColumn n (BoxedColumn _ (_ :: VB.Vector a)) =
    MBoxedColumn <$> (VBM.new n :: IO (VBM.IOVector a))
newMutableColumn n (UnboxedColumn _ (_ :: VU.Vector a)) =
    MUnboxedColumn <$> (VUM.new n :: IO (VUM.IOVector a))
newMutableColumn n c@(PackedText _ _) = newMutableColumn n (materializePacked c)
newMutableColumn n c@(MergedColumn _ _) = newMutableColumn n (materializeMerged c)

-- | Copy a column chunk into a mutable column starting at offset @off@.
copyIntoMutableColumn :: MutableColumn -> Int -> Column -> IO ()
copyIntoMutableColumn mv off c@(MergedColumn _ _) =
    copyIntoMutableColumn mv off (materializeMerged c)
copyIntoMutableColumn (MBoxedColumn (mv :: VBM.IOVector b)) off (BoxedColumn _ (v :: VB.Vector a)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> VG.imapM_ (\i x -> VBM.unsafeWrite mv (off + i) x) v
        Nothing -> error "copyIntoMutableColumn: Boxed type mismatch"
copyIntoMutableColumn (MUnboxedColumn (mv :: VUM.IOVector b)) off (UnboxedColumn _ (v :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> VG.imapM_ (\i x -> VUM.unsafeWrite mv (off + i) x) v
        Nothing -> error "copyIntoMutableColumn: Unboxed type mismatch"
copyIntoMutableColumn mc off c@(PackedText _ _) =
    copyIntoMutableColumn mc off (materializePacked c)
copyIntoMutableColumn _ _ _ =
    error "copyIntoMutableColumn: constructor mismatch"

-- | Freeze a mutable column into an immutable column.
freezeMutableColumn :: MutableColumn -> IO Column
freezeMutableColumn (MBoxedColumn mv) = BoxedColumn Nothing <$> VB.unsafeFreeze mv
freezeMutableColumn (MUnboxedColumn mv) = UnboxedColumn Nothing <$> VU.unsafeFreeze mv

-- | An internal function to map a function over the values of a column.
mapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (b -> c) -> Column -> Either DataFrameException Column
mapColumn f = \case
    BoxedColumn bm (col :: VB.Vector a) -> runBoxed bm col
    UnboxedColumn bm (col :: VU.Vector a) -> runUnboxed bm col
    c@(PackedText _ _) -> mapColumn f (materializePacked c)
    c@(MergedColumn _ _) -> mapColumn f (materializeMerged c)
  where
    runBoxed ::
        forall a.
        (Columnable a) =>
        Maybe Bitmap -> VB.Vector a -> Either DataFrameException Column
    runBoxed bm col = case testEquality (typeRep @b) (typeRep @(Maybe a)) of
        Just Refl ->
            let !n = VB.length col
             in Right $ case sUnbox @c of
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
                Right $ case sUnbox @c of
                    STrue -> UnboxedColumn bm (VU.generate (VB.length col) (f . VB.unsafeIndex col))
                    SFalse -> case bm of
                        Nothing -> fromVector @c (VB.map f col)
                        Just _ -> BoxedColumn bm (VB.map f col)
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
                SFalse -> case bm of
                    Nothing -> fromVector @c (VB.generate (VU.length col) (f . VU.unsafeIndex col))
                    Just _ -> BoxedColumn bm (VB.generate (VU.length col) (f . VU.unsafeIndex col))
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
    c@(PackedText _ _) -> imapColumn f (materializePacked c)
    c@(MergedColumn _ _) -> imapColumn f (materializeMerged c)
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

-- | O(n) Takes the last n values of a column.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

-- | O(n) Takes n values after a given column index.
sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (MergedColumn a b) =
    MergedColumn (sliceColumn start n a) (sliceColumn start n b)
sliceColumn start n (BoxedColumn bm xs) =
    BoxedColumn (fmap (bitmapSlice start n) bm) (VG.slice start n xs)
sliceColumn start n (UnboxedColumn bm xs) =
    UnboxedColumn (fmap (bitmapSlice start n) bm) (VG.slice start n xs)
sliceColumn start n c@(PackedText _ _) = sliceColumn start n (materializePacked c)
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
atIndicesStable indexes (MergedColumn a b) =
    MergedColumn (atIndicesStable indexes a) (atIndicesStable indexes b)
atIndicesStable indexes (PackedText bm p) =
    PackedText
        ( fmap
            ( \bm0 ->
                buildBitmapFromValid $
                    VU.map (\i -> if bitmapTestBit bm0 i then 1 else 0) indexes
            )
            bm
        )
        (packedGather indexes p)
{-# INLINE atIndicesStable #-}

{- | Like 'atIndicesStable' but treats negative indices as null.
Keeps the index vector fully unboxed (no @VB.Vector (Maybe Int)@).
-}
gatherWithSentinel :: VU.Vector Int -> Column -> Column
gatherWithSentinel indices c@(MergedColumn _ _) =
    gatherWithSentinel indices (materializeMerged c)
gatherWithSentinel indices col =
    let !n = VU.length indices
        newBm = buildBitmapFromValid $ VU.generate n $ \i ->
            if VU.unsafeIndex indices i < 0 then 0 else 1
     in case col of
            PackedText srcBm p ->
                let bm = case srcBm of
                        Nothing -> Just newBm
                        Just sb ->
                            Just
                                ( andBitmaps
                                    newBm
                                    ( buildBitmapFromValid $ VU.generate n $ \i ->
                                        let idx = VU.unsafeIndex indices i
                                         in if idx >= 0 && bitmapTestBit sb idx then 1 else 0
                                    )
                                )
                 in PackedText bm (packedGather indices p)
            BoxedColumn srcBm v ->
                let dat = VB.generate n $ \i ->
                        let !idx = VU.unsafeIndex indices i
                         in if idx < 0 then VB.unsafeIndex v 0 else VB.unsafeIndex v idx
                    bm = case srcBm of
                        Nothing -> Just newBm
                        Just sb ->
                            Just
                                ( andBitmaps
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
                                ( andBitmaps
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
    c@(PackedText _ _) -> findIndices predicate (materializePacked c)
    c@(MergedColumn _ _) -> findIndices predicate (materializeMerged c)
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
    c@(PackedText _ _) -> ifoldrColumn f acc (materializePacked c)
    c@(MergedColumn _ _) -> ifoldrColumn f acc (materializeMerged c)
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
    c@(PackedText _ _) -> foldlColumn f acc (materializePacked c)
    c@(MergedColumn _ _) -> foldlColumn f acc (materializeMerged c)
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
    c@(PackedText _ _) -> foldl1Column f (materializePacked c)
    c@(MergedColumn _ _) -> foldl1Column f (materializeMerged c)
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
        PackedText _ _ -> foldl1DirectGroups f (materializePacked col) valueIndices offsets
        MergedColumn _ _ -> foldl1DirectGroups f (materializeMerged col) valueIndices offsets
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

{- | O(n) fold over groups by scanning the column linearly (rowToGroup[i] = group
of row i). Random writes hit the small per-group accumulator array; when @acc@ is
unboxable that array is unboxed, avoiding pointer indirection.
-}
foldLinearGroups ::
    forall b acc.
    (Columnable b, Columnable acc) =>
    (acc -> b -> acc) ->
    acc ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
foldLinearGroups f seed col rowToGroup nGroups
    | nGroups == 0 = Right (fromVector @acc VB.empty)
    | otherwise = case col of
        UnboxedColumn _ (vec :: VU.Vector d) -> foldLinearWorker vec
        BoxedColumn _ (vec :: VB.Vector d) -> foldLinearWorker vec
        PackedText _ _ ->
            foldLinearGroups f seed (materializePacked col) rowToGroup nGroups
        MergedColumn _ _ ->
            foldLinearGroups f seed (materializeMerged col) rowToGroup nGroups
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
    c@(PackedText _ _) -> headColumn (materializePacked c)
    c@(MergedColumn _ _) -> headColumn (mergedHead c)
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
zipColumns l@(MergedColumn _ _) r = zipColumns (materializeMerged l) r
zipColumns l r@(MergedColumn _ _) = zipColumns l (materializeMerged r)
zipColumns l@(PackedText _ _) r = zipColumns (materializePacked l) r
zipColumns l r@(PackedText _ _) = zipColumns l (materializePacked r)
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

-- | An internal, column version of zipWith.
zipWithColumns ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Column -> Column -> Either DataFrameException Column
zipWithColumns f (UnboxedColumn bmL (column :: VU.Vector d)) (UnboxedColumn bmR (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
        Just Refl
            | isNothing bmL
            , isNothing bmR ->
                pure $ case sUnbox @c of
                    STrue -> UnboxedColumn Nothing (VU.zipWith f column other)
                    SFalse -> fromVector $ VB.zipWith f (VG.convert column) (VG.convert other)
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

-- writeColumn and freezeColumn' (CSV-ingest helpers) moved to
-- DataFrame.IO.Internal.MutableColumn so the core column module does not
-- need to depend on DataFrame.Internal.Parsing.

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
ensureOptional c@(MergedColumn _ _) = ensureOptional (materializeMerged c)
ensureOptional c@(BoxedColumn (Just _) _) = c
ensureOptional (BoxedColumn Nothing col) =
    BoxedColumn (Just (allValidBitmap (VB.length col))) col
ensureOptional c@(UnboxedColumn (Just _) _) = c
ensureOptional (UnboxedColumn Nothing col) =
    UnboxedColumn (Just (allValidBitmap (VU.length col))) col
ensureOptional c@(PackedText (Just _) _) = c
ensureOptional (PackedText Nothing p) =
    PackedText (Just (allValidBitmap (packedLength p))) p

-- | Fills the end of a column, up to n, with null rows. Does nothing if column has length >= n.
expandColumn :: Int -> Column -> Column
expandColumn n c@(MergedColumn a b)
    | n <= min (columnLength a) (columnLength b) = c
    | otherwise = expandColumn n (materializeMerged c)
expandColumn n c@(PackedText _ p)
    | n <= packedLength p = c
    | otherwise = expandColumn n (materializePacked c)
expandColumn n column@(BoxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls' n (VU.enumFromN (VG.length col) extra))
                Just b ->
                    Just
                        (bitmapConcat (VG.length col) b extra (VU.replicate ((extra + 7) `shiftR` 3) 0))
            newCol = col <> VB.replicate extra (errorWithoutStackTrace "expandColumn: null slot")
         in BoxedColumn newBm newCol
expandColumn n column@(UnboxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls' n (VU.enumFromN (VG.length col) extra))
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
leftExpandColumn n c@(MergedColumn a b)
    | n <= min (columnLength a) (columnLength b) = c
    | otherwise = leftExpandColumn n (materializeMerged c)
leftExpandColumn n c@(PackedText _ p)
    | n <= packedLength p = c
    | otherwise = leftExpandColumn n (materializePacked c)
leftExpandColumn n column@(BoxedColumn bm col)
    | n <= VG.length col = column
    | otherwise =
        let extra = n - VG.length col
            origLen = VG.length col
            newBm = case bm of
                Nothing -> Just (buildBitmapFromNulls' n (VU.enumFromN 0 extra))
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
                Nothing -> Just (buildBitmapFromNulls' n (VU.enumFromN 0 extra))
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
mappendColumns :: Column -> Column -> Either DataFrameException Column
mappendColumns left right = case (left, right) of
    (MergedColumn _ _, _) -> mappendColumns (materializeMerged left) right
    (_, MergedColumn _ _) -> mappendColumns left (materializeMerged right)
    (PackedText _ _, _) -> mappendColumns (materializePacked left) right
    (_, PackedText _ _) -> mappendColumns left (materializePacked right)
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
                        , callingFunctionName = Just "mappendColumns"
                        , errorColumnName = Nothing
                        }
                    )

{- | Like 'mappendColumns' but also combines columns of different types by wrapping
values in 'Either' (e.g. @[1,2]@ and @["a","b"]@ become
@[Left 1, Left 2, Right "a", Right "b"]@).
-}

{- | O(n) Concatenate a list of same-type columns in a single allocation.
All columns must have the same constructor and element type (as they will
within a single Parquet column). Calls 'error' on mismatch.
-}
concatManyColumns :: [Column] -> Column
concatManyColumns [] = fromList ([] :: [Maybe Int])
concatManyColumns [c] = c
concatManyColumns all'
    | any isMergedColumn all' =
        concatManyColumns (map materializeMerged all')
    | any isPackedText all' =
        concatManyColumns (map materializePacked all')
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
    PackedText _ _ -> concatManyColumns (map materializePacked (c0 : cs))
    MergedColumn _ _ -> concatManyColumns (map materializeMerged (c0 : cs))

mappendColumnsEither :: Column -> Column -> Column
mappendColumnsEither l@(MergedColumn _ _) r =
    mappendColumnsEither (materializeMerged l) r
mappendColumnsEither l r@(MergedColumn _ _) =
    mappendColumnsEither l (materializeMerged r)
mappendColumnsEither l@(PackedText _ _) r = mappendColumnsEither (materializePacked l) r
mappendColumnsEither l r@(PackedText _ _) = mappendColumnsEither l (materializePacked r)
mappendColumnsEither (BoxedColumn bmL left) (BoxedColumn bmR right) = case testEquality (typeOf left) (typeOf right) of
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
mappendColumnsEither (UnboxedColumn bmL left) (UnboxedColumn bmR right) = case testEquality (typeOf left) (typeOf right) of
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
mappendColumnsEither (BoxedColumn _ left) (UnboxedColumn _ right) =
    BoxedColumn Nothing $ fmap Left left <> fmap Right (VG.convert right)
mappendColumnsEither (UnboxedColumn _ left) (BoxedColumn _ right) =
    BoxedColumn Nothing $ fmap Left (VG.convert left) <> fmap Right right
