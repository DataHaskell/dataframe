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
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Bulk operations over columns: map, fold, slice, gather, zip, append.
module DataFrame.Internal.Column.Operations where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Text.Internal (Text (Text))
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (forM_, join, when)
import Control.Monad.ST (runST)
import Data.Bits (setBit, shiftL, shiftR)
import Data.Int (Int32)
import Data.Kind (Type)
import Data.Maybe (catMaybes, isNothing)
import Data.Type.Equality (TestEquality (..))
import Data.Word (Word8)
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
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    forkJoin_,
    parThreshold,
    parallelChunks,
    parallelChunks_,
    shouldParallelize,
    splitChunkRange,
 )
import DataFrame.Internal.Data.PackedText (
    PackedSel (..),
    PackedTextData (..),
    mkPackedContiguous,
    offCount,
    packedGather,
    packedIndexText,
    packedLength,
 )
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (
    TypeRep,
    Typeable,
    eqTypeRep,
    typeOf,
    typeRep,
    withTypeable,
    pattern App,
    type (:~:) (Refl),
    type (:~~:) (HRefl),
 )

-- | Force evaluation of all elements in a column.
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

-- | Surface the deferred both-sides-null error of a 'MergedColumn'.
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

-- | Allocate a mutable column of size @n@ matching the given column.
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
    PackedText Nothing p
        | Just Refl <- testEquality (typeRep @b) (typeRep @T.Text) ->
            Right (mapPacked p)
    c@(PackedText _ _) -> mapColumn f (materializePacked c)
    c@(MergedColumn _ _) -> mapColumn f (materializeMerged c)
  where
    -- Rows decode one at a time from packed bytes; text results are re-packed.
    mapPacked :: (b ~ T.Text) => PackedTextData -> Column
    mapPacked p =
        let !n = packedLength p
         in case testEquality (typeRep @c) (typeRep @T.Text) of
                Just Refl -> packedMapText n (f . packedIndexText p)
                Nothing -> case sUnbox @c of
                    STrue -> UnboxedColumn Nothing (parGenerateUnboxed n (f . packedIndexText p))
                    SFalse -> fromVector @c (VB.generate n (f . packedIndexText p))
    runBoxed ::
        forall a.
        (Columnable a) =>
        Maybe Bitmap -> VB.Vector a -> Either DataFrameException Column
    runBoxed bm col = case testEquality (typeRep @b) (typeRep @(Maybe a)) of
        Just Refl ->
            let !n = VB.length col
             in Right $ case sUnbox @c of
                    STrue -> UnboxedColumn Nothing $
                        parGenerateUnboxed n $ \i ->
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
                let !n = VB.length col
                 in Right $ case sUnbox @c of
                        STrue -> UnboxedColumn bm $ case nullsIn n bm of
                            Nothing -> parGenerateUnboxed n (f . VB.unsafeIndex col)
                            Just nulls -> generateValidUnboxed nulls n (f . VB.unsafeIndex col)
                        SFalse -> case bm of
                            Nothing -> fromVector @c (VB.map f col)
                            Just b' -> generateValidBoxed b' n (f . VB.unsafeIndex col)
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
                        parGenerateUnboxed n $ \i ->
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
            Just Refl ->
                let !n = VU.length col
                 in Right $ case sUnbox @c of
                        STrue -> UnboxedColumn bm $ case nullsIn n bm of
                            Nothing -> fromMaybe mapLoop compiledMap f col
                            Just nulls -> generateValidUnboxed nulls n (f . VU.unsafeIndex col)
                        SFalse -> case bm of
                            Nothing -> fromVector @c (VB.generate n (f . VU.unsafeIndex col))
                            Just b' -> generateValidBoxed b' n (f . VU.unsafeIndex col)
            Nothing -> throwTypeMismatch @a @b
{-# INLINEABLE mapColumn #-}

-- | Apply a function returning an unboxed result to an unboxed vector.
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
        Just Refl ->
            let !n = VB.length col
             in Right $ case sUnbox @c of
                    STrue -> UnboxedColumn bm $ case nullsIn n bm of
                        Nothing -> VU.generate n (\i -> f i (VB.unsafeIndex col i))
                        Just nulls -> generateValidUnboxed nulls n (\i -> f i (VB.unsafeIndex col i))
                    SFalse -> case bm of
                        Nothing -> fromVector @c (VB.imap f col)
                        Just b' -> generateValidBoxed b' n (\i -> f i (VB.unsafeIndex col i))
        Nothing -> throwTypeMismatch @a @b

    runUnboxed ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        Maybe Bitmap -> VU.Vector a -> Either DataFrameException Column
    runUnboxed bm col = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl ->
            let !n = VU.length col
             in Right $ case sUnbox @c of
                    STrue -> UnboxedColumn bm $ case nullsIn n bm of
                        Nothing -> VU.imap f col
                        Just nulls -> generateValidUnboxed nulls n (\i -> f i (VU.unsafeIndex col i))
                    SFalse -> case bm of
                        Nothing -> fromVector @c (VB.imap f (VG.convert col))
                        Just b' -> generateValidBoxed b' n (\i -> f i (VU.unsafeIndex col i))
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
sliceColumn start n (PackedText bm p)
    -- packedGather reads out-of-range as "", so check the slice bounds first.
    | start < 0 || n < 0 || start + n > packedLength p =
        errorWithoutStackTrace
            ( "sliceColumn: invalid slice ("
                ++ show start
                ++ ","
                ++ show n
                ++ ","
                ++ show (packedLength p)
                ++ ")"
            )
    | otherwise =
        PackedText
            (fmap (bitmapSlice start n) bm)
            (packedGather (VU.enumFromN start n) p)
{-# INLINE sliceColumn #-}

-- (n) Selects the elements at a given set of indices, keeping order.

-- | Parallel unboxed 'VU.generate', one contiguous chunk per capability.
parGenerateUnboxed :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenerateUnboxed n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n (fillGenerate mv f)
        VU.unsafeFreeze mv
{-# NOINLINE parGenerateUnboxed #-}

-- | The chunk body shared by 'parGenerateUnboxed' and its inline variant.
fillGenerate ::
    (VU.Unbox c) => VUM.IOVector c -> (Int -> c) -> Int -> Int -> IO ()
fillGenerate mv f !lo !hi =
    let go !i
            | i >= hi = pure ()
            | otherwise = VUM.unsafeWrite mv i (f i) >> go (i + 1)
     in go lo
{-# INLINE fillGenerate #-}

-- | Parallel unboxed gather: element @i@ is @v ! (ix ! i)@.
parBackpermuteUnboxed ::
    (VU.Unbox a) => VU.Vector a -> VU.Vector Int -> VU.Vector a
parBackpermuteUnboxed v ix =
    parGenerateUnboxed (VU.length ix) (VU.unsafeIndex v . VU.unsafeIndex ix)
{-# INLINE parBackpermuteUnboxed #-}

-- | 'parGenerateUnboxed' with an INLINE body for monomorphic wrappers.
parGenerateUnboxedInline :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenerateUnboxedInline n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n (fillGenerate mv f)
        VU.unsafeFreeze mv
{-# INLINE parGenerateUnboxedInline #-}

-- | Run @k@ with the element type known, or 'Nothing' for other types.
withKnownElem ::
    forall a r. (Typeable a) => (forall x. (VU.Unbox x) => a :~: x -> r) -> Maybe r
withKnownElem k
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        Just (k @Double Refl)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = Just (k @Int Refl)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Bool) = Just (k @Bool Refl)
    | otherwise = Nothing
{-# INLINE withKnownElem #-}

-- | 'Nothing' for element types without a compiled loop.
compiledMap ::
    forall a c.
    (Typeable a, Typeable c) => Maybe ((a -> c) -> VU.Vector a -> VU.Vector c)
compiledMap = join (withKnownElem @a (\Refl -> withKnownElem @c (\Refl -> mapLoop)))

compiledZip ::
    forall a c.
    (Typeable a, Typeable c) =>
    Maybe ((a -> a -> c) -> VU.Vector a -> VU.Vector a -> VU.Vector c)
compiledZip = join (withKnownElem @a (\Refl -> withKnownElem @c (\Refl -> zipLoop)))

compiledFoldl ::
    forall a b.
    (Typeable a, Typeable b) => Maybe ((b -> a -> b) -> b -> VU.Vector a -> b)
compiledFoldl = join (withKnownElem @a (\Refl -> withKnownElem @b (\Refl -> VU.foldl')))

compiledFoldl1 ::
    forall a. (Typeable a) => Maybe ((a -> a -> a) -> VU.Vector a -> a)
compiledFoldl1 = withKnownElem @a (\Refl -> VU.foldl1')

mapLoop :: (VU.Unbox a, VU.Unbox c) => (a -> c) -> VU.Vector a -> VU.Vector c
mapLoop f v = parGenerateUnboxedInline (VU.length v) (f . VU.unsafeIndex v)
{-# INLINE mapLoop #-}

-- | Element-wise over two vectors, to the shorter length.
zipLoop ::
    (VU.Unbox a, VU.Unbox b, VU.Unbox c) =>
    (a -> b -> c) -> VU.Vector a -> VU.Vector b -> VU.Vector c
zipLoop f l r =
    parGenerateUnboxedInline
        (min (VU.length l) (VU.length r))
        (\i -> f (VU.unsafeIndex l i) (VU.unsafeIndex r i))
{-# INLINE zipLoop #-}

-- | The bitmap, if it marks any of the first @n@ rows null.
nullsIn :: Int -> Maybe Bitmap -> Maybe Bitmap
nullsIn n (Just bm) | bitmapHasNulls n bm = Just bm
nullsIn _ _ = Nothing
{-# INLINE nullsIn #-}

validAt :: Maybe Bitmap -> Int -> Bool
validAt bm i = maybe True (`bitmapTestBit` i) bm
{-# INLINE validAt #-}

-- | @g i@ for the valid rows only; null rows hold zero.
generateValidUnboxed ::
    (VU.Unbox c) => Bitmap -> Int -> (Int -> c) -> VU.Vector c
generateValidUnboxed bm n g = unsafePerformIO $ do
    mv <- VUM.new n
    let fill !lo !hi =
            let go !i
                    | i >= hi = pure ()
                    | bitmapTestBit bm i = VUM.unsafeWrite mv i (g i) >> go (i + 1)
                    | otherwise = go (i + 1)
             in go lo
    if shouldParallelize parThreshold n
        then parallelChunks_ parThreshold n fill
        else fill 0 n
    VU.unsafeFreeze mv
{-# NOINLINE generateValidUnboxed #-}

-- | A boxed column of @g i@ for the valid rows only; null rows stay null.
generateValidBoxed ::
    forall c. (Columnable c) => Bitmap -> Int -> (Int -> c) -> Column
generateValidBoxed bm n g = case typeRep @c of
    App tMaybe _
        | Just HRefl <- eqTypeRep tMaybe (typeRep @Maybe) ->
            fromVector @c
                (VB.generate n (\i -> if bitmapTestBit bm i then g i else Nothing))
    _ ->
        BoxedColumn
            (Just bm)
            ( VB.generate
                n
                (\i -> if bitmapTestBit bm i then g i else errorWithoutStackTrace "null slot")
            )

-- | Closure-free parallel 'Int' gather: @out!i = v ! (ix!i)@.
parBackpermuteInt :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
parBackpermuteInt v ix =
    parGenerateUnboxedInline
        (VU.length ix)
        (VU.unsafeIndex v . VU.unsafeIndex ix)
{-# NOINLINE parBackpermuteInt #-}

-- | Closure-free parallel 'Double' gather: @out!i = v ! (ix!i)@.
parBackpermuteDouble :: VU.Vector Double -> VU.Vector Int -> VU.Vector Double
parBackpermuteDouble v ix =
    parGenerateUnboxedInline
        (VU.length ix)
        (VU.unsafeIndex v . VU.unsafeIndex ix)
{-# NOINLINE parBackpermuteDouble #-}

-- | Closure-free double-indirection gather: @out!g = vis ! (offs!g)@.
parBackpermute2Int :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
parBackpermute2Int vis offs =
    parGenerateUnboxedInline
        (max 0 (VU.length offs - 1))
        (VU.unsafeIndex vis . VU.unsafeIndex offs)
{-# NOINLINE parBackpermute2Int #-}

-- | Parallel boxed gather that fetches array slots eagerly.
parBackpermuteBoxed :: VB.Vector a -> VU.Vector Int -> VB.Vector a
parBackpermuteBoxed v ix
    | not (shouldParallelize parThreshold n) =
        VB.generate n ((v `VB.unsafeIndex`) . (ix `VU.unsafeIndex`))
    | otherwise = unsafePerformIO $ do
        mv <- VBM.unsafeNew n
        parallelChunks_ parThreshold n $ \ !lo !hi ->
            let go !i
                    | i >= hi = pure ()
                    | otherwise = do
                        x <- VB.unsafeIndexM v (VU.unsafeIndex ix i)
                        VBM.unsafeWrite mv i x
                        go (i + 1)
             in go lo
        VB.unsafeFreeze mv
  where
    !n = VU.length ix
{-# NOINLINE parBackpermuteBoxed #-}

-- | Clamp sentinel (negative) indices to 0 in one parallel pass.
parClampNonNeg :: VU.Vector Int -> VU.Vector Int
parClampNonNeg ix =
    parGenerateUnboxedInline
        (VU.length ix)
        (\i -> let !x = VU.unsafeIndex ix i in max x 0)
{-# NOINLINE parClampNonNeg #-}

-- | Validity bitmap from sentinel indices, built in parallel.
parBitmapNonNeg :: VU.Vector Int -> Bitmap
parBitmapNonNeg ix =
    let !n = VU.length ix
        !nBytes = (n + 7) `shiftR` 3
     in parGenerateUnboxedInline nBytes $ \b ->
            let !base = b `shiftL` 3
                go !acc !bit
                    | bit >= 8 = acc
                    | otherwise =
                        let !idx = base + bit
                            !acc' =
                                if idx < n && VU.unsafeIndex ix idx >= 0
                                    then setBit acc bit
                                    else acc
                         in go acc' (bit + 1)
             in go (0 :: Word8) 0
{-# NOINLINE parBitmapNonNeg #-}

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
        (parBackpermuteBoxed column indexes)
atIndicesStable indexes (UnboxedColumn bm (column :: VU.Vector a)) =
    UnboxedColumn
        ( fmap
            ( \bm0 ->
                buildBitmapFromValid $
                    VU.map (\i -> if bitmapTestBit bm0 i then 1 else 0) indexes
            )
            bm
        )
        -- Int/Double use closure-free kernels; others take the generic path.
        ( case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> parBackpermuteInt column indexes
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> parBackpermuteDouble column indexes
                Nothing -> parBackpermuteUnboxed column indexes
        )
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

-- | Like 'atIndicesStable' but treats negative indices as null.
gatherWithSentinel :: VU.Vector Int -> Column -> Column
gatherWithSentinel indices c@(MergedColumn _ _) =
    gatherWithSentinel indices (materializeMerged c)
gatherWithSentinel indices col =
    let !n = VU.length indices
        !newBm = parBitmapNonNeg indices
        withBm srcBm = case srcBm of
            Nothing -> Just newBm
            Just sb -> Just (andBitmaps newBm (gatherSrcBm sb))
        -- Sequential fallback: gather a nullable source bitmap by raw indices.
        gatherSrcBm sb =
            buildBitmapFromValid $ VU.generate n $ \i ->
                let idx = VU.unsafeIndex indices i
                 in if idx >= 0 && bitmapTestBit sb idx then 1 else 0
     in case col of
            -- packedGather handles -1 sentinels itself; pass raw indices.
            PackedText srcBm p -> PackedText (withBm srcBm) (packedGather indices p)
            BoxedColumn srcBm v
                -- An empty source means every index is a sentinel.
                | VB.null v -> BoxedColumn (withBm srcBm) (allNullBoxed n v)
                | otherwise ->
                    BoxedColumn
                        (withBm srcBm)
                        (parBackpermuteBoxed v (parClampNonNeg indices))
            UnboxedColumn srcBm v
                | VU.null v -> UnboxedColumn (withBm srcBm) (allNullUnboxed n v)
                | otherwise ->
                    -- Reuse atIndicesStable's monomorphic Int/Double kernels.
                    case atIndicesStable (parClampNonNeg indices) (UnboxedColumn Nothing v) of
                        UnboxedColumn _ dat -> UnboxedColumn (withBm srcBm) dat
                        other -> other
{-# INLINE gatherWithSentinel #-}

-- | An @n@-row payload for a gather whose source is empty.
allNullBoxed :: Int -> VB.Vector a -> VB.Vector a
allNullBoxed n _ = VB.replicate n (error "gatherWithSentinel: null row forced")

-- | 'allNullBoxed' for unboxed payloads; the buffer is left uninitialised.
allNullUnboxed :: (VU.Unbox a) => Int -> VU.Vector a -> VU.Vector a
allNullUnboxed n _ = runST (VUM.new n >>= VU.unsafeFreeze)

-- | Internal helper to get indices in a boxed vector.
getIndices :: VU.Vector Int -> VB.Vector a -> VB.Vector a
getIndices indices xs = VB.generate (VU.length indices) (\i -> xs VB.! (indices VU.! i))
{-# INLINE getIndices #-}

-- | Internal helper to get indices in an unboxed vector.
getIndicesUnboxed :: (VU.Unbox a) => VU.Vector Int -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = VU.generate (VU.length indices) (\i -> xs VU.! (indices VU.! i))
{-# INLINE getIndicesUnboxed #-}

-- | Positions of the 'True' entries, in order, computed in parallel.
maskIndices :: VU.Vector Bool -> VU.Vector Int
maskIndices mask
    | not (shouldParallelize parThreshold n) = VU.findIndices id mask
    | otherwise = unsafePerformIO $ do
        let chunks = splitChunkRange capabilities n
            countChunk (lo, hi) = pure $! countFrom lo hi 0
            countFrom !i !hi !c
                | i >= hi = c
                | otherwise = countFrom (i + 1) hi (c + fromEnum (VU.unsafeIndex mask i))
        counts <- forkJoin (map countChunk chunks)
        let starts = scanl (+) 0 counts
        out <- VUM.unsafeNew (last starts)
        let fill (lo, hi) start =
                let go !i !k
                        | i >= hi = pure ()
                        | VU.unsafeIndex mask i = VUM.unsafeWrite out k i >> go (i + 1) (k + 1)
                        | otherwise = go (i + 1) k
                 in go lo start
        forkJoin_ (zipWith fill chunks starts)
        VU.unsafeFreeze out
  where
    !n = VU.length mask

findIndices ::
    forall a.
    (Columnable a) =>
    (a -> Bool) ->
    Column ->
    Either DataFrameException (VU.Vector Int)
findIndices predicate = \case
    BoxedColumn bm (v :: VB.Vector b) -> run bm v
    UnboxedColumn bm (v :: VU.Vector b) -> run bm v
    c@(PackedText _ _) -> findIndices predicate (materializePacked c)
    c@(MergedColumn _ _) -> findIndices predicate (materializeMerged c)
  where
    -- Null rows never match.
    run ::
        forall b v.
        (Typeable b, VG.Vector v b, VG.Vector v Int) =>
        Maybe Bitmap ->
        v b ->
        Either DataFrameException (VU.Vector Int)
    run bm column = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ case nullsIn (VG.length column) bm of
            Nothing -> VG.convert (VG.findIndices predicate column)
            Just nulls ->
                VU.filter
                    (\i -> bitmapTestBit nulls i && predicate (VG.unsafeIndex column i))
                    (VU.enumFromN 0 (VG.length column))
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "findIndices"
                        , errorColumnName = Nothing
                        }

-- | Fold (right) column with index, skipping null rows.
ifoldrColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (Int -> a -> b -> b) -> b -> Column -> Either DataFrameException b
ifoldrColumn f acc = \case
    BoxedColumn bm column -> foldrWorker bm column
    UnboxedColumn bm column -> foldrWorker bm column
    c@(PackedText _ _) -> ifoldrColumn f acc (materializePacked c)
    c@(MergedColumn _ _) -> ifoldrColumn f acc (materializeMerged c)
  where
    foldrWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException b
    foldrWorker bm vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> pure $ case nullsIn (VG.length vec) bm of
            Nothing -> VG.ifoldr f acc vec
            Just nulls -> VG.ifoldr (\i x r -> if bitmapTestBit nulls i then f i x r else r) acc vec
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "ifoldrColumn"
                        , errorColumnName = Nothing
                        }

-- | Left fold over a column, skipping null rows.
foldlColumn ::
    forall a b.
    (Columnable a, Columnable b) =>
    (b -> a -> b) -> b -> Column -> Either DataFrameException b
foldlColumn f acc = \case
    BoxedColumn bm column -> foldlWorker bm column
    UnboxedColumn bm (column :: VU.Vector c)
        | Just Refl <- testEquality (typeRep @a) (typeRep @c)
        , Nothing <- nullsIn (VU.length column) bm ->
            pure (fromMaybe VU.foldl' compiledFoldl f acc column)
        | otherwise -> foldlWorker bm column
    c@(PackedText _ _) -> foldlColumn f acc (materializePacked c)
    c@(MergedColumn _ _) -> foldlColumn f acc (materializeMerged c)
  where
    foldlWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException b
    foldlWorker bm vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> pure $ case nullsIn (VG.length vec) bm of
            Nothing -> VG.foldl' f acc vec
            Just nulls -> VG.ifoldl' (\r i x -> if bitmapTestBit nulls i then f r x else r) acc vec
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "foldlColumn"
                        , errorColumnName = Nothing
                        }

-- | Left fold seeded with the first valid row, skipping null rows.
foldl1Column ::
    forall a.
    (Columnable a) =>
    (a -> a -> a) -> Column -> Either DataFrameException a
foldl1Column f = \case
    BoxedColumn bm column -> foldl1Worker bm column
    UnboxedColumn bm (column :: VU.Vector c)
        | Just Refl <- testEquality (typeRep @a) (typeRep @c)
        , Nothing <- nullsIn (VU.length column) bm ->
            pure (fromMaybe VU.foldl1' compiledFoldl1 f column)
        | otherwise -> foldl1Worker bm column
    c@(PackedText _ _) -> foldl1Column f (materializePacked c)
    c@(MergedColumn _ _) -> foldl1Column f (materializeMerged c)
  where
    foldl1Worker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException a
    foldl1Worker bm vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> case nullsIn (VG.length vec) bm of
            Nothing -> pure (VG.foldl1' f vec)
            Just nulls ->
                let step r i x
                        | not (bitmapTestBit nulls i) = r
                        | otherwise = Just $! maybe x (`f` x) r
                 in maybe
                        (Left (EmptyDataSetException "foldl1Column"))
                        Right
                        (VG.ifoldl' step Nothing vec)
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "foldl1Column"
                        , errorColumnName = Nothing
                        }

-- | O(n) Seedless fold over groups using each group's first element as seed.
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
        UnboxedColumn bm (vec :: VU.Vector d) -> uncurry UnboxedColumn <$> foldl1Worker bm vec
        BoxedColumn bm (vec :: VB.Vector d) -> uncurry BoxedColumn <$> foldl1Worker bm vec
        PackedText _ _ -> foldl1DirectGroups f (materializePacked col) valueIndices offsets
        MergedColumn _ _ -> foldl1DirectGroups f (materializeMerged col) valueIndices offsets
  where
    nGroups = VU.length offsets - 1
    foldl1Worker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException (Maybe Bitmap, v c)
    foldl1Worker bm vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> Right $ case nullsIn (VG.length vec) bm of
            Nothing -> (Nothing, VG.generate nGroups foldGroup)
            Just nulls ->
                -- A group of all-null rows has no seed, so it is null.
                let groups = VG.generate nGroups (foldValidGroup nulls) :: VB.Vector (Maybe c)
                    empty = VU.filter (isNothing . VB.unsafeIndex groups) (VU.enumFromN 0 nGroups)
                    placeholder k = VG.unsafeIndex vec (VU.unsafeIndex valueIndices (VU.unsafeIndex offsets k))
                 in ( Just (buildBitmapFromNulls' nGroups empty)
                    , VG.generate nGroups (\k -> fromMaybe (placeholder k) (VB.unsafeIndex groups k))
                    )
          where
            row i = VG.unsafeIndex vec (VU.unsafeIndex valueIndices i)
            foldGroup k =
                let !s = VU.unsafeIndex offsets k
                    !e = VU.unsafeIndex offsets (k + 1)
                 in go (s + 1) e (row s)
            go !i !e !acc
                | i >= e = acc
                | otherwise = go (i + 1) e $! f acc (row i)
            foldValidGroup nulls k = goValid (VU.unsafeIndex offsets k) Nothing
              where
                !e = VU.unsafeIndex offsets (k + 1)
                goValid !i acc
                    | i >= e = acc
                    | not (bitmapTestBit nulls (VU.unsafeIndex valueIndices i)) =
                        goValid (i + 1) acc
                    | otherwise = goValid (i + 1) (Just $! maybe (row i) (`f` row i) acc)
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

-- | O(n) fold over groups by scanning the column linearly.
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
        UnboxedColumn bm (vec :: VU.Vector d) -> foldLinearWorker bm vec
        BoxedColumn bm (vec :: VB.Vector d) -> foldLinearWorker bm vec
        PackedText _ _ ->
            foldLinearGroups f seed (materializePacked col) rowToGroup nGroups
        MergedColumn _ _ ->
            foldLinearGroups f seed (materializeMerged col) rowToGroup nGroups
  where
    -- Null rows are skipped; a group with no valid rows keeps the seed.
    foldLinearWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException Column
    foldLinearWorker bm vec = case testEquality (typeRep @b) (typeRep @c) of
        Just Refl ->
            let nulls = nullsIn (VG.length vec) bm
             in Right $
                    unsafePerformIO $
                        runWith
                            ( \readAt writeAt ->
                                VG.iforM_ vec $ \row x -> when (validAt nulls row) $ do
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

-- | The first valid row.
headColumn :: forall a. (Columnable a) => Column -> Either DataFrameException a
headColumn = \case
    BoxedColumn bm col -> headWorker bm col
    UnboxedColumn bm col -> headWorker bm col
    c@(PackedText _ _) -> headColumn (materializePacked c)
    c@(MergedColumn _ _) -> headColumn (mergedHead c)
  where
    headWorker ::
        forall c v.
        (Typeable c, VG.Vector v c) =>
        Maybe Bitmap ->
        v c ->
        Either DataFrameException a
    headWorker bm vec = case testEquality (typeRep @a) (typeRep @c) of
        Just Refl ->
            case VU.find (validAt bm) (VU.enumFromN 0 (VG.length vec)) of
                Nothing -> Left (EmptyDataSetException "headColumn")
                Just i -> pure (VG.unsafeIndex vec i)
        Nothing ->
            Left $
                TypeMismatchException
                    MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @c)
                        , callingFunctionName = Just "headColumn"
                        , errorColumnName = Nothing
                        }

-- | An internal, column version of zip.
zipColumns :: Column -> Column -> Column
zipColumns l@(MergedColumn _ _) r = zipColumns (materializeMerged l) r
zipColumns l r@(MergedColumn _ _) = zipColumns l (materializeMerged r)
zipColumns l@(PackedText _ _) r = zipColumns (materializePacked l) r
zipColumns l r@(PackedText _ _) = zipColumns l (materializePacked r)
zipColumns (BoxedColumn bmL column) (BoxedColumn bmR other) = withBitmap (bothValid bmL bmR) (zipVectors column other)
zipColumns (BoxedColumn bmL column) (UnboxedColumn bmR other) = withBitmap (bothValid bmL bmR) (zipVectors column other)
zipColumns (UnboxedColumn bmL column) (BoxedColumn bmR other) = withBitmap (bothValid bmL bmR) (zipVectors column other)
zipColumns (UnboxedColumn bmL column) (UnboxedColumn bmR other) = withBitmap (bothValid bmL bmR) (zipVectors column other)
{-# INLINE zipColumns #-}

zipVectors ::
    forall a b va vb.
    (Columnable a, Columnable b, VG.Vector va a, VG.Vector vb b) =>
    va a -> vb b -> Column
zipVectors column other =
    case sUnbox @a of
        STrue -> case sUnbox @b of
            STrue -> UnboxedColumn Nothing (VU.generate n pairAt)
            SFalse -> BoxedColumn Nothing (VB.generate n pairAt)
        SFalse -> BoxedColumn Nothing (VB.generate n pairAt)
  where
    !n = min (VG.length column) (VG.length other)
    pairAt i = (VG.unsafeIndex column i, VG.unsafeIndex other i)
{-# INLINE zipVectors #-}

-- | An internal, column version of zipWith.
zipWithColumns ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Column -> Column -> Either DataFrameException Column
zipWithColumns f left right = case (left, right) of
    (PackedText _ _, _) -> zipWithColumns f (materializePacked left) right
    (_, PackedText _ _) -> zipWithColumns f left (materializePacked right)
    (MergedColumn _ _, _) -> zipWithColumns f (materializeMerged left) right
    (_, MergedColumn _ _) -> zipWithColumns f left (materializeMerged right)
    ( UnboxedColumn Nothing (l :: VU.Vector d)
        , UnboxedColumn Nothing (r :: VU.Vector e)
        )
            | Just Refl <- testEquality (typeRep @a) (typeRep @d)
            , Just Refl <- testEquality (typeRep @b) (typeRep @e)
            , STrue <- sUnbox @c ->
                pure . UnboxedColumn Nothing $ case testEquality (typeRep @a) (typeRep @b) of
                    Just Refl -> fromMaybe zipLoop compiledZip f l r
                    Nothing -> zipLoop f l r
    (BoxedColumn bmL l, BoxedColumn bmR r) -> elements bmL l bmR r
    (BoxedColumn bmL l, UnboxedColumn bmR r) -> elements bmL l bmR r
    (UnboxedColumn bmL l, BoxedColumn bmR r) -> elements bmL l bmR r
    (UnboxedColumn bmL l, UnboxedColumn bmR r) -> elements bmL l bmR r
  where
    -- A row is null if either side is; f never sees a null row.
    elements ::
        forall d e vl vr.
        (Typeable d, Typeable e, VG.Vector vl d, VG.Vector vr e) =>
        Maybe Bitmap -> vl d -> Maybe Bitmap -> vr e -> Either DataFrameException Column
    elements bmL l bmR r
        | Just Refl <- testEquality (typeRep @a) (typeRep @d)
        , Just Refl <- testEquality (typeRep @b) (typeRep @e) =
            let !n = min (VG.length l) (VG.length r)
                g i = f (VG.unsafeIndex l i) (VG.unsafeIndex r i)
             in pure $ case (bothValid bmL bmR, sUnbox @c) of
                    (Nothing, STrue) -> UnboxedColumn Nothing (parGenerateUnboxed n g)
                    (Nothing, SFalse) -> fromVector @c (VB.generate n g)
                    (Just bm, STrue) -> UnboxedColumn (Just bm) $ case nullsIn n (Just bm) of
                        Nothing -> parGenerateUnboxed n g
                        Just nulls -> generateValidUnboxed nulls n g
                    (Just bm, SFalse) -> generateValidBoxed bm n g
        | otherwise = zipWithColumnsGeneral f left right

-- | Valid where both are valid; a missing bitmap means every row is valid.
bothValid :: Maybe Bitmap -> Maybe Bitmap -> Maybe Bitmap
bothValid (Just l) (Just r) = Just (andBitmaps l r)
bothValid l Nothing = l
bothValid Nothing r = r

withBitmap :: Maybe Bitmap -> Column -> Column
withBitmap bm (BoxedColumn _ v) = BoxedColumn bm v
withBitmap bm (UnboxedColumn _ v) = UnboxedColumn bm v
withBitmap _ c = c

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

-- writeColumn and freezeColumn' moved to DataFrame.IO.Internal.MutableColumn.

-- | Freeze a mutable column into an @Either Text a@ column.
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

-- | Promote a non-nullable column to a nullable one (all-valid bitmap).
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

-- | Fill the end of a column, up to n, with null rows.
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

-- | Fill the beginning of a column, up to n, with null rows.
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

-- | Concatenates two columns; fails if the column types differ.
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

-- ike 'mappendColumns' but wraps values in 'Either' across types.

-- | O(n) Concatenate a list of same-type columns in a single allocation.
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
            newBm = concatValidity (zipWith Validity allBms (map VB.length allVecs))
         in BoxedColumn newBm (VB.concat allVecs)
    UnboxedColumn bm0 v0 ->
        let getCol (UnboxedColumn bm v) = case testEquality (typeOf v0) (typeOf v) of
                Just Refl -> (bm, v)
                Nothing -> error "concatManyColumns: UnboxedColumn type mismatch"
            getCol _ = error "concatManyColumns: column constructor mismatch"
            rest = map getCol cs
            allVecs = v0 : map snd rest
            allBms = bm0 : map fst rest
            newBm = concatValidity (zipWith Validity allBms (map VU.length allVecs))
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

-- | Gather one in-bounds index vector through several columns in one pass.
atIndicesStableMulti :: VU.Vector Int -> [Column] -> [Column]
atIndicesStableMulti ixs cols =
    let specs = map mgClassify cols
        nFused = length [() | Just _ <- specs]
     in if nFused < 2
            then map (atIndicesStable ixs) cols
            else
                let fused = multiGatherRun ixs (catMaybes specs)
                    go [] _ = []
                    go (Nothing : ss) !k = atIndicesStable ixs (cols !! k) : go ss (k + 1)
                    go (Just _ : ss) !k =
                        let !r = mgRank k
                         in (fused VB.! r) : go ss (k + 1)
                    -- fused-output rank of column position k.
                    mgRank k = length [() | Just _ <- take k specs]
                 in go specs 0

-- | One fusable source column shape (all bitmap-free).
data MGSpec
    = -- | Clean unboxed 'Int' payload.
      MGInt !(VU.Vector Int)
    | -- | Clean unboxed 'Double' payload.
      MGDouble !(VU.Vector Double)
    | -- | Packed text with an 'Int32' selector and an 'Int32'-sized base.
      MGSel32 !PackedTextData !(VU.Vector Int32)
    | -- | Packed text with an 'Int' selector and an 'Int32'-sized base.
      MGSel64To32 !PackedTextData !(VU.Vector Int)
    | -- | Packed text with an 'Int' selector and a wide base.
      MGSel64To64 !PackedTextData !(VU.Vector Int)

mgClassify :: Column -> Maybe MGSpec
mgClassify (UnboxedColumn Nothing (v :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> Just (MGInt v)
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> Just (MGDouble v)
            Nothing -> Nothing
mgClassify (PackedText Nothing p) = case ptSel p of
    Just (Sel32 s) -> Just (MGSel32 p s)
    Just (Sel64 s)
        | offCount (ptOffsets p) - 1 <= mgInt32Max -> Just (MGSel64To32 p s)
        | otherwise -> Just (MGSel64To64 p s)
    Nothing -> Nothing
mgClassify _ = Nothing

mgInt32Max :: Int
mgInt32Max = fromIntegral (maxBound :: Int32)

-- | Run the fused pass over the fusable specs, one gathered column per spec.
multiGatherRun :: VU.Vector Int -> [MGSpec] -> VB.Vector Column
multiGatherRun ixs specs = unsafePerformIO $ do
    let !n = VU.length ixs
    opened <- mapM (mgOpen n ixs) specs
    let fills = map fst opened
        !block = 4096
        worker !lo !hi
            | lo >= hi = pure ()
            | otherwise = do
                let !e = min hi (lo + block)
                mapM_ (\fill -> fill lo e) fills
                worker e hi
    parallelChunks_ parThreshold n worker
    VB.fromList <$> mapM snd opened
{-# NOINLINE multiGatherRun #-}

-- | Allocate a spec's destination; return its block-fill action and finalizer.
mgOpen :: Int -> VU.Vector Int -> MGSpec -> IO (Int -> Int -> IO (), IO Column)
mgOpen n ixs spec = case spec of
    MGInt v -> do
        mv <- VUM.unsafeNew n
        pure
            ( mgFillInt ixs v mv
            , UnboxedColumn Nothing <$> VU.unsafeFreeze mv
            )
    MGDouble v -> do
        mv <- VUM.unsafeNew n
        pure
            ( mgFillDouble ixs v mv
            , UnboxedColumn Nothing <$> VU.unsafeFreeze mv
            )
    MGSel32 p s -> do
        mv <- VUM.unsafeNew n
        pure
            ( mgFillSel32 ixs s (offCount (ptOffsets p) - 1) mv
            , (\out -> PackedText Nothing p{ptSel = Just (Sel32 out)}) <$> VU.unsafeFreeze mv
            )
    MGSel64To32 p s -> do
        mv <- VUM.unsafeNew n
        pure
            ( mgFillSel64To32 ixs s (offCount (ptOffsets p) - 1) mv
            , (\out -> PackedText Nothing p{ptSel = Just (Sel32 out)}) <$> VU.unsafeFreeze mv
            )
    MGSel64To64 p s -> do
        mv <- VUM.unsafeNew n
        pure
            ( mgFillSel64To64 ixs s (offCount (ptOffsets p) - 1) mv
            , (\out -> PackedText Nothing p{ptSel = Just (Sel64 out)}) <$> VU.unsafeFreeze mv
            )

mgFillInt ::
    VU.Vector Int -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
mgFillInt ixs v dst lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite dst i (VU.unsafeIndex v (VU.unsafeIndex ixs i))
            go (i + 1)
{-# NOINLINE mgFillInt #-}

mgFillDouble ::
    VU.Vector Int -> VU.Vector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
mgFillDouble ixs v dst lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite dst i (VU.unsafeIndex v (VU.unsafeIndex ixs i))
            go (i + 1)
{-# NOINLINE mgFillDouble #-}

mgFillSel32 ::
    VU.Vector Int ->
    VU.Vector Int32 ->
    Int ->
    VUM.IOVector Int32 ->
    Int ->
    Int ->
    IO ()
mgFillSel32 ixs s !base dst lo hi = go lo
  where
    !sn = VU.length s
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !j = VU.unsafeIndex ixs i
                !out =
                    if j >= 0 && j < sn
                        then
                            let !r = fromIntegral (VU.unsafeIndex s j) :: Int
                             in if r >= 0 && r < base then fromIntegral r else -1
                        else -1
            VUM.unsafeWrite dst i out
            go (i + 1)
{-# NOINLINE mgFillSel32 #-}

mgFillSel64To32 ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VUM.IOVector Int32 ->
    Int ->
    Int ->
    IO ()
mgFillSel64To32 ixs s !base dst lo hi = go lo
  where
    !sn = VU.length s
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !j = VU.unsafeIndex ixs i
                !out =
                    if j >= 0 && j < sn
                        then
                            let !r = VU.unsafeIndex s j
                             in if r >= 0 && r < base then fromIntegral r else -1
                        else -1
            VUM.unsafeWrite dst i out
            go (i + 1)
{-# NOINLINE mgFillSel64To32 #-}

mgFillSel64To64 ::
    VU.Vector Int -> VU.Vector Int -> Int -> VUM.IOVector Int -> Int -> Int -> IO ()
mgFillSel64To64 ixs s !base dst lo hi = go lo
  where
    !sn = VU.length s
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !j = VU.unsafeIndex ixs i
                !out =
                    if j >= 0 && j < sn
                        then
                            let !r = VU.unsafeIndex s j
                             in if r >= 0 && r < base then r else -1
                        else -1
            VUM.unsafeWrite dst i out
            go (i + 1)
{-# NOINLINE mgFillSel64To64 #-}

-- | A packed text column of @f i@ per row, built chunk by chunk in parallel.
packedMapText :: Int -> (Int -> T.Text) -> Column
packedMapText n f = unsafePerformIO $ do
    chunks <-
        if shouldParallelize parThreshold n
            then parallelChunks parThreshold n (\lo hi -> pure $! packRows lo hi)
            else pure [packRows 0 n]
    let totalBytes = sum [VU.unsafeIndex o (VU.length o - 1) | (_, o) <- chunks]
        totalRows = sum [VU.length o - 1 | (_, o) <- chunks]
    pure $! runST $ do
        arr <- A.new (max 1 totalBytes)
        offs <- VUM.unsafeNew (totalRows + 1)
        VUM.unsafeWrite offs 0 0
        let splice !byteBase !rowBase ((a, o) : rest) = do
                let rows = VU.length o - 1
                    used = VU.unsafeIndex o rows
                A.copyI used arr byteBase a 0
                forM_ [1 .. rows] $ \i ->
                    VUM.unsafeWrite offs (rowBase + i) (byteBase + VU.unsafeIndex o i)
                splice (byteBase + used) (rowBase + rows) rest
            splice _ _ [] = pure ()
        splice 0 0 chunks
        farr <- A.unsafeFreeze arr
        foffs <- VU.unsafeFreeze offs
        pure (PackedText Nothing (mkPackedContiguous farr foffs))
  where
    -- One chunk's rows as (bytes, offsets); offsets are chunk-relative.
    packRows lo hi =
        let texts = VB.generate (hi - lo) (\i -> f (lo + i))
            lens = VU.generate (hi - lo) (textBytes . VB.unsafeIndex texts)
            offs = VU.scanl' (+) 0 lens
            bytes = VU.unsafeIndex offs (hi - lo)
            arr = runST $ do
                m <- A.new (max 1 bytes)
                VB.iforM_ texts $ \i (Text src off len) ->
                    A.copyI len m (VU.unsafeIndex offs i) src off
                A.unsafeFreeze m
         in (arr, offs)
    textBytes (Text _ _ len) = len
