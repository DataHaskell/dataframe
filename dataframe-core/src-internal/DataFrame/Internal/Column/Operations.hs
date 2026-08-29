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
import Data.Bits (setBit, shiftL, shiftR)
import Data.Int (Int32)
import Data.Kind (Type)
import Data.Maybe (catMaybes, fromMaybe, isNothing)
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
    parThreshold,
    parallelChunks_,
    shouldParallelize,
 )
import DataFrame.Internal.Data.PackedText (
    PackedOffsets,
    PackedSel (..),
    PackedTextData (..),
    offCount,
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
                Right $ case sUnbox @c of
                    STrue ->
                        UnboxedColumn
                            bm
                            (parGenerateUnboxed (VB.length col) (f . VB.unsafeIndex col))
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
            Just Refl -> Right $ case sUnbox @c of
                STrue ->
                    UnboxedColumn
                        bm
                        (parGenerateUnboxed (VU.length col) (f . VU.unsafeIndex col))
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

-------------------------------------------------------------------------------
-- Parallel element-wise kernels
-------------------------------------------------------------------------------

{- | Parallel unboxed 'VU.generate': splits the index space into one contiguous
chunk per capability, evaluates each chunk into its disjoint slice of a single
pre-allocated mutable vector, then freezes. Element @i@ depends only on @f i@,
so the result is bit-identical to the sequential 'VU.generate' regardless of
capability count. Falls back to 'VU.generate' below 'parThreshold'.
-}
{-# SPECIALIZE parGenerateUnboxed ::
    Int -> (Int -> Double) -> VU.Vector Double
    #-}
{-# SPECIALIZE parGenerateUnboxed ::
    Int -> (Int -> Float) -> VU.Vector Float
    #-}
{-# SPECIALIZE parGenerateUnboxed :: Int -> (Int -> Int) -> VU.Vector Int #-}
{-# SPECIALIZE parGenerateUnboxed :: Int -> (Int -> Bool) -> VU.Vector Bool #-}
parGenerateUnboxed :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenerateUnboxed n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n (fillGenerate mv f)
        VU.unsafeFreeze mv
{-# NOINLINE parGenerateUnboxed #-}

{- | The chunk body shared by 'parGenerateUnboxed' and
'parGenerateUnboxedInline'. INLINE so the latter's monomorphic wrappers each
get their own copy with @f@ inlined.
-}
fillGenerate ::
    (VU.Unbox c) => VUM.IOVector c -> (Int -> c) -> Int -> Int -> IO ()
fillGenerate mv f !lo !hi =
    let go !i
            | i >= hi = pure ()
            | otherwise = VUM.unsafeWrite mv i (f i) >> go (i + 1)
     in go lo
{-# INLINE fillGenerate #-}

{- | Parallel unboxed gather: element @i@ of the result is
@v ! (ix ! i)@ (unsafe indexing — callers pass in-bounds index vectors, e.g.
grouping-produced representative rows). Same chunking as 'parGenerateUnboxed'
(one contiguous chunk per capability into disjoint slices of one buffer), so
the result is bit-identical to the sequential backpermute at any capability
count; falls back to a sequential loop below 'parThreshold'.
-}
parBackpermuteUnboxed ::
    (VU.Unbox a) => VU.Vector a -> VU.Vector Int -> VU.Vector a
parBackpermuteUnboxed v ix =
    parGenerateUnboxed (VU.length ix) (VU.unsafeIndex v . VU.unsafeIndex ix)
{-# INLINE parBackpermuteUnboxed #-}

{- | 'parGenerateUnboxed' with an INLINE body: each monomorphic NOINLINE
wrapper below gets its own copy of the fill loop with @f@ inlined, so the
per-element unknown closure call (and the boxed result it returns) disappears
— on a 1e8-row gather that call+alloc dominated the whole pass. Same chunking,
bit-identical results; wrappers must stay NOINLINE so the 'unsafePerformIO'
runs once per call.
-}
parGenerateUnboxedInline :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenerateUnboxedInline n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n (fillGenerate mv f)
        VU.unsafeFreeze mv
{-# INLINE parGenerateUnboxedInline #-}

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

{- | Closure-free double-indirection gather: @out!g = vis ! (offs!g)@ over
@length offs - 1@ groups (the representative-row build of the 'Grouped'
pattern).
-}
parBackpermute2Int :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
parBackpermute2Int vis offs =
    parGenerateUnboxedInline
        (max 0 (VU.length offs - 1))
        (VU.unsafeIndex vis . VU.unsafeIndex offs)
{-# NOINLINE parBackpermute2Int #-}

{- | Parallel boxed gather. The read side uses 'VB.unsafeIndexM' so the array
slot is fetched eagerly (element pointers are shared, elements themselves stay
un-forced, exactly as the sequential 'VB.generate' gather). Bit-identical
element values; sequential below 'parThreshold'.
-}
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

{- | Clamp sentinel (negative) indices to 0 in one closure-free parallel pass.
The clamped rows read row 0's value; callers mask them via the sentinel bitmap.
-}
parClampNonNeg :: VU.Vector Int -> VU.Vector Int
parClampNonNeg ix =
    parGenerateUnboxedInline
        (VU.length ix)
        (\i -> let !x = VU.unsafeIndex ix i in max x 0)
{-# NOINLINE parClampNonNeg #-}

{- | Validity bitmap from sentinel indices (bit @i@ valid iff @ix!i >= 0@),
built one byte (8 rows) per element in parallel.
-}
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
        -- Int/Double hit the closure-free monomorphic kernels; anything else
        -- keeps the generic (per-element closure call) path.
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

{- | Like 'atIndicesStable' but treats negative indices as null.
Keeps the index vector fully unboxed (no @VB.Vector (Maybe Int)@).
-}
gatherWithSentinel :: VU.Vector Int -> Column -> Column
gatherWithSentinel indices c@(MergedColumn _ _) =
    gatherWithSentinel indices (materializeMerged c)
gatherWithSentinel indices col =
    let !n = VU.length indices
        !newBm = parBitmapNonNeg indices
        withBm srcBm = case srcBm of
            Nothing -> Just newBm
            Just sb -> Just (andBitmaps newBm (gatherSrcBm sb))
        -- Sequential fallback: gather an existing source bitmap through the
        -- raw indices (negative-guarded). Only runs when the source side is
        -- itself nullable, which join build sides normally are not.
        gatherSrcBm sb =
            buildBitmapFromValid $ VU.generate n $ \i ->
                let idx = VU.unsafeIndex indices i
                 in if idx >= 0 && bitmapTestBit sb idx then 1 else 0
     in case col of
            -- packedGather composes -1 sentinels into the selector natively
            -- (and shares the byte buffers), so text takes the raw indices.
            PackedText srcBm p -> PackedText (withBm srcBm) (packedGather indices p)
            BoxedColumn srcBm v
                -- An empty source means every index is a sentinel; clamping
                -- would read row 0 of an empty vector.
                | VB.null v -> BoxedColumn (withBm srcBm) (allNullBoxed n v)
                | otherwise ->
                    BoxedColumn
                        (withBm srcBm)
                        (parBackpermuteBoxed v (parClampNonNeg indices))
            UnboxedColumn srcBm v
                | VU.null v -> UnboxedColumn (withBm srcBm) (allNullUnboxed n v)
                | otherwise ->
                    -- Reuse atIndicesStable's Int/Double monomorphic kernel
                    -- dispatch for the payload; the bitmap is replaced below.
                    case atIndicesStable (parClampNonNeg indices) (UnboxedColumn Nothing v) of
                        UnboxedColumn _ dat -> UnboxedColumn (withBm srcBm) dat
                        other -> other
{-# INLINE gatherWithSentinel #-}

{- | An @n@-row payload for a gather whose source is empty: every index is a
sentinel, so the sentinel bitmap masks every row and no element is ever read.
The source vector is passed only to fix the element type.
-}
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
                    STrue ->
                        let !n = min (VU.length column) (VU.length other)
                         in UnboxedColumn Nothing $
                                parGenerateUnboxed n $ \i ->
                                    f (VU.unsafeIndex column i) (VU.unsafeIndex other i)
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

-------------------------------------------------------------------------------
-- Fused multi-column gather
-------------------------------------------------------------------------------

{- | Gather ONE in-bounds index vector through several columns in a single
parallel pass. Result columns are identical to @map (atIndicesStable ixs)@;
columns whose shape the fused kernel does not cover (bitmapped, boxed, merged,
unusual element types) fall back to per-column 'atIndicesStable'. All fused
outputs are backed by one shared deferred computation: forcing ANY of them
runs the single pass that fills ALL of them. The pass reads the index vector
once per block instead of once per column, and every iteration of a block
issues each column's independent random load back-to-back, so their cache/TLB
misses overlap instead of forming one latency chain per column (the aggregate
key materialization of a 1e8-group result was 6 sequential latency-bound
passes without this).
-}
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
    | {- | Packed text with an 'Int32' selector (base row count necessarily
      fits 'Int32'); carries the payload for rebuilding and the canon flag.
      -}
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

{- | Run the fused pass over the fusable specs; element @r@ of the result is
spec @r@'s gathered column. Deferred as one shared thunk (see
'atIndicesStableMulti'). Pure w.r.t. its immutable inputs, so the
'unsafePerformIO' is safe.
-}
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

{- | Allocate a spec's destination; return its block-fill action and its
finalizer. Selector gathers reproduce 'packedGather' exactly (composition
clamps against the source selector length and base row count); unboxed gathers
reproduce the unclamped 'parBackpermuteUnboxed'.
-}
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
