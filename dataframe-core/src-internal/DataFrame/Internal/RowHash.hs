{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Row-hash kernels with a parallel driver, feeding grouping and the join
build/probe. Each row's hash depends only on its own bytes, so hashing disjoint
ranges in parallel is race-free and bit-identical to the sequential pass.
-}
module DataFrame.Internal.RowHash (
    computeRowHashesIO,
    hashRowRange,
    parRowHashThreshold,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Bitmap, Column (..), bitmapTestBit)
import DataFrame.Internal.Hash (
    fnvOffset,
    mixBytes,
    mixDouble,
    mixInt,
    mixShow,
    mixText,
    nullSalt,
 )
import DataFrame.Internal.PackedText (
    PackedTextData (..),
    packedSlice,
 )
import DataFrame.Internal.Types (
    SBool (..),
    sFloating,
    sIntegral,
 )

{- | At least this many rows make the fork/coordination overhead of the parallel
hash worth it. Below it the sequential single range is used. Matches the
grouping/join parallel thresholds so the whole pipeline switches together.
-}
parRowHashThreshold :: Int
parRowHashThreshold = 200000

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

{- | Compute the per-row key hash over the selected key columns of an @n@-row
frame. Forks one worker per capability over disjoint row ranges when the row
count justifies it, else hashes the single full range; output is capability-independent.
-}
computeRowHashesIO :: Int -> [Column] -> IO (VU.Vector Int)
computeRowHashesIO n selected = do
    mv <- VUM.unsafeNew (max 1 n)
    let runRange lo hi = hashRowRange mv lo hi selected
    if n >= parRowHashThreshold && capabilities > 1
        then do
            let !caps = capabilities
                !per = (n + caps - 1) `div` caps
                spawn w = do
                    var <- newEmptyMVar
                    let !lo = min n (w * per)
                        !hi = min n (lo + per)
                    _ <- forkIO (try (runRange lo hi) >>= putMVar var)
                    pure var
            vars <- mapM spawn [0 .. caps - 1]
            rs <- mapM takeMVar vars
            mapM_ (either (throwIO @SomeException) pure) rs
        else runRange 0 n
    VU.unsafeFreeze (VUM.slice 0 n mv)

{- | Mix every selected column over the row range @[lo, hi)@ into @mv@, seeding
each slot with 'fnvOffset'. Must match the sequential grouping hash byte-for-byte
so grouping and joins bucket identically.
-}
hashRowRange :: VUM.IOVector Int -> Int -> Int -> [Column] -> IO ()
hashRowRange mv lo hi cols = do
    seedRange mv lo hi
    mapM_ (mixColumnRange mv lo hi) cols

seedRange :: VUM.IOVector Int -> Int -> Int -> IO ()
seedRange mv lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = VUM.unsafeWrite mv i fnvOffset >> go (i + 1)

{- | Fold one column's values over @[lo, hi)@ into the running hashes. The branch
structure mirrors the sequential grouping hash: typed unboxed fast paths, then a
'mixShow' fallback, with the null bitmap mixing 'nullSalt'.
-}
mixColumnRange :: VUM.IOVector Int -> Int -> Int -> Column -> IO ()
mixColumnRange mv lo hi = \case
    UnboxedColumn ubm (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> unboxedRange mv lo hi ubm mixInt v
            Nothing ->
                case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> unboxedRange mv lo hi ubm mixDouble v
                    Nothing ->
                        case sIntegral @a of
                            STrue ->
                                unboxedRange mv lo hi ubm (\h d -> mixInt h (fromIntegral @a @Int d)) v
                            SFalse ->
                                case sFloating @a of
                                    STrue ->
                                        unboxedRange mv lo hi ubm (\h d -> mixDouble h (realToFrac d :: Double)) v
                                    SFalse ->
                                        unboxedRange mv lo hi ubm mixShow v
    BoxedColumn bm (v :: V.Vector a) ->
        case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> boxedRange mv lo hi bm mixText v
            Nothing -> boxedRange mv lo hi bm mixShow v
    PackedText bm p -> packedRange mv lo hi bm p

{- | Mix an unboxed column's range, mixing 'nullSalt' at null slots. @INLINE@d to
specialise on the element type and mixing function per call site.
-}
unboxedRange ::
    (VU.Unbox a) =>
    VUM.IOVector Int ->
    Int ->
    Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    VU.Vector a ->
    IO ()
unboxedRange mv lo hi ubm mix v = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            h <- VUM.unsafeRead mv i
            let !h' = case ubm of
                    Just bm | not (bitmapTestBit bm i) -> mixInt h nullSalt
                    _ -> mix h (VU.unsafeIndex v i)
            VUM.unsafeWrite mv i h'
            go (i + 1)
{-# INLINE unboxedRange #-}

boxedRange ::
    VUM.IOVector Int ->
    Int ->
    Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    V.Vector a ->
    IO ()
boxedRange mv lo hi bm mix v = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            h <- VUM.unsafeRead mv i
            let !h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> mix h (V.unsafeIndex v i)
            VUM.unsafeWrite mv i h'
            go (i + 1)
{-# INLINE boxedRange #-}

{- | Mix a packed-text column's range over its raw UTF-8 byte slices. The
unselected payload is the hot path (indexes the offset vector directly); a
selected payload (a gather/join result) falls back to 'packedSlice'.
-}
packedRange ::
    VUM.IOVector Int ->
    Int ->
    Int ->
    Maybe Bitmap ->
    PackedTextData ->
    IO ()
packedRange mv lo hi bm p =
    case ptSel p of
        Nothing -> contiguous (ptBytes p) (ptOffsets p)
        Just _ -> selected
  where
    valid i = case bm of
        Just bm' -> bitmapTestBit bm' i
        Nothing -> True
    contiguous !arr !offs = go lo
      where
        go !i
            | i >= hi = pure ()
            | otherwise = do
                h <- VUM.unsafeRead mv i
                let !o = VU.unsafeIndex offs i
                    !l = VU.unsafeIndex offs (i + 1) - o
                    !h' = if valid i then mixBytes h arr o l else mixInt h nullSalt
                VUM.unsafeWrite mv i h'
                go (i + 1)
    selected = go lo
      where
        go !i
            | i >= hi = pure ()
            | otherwise = do
                h <- VUM.unsafeRead mv i
                let !h' =
                        if valid i
                            then let (arr, o, l) = packedSlice p i in mixBytes h arr o l
                            else mixInt h nullSalt
                VUM.unsafeWrite mv i h'
                go (i + 1)
{-# INLINE packedRange #-}
