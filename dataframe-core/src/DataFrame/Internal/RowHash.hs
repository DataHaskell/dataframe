{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Row-hash kernels with a parallel driver.

The per-row key hash is the sole input to grouping and the join build/probe.
For a single wide pass over many rows (notably a 1e7-row text/factor join key)
the hashing is the dominant cost and is embarrassingly parallel: each row's hash
depends only on that row's own bytes, so hashing disjoint row ranges into
disjoint slots of one shared vector is race-free and produces a result
/bit-for-bit identical/ to the sequential single-pass hash.

'hashRowRange' is the shared per-range kernel (used sequentially and by every
worker); 'computeRowHashesIO' forks one worker per capability over contiguous
row ranges above 'parRowHashThreshold', else runs the range once. The mixing per
column type mirrors the grouping hash exactly so grouping and joins agree.
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

{- | Compute the per-row key hash over the (already selected) key columns of an
@n@-row frame. Forks one worker per capability over contiguous row ranges when
the row count justifies it (>= 'parRowHashThreshold' and more than one
capability); otherwise hashes the single full range. The output is identical for
any capability count: each row's hash is a pure function of its own bytes and
workers own disjoint row ranges.
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
each slot with 'fnvOffset' first. The seeding and per-column mixing must match
'DataFrame.Operations.Aggregation.computeRowHashes' byte-for-byte so grouping
and joins bucket identically.
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
contiguous (unselected) payload is the hot path: hoist the byte buffer and the
@n+1@ offset vector out of the loop and index them directly, so each row mixes
@[offs!i, offs!(i+1))@ with no per-row selection 'Maybe' test or 'packedSlice'
tuple. A selected payload (a gather/join result) falls back to 'packedSlice'.
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
