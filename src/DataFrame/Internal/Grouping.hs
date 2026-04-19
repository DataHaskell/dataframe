{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Grouping (
    groupBy,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Radix as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import Data.Bits
import Data.Hashable
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Types
import Type.Reflection (typeRep)

{- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
into vector that should be reduced later.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | nRows df == 0 =
        Grouped
            df
            names
            VU.empty
            (VU.fromList [0])
            VU.empty
    | otherwise =
        let !vis = VU.map fst valIndices
            !os = changingPoints valIndices
            !n = nRows df
         in Grouped
                df
                names
                vis
                os
                (buildRowToGroup n vis os)
  where
    indicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)
    doubleToInt :: Double -> Int
    doubleToInt = floor . (* 1000)
    valIndices = runST $ do
        let n = nRows df
        mv <- VUM.new n

        let selectedCols = map (columns df V.!) indicesToGroup

        forM_ selectedCols $ \case
            UnboxedColumn _ (v :: VU.Vector a) ->
                case testEquality (typeRep @a) (typeRep @Int) of
                    Just Refl ->
                        VU.imapM_
                            ( \i x -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                VUM.unsafeWrite mv i (i, hashWithSalt h x)
                            )
                            v
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Double) of
                            Just Refl ->
                                VU.imapM_
                                    ( \i d -> do
                                        (_, !h) <- VUM.unsafeRead mv i
                                        VUM.unsafeWrite mv i (i, hashWithSalt h (doubleToInt d))
                                    )
                                    v
                            Nothing ->
                                case sIntegral @a of
                                    STrue ->
                                        VU.imapM_
                                            ( \i d -> do
                                                let x :: Int
                                                    x = fromIntegral @a @Int d
                                                (_, !h) <- VUM.unsafeRead mv i
                                                VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                            )
                                            v
                                    SFalse ->
                                        case sFloating @a of
                                            STrue ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        let x :: Int
                                                            x = doubleToInt (realToFrac d :: Double)
                                                        (_, !h) <- VUM.unsafeRead mv i
                                                        VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                                    )
                                                    v
                                            SFalse ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        let x = hash (show d)
                                                        (_, !h) <- VUM.unsafeRead mv i
                                                        VUM.unsafeWrite mv i (i, hashWithSalt h x)
                                                    )
                                                    v
            BoxedColumn bm (v :: V.Vector a) ->
                case testEquality (typeRep @a) (typeRep @T.Text) of
                    Just Refl ->
                        V.imapM_
                            ( \i t -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int) -- null sentinel
                                        _ -> hashWithSalt h t
                                VUM.unsafeWrite mv i (i, h')
                            )
                            v
                    Nothing ->
                        V.imapM_
                            ( \i d -> do
                                (_, !h) <- VUM.unsafeRead mv i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int) -- null sentinel
                                        _ -> hashWithSalt h (hash (show d))
                                VUM.unsafeWrite mv i (i, h')
                            )
                            v

        let numPasses = 4
            bucketSize = 65536
            radixFunc k (_, !h) =
                let h' = fromIntegral h `xor` (1 `unsafeShiftL` 63) :: Word
                    shiftBits = k * 16
                 in fromIntegral ((h' `unsafeShiftR` shiftBits) .&. 65535)
        VA.sortBy numPasses bucketSize radixFunc mv
        VU.unsafeFreeze mv

-- Inline accessors to avoid depending on Operations.Core

columnNames :: DataFrame -> [T.Text]
columnNames = M.keys . columnIndices

nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

{- | Build the rowToGroup lookup vector from valueIndices and offsets.
rowToGroup[i] = k means row i belongs to group k.
-}
buildRowToGroup :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
buildRowToGroup n vis os = runST $ do
    rtg <- VUM.new n
    let nGroups = VU.length os - 1
    forM_ [0 .. nGroups - 1] $ \k ->
        let s = VU.unsafeIndex os k
            e = VU.unsafeIndex os (k + 1)
         in forM_ [s .. e - 1] $ \i ->
                VUM.unsafeWrite rtg (VU.unsafeIndex vis i) k
    VU.unsafeFreeze rtg
{-# NOINLINE buildRowToGroup #-}

changingPoints :: VU.Vector (Int, Int) -> VU.Vector Int
changingPoints vs =
    VU.reverse
        (VU.fromList (VU.length vs : fst (VU.ifoldl' findChangePoints initialState vs)))
  where
    initialState = ([0], snd (VU.head vs))
    findChangePoints (!offs, !currentVal) index (_, !newVal)
        | currentVal == newVal = (offs, currentVal)
        | otherwise = (index : offs, newVal)
