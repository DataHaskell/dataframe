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

import qualified Data.IntMap.Strict as IM
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Hash
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
    -- valIndices is a vector of (rowIdx, rowHash) sorted by rowHash so that
    -- runs of equal-hash rows are adjacent. Hashes are computed with the
    -- in-tree FNV combinators from "DataFrame.Internal.Hash", and bucketed
    -- with 'Data.IntMap.Strict' to keep the dep set minimal (no hashable,
    -- no vector-algorithms).
    valIndices = runST $ do
        let n = nRows df
        mh <- VUM.replicate n fnvOffset
        let selectedCols = map (columns df V.!) indicesToGroup
        forM_ selectedCols $ \case
            UnboxedColumn _ (v :: VU.Vector a) ->
                case testEquality (typeRep @a) (typeRep @Int) of
                    Just Refl ->
                        VU.imapM_
                            ( \i x -> do
                                !h <- VUM.unsafeRead mh i
                                VUM.unsafeWrite mh i (mixInt h x)
                            )
                            v
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Double) of
                            Just Refl ->
                                VU.imapM_
                                    ( \i d -> do
                                        !h <- VUM.unsafeRead mh i
                                        VUM.unsafeWrite mh i (mixDouble h d)
                                    )
                                    v
                            Nothing ->
                                case sIntegral @a of
                                    STrue ->
                                        VU.imapM_
                                            ( \i d -> do
                                                !h <- VUM.unsafeRead mh i
                                                VUM.unsafeWrite mh i (mixInt h (fromIntegral @a @Int d))
                                            )
                                            v
                                    SFalse ->
                                        case sFloating @a of
                                            STrue ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        !h <- VUM.unsafeRead mh i
                                                        VUM.unsafeWrite mh i (mixDouble h (realToFrac d :: Double))
                                                    )
                                                    v
                                            SFalse ->
                                                VU.imapM_
                                                    ( \i d -> do
                                                        !h <- VUM.unsafeRead mh i
                                                        VUM.unsafeWrite mh i (mixShow h d)
                                                    )
                                                    v
            BoxedColumn bm (v :: V.Vector a) ->
                case testEquality (typeRep @a) (typeRep @T.Text) of
                    Just Refl ->
                        V.imapM_
                            ( \i t -> do
                                !h <- VUM.unsafeRead mh i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> mixInt h 0 -- null sentinel
                                        _ -> mixText h t
                                VUM.unsafeWrite mh i h'
                            )
                            v
                    Nothing ->
                        V.imapM_
                            ( \i d -> do
                                !h <- VUM.unsafeRead mh i
                                let h' = case bm of
                                        Just bm' | not (bitmapTestBit bm' i) -> mixInt h 0 -- null sentinel
                                        _ -> mixShow h d
                                VUM.unsafeWrite mh i h'
                            )
                            v
        hashes <- VU.unsafeFreeze mh
        -- Bucket row indices by hash using an IntMap, then walk it in
        -- ascending key order to emit (rowIdx, hash) pairs grouped by
        -- hash. Each bucket's accumulated list is reversed so rows come
        -- out in the original row order.
        let buckets =
                VU.ifoldl'
                    (\acc i h -> IM.insertWith (++) h [i] acc)
                    IM.empty
                    hashes
            ordered =
                [ (i, h)
                | (h, is) <- IM.toAscList buckets
                , i <- reverse is
                ]
        return (VU.fromList ordered)

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
