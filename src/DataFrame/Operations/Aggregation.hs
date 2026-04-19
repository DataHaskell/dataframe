{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation (
    module DataFrame.Operations.Aggregation,
    groupBy,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import Data.Hashable
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Expression
import DataFrame.Internal.Grouping (buildRowToGroup, changingPoints, groupBy)
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Type.Reflection (typeRep)

computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df = runST $ do
    let n = fst (dimensions df)
    mv <- VUM.new n

    let selectedCols = map (columns df V.!) indices

    forM_ selectedCols $ \case
        UnboxedColumn _ (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    VU.imapM_
                        ( \i (x :: Int) -> do
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl ->
                            VU.imapM_
                                ( \i (d :: Double) -> do
                                    h <- VUM.unsafeRead mv i
                                    VUM.unsafeWrite mv i (hashWithSalt h (doubleToInt d))
                                )
                                v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    VU.imapM_
                                        ( \i d -> do
                                            let x :: Int
                                                x = fromIntegral @a @Int d
                                            h <- VUM.unsafeRead mv i
                                            VUM.unsafeWrite mv i (hashWithSalt h x)
                                        )
                                        v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x :: Int
                                                        x = doubleToInt (realToFrac d :: Double)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
                                        SFalse ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x = hash (show d)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i (t :: T.Text) -> do
                            h <- VUM.unsafeRead mv i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> hashWithSalt h (0 :: Int)
                                    _ -> hashWithSalt h t
                            VUM.unsafeWrite mv i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            let x = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> 0 :: Int
                                    _ -> hash (show d)
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v

    VU.unsafeFreeze mv
  where
    doubleToInt :: Double -> Int
    doubleToInt = floor . (* 1000)

{- | Aggregate a grouped dataframe using the expressions given.
All ungrouped columns will be dropped.
-}
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf@(Grouped df groupingColumns valIndices offs _rowToGroup) =
    let
        df' =
            selectIndices
                (VU.map (valIndices VU.!) (VU.init offs))
                (select groupingColumns df)

        f (name, UExpr (expr :: Expr a)) d =
            let
                value = case interpretAggregation @a gdf expr of
                    Left e -> throw e
                    Right (UnAggregated _) -> throw $ UnaggregatedException (T.pack $ show expr)
                    Right (Aggregated (TColumn col)) -> col
             in
                insertColumn name value d
     in
        fold f aggs df'

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = V.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VU.length xs, V.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VU.map (indices VU.!) (VU.init os)) df
  where
    (Grouped _ _ indices os _rtg) = groupBy (columnNames df) df
