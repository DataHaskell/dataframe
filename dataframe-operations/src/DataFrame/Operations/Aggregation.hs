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
import Control.Monad.ST (ST, runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    TypedColumn (..),
    atIndicesStable,
    bitmapTestBit,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
    insertColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Grouping (buildRowToGroup, changingPoints, groupBy)
import DataFrame.Internal.Hash (
    fnvOffset,
    mixDouble,
    mixInt,
    mixShow,
    mixText,
    nullSalt,
 )
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Type.Reflection (typeRep)

computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df = runST $ do
    let n = fst (dimensions df)
    mv <- VUM.replicate n fnvOffset

    let selectedCols = map (columns df V.!) indices

    forM_ selectedCols $ \case
        UnboxedColumn ubm (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> hashKeyUnboxed mv ubm mixInt v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl ->
                            hashKeyUnboxed mv ubm mixDouble v
                        Nothing ->
                            case sIntegral a of
                                STrue ->
                                    hashKeyUnboxed mv ubm (\h d -> mixInt h (fromIntegral @a @Int d)) v
                                SFalse ->
                                    case sFloating a of
                                        STrue ->
                                            hashKeyUnboxed mv ubm (\h d -> mixDouble h (realToFrac d :: Double)) v
                                        SFalse ->
                                            hashKeyUnboxed mv ubm mixShow v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i (t :: T.Text) -> do
                            h <- VUM.unsafeRead mv i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixText h t
                            VUM.unsafeWrite mv i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            h <- VUM.unsafeRead mv i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixShow h d
                            VUM.unsafeWrite mv i h'
                        )
                        v

    VU.unsafeFreeze mv

{- | Fold a value-mix over an unboxed key column into the running hash vector,
respecting the null bitmap (a null slot mixes 'nullSalt' instead of its
uninitialised stored value, so @Nothing@ never matches a present @Just x@).
The non-nullable case is the original tight loop. @INLINE@d to specialise per call.
-}
hashKeyUnboxed ::
    (VU.Unbox a) =>
    VUM.MVector s Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    VU.Vector a ->
    ST s ()
hashKeyUnboxed mv ubm mix v = case ubm of
    Nothing ->
        VU.imapM_
            ( \i x -> do
                h <- VUM.unsafeRead mv i
                VUM.unsafeWrite mv i (mix h x)
            )
            v
    Just bm ->
        VU.imapM_
            ( \i x -> do
                h <- VUM.unsafeRead mv i
                VUM.unsafeWrite
                    mv
                    i
                    (if bitmapTestBit bm i then mix h x else mixInt h nullSalt)
            )
            v
{-# INLINE hashKeyUnboxed #-}

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
