{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Subset (
    -- * Row slicing
    take,
    takeLast,
    drop,
    dropLast,
    range,
    cube,
    selectRows,

    -- * Filtering
    filter,
    filterBy,
    filterWhere,
    filterJust,
    filterNothing,
    filterAllJust,
    filterAllNothing,

    -- * Column selection
    select,
    selectBy,
    exclude,
    SelectionCriteria,
    byName,
    byProperty,
    byNameProperty,
    byNameRange,
    byIndexRange,

    -- * Sampling & splitting
    SplittableGen,
    sample,
    randomSplit,
    kFolds,
    stratifiedSample,
    stratifiedSplit,

    -- * Label helpers (exported for satellite packages)
    rowsAtIndices,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Prelude

import Control.Exception (throw)
import Data.Function ((&))
import Data.Maybe (
    fromJust,
    fromMaybe,
    isJust,
    isNothing,
 )
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors (
    DataFrameException (..),
    TypeErrorContext (..),
 )
import DataFrame.Expression.Operators (
    col,
    name,
    (.<.),
    (.<=.),
    (.>.),
    (.>=.),
 )
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    TypedColumn (TColumn),
    atIndicesStable,
    columnToTextVec,
    findIndices,
    hasMissing,
    materializeMerged,
    materializePacked,
    mkRandom,
    sliceColumn,
    takeColumn,
    takeLastColumn,
 )
import DataFrame.Internal.Column.Bitmap (bitmapTestBit)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnNames,
    dataframeDimensions,
    derivingExpressions,
    empty,
    getColumn,
    insertColumn,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (Expr (Col, Lit), normalize)
import DataFrame.Internal.Interpreter (Ctx (..), eval, interpret, materialize)
import DataFrame.Operations.Core ()
import DataFrame.Operations.Merge ()
import DataFrame.Operations.Transformations (apply)
import System.Random (RandomGen, SplitGen (..))
import Type.Reflection (
    eqTypeRep,
    typeRep,
    pattern App,
    type (:~:) (Refl),
    type (:~~:) (HRefl),
 )
import Prelude hiding (drop, filter, take)

#if MIN_VERSION_random(1,3,0)
type SplittableGen g = (SplitGen g, RandomGen g)

splitForStratified :: SplittableGen g => g -> (g, g)
splitForStratified = splitGen
#else
type SplittableGen g = RandomGen g

splitForStratified :: SplittableGen g => g -> (g, g)
splitForStratified = split
#endif

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d{columns = V.map (takeColumn n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Take the last n rows of a DataFrame.
takeLast :: Int -> DataFrame -> DataFrame
takeLast n d =
    d
        { columns = V.map (takeLastColumn n') (columns d)
        , dataframeDimensions = (n', c)
        }
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Drop the first n rows of a DataFrame.
drop :: Int -> DataFrame -> DataFrame
drop n d =
    d
        { columns = V.map (sliceColumn n' (max (r - n') 0)) (columns d)
        , dataframeDimensions = (max (r - n') 0, c)
        }
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Drop the last n rows of a DataFrame.
dropLast :: Int -> DataFrame -> DataFrame
dropLast n d =
    d{columns = V.map (sliceColumn 0 n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (r - n) 0 r

-- | O(k * n) Take a range of rows of a DataFrame.
range :: (Int, Int) -> DataFrame -> DataFrame
range (start, end) d =
    d
        { columns = V.map (sliceColumn start' n') (columns d)
        , dataframeDimensions = (n', c)
        }
  where
    (r, c) = dataframeDimensions d
    start' = clip start 0 r
    -- Clamp both endpoints before subtracting: end - start' on an unclamped
    -- end wraps for very negative values and reopens the range.
    end' = clip end start' r
    n' = end' - start'

clip :: Int -> Int -> Int -> Int
clip n left right = min right $ max n left

{- | O(n * k) Filter rows by a given condition.

> filter "x" even df
-}
filter ::
    forall a.
    (Columnable a) =>
    -- | Column to filter by
    Expr a ->
    -- | Filter condition
    (a -> Bool) ->
    -- | Dataframe to filter
    DataFrame ->
    DataFrame
filter e@(Col filterColumnName) condition df = case getColumn filterColumnName df of
    Nothing ->
        throw $
            ColumnsNotFoundException [filterColumnName] "filter" (M.keys $ columnIndices df)
    Just c@(PackedText _ _) ->
        filter e condition (insertColumn filterColumnName (materializePacked c) df)
    Just c@(MergedColumn _ _) ->
        filter e condition (insertColumn filterColumnName (materializeMerged c) df)
    Just _col@(BoxedColumn bm (column :: V.Vector b)) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> filterByVector filterColumnName column condition df
            Nothing -> case (bm, typeRep @a) of
                (Just bm', App tMaybe tInner) -> case eqTypeRep tMaybe (typeRep @Maybe) of
                    Just HRefl -> case testEquality tInner (typeRep @b) of
                        Just Refl ->
                            let maybeVec = V.imap (\i v -> if bitmapTestBit bm' i then Just v else Nothing) column
                             in filterByVector filterColumnName maybeVec condition df
                        Nothing -> filterByVector filterColumnName column condition df
                    Nothing -> filterByVector filterColumnName column condition df
                _ -> filterByVector filterColumnName column condition df
    Just _col@(UnboxedColumn bm (column :: VU.Vector b)) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> filterByVector filterColumnName column condition df
            Nothing -> case (bm, typeRep @a) of
                (Just bm', App tMaybe tInner) -> case eqTypeRep tMaybe (typeRep @Maybe) of
                    Just HRefl -> case testEquality tInner (typeRep @b) of
                        Just Refl ->
                            let maybeVec = V.generate (VU.length column) $ \i ->
                                    if bitmapTestBit bm' i then Just (VU.unsafeIndex column i) else Nothing
                             in filterByVector filterColumnName maybeVec condition df
                        Nothing -> filterByVector filterColumnName column condition df
                    Nothing -> filterByVector filterColumnName column condition df
                _ -> filterByVector filterColumnName column condition df
filter expr condition df =
    let
        (TColumn col') = case interpret @a df (normalize expr) of
            Left e -> throw e
            Right c -> c
        indexes = case findIndices condition col' of
            Right ixs -> ixs
            Left e -> throw e
        c' = snd $ dataframeDimensions df
     in
        df
            { columns = V.map (atIndicesStable indexes) (columns df)
            , dataframeDimensions = (VU.length indexes, c')
            }

filterByVector ::
    forall a b v.
    (VG.Vector v b, VG.Vector v Int, Columnable a, Columnable b) =>
    T.Text -> v b -> (a -> Bool) -> DataFrame -> DataFrame
filterByVector filterColumnName column condition df = case testEquality (typeRep @a) (typeRep @b) of
    Nothing ->
        throw $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right $ typeRep @a
                    , expectedType = Right $ typeRep @b
                    , errorColumnName = Just (T.unpack filterColumnName)
                    , callingFunctionName = Just "filter"
                    }
                )
    Just Refl ->
        let
            ixs = VG.convert (VG.findIndices condition column)
         in
            df
                { columns = V.map (atIndicesStable ixs) (columns df)
                , dataframeDimensions = (VG.length ixs, snd (dataframeDimensions df))
                }

{- | O(k) a version of filter where the predicate comes first.

> filterBy even "x" df
-}
filterBy :: (Columnable a) => (a -> Bool) -> Expr a -> DataFrame -> DataFrame
filterBy = flip filter

{- | O(k) filters the dataframe with a boolean expression.

> filterWhere (F.col @Int x + F.col y F.> 5) df
-}
filterWhere :: Expr Bool -> DataFrame -> DataFrame
filterWhere expr df =
    let
        (TColumn col') = case interpret @Bool df (normalize expr) of
            Left e -> throw e
            Right c -> c
        indexes = case findIndices id col' of
            Right ixs -> ixs
            Left e -> throw e
        c' = snd $ dataframeDimensions df
     in
        df
            { columns = V.map (atIndicesStable indexes) (columns df)
            , dataframeDimensions = (VU.length indexes, c')
            }

{- | O(k) removes all rows with `Nothing` in a given column from the dataframe.

> filterJust "col" df
-}
filterJust :: T.Text -> DataFrame -> DataFrame
filterJust colName df = case getColumn colName df of
    Nothing ->
        throw $
            ColumnsNotFoundException [colName] "filterJust" (M.keys $ columnIndices df)
    Just column | hasMissing column -> case column of
        BoxedColumn (Just _) (_col :: V.Vector a) ->
            filter (Col @(Maybe a) colName) isJust df & apply @(Maybe a) fromJust colName
        UnboxedColumn (Just _) (_col :: VU.Vector a) ->
            filter (Col @(Maybe a) colName) isJust df & apply @(Maybe a) fromJust colName
        c@(PackedText (Just _) _) ->
            filterJust colName (insertColumn colName (materializePacked c) df)
        _ -> df
    Just _ -> df

{- | O(k) returns all rows with `Nothing` in a give column.

> filterNothing "col" df
-}
filterNothing :: T.Text -> DataFrame -> DataFrame
filterNothing colName df = case getColumn colName df of
    Nothing ->
        throw $
            ColumnsNotFoundException [colName] "filterNothing" (M.keys $ columnIndices df)
    Just column | hasMissing column -> case column of
        BoxedColumn (Just _) (_col :: V.Vector a) -> filter (Col @(Maybe a) colName) isNothing df
        UnboxedColumn (Just _) (_col :: VU.Vector a) -> filter (Col @(Maybe a) colName) isNothing df
        c@(PackedText (Just _) _) ->
            filterNothing colName (insertColumn colName (materializePacked c) df)
        _ -> df
    _ -> df

{- | O(n * k) removes all rows with `Nothing` from the dataframe.

> filterAllJust df
-}
filterAllJust :: DataFrame -> DataFrame
filterAllJust df = foldr filterJust df (columnNames df)

{- | O(n * k) keeps any row with a null value.

> filterAllNothing df
-}
filterAllNothing :: DataFrame -> DataFrame
filterAllNothing df = foldr filterNothing df (columnNames df)

{- | O(k) cuts the dataframe in a cube of size (a, b) where
  a is the length and b is the width.

> cube (10, 5) df
-}
cube :: (Int, Int) -> DataFrame -> DataFrame
cube (len, width) = take len . selectBy [ColumnIndexRange (0, width - 1)]

{- | O(n) Selects a number of columns in a given dataframe.

> select ["name", "age"] df
-}
select ::
    [T.Text] ->
    DataFrame ->
    DataFrame
select cs df
    | L.null cs = empty
    | any (`notElem` columnNames df) cs =
        throw $
            ColumnsNotFoundException
                (cs L.\\ columnNames df)
                "select"
                (columnNames df)
    | otherwise =
        let result = L.foldl' addKeyValue empty cs
            filteredExprs = M.filterWithKey (\k _ -> k `L.elem` cs) (derivingExpressions df)
         in result{derivingExpressions = filteredExprs}
  where
    addKeyValue d k = fromMaybe df $ do
        col' <- getColumn k df
        pure $ insertColumn k col' d

data SelectionCriteria
    = ColumnProperty (Column -> Bool)
    | ColumnNameProperty (T.Text -> Bool)
    | ColumnTextRange (T.Text, T.Text)
    | ColumnIndexRange (Int, Int)
    | ColumnName T.Text

{- | Criteria for selecting a column by name.

> selectBy [byName "Age"] df

equivalent to:

> select ["Age"] df
-}
byName :: T.Text -> SelectionCriteria
byName = ColumnName

{- | Criteria for selecting columns whose property satisfies given predicate.

> selectBy [byProperty isNumeric] df
-}
byProperty :: (Column -> Bool) -> SelectionCriteria
byProperty = ColumnProperty

{- | Criteria for selecting columns whose name satisfies given predicate.

> selectBy [byNameProperty (T.isPrefixOf "weight")] df
-}
byNameProperty :: (T.Text -> Bool) -> SelectionCriteria
byNameProperty = ColumnNameProperty

{- | Criteria for selecting columns whose names are in the given lexicographic range (inclusive).

> selectBy [byNameRange ("a", "c")] df
-}
byNameRange :: (T.Text, T.Text) -> SelectionCriteria
byNameRange = ColumnTextRange

{- | Criteria for selecting columns whose indices are in the given (inclusive) range.

> selectBy [byIndexRange (0, 5)] df
-}
byIndexRange :: (Int, Int) -> SelectionCriteria
byIndexRange = ColumnIndexRange

-- | O(n) select columns by column predicate name.
selectBy :: [SelectionCriteria] -> DataFrame -> DataFrame
selectBy xs df = select finalSelection df
  where
    finalSelection = Prelude.filter (`S.member` columnsWithProperties) (columnNames df)
    columnsWithProperties = S.fromList (L.foldl' columnWithProperty [] xs)
    columnWithProperty acc (ColumnName colName) = acc ++ [colName]
    columnWithProperty acc (ColumnNameProperty f) = acc ++ L.filter f (columnNames df)
    columnWithProperty acc (ColumnTextRange (from, to)) =
        acc
            ++ reverse
                (Prelude.dropWhile (to /=) $ reverse $ dropWhile (from /=) (columnNames df))
    columnWithProperty acc (ColumnIndexRange (from, to)) = acc ++ Prelude.take (to - from + 1) (Prelude.drop from (columnNames df))
    columnWithProperty acc (ColumnProperty f) =
        acc
            ++ map fst (L.filter (\(_k, v) -> v `elem` ixs) (M.toAscList (columnIndices df)))
      where
        ixs = V.ifoldl' (\acc' i c -> if f c then i : acc' else acc') [] (columns df)

{- | O(k * n) select rows by index

> selectRows [0, 2, 4] df
-}
selectRows :: [Int] -> DataFrame -> DataFrame
selectRows ixs df =
    df
        { columns = V.map (atIndicesStable ixs') (columns df)
        , dataframeDimensions = (VU.length ixs', snd (dataframeDimensions df))
        }
  where
    ixs' = VU.fromList ixs

{- | O(n) inverse of select

> exclude ["Name"] df
-}
exclude ::
    [T.Text] ->
    DataFrame ->
    DataFrame
exclude cs df =
    let keysToKeep = columnNames df L.\\ cs
     in select keysToKeep df

{- | Sample a dataframe. The double parameter must be between 0 and 1 (inclusive).

==== __Example__
@
ghci> import System.Random
ghci> D.sample (mkStdGen 137) 0.1 df

@
-}
sample :: (RandomGen g) => g -> Double -> DataFrame -> DataFrame
sample pureGen p df =
    let
        rand = mkRandom pureGen (fst (dataframeDimensions df)) (0 :: Double) 1
        cRand = col @Double "__rand__"
     in
        df
            & insertColumn (name cRand) rand
            & filterWhere (cRand .>=. Lit (1 - p))
            & exclude [name cRand]

{- | Split a dataset into two. The first in the tuple gets a sample of p (0 <= p <= 1) and the second gets (1 - p). This is useful for creating test and train splits.

==== __Example__
@
ghci> import System.Random
ghci> D.randomSplit (mkStdGen 137) 0.9 df

@
-}
randomSplit ::
    (RandomGen g) => g -> Double -> DataFrame -> (DataFrame, DataFrame)
randomSplit pureGen p df =
    let
        rand = mkRandom pureGen (fst (dataframeDimensions df)) (0 :: Double) 1
        cRand = col @Double "__rand__"
        withRand = df & insertColumn (name cRand) rand
     in
        ( withRand
            & filterWhere (cRand .<=. Lit p)
            & exclude [name cRand]
        , withRand
            & filterWhere
                (cRand .>. Lit p)
            & exclude [name cRand]
        )

{- | Creates n folds of a dataframe.

==== __Example__
@
ghci> import System.Random
ghci> D.kFolds (mkStdGen 137) 5 df

@
-}
kFolds :: (RandomGen g) => g -> Int -> DataFrame -> [DataFrame]
kFolds pureGen folds df =
    let
        rand = mkRandom pureGen (fst (dataframeDimensions df)) (0 :: Double) 1
        cRand = col @Double "__rand__"
        withRand = df & insertColumn (name cRand) rand
        partitionSize = 1 / fromIntegral folds
        singleFold n d =
            d & filterWhere (cRand .>=. Lit (fromIntegral n * partitionSize))
        go (-1) _ = []
        go n d =
            let
                d' = singleFold n d
                d'' = d & filterWhere (cRand .<. Lit (fromIntegral n * partitionSize))
             in
                d' : go (n - 1) d''
     in
        map (exclude [name cRand]) (go (folds - 1) withRand)

-- | Build a map from stringified label to row indices.
groupByIndices :: Column -> M.Map T.Text (VU.Vector Int)
groupByIndices col' =
    let textVec = columnToTextVec col'
        (grouped, _) =
            V.foldl'
                (\(!m, !i) key -> (M.insertWith (++) key [i] m, i + 1))
                (M.empty, 0)
                textVec
     in M.map (VU.fromList . L.reverse) grouped

-- | Select rows at the given indices from all columns.
rowsAtIndices :: VU.Vector Int -> DataFrame -> DataFrame
rowsAtIndices ixs df =
    df
        { columns = V.map (atIndicesStable ixs) (columns df)
        , dataframeDimensions = (VU.length ixs, snd (dataframeDimensions df))
        }

{- | Sample a dataframe, preserving per-stratum proportions.

==== __Example__
@
ghci> import System.Random
ghci> D.stratifiedSample (mkStdGen 42) 0.8 "label" df
@
-}
stratifiedSample ::
    forall a g.
    (SplittableGen g, Columnable a) =>
    g -> Double -> Expr a -> DataFrame -> DataFrame
stratifiedSample gen p strataCol df =
    let col' = case strataCol of
            Col colName -> unsafeGetColumn colName df
            _ -> either throw id $ do
                v <- eval (FlatCtx df) strataCol
                pure $ materialize @a (fst (dataframeDimensions df)) v
        groups = M.elems (groupByIndices col')
        go _ [] = mempty
        go g (ixs : rest) =
            let stratum = rowsAtIndices ixs df
                (g1, g2) = splitForStratified g
             in sample g1 p stratum <> go g2 rest
     in go gen groups

{- | Split a dataframe into two, preserving per-stratum proportions.

==== __Example__
@
ghci> import System.Random
ghci> D.stratifiedSplit (mkStdGen 42) 0.8 "label" df
@
-}
stratifiedSplit ::
    forall a g.
    (SplittableGen g, Columnable a) =>
    g -> Double -> Expr a -> DataFrame -> (DataFrame, DataFrame)
stratifiedSplit gen p strataCol df =
    let col' = case strataCol of
            Col colName -> unsafeGetColumn colName df
            _ -> either throw id $ do
                v <- eval (FlatCtx df) strataCol
                pure $ materialize @a (fst (dataframeDimensions df)) v
        groups = M.elems (groupByIndices col')
        go _ [] = (mempty, mempty)
        go g (ixs : rest) =
            let stratum = rowsAtIndices ixs df
                (g1, g2) = splitForStratified g
                (tr, va) = randomSplit g1 p stratum
                (trAcc, vaAcc) = go g2 rest
             in (tr <> trAcc, va <> vaAcc)
     in go gen groups
