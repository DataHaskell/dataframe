{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Subset where

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
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    empty,
    getColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Interpreter
import DataFrame.Operations.Core
import DataFrame.Operations.Transformations (apply)
import System.Random
import Type.Reflection
import Prelude hiding (filter, take)

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
        { columns = V.map (sliceColumn (clip start 0 r) n') (columns d)
        , dataframeDimensions = (n', c)
        }
  where
    (r, c) = dataframeDimensions d
    n' = clip (end - start) 0 r

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
filter (Col filterColumnName) condition df = case getColumn filterColumnName df of
    Nothing ->
        throw $
            ColumnNotFoundException filterColumnName "filter" (M.keys $ columnIndices df)
    Just (BoxedColumn (column :: V.Vector b)) -> filterByVector filterColumnName column condition df
    Just (OptionalColumn (column :: V.Vector b)) -> filterByVector filterColumnName column condition df
    Just (UnboxedColumn (column :: VU.Vector b)) -> filterByVector filterColumnName column condition df
filter expr condition df =
    let
        (TColumn col) = case interpret @a df (normalize expr) of
            Left e -> throw e
            Right c -> c
        indexes = case findIndices condition col of
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
        (TColumn col) = case interpret @Bool df (normalize expr) of
            Left e -> throw e
            Right c -> c
        indexes = case findIndices id col of
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
filterJust name df = case getColumn name df of
    Nothing ->
        throw $ ColumnNotFoundException name "filterJust" (M.keys $ columnIndices df)
    Just column@(OptionalColumn (col :: V.Vector (Maybe a))) -> filter (Col @(Maybe a) name) isJust df & apply @(Maybe a) fromJust name
    Just column -> df

{- | O(k) returns all rows with `Nothing` in a give column.

> filterNothing "col" df
-}
filterNothing :: T.Text -> DataFrame -> DataFrame
filterNothing name df = case getColumn name df of
    Nothing ->
        throw $ ColumnNotFoundException name "filterNothing" (M.keys $ columnIndices df)
    Just (OptionalColumn (col :: V.Vector (Maybe a))) -> filter (Col @(Maybe a) name) isNothing df
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
cube (length, width) = take length . selectBy [ColumnIndexRange (0, width - 1)]

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
            ColumnNotFoundException
                (T.pack $ show $ cs L.\\ columnNames df)
                "select"
                (columnNames df)
    | otherwise = L.foldl' addKeyValue empty cs
  where
    addKeyValue d k = fromMaybe df $ do
        col <- getColumn k df
        pure $ insertColumn k col d

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
    columnWithProperty acc (ColumnName name) = acc ++ [name]
    columnWithProperty acc (ColumnNameProperty f) = acc ++ L.filter f (columnNames df)
    columnWithProperty acc (ColumnTextRange (from, to)) =
        acc
            ++ reverse
                (Prelude.dropWhile (to /=) $ reverse $ dropWhile (from /=) (columnNames df))
    columnWithProperty acc (ColumnIndexRange (from, to)) = acc ++ Prelude.take (to - from + 1) (Prelude.drop from (columnNames df))
    columnWithProperty acc (ColumnProperty f) =
        acc
            ++ map fst (L.filter (\(k, v) -> v `elem` ixs) (M.toAscList (columnIndices df)))
      where
        ixs = V.ifoldl' (\acc i c -> if f c then i : acc else acc) [] (columns df)

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
        rand = generateRandomVector pureGen (fst (dataframeDimensions df))
     in
        df
            & insertUnboxedVector "__rand__" rand
            & filterWhere (BinaryOp "geq" (>=) (Col @Double "__rand__") (Lit (1 - p)))
            & exclude ["__rand__"]

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
        rand = generateRandomVector pureGen (fst (dataframeDimensions df))
        withRand = df & insertUnboxedVector "__rand__" rand
     in
        ( withRand
            & filterWhere (BinaryOp "leq" (<=) (Col @Double "__rand__") (Lit p))
            & exclude ["__rand__"]
        , withRand
            & filterWhere (BinaryOp "gt" (>) (Col @Double "__rand__") (Lit p))
            & exclude ["__rand__"]
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
        rand = generateRandomVector pureGen (fst (dataframeDimensions df))
        withRand = df & insertUnboxedVector "__rand__" rand
        partitionSize = 1 / fromIntegral folds
        singleFold n d =
            d
                & filterWhere
                    ( BinaryOp
                        "geq"
                        (>=)
                        (Col @Double "__rand__")
                        (Lit (fromIntegral n * partitionSize))
                    )
        go (-1) _ = []
        go n d =
            let
                d' = singleFold n d
                d'' =
                    d
                        & filterWhere
                            ( BinaryOp
                                "lt"
                                (<)
                                (Col @Double "__rand__")
                                (Lit (fromIntegral n * partitionSize))
                            )
             in
                d' : go (n - 1) d''
     in
        map (exclude ["__rand__"]) (go (folds - 1) withRand)

generateRandomVector :: (RandomGen g) => g -> Int -> VU.Vector Double
generateRandomVector pureGen k = VU.fromList $ go pureGen k
  where
    go g 0 = []
    go g n =
        let
            (v, g') = uniformR (0 :: Double, 1 :: Double) g
         in
            v : go g' (n - 1)
