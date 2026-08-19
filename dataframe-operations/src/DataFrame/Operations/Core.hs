{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Core (
    -- * Dimensions
    dimensions,
    nRows,
    nColumns,

    -- * Construction
    fromUnnamedColumns,
    fromRows,

    -- * Insertion
    insert,
    insertVector,
    insertUnboxedVector,
    insertWithDefault,
    insertVectorWithDefault,

    -- * Column management
    cloneColumn,
    rename,
    renameMany,

    -- * Inspection
    describeColumns,
    valueCounts,
    valueProportions,
    showDerivedExpressions,

    -- * Folds & matrix/vector conversions
    fold,
    toFloatMatrix,
    toDoubleMatrix,
    toIntMatrix,
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Bits (popCount)
import Data.Either
import qualified Data.Foldable as Fold
import Data.Function (on, (&))
import Data.Maybe
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    TypedColumn (..),
    columnLength,
    columnTypeString,
    fromList,
    fromVector,
    materializeMerged,
    materializePacked,
    mergedHead,
    toDoubleVector,
    toFloatVector,
    toIntVector,
    toUnboxedVector,
    toVector,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columnNames,
    derivingExpressions,
    empty,
    fromNamedColumns,
    getColumn,
    insertColumn,
    null,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Parsing (isNullish)
import DataFrame.Internal.Row (Any, mkColumnFromRow)
import Type.Reflection
import Prelude hiding (null)

{- | O(1) Get DataFrame dimensions i.e. (rows, columns)

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("a", D.fromList [1..100]), ("b", D.fromList [1..100]), ("c", D.fromList [1..100])]
>>> D.dimensions df

(100, 3)
@
-}
dimensions :: DataFrame -> (Int, Int)
dimensions = dataframeDimensions
{-# INLINE dimensions #-}

{- | O(1) Get number of rows in a dataframe.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("a", D.fromList [1..100]), ("b", D.fromList [1..100]), ("c", D.fromList [1..100])]
>>> D.nRows df
100
@
-}
nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

{- | O(1) Get number of columns in a dataframe.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("a", D.fromList [1..100]), ("b", D.fromList [1..100]), ("c", D.fromList [1..100])]
>>> D.nColumns df
3
@
-}
nColumns :: DataFrame -> Int
nColumns = snd . dataframeDimensions

{- | O(k) Get column names of the DataFrame in order of insertion.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("a", D.fromList [1..100]), ("b", D.fromList [1..100]), ("c", D.fromList [1..100])]
>>> D.columnNames df

["a", "b", "c"]
@
-}

-- 'columnNames' is now defined in "DataFrame.Internal.DataFrame" and
-- re-exported from "DataFrame" at the top level.

{- | Adds a vector as a named column. Size mismatches are reconciled by making
the shorter side nullable (`Maybe a`) and padding with `Nothing`.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> import qualified Data.Vector as V
>>> D.insertVector "numbers" (V.fromList [(1 :: Int)..10]) D.empty

--------
 numbers
--------
   Int
--------
 1
 2
 3
 4
 5
 6
 7
 8
 9
 10

@
-}
insertVector ::
    forall a.
    (Columnable a) =>
    -- | Column Name
    T.Text ->
    -- | Vector to add to column
    V.Vector a ->
    -- | DataFrame to add column to
    DataFrame ->
    DataFrame
insertVector name xs = insertColumn name (fromVector xs)
{-# INLINE insertVector #-}

{- | Adds a foldable collection as a named column. Size mismatches are reconciled by
making the shorter side nullable (`Maybe a`) and padding with `Nothing`.
Do not pass infinite collections: they are fully forced.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> D.insert "numbers" [(1 :: Int)..10] D.empty

--------
 numbers
--------
   Int
--------
 1
 2
 3
 4
 5
 6
 7
 8
 9
 10

@
-}
insert ::
    forall a t.
    (Columnable a, Foldable t) =>
    -- | Column Name
    T.Text ->
    -- | Sequence to add to dataframe
    t a ->
    -- | DataFrame to add column to
    DataFrame ->
    DataFrame
insert name xs = insertColumn name (fromList (Fold.foldr' (:) [] xs))
{-# INLINE insert #-}

{- | Adds a vector to the dataframe and pads it with a default value if it has less elements than the number of rows.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified Data.Vector as V
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("x", D.fromList [(1 :: Int)..10])]
>>> D.insertVectorWithDefault 0 "numbers" (V.fromList [(1 :: Int),2,3]) df

-------------
 x  | numbers
----|--------
Int |   Int
----|--------
1   | 1
2   | 2
3   | 3
4   | 0
5   | 0
6   | 0
7   | 0
8   | 0
9   | 0
10  | 0

@
-}
insertVectorWithDefault ::
    forall a.
    (Columnable a) =>
    -- | Default Value
    a ->
    -- | Column name
    T.Text ->
    -- | Data to add to column
    V.Vector a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertVectorWithDefault defaultValue name xs d =
    let (rows, _) = dataframeDimensions d
        values = xs V.++ V.replicate (rows - V.length xs) defaultValue
     in insertColumn name (fromVector values) d

{- | Adds a list to the dataframe and pads it with a default value if it has less elements than the number of rows.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> df = D.fromNamedColumns [("x", D.fromList [(1 :: Int)..10])]
>>> D.insertWithDefault 0 "numbers" [(1 :: Int),2,3] df

-------------
 x  | numbers
----|--------
Int |   Int
----|--------
1   | 1
2   | 2
3   | 3
4   | 0
5   | 0
6   | 0
7   | 0
8   | 0
9   | 0
10  | 0

@
-}
insertWithDefault ::
    forall a t.
    (Columnable a, Foldable t) =>
    -- | Default Value
    a ->
    -- | Column name
    T.Text ->
    -- | Data to add to column
    t a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertWithDefault defaultValue name xs d =
    let (rows, _) = dataframeDimensions d
        xs' = Fold.foldr' (:) [] xs
        values = xs' ++ replicate (rows - length xs') defaultValue
     in insertColumn name (fromList values) d

{- | /O(n)/ Like 'insertVector' but takes an already-unboxed vector,
skipping the boxed-to-unboxed conversion 'insertVector' would do for numbers.
-}
insertUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    -- | Column Name
    T.Text ->
    -- | Unboxed vector to add to column
    VU.Vector a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertUnboxedVector name xs = insertColumn name (UnboxedColumn Nothing xs)

{- | /O(n)/ Add a column to the dataframe.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> D.insertColumn "numbers" (D.fromList [(1 :: Int)..10]) D.empty

--------
 numbers
--------
   Int
--------
 1
 2
 3
 4
 5
 6
 7
 8
 9
 10

@
-}

-- 'insertColumn' is now defined in "DataFrame.Internal.DataFrame".

{- | /O(n)/ Clones a column and places it under a new name in the dataframe.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified Data.Vector as V
>>> df = insertVector "numbers" (V.fromList [1..10]) D.empty
>>> D.cloneColumn "numbers" "others" df

-----------------
 numbers | others
---------|-------
   Int   |  Int
---------|-------
 1       | 1
 2       | 2
 3       | 3
 4       | 4
 5       | 5
 6       | 6
 7       | 7
 8       | 8
 9       | 9
 10      | 10

@
-}
cloneColumn :: T.Text -> T.Text -> DataFrame -> DataFrame
cloneColumn original new df
    | null df = throw (EmptyDataSetException "cloneColumn")
    | otherwise = fromMaybe
        ( throw $
            ColumnsNotFoundException [original] "cloneColumn" (M.keys $ columnIndices df)
        )
        $ do
            column <- getColumn original df
            return $ insertColumn new column df

{- | /O(n)/ Renames a single column.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> import qualified Data.Vector as V
>>> df = insertVector "numbers" (V.fromList [1..10]) D.empty
>>> D.rename "numbers" "others" df

-------
 others
-------
  Int
-------
 1
 2
 3
 4
 5
 6
 7
 8
 9
 10

@
-}
rename :: T.Text -> T.Text -> DataFrame -> DataFrame
rename orig new df = either throw id (renameSafe orig new df)

{- | /O(n)/ Renames many columns.

==== __Example__
@
>>> :set -XOverloadedStrings
>>> import qualified DataFrame as D
>>> import qualified Data.Vector as V
>>> df = D.insertVector "others" (V.fromList [11..20]) (D.insertVector "numbers" (V.fromList [1..10]) D.empty)
>>> df

-----------------
 numbers | others
---------|-------
   Int   |  Int
---------|-------
 1       | 11
 2       | 12
 3       | 13
 4       | 14
 5       | 15
 6       | 16
 7       | 17
 8       | 18
 9       | 19
 10      | 20

>>> D.renameMany [("numbers", "first_10"), ("others", "next_10")] df

-------------------
 first_10 | next_10
----------|--------
   Int    |   Int
----------|--------
 1        | 11
 2        | 12
 3        | 13
 4        | 14
 5        | 15
 6        | 16
 7        | 17
 8        | 18
 9        | 19
 10       | 20

@
-}
renameMany :: [(T.Text, T.Text)] -> DataFrame -> DataFrame
renameMany = fold (uncurry rename)

renameSafe ::
    T.Text -> T.Text -> DataFrame -> Either DataFrameException DataFrame
renameSafe orig new df
    | null df = throw (EmptyDataSetException "rename")
    | otherwise = fromMaybe
        (Left $ ColumnsNotFoundException [orig] "rename" (M.keys $ columnIndices df))
        $ do
            columnIndex <- M.lookup orig (columnIndices df)
            let origRemoved = M.delete orig (columnIndices df)
            let newAdded = M.insert new columnIndex origRemoved
            return (Right df{columnIndices = newAdded})

data ColumnInfo = ColumnInfo
    { nameOfColumn :: !T.Text
    , nonNullValues :: !Int
    , nullValues :: !Int
    , typeOfColumn :: !T.Text
    }

{- | O(n * k ^ 2) Returns the number of non-null columns in the dataframe and the type associated with each column.

==== __Example__
@
>>> import qualified Data.Vector as V
>>> df = D.insertVector "others" (V.fromList [11..20]) (D.insertVector "numbers" (V.fromList [1..10]) D.empty)
>>> D.describeColumns df

--------------------------------------------------------
 Column Name | # Non-null Values | # Null Values | Type
-------------|-------------------|---------------|-----
    Text     |        Int        |      Int      | Text
-------------|-------------------|---------------|-----
 others      | 10                | 0             | Int
 numbers     | 10                | 0             | Int

@
-}
describeColumns :: DataFrame -> DataFrame
describeColumns df =
    empty
        & insertColumn "Column Name" (fromList (map nameOfColumn infos))
        & insertColumn "# Non-null Values" (fromList (map nonNullValues infos))
        & insertColumn "# Null Values" (fromList (map nullValues infos))
        & insertColumn "Type" (fromList (map typeOfColumn infos))
  where
    infos =
        L.sortBy (compare `on` nonNullValues) (V.ifoldl' go [] (columns df)) ::
            [ColumnInfo]
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    columnName i = M.lookup i indexMap
    go acc i col@(BoxedColumn _bm (_c :: V.Vector a)) =
        let
            cname = columnName i
            countNulls = nulls col
            columnType = T.pack $ columnTypeString col
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col - countNulls)
                        countNulls
                        columnType
                        : acc
    go acc i col@(UnboxedColumn _bm _c) =
        let
            cname = columnName i
            countNulls = nulls col
            columnType = T.pack $ columnTypeString col
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col - countNulls)
                        countNulls
                        columnType
                        : acc
    go acc i col@(PackedText _ _) = go acc i (materializePacked col)
    go acc i col@(MergedColumn _ _) = go acc i (materializeMerged col)

nulls :: Column -> Int
nulls (BoxedColumn (Just bm) xs) =
    let n = VG.length xs
     in n - VU.foldl' (\acc b -> acc + popCount b) 0 bm
nulls (BoxedColumn Nothing (xs :: V.Vector a)) = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> VG.length $ VG.filter isNullish xs
    Nothing -> case testEquality (typeRep @a) (typeRep @String) of
        Just Refl -> VG.length $ VG.filter (isNullish . T.pack) xs
        Nothing -> 0
nulls (UnboxedColumn (Just bm) xs) =
    let n = VG.length xs
     in n - VU.foldl' (\acc b -> acc + popCount b) 0 bm
nulls c@(PackedText _ _) = nulls (materializePacked c)
nulls _ = 0

{- | Creates a dataframe from a list of tuples with name and column.

==== __Example__
@
>>> df = D.fromNamedColumns [("numbers", D.fromList [1..10]), ("others", D.fromList [11..20])]
>>> df
-----------------
 numbers | others
---------|-------
   Int   |  Int
---------|-------
 1       | 11
 2       | 12
 3       | 13
 4       | 14
 5       | 15
 6       | 16
 7       | 17
 8       | 18
 9       | 19
 10      | 20

@
-}

-- 'fromNamedColumns' is now defined in "DataFrame.Internal.DataFrame".

{- | Create a dataframe from a list of columns. The column names are "0", "1"... etc.
Useful for quick exploration but you should probably always rename the columns after
or drop the ones you don't want.

==== __Example__
@
>>> df = D.fromUnnamedColumns [D.fromList [1..10], D.fromList [11..20]]
>>> df
-----------------
  0  |  1
-----|----
 Int | Int
-----|----
 1   | 11
 2   | 12
 3   | 13
 4   | 14
 5   | 15
 6   | 16
 7   | 17
 8   | 18
 9   | 19
 10  | 20

@
-}
fromUnnamedColumns :: [Column] -> DataFrame
fromUnnamedColumns = fromNamedColumns . zip (map (T.pack . show) [(0 :: Int) ..])

{- | Create a dataframe from a list of column names and rows.

==== __Example__
@
>>> df = D.fromRows ["A", "B"] [[D.toAny 1, D.toAny 11], [D.toAny 2, D.toAny 12], [D.toAny 3, D.toAny 13]]

>>> df

----------
  A  |  B
-----|----
 Int | Int
-----|----
 1   | 11
 2   | 12
 3   | 13

@
-}
fromRows :: [T.Text] -> [[Any]] -> DataFrame
fromRows names rows =
    L.foldl'
        (\df (i, name) -> insertColumn name (mkColumnFromRow name i rows) df)
        empty
        (zip [0 ..] names)

{- | O (k * n) Counts the occurences of each value in a given column.

==== __Example__
@
>>> df = D.fromUnnamedColumns [D.fromList [1..10], D.fromList [11..20]]

>>> D.valueCounts @Int "0" df

[(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1)]

@
-}
valueCounts ::
    forall a. (Ord a, Columnable a) => Expr a -> DataFrame -> [(a, Int)]
valueCounts expr df
    | null df = throw (EmptyDataSetException "valueCounts")
    | otherwise = case columnAsVector expr df of
        Left e -> throw e
        Right column' ->
            let
                column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column'
             in
                M.toAscList column

{- | O (k * n) Shows the proportions of each value in a given column.

==== __Example__
@
>>> df = D.fromUnnamedColumns [D.fromList [1..10], D.fromList [11..20]]

>>> D.valueCounts @Int "0" df

[(1,0.1),(2,0.1),(3,0.1),(4,0.1),(5,0.1),(6,0.1),(7,0.1),(8,0.1),(9,0.1),(10,0.1)]

@
-}
valueProportions ::
    forall a. (Ord a, Columnable a) => Expr a -> DataFrame -> [(a, Double)]
valueProportions expr df
    | null df = throw (EmptyDataSetException "valueCounts")
    | otherwise = case columnAsVector expr df of
        Left e -> throw e
        Right column' ->
            let
                counts =
                    M.toAscList
                        (V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column')
                total = fromIntegral (sum (map snd counts))
             in
                map (fmap ((/ total) . fromIntegral)) counts

{- | A left fold for dataframes that takes the dataframe as the last object.
This makes it easier to chain operations.

==== __Example__
@
>>> df = D.fromNamedColumns [("x", D.fromList [1..100]), ("y", D.fromList [11..110])]
>>> D.fold D.dropLast [1..5] df

---------
 x  |  y
----|----
Int | Int
----|----
1   | 11
2   | 12
3   | 13
4   | 14
5   | 15
6   | 16
7   | 17
8   | 18
9   | 19
10  | 20
11  | 21
12  | 22
13  | 23
14  | 24
15  | 25
16  | 26
17  | 27
18  | 28
19  | 29
20  | 30

Showing 20 rows out of 85

@
-}
fold :: (a -> DataFrame -> DataFrame) -> [a] -> DataFrame -> DataFrame
fold f xs acc = L.foldl' (flip f) acc xs

{- | The dataframe as a row-major matrix of floats, for handing data to ML systems.
'Left' if any column cannot be converted to floats.
-}
toFloatMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Float))
toFloatMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toFloatVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Float)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

{- | The dataframe as a row-major matrix of doubles, for handing data to ML systems.
'Left' if any column cannot be converted to doubles.
-}
toDoubleMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Double))
toDoubleMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toDoubleVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Double)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

{- | The dataframe as a row-major matrix of ints, for handing data to ML systems.
'Left' if any column cannot be converted to ints.
-}
toIntMatrix :: DataFrame -> Either DataFrameException (V.Vector (VU.Vector Int))
toIntMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toIntVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Int)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

withColumnName ::
    T.Text -> Either DataFrameException a -> Either DataFrameException a
withColumnName name (Left (TypeMismatchException ctx)) =
    Left
        (TypeMismatchException ctx{errorColumnName = orFallback (errorColumnName ctx)})
  where
    orFallback (Just n) = Just n
    orFallback Nothing = Just (T.unpack name)
withColumnName _ result = result

{- | Get a specific column as a vector.

You must specify the type via type applications.

==== __Examples__

>>> columnAsVector (F.col @Int "age") df
Right [25, 30, 35, ...]

>>> columnAsVector (F.col @Text "name") df
Right ["Alice", "Bob", "Charlie", ...]
-}
columnAsVector ::
    forall a.
    (Columnable a) => Expr a -> DataFrame -> Either DataFrameException (V.Vector a)
columnAsVector expr df
    | null df = throw (EmptyDataSetException "columnAsVector")
    | otherwise = case expr of
        (Col name) -> case getColumn name df of
            Just col -> withColumnName name (toVector col)
            Nothing ->
                Left $
                    ColumnsNotFoundException [name] "columnAsVector" (M.keys $ columnIndices df)
        _ -> case interpret df expr of
            Left e -> throw e
            Right (TColumn col) -> toVector col

{- | A column as an unboxed vector of 'Int' values.
'Left' if the column cannot be converted to ints (non-numeric or out of range).
-}
columnAsIntVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Int)
columnAsIntVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toIntVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException [name] "columnAsIntVector" (M.keys $ columnIndices df)
columnAsIntVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toIntVector col

{- | A column as an unboxed vector of 'Double' values.
'Left' if the column cannot be converted to doubles (e.g. non-numeric data).
-}
columnAsDoubleVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Double)
columnAsDoubleVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toDoubleVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsDoubleVector"
                (M.keys $ columnIndices df)
columnAsDoubleVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toDoubleVector col

{- | A column as an unboxed vector of 'Float' values.
'Left' if the column cannot be converted to floats (e.g. non-numeric data).
-}
columnAsFloatVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Float)
columnAsFloatVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toFloatVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsFloatVector"
                (M.keys $ columnIndices df)
columnAsFloatVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toFloatVector col

columnAsUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector a)
columnAsUnboxedVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toUnboxedVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsFloatVector"
                (M.keys $ columnIndices df)
columnAsUnboxedVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toUnboxedVector col
{-# SPECIALIZE columnAsUnboxedVector ::
    Expr Double -> DataFrame -> Either DataFrameException (VU.Vector Double)
    #-}
{-# INLINE columnAsUnboxedVector #-}

{- | Get a specific column as a list.

You must specify the type via type applications.

==== __Examples__

>>> columnAsList @Int "age" df
[25, 30, 35, ...]

>>> columnAsList @Text "name" df
["Alice", "Bob", "Charlie", ...]

==== __Throws__

* 'error' - if the column type doesn't match the requested type
-}
columnAsList :: forall a. (Columnable a) => Expr a -> DataFrame -> [a]
columnAsList expr df = either throw V.toList (columnAsVector expr df)

{- | Returns the provenance of all columns in the DataFrame as a list of
@(name, expression)@ pairs. Derived columns show their expression;
raw columns show an identity @col \@type name@ expression.
-}

-- TODO: mchavinda - Expand out these expressions if possible.
showDerivedExpressions :: DataFrame -> [NamedExpr]
showDerivedExpressions df =
    let exprs = derivingExpressions df
        names = columnNames df
        toNamedExpr name = case M.lookup name exprs of
            Just uexpr -> (name, uexpr)
            Nothing -> (name, identityUExpr name)
     in map toNamedExpr names
  where
    identityUExpr name = case getColumn name df of
        Just (BoxedColumn (Just _) (_ :: V.Vector a)) -> UExpr (Col @(Maybe a) name)
        Just (BoxedColumn Nothing (_ :: V.Vector a)) -> UExpr (Col @a name)
        Just (UnboxedColumn (Just _) (_ :: VU.Vector a)) -> UExpr (Col @(Maybe a) name)
        Just (UnboxedColumn Nothing (_ :: VU.Vector a)) -> UExpr (Col @a name)
        Just (PackedText (Just _) _) -> UExpr (Col @(Maybe T.Text) name)
        Just (PackedText Nothing _) -> UExpr (Col @T.Text name)
        Just c@(MergedColumn _ _) -> case mergedHead c of
            BoxedColumn (Just _) (_ :: V.Vector a) -> UExpr (Col @(Maybe a) name)
            BoxedColumn Nothing (_ :: V.Vector a) -> UExpr (Col @a name)
            _ ->
                error $
                    "showDerivedExpressions: merged column did not materialize boxed: "
                        ++ T.unpack name
        Nothing -> error $ "showDerivedExpressions: column not found: " ++ T.unpack name
