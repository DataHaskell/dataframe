{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Core where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
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
    expandColumn,
    fromList,
    fromVector,
    toDoubleVector,
    toFloatVector,
    toIntVector,
    toUnboxedVector,
    toVector,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    empty,
    getColumn,
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
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices
{-# INLINE columnNames #-}

{- | Adds a vector to the dataframe. If the vector has less elements than the dataframe and the dataframe is not empty
the vector is converted to type `Maybe a` filled with `Nothing` to match the size of the dataframe. Similarly,
if the vector has more elements than what's currently in the dataframe, the other columns in the dataframe are
change to `Maybe <Type>` and filled with `Nothing`.

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

{- | Adds a foldable collection to the dataframe. If the collection has less elements than the
dataframe and the dataframe is not empty
the collection is converted to type `Maybe a` filled with `Nothing` to match the size of the dataframe. Similarly,
if the collection has more elements than what's currently in the dataframe, the other columns in the dataframe are
change to `Maybe <Type>` and filled with `Nothing`.

Be careful not to insert infinite collections with this function as that will crash the program.

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
insert name xs = insertColumn name (fromList (Fold.foldr' (:) [] xs)) -- TODO: Do reflection on container type so we can sometimes avoid the list construction.
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

{- | /O(n)/ Adds an unboxed vector to the dataframe.

Same as insertVector but takes an unboxed vector. If you insert a vector of numbers through insertVector it will either way be converted
into an unboxed vector so this function saves that extra work/conversion.
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
insertUnboxedVector name xs = insertColumn name (UnboxedColumn xs)

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
insertColumn ::
    -- | Column Name
    T.Text ->
    -- | Column to add
    Column ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertColumn name column d =
    let
        (r, c) = dataframeDimensions d
        n = max (columnLength column) r
     in
        case M.lookup name (columnIndices d) of
            Just i ->
                DataFrame
                    (V.map (expandColumn n) (columns d V.// [(i, column)]))
                    (columnIndices d)
                    (n, c)
                    M.empty
            Nothing ->
                DataFrame
                    (V.map (expandColumn n) (columns d `V.snoc` column))
                    (M.insert name c (columnIndices d))
                    (n, c + 1)
                    M.empty

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
cloneColumn original new df = fromMaybe
    ( throw $
        ColumnNotFoundException original "cloneColumn" (M.keys $ columnIndices df)
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
renameSafe orig new df = fromMaybe
    (Left $ ColumnNotFoundException orig "rename" (M.keys $ columnIndices df))
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
    go acc i col@(OptionalColumn (c :: V.Vector a)) =
        let
            cname = columnName i
            countNulls = nulls col
            columnType = T.pack $ show $ typeRep @a
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
    go acc i col@(BoxedColumn (c :: V.Vector a)) =
        let
            cname = columnName i
            columnType = T.pack $ show $ typeRep @a
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col)
                        0
                        columnType
                        : acc
    go acc i col@(UnboxedColumn c) =
        let
            cname = columnName i
            columnType = T.pack $ columnTypeString col
         in
            -- Unboxed columns cannot have nulls since Maybe
            -- is not an instance of Unbox a
            if isNothing cname
                then acc
                else
                    ColumnInfo (fromMaybe "" cname) (columnLength col) 0 columnType : acc

nulls :: Column -> Int
nulls (OptionalColumn xs) = VG.length $ VG.filter isNothing xs
nulls (BoxedColumn (xs :: V.Vector a)) = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> VG.length $ VG.filter isNullish xs
    Nothing -> case testEquality (typeRep @a) (typeRep @String) of
        Just Refl -> VG.length $ VG.filter (isNullish . T.pack) xs
        Nothing -> case typeRep @a of
            App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
                Just HRefl -> VG.length $ VG.filter isNothing xs
                Nothing -> 0
            _ -> 0
nulls _ = 0

partiallyParsed :: Column -> Int
partiallyParsed (BoxedColumn (xs :: V.Vector a)) =
    case typeRep @a of
        App (App tycon t1) t2 -> case eqTypeRep tycon (typeRep @Either) of
            Just HRefl -> VG.length $ VG.filter isLeft xs
            Nothing -> 0
        _ -> 0
partiallyParsed _ = 0

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
fromNamedColumns :: [(T.Text, Column)] -> DataFrame
fromNamedColumns = L.foldl' (\df (name, column) -> insertColumn name column df) empty

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
fromUnnamedColumns = fromNamedColumns . zip (map (T.pack . show) [0 ..])

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
        (\df i -> insertColumn (names !! i) (mkColumnFromRow i rows) df)
        empty
        [0 .. length names - 1]

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
valueCounts expr df = case columnAsVector expr df of
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
valueProportions expr df = case columnAsVector expr df of
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

{- | Returns a dataframe as a two dimensional vector of floats.

Converts all columns in the dataframe to float vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to floats.
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
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

{- | Returns a dataframe as a two dimensional vector of doubles.

Converts all columns in the dataframe to double vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to doubles.
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
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

{- | Returns a dataframe as a two dimensional vector of ints.

Converts all columns in the dataframe to int vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to ints.
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
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

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
columnAsVector (Col name) df = case getColumn name df of
    Just col -> toVector col
    Nothing ->
        Left $ ColumnNotFoundException name "columnAsVector" (M.keys $ columnIndices df)
columnAsVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toVector col

{- | Retrieves a column as an unboxed vector of 'Int' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to ints.
This may occur if the column contains non-numeric data or values outside the 'Int' range.
-}
columnAsIntVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Int)
columnAsIntVector (Col name) df = case getColumn name df of
    Just col -> toIntVector col
    Nothing ->
        Left $
            ColumnNotFoundException name "columnAsIntVector" (M.keys $ columnIndices df)
columnAsIntVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toIntVector col

{- | Retrieves a column as an unboxed vector of 'Double' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to doubles.
This may occur if the column contains non-numeric data.
-}
columnAsDoubleVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Double)
columnAsDoubleVector (Col name) df = case getColumn name df of
    Just col -> toDoubleVector col
    Nothing ->
        Left $
            ColumnNotFoundException name "columnAsDoubleVector" (M.keys $ columnIndices df)
columnAsDoubleVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toDoubleVector col

{- | Retrieves a column as an unboxed vector of 'Float' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to floats.
This may occur if the column contains non-numeric data.
-}
columnAsFloatVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Float)
columnAsFloatVector (Col name) df = case getColumn name df of
    Just col -> toFloatVector col
    Nothing ->
        Left $
            ColumnNotFoundException name "columnAsFloatVector" (M.keys $ columnIndices df)
columnAsFloatVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toFloatVector col

columnAsUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector a)
columnAsUnboxedVector (Col name) df = case getColumn name df of
    Just col -> toUnboxedVector col
    Nothing ->
        Left $
            ColumnNotFoundException name "columnAsFloatVector" (M.keys $ columnIndices df)
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
