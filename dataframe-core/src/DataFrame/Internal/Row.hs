{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Row where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Function (on)
import Data.Maybe (catMaybes, fromMaybe, isNothing, mapMaybe)
import Data.Type.Equality (TestEquality (..))
import Data.Typeable (type (:~:) (..))
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.PackedText (packedIndexText, packedLength)
import Type.Reflection (typeOf, typeRep)

data Any where
    Value :: (Columnable a) => a -> Any
    -- Saves us the extra indirection we get from making Value (Maybe a)
    -- and having to unpack it again to check for nulls.
    -- Instead, we just have Null as a separate constructor.
    Null :: Any

instance Eq Any where
    (==) :: Any -> Any -> Bool
    (Value a) == (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a == b
    Null == Null = True
    _ == _ = False

instance Show Any where
    show :: Any -> String
    show (Value a) = T.unpack (showValue a)
    show Null = "null"

showValue :: forall a. (Columnable a) => a -> T.Text
showValue v = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> v
    Nothing -> case testEquality (typeRep @a) (typeRep @String) of
        Just Refl -> T.pack v
        Nothing -> (T.pack . show) v

-- | Wraps a value into an \Any\ type. This helps up represent rows as heterogenous lists.
toAny :: forall a. (Columnable a) => a -> Any
toAny = Value

-- | Unwraps a value from an \Any\ type. A 'Null' cell yields 'Nothing'.
fromAny :: forall a. (Columnable a) => Any -> Maybe a
fromAny Null = Nothing
fromAny (Value (v :: b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure v

{- | Wrap a column cell into an 'Any', honouring the column's null bitmap: a slot
marked invalid becomes 'Null', any other slot becomes a 'Value'. Only needs
@Columnable a@ (the stored element type), not @Columnable (Maybe a)@.
-}
cellAny :: (Columnable a) => Maybe Bitmap -> Int -> a -> Any
cellAny Nothing _ x = Value x
cellAny (Just bm) i x = if bitmapTestBit bm i then Value x else Null

type Row = V.Vector Any

(!?) :: [a] -> Int -> Maybe a
(!?) [] _ = Nothing
(!?) (x : _) 0 = Just x
(!?) (_x : xs) n = (!?) xs (n - 1)

{- | Reconstruct column @i@ from a list of rows. The element type is taken from
the first non-'Null' cell; cells of a different type are skipped. If any cell is
'Null' the result is a nullable column (built with 'fromMaybeVec', which needs
only @Columnable a@), so a round-trip through 'toRowList'/'fromRows' preserves
nulls.
-}
mkColumnFromRow :: Int -> [[Any]] -> Column
mkColumnFromRow i rows =
    let cells = mapMaybe (!? i) rows
     in case L.find isValue cells of
            Nothing -> fromList ([] :: [T.Text])
            Just (Value (_ :: a)) ->
                let collect Null = Just (Nothing :: Maybe a)
                    collect (Value (v' :: b)) =
                        case testEquality (typeRep @a) (typeRep @b) of
                            Just Refl -> Just (Just v')
                            Nothing -> Nothing
                    maybes = mapMaybe collect cells
                 in if any isNothing maybes
                        then fromMaybeVec (V.fromList maybes)
                        else fromList (catMaybes maybes)
            Just Null -> fromList ([] :: [T.Text]) -- unreachable: find isValue
  where
    isValue (Value _) = True
    isValue Null = False

{- | Converts the entire dataframe to a list of rows.

Each row contains all columns in the dataframe, ordered by their column indices.
The rows are returned in their natural order (from index 0 to n-1).

==== __Examples__

>>> toRowList df
[[("name", "Alice"), ("age", 25), ...], [("name", "Bob"), ("age", 30), ...], ...]

==== __Performance note__

This function materializes all rows into a list, which may be memory-intensive
for large dataframes. Consider using 'toRowVector' if you need random access
or streaming operations.
-}
toRowList :: DataFrame -> [[(T.Text, Any)]]
toRowList df =
    let
        names = map fst (L.sortBy (compare `on` snd) $ M.toList (columnIndices df))
     in
        map
            (zip names . V.toList . mkRowRep df names)
            [0 .. (fst (dataframeDimensions df) - 1)]

{- | Converts the dataframe to a vector of rows with only the specified columns.

Each row will contain only the columns named in the @names@ parameter.
This is useful when you only need a subset of columns or want to control
the column order in the resulting rows.

==== __Parameters__

[@names@] List of column names to include in each row. The order of names
          determines the order of fields in the resulting rows.

[@df@] The dataframe to convert.

==== __Examples__

>>> toRowVector ["name", "age"] df
Vector of rows with only name and age fields

>>> toRowVector [] df  -- Empty column list
Vector of empty rows (one per dataframe row)
-}
toRowVector :: [T.Text] -> DataFrame -> V.Vector Row
toRowVector names df = V.generate (fst (dataframeDimensions df)) (mkRowRep df names)

{- | Given a row gets the value associated with a field.

==== __Examples__

>>> map (rowValue (F.col @Int "age")) (toRowList df)
[25,30, ...]
-}
rowValue :: forall a. Expr a -> [(T.Text, Any)] -> Maybe a
rowValue (Col name) row = lookup name row >>= fromAny @a
rowValue _ _ = error "Can only get rowValue of column reference"

mkRowFromArgs :: [T.Text] -> DataFrame -> Int -> Row
mkRowFromArgs names df i = V.map get (V.fromList names)
  where
    get name = case getColumn name df of
        Nothing ->
            throw $
                ColumnsNotFoundException
                    [name]
                    "[INTERNAL] mkRowFromArgs"
                    (M.keys $ columnIndices df)
        Just (BoxedColumn bm column) -> cellAny bm i (column V.! i)
        Just (UnboxedColumn bm column) -> cellAny bm i (column VU.! i)
        Just (PackedText bm p) -> cellAny bm i (packedIndexText p i)

-- This function will return the items in the order that is specified
-- by the user. For example, if the dataframe consists of the columns
-- "Age", "Pclass", "Name", and the user asks for ["Name", "Age"],
-- this will order the values in the order ["Mr Smith", 50]
mkRowRep :: DataFrame -> [T.Text] -> Int -> Row
mkRowRep df names i = V.generate (L.length names) (\index -> get (names' V.! index))
  where
    names' = V.fromList names
    throwError name =
        error $
            "Column "
                ++ T.unpack name
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    get name = case getColumn name df of
        Just (BoxedColumn bm c) -> case c V.!? i of
            Just e -> cellAny bm i e
            Nothing -> throwError name
        Just (UnboxedColumn bm c) -> case c VU.!? i of
            Just e -> cellAny bm i e
            Nothing -> throwError name
        Just (PackedText bm p)
            | i < packedLength p -> cellAny bm i (packedIndexText p i)
            | otherwise -> throwError name
        Nothing ->
            throw $ ColumnsNotFoundException [name] "mkRowRep" (M.keys $ columnIndices df)
