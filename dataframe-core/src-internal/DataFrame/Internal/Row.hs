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
import Data.Maybe (catMaybes, fromMaybe, isNothing)
import Data.Type.Equality (TestEquality (..))
import Data.Typeable (Typeable, type (:~:) (..))
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.PackedText (packedIndexText, packedLength)
import Type.Reflection (TypeRep, typeOf, typeRep)

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

mkColumnFromRow :: T.Text -> Int -> [[Any]] -> Column
mkColumnFromRow name i rows =
    case L.find isValue cells of
        Just (Value (_ :: a)) ->
            let collect _ Null = Nothing :: Maybe a
                collect r (Value (v' :: b)) =
                    case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl -> Just v'
                        Nothing -> throw (mismatchAt r (typeRep @b) (typeRep @a))
                maybes = zipWith collect [0 :: Int ..] cells
             in if any isNothing maybes
                    then fromMaybeVec (V.fromList maybes)
                    else fromList (catMaybes maybes)
        _ -> fromMaybeVec (V.fromList (map (const (Nothing :: Maybe T.Text)) cells))
  where
    cells = zipWith cellAt [0 :: Int ..] rows
    cellAt r row = fromMaybe (throw (missingCellAt r)) (row !? i)
    isValue (Value _) = True
    isValue Null = False
    mismatchAt ::
        forall x y.
        (Typeable x, Typeable y) => Int -> TypeRep x -> TypeRep y -> DataFrameException
    mismatchAt r actual expected =
        TypeMismatchException
            MkTypeErrorContext
                { userType = Right actual
                , expectedType = Right expected
                , errorColumnName = Just (T.unpack name ++ ", row " ++ show r)
                , callingFunctionName = Just "fromRows"
                }
    missingCellAt r =
        InternalException
            ( "fromRows: row "
                <> T.pack (show r)
                <> " has no cell for column "
                <> name
            )

{- | Convert the whole dataframe to a list of rows, one per row index in natural
order; each row lists all columns ordered by column index. Materializes every
row, so prefer 'toRowVector' for large frames.

>>> toRowList df
[[("name", "Alice"), ("age", 25), ...], [("name", "Bob"), ("age", 30), ...], ...]
-}
toRowList :: DataFrame -> [[(T.Text, Any)]]
toRowList df =
    let
        names = map fst (L.sortBy (compare `on` snd) $ M.toList (columnIndices df))
     in
        map
            (zip names . V.toList . mkRowRep df names)
            [0 .. (fst (dataframeDimensions df) - 1)]

{- | Convert the dataframe to a vector of rows containing only the named columns,
in the given order. An empty name list yields one empty row per dataframe row.

>>> toRowVector ["name", "age"] df
Vector of rows with only name and age fields
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

-- Returns row values in the caller's requested column order, not the
-- dataframe's storage order.
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
