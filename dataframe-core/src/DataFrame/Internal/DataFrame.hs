{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.DataFrame where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Function (on)
import Data.List (sortBy, (\\))
import Data.Maybe (fromMaybe)
import Data.Type.Equality (
    TestEquality (testEquality),
    type (:~:) (Refl),
    type (:~~:) (HRefl),
 )
import DataFrame.Display.Terminal.PrettyPrint
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.Expression
import Text.Printf
import Type.Reflection (Typeable, eqTypeRep, typeRep, pattern App)
import Prelude hiding (null)

data DataFrame = DataFrame
    { columns :: V.Vector Column
    {- ^ Our main data structure stores a dataframe as
    a vector of columns. This improv
    -}
    , columnIndices :: M.Map T.Text Int
    -- ^ Keeps the column names in the order they were inserted in.
    , dataframeDimensions :: (Int, Int)
    -- ^ (rows, columns)
    , derivingExpressions :: M.Map T.Text UExpr
    }

{- | Force evaluation of all columns in a DataFrame. Replacement for the removed
@instance NFData DataFrame@; used by the IO and lazy-executor strict paths.
-}
forceDataFrame :: DataFrame -> DataFrame
forceDataFrame df@(DataFrame cols idx dims _exprs) =
    V.foldl' (\() c -> forceColumn c) () cols `seq` idx `seq` dims `seq` df

{- | A record that contains information about how and what
rows are grouped in the dataframe. This can only be used with
`aggregate`.
-}
data GroupedDataFrame = Grouped
    { fullDataframe :: DataFrame
    , groupedColumns :: [T.Text]
    , valueIndices :: VU.Vector Int
    , offsets :: VU.Vector Int
    , rowToGroup :: VU.Vector Int
    {- ^ rowToGroup[i] = group index for row i.  Length n (one per row).
    Built once in 'groupBy'; reused by every aggregation.
    -}
    }

instance Show GroupedDataFrame where
    show (Grouped df cols _indices _os _rtg) =
        printf
            "{ keyColumns: %s groupedColumns: %s }"
            (show cols)
            (show (M.keys (columnIndices df) \\ cols))

instance Eq GroupedDataFrame where
    (==) (Grouped df cols _indices _os _rtg) (Grouped df' cols' _indices' _os' _rtg') = (df == df') && (cols == cols')

instance Eq DataFrame where
    (==) :: DataFrame -> DataFrame -> Bool
    a == b =
        M.keys (columnIndices a) == M.keys (columnIndices b)
            && foldr
                ( \(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))
                )
                True
                (M.toList $ columnIndices a)

instance Show DataFrame where
    show :: DataFrame -> String
    show d =
        let (r, _) = dataframeDimensions d
            cfg = defaultTruncateConfig
            shown = if maxRows cfg > 0 then min (maxRows cfg) r else r
            body = asTextWith Plain (Just cfg) d
            footer
                | shown < r =
                    "\nShowing "
                        <> T.pack (show shown)
                        <> " rows out of "
                        <> T.pack (show r)
                | otherwise = T.empty
         in T.unpack (body <> footer)

{- | Configures how a 'DataFrame' is rendered as text. A non-positive value on
any field means \"no limit\" on that axis.

* 'maxRows' — render at most this many rows from the top of the frame.
* 'maxColumns' — when the frame has more columns than this, the middle columns
  are collapsed into a single ellipsis column.
* 'maxCellWidth' — text in any individual cell (including headers and type
  rows) longer than this is truncated with a trailing ellipsis.
-}
data TruncateConfig = TruncateConfig
    { maxRows :: Int
    , maxColumns :: Int
    , maxCellWidth :: Int
    }
    deriving (Show, Eq)

-- | Sensible defaults for GHCi: 20 rows, 10 columns, 30 characters per cell.
defaultTruncateConfig :: TruncateConfig
defaultTruncateConfig =
    TruncateConfig{maxRows = 20, maxColumns = 10, maxCellWidth = 30}

-- | Ellipsis character used to mark elided columns and clipped cells.
ellipsisText :: T.Text
ellipsisText = "\x2026"

-- | For showing the dataframe as markdown in notebooks.
toMarkdown :: DataFrame -> T.Text
toMarkdown = asText Markdown

-- | For showing the dataframe as a string markdown in notebooks.
toMarkdown' :: DataFrame -> String
toMarkdown' = T.unpack . toMarkdown

asText :: RenderFormat -> DataFrame -> T.Text
asText fmt = asTextWith fmt Nothing

asTextWith :: RenderFormat -> Maybe TruncateConfig -> DataFrame -> T.Text
asTextWith fmt mTrunc d =
    let allHeaders =
            map fst (sortBy (compare `on` snd) (M.toList (columnIndices d)))
        nCols = length allHeaders
        (totalRows, _) = dataframeDimensions d

        rowCap = case mTrunc of
            Just cfg | maxRows cfg > 0 -> min totalRows (maxRows cfg)
            _ -> totalRows

        (visibleHeaders, ellipsisAt) = pickColumns mTrunc nCols allHeaders

        lookupCol name =
            fmap
                (takeColumn rowCap)
                ((V.!?) (columns d) ((M.!) (columnIndices d) name))
        survivingCols = map lookupCol visibleHeaders
        survivingTypes = map (maybe "" getType) survivingCols
        survivingData = map get survivingCols

        clipCell = case mTrunc of
            Just cfg | maxCellWidth cfg > 0 -> truncateCell (maxCellWidth cfg)
            _ -> id

        (finalHeaders, finalTypes, finalCols) = case ellipsisAt of
            Nothing -> (visibleHeaders, survivingTypes, survivingData)
            Just i ->
                let ellipsisCol = V.replicate rowCap ellipsisText
                 in ( insertAt i ellipsisText visibleHeaders
                    , insertAt i ellipsisText survivingTypes
                    , insertAt i ellipsisCol survivingData
                    )

        getType :: Column -> T.Text
        showMaybeType :: forall a. (Typeable a) => String
        showMaybeType =
            let s = show (typeRep @a)
             in "Maybe " <> if ' ' `elem` s then "(" <> s <> ")" else s
        getType (BoxedColumn Nothing (_ :: V.Vector a)) = T.pack $ show (typeRep @a)
        getType (BoxedColumn (Just _) (_ :: V.Vector a)) = T.pack $ showMaybeType @a
        getType (UnboxedColumn Nothing (_ :: VU.Vector a)) = T.pack $ show (typeRep @a)
        getType (UnboxedColumn (Just _) (_ :: VU.Vector a)) = T.pack $ showMaybeType @a

        -- Separate out cases dynamically so we don't end up making round trip
        -- string copies.
        get :: Maybe Column -> V.Vector T.Text
        get (Just (BoxedColumn (Just bm) (column :: V.Vector a))) =
            V.generate (V.length column) $ \i ->
                if bitmapTestBit bm i
                    then T.pack (show (Just (V.unsafeIndex column i)))
                    else "Nothing"
        get (Just (BoxedColumn Nothing (column :: V.Vector a))) =
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> column
                Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                    Just Refl -> V.map T.pack column
                    Nothing -> V.map (T.pack . show) column
        get (Just (UnboxedColumn (Just bm) column)) =
            V.generate (VU.length column) $ \i ->
                if bitmapTestBit bm i
                    then T.pack (show (Just (VU.unsafeIndex column i)))
                    else "Nothing"
        get (Just (UnboxedColumn Nothing column)) =
            V.generate (VU.length column) (T.pack . show . VU.unsafeIndex column)
        get Nothing = V.empty
     in showTable
            fmt
            (map clipCell finalHeaders)
            (map clipCell finalTypes)
            (map (V.map clipCell) finalCols)

{- | Decide which columns survive horizontal truncation and where (if anywhere)
to splice in the ellipsis column. The split puts the extra column on the
left for odd 'maxColumns'; the ellipsis is only inserted when it actually
saves space (i.e. the frame has more than 'maxColumns' + 1 columns).
-}
pickColumns ::
    Maybe TruncateConfig ->
    Int ->
    [a] ->
    ([a], Maybe Int)
pickColumns mTrunc nCols xs = case mTrunc of
    Just cfg
        | let c = maxColumns cfg
        , c > 0
        , nCols > c + 1 ->
            let leftN = (c + 1) `div` 2
                rightN = c - leftN
             in ( Prelude.take leftN xs ++ Prelude.drop (nCols - rightN) xs
                , Just leftN
                )
    _ -> (xs, Nothing)

-- | Splice @x@ into @xs@ at index @i@ (0-based), shifting later elements right.
insertAt :: Int -> a -> [a] -> [a]
insertAt i x xs = let (l, r) = splitAt i xs in l ++ x : r

-- | Cap a single cell's rendered length, appending an ellipsis when shortened.
truncateCell :: Int -> T.Text -> T.Text
truncateCell n t
    | n <= 0 = t
    | T.compareLength t n /= GT = t
    | n == 1 = ellipsisText
    | otherwise = T.take (n - 1) t <> ellipsisText

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty =
    DataFrame
        { columns = V.empty
        , columnIndices = M.empty
        , dataframeDimensions = (0, 0)
        , derivingExpressions = M.empty
        }

-- | O(k) Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . sortBy (compare `on` snd) . M.toList . columnIndices
{-# INLINE columnNames #-}

{- | Insert a column into a DataFrame. If a column with the same name already
exists it is replaced in-place; otherwise the column is appended at the end.
Other columns are expanded (padded with nulls) to match the new row count.
-}
insertColumn :: T.Text -> Column -> DataFrame -> DataFrame
insertColumn name column d =
    let
        (r, c) = dataframeDimensions d
        n = max (columnLength column) r
        exprs = M.delete name (derivingExpressions d)
     in
        case M.lookup name (columnIndices d) of
            Just i ->
                DataFrame
                    (V.map (expandColumn n) (columns d V.// [(i, column)]))
                    (columnIndices d)
                    (n, c)
                    exprs
            Nothing ->
                DataFrame
                    (V.map (expandColumn n) (columns d `V.snoc` column))
                    (M.insert name c (columnIndices d))
                    (n, c + 1)
                    exprs

-- | Build a DataFrame from a list of @(name, column)@ pairs using 'insertColumn'.
fromNamedColumns :: [(T.Text, Column)] -> DataFrame
fromNamedColumns = foldl (\df (name, column) -> insertColumn name column df) empty

{- | Safely retrieves a column by name from the dataframe.

Returns 'Nothing' if the column does not exist.

==== __Examples__

>>> getColumn "age" df
Just (UnboxedColumn ...)

>>> getColumn "nonexistent" df
Nothing
-}
getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df
    | null df = Nothing
    | otherwise = do
        i <- columnIndices df M.!? name
        columns df V.!? i

{- | Retrieves a column by name from the dataframe, throwing an exception if not found.

This is an unsafe version of 'getColumn' that throws 'ColumnsNotFoundException'
if the column does not exist. Use this when you are certain the column exists.

==== __Throws__

* 'ColumnsNotFoundException' - if the column with the given name does not exist
-}
unsafeGetColumn :: T.Text -> DataFrame -> Column
unsafeGetColumn name df = case getColumn name df of
    Nothing -> throw $ ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
    Just col -> col

{- | Checks if the dataframe is empty (has no columns).

Returns 'True' if the dataframe has no columns, 'False' otherwise.
Note that a dataframe with columns but no rows is not considered null.
-}
null :: DataFrame -> Bool
null df = V.null (columns df)

-- | Convert a DataFrame to a CSV (comma-separated) text.
toCsv :: DataFrame -> T.Text
toCsv = toSeparated ','

-- | Convert a DataFrame to a CSV (comma-separated) string.
toCsv' :: DataFrame -> String
toCsv' = T.unpack . toSeparated ','

-- | Convert a DataFrame to a text representation with a custom separator.
toSeparated :: Char -> DataFrame -> T.Text
toSeparated sep df
    | null df = T.empty
    | otherwise =
        let (rows, _) = dataframeDimensions df
            headers = map fst (sortBy (compare `on` snd) (M.toList (columnIndices df)))
            sepText = T.singleton sep
            headerLine = T.intercalate sepText headers
            dataLines = map (T.intercalate sepText . getRowAsText df) [0 .. rows - 1]
         in T.unlines (headerLine : dataLines)

getRowAsText :: DataFrame -> Int -> [T.Text]
getRowAsText df i = map (`showElement` i) (V.toList (columns df))

showElement :: Column -> Int -> T.Text
showElement (BoxedColumn _ (c :: V.Vector a)) i = case c V.!? i of
    Nothing -> error $ "Column index out of bounds at row " ++ show i
    Just e
        | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) -> e
        | App t1 t2 <- typeRep @a
        , Just HRefl <- eqTypeRep t1 (typeRep @Maybe) ->
            case testEquality t2 (typeRep @T.Text) of
                Just Refl -> fromMaybe "null" e
                Nothing -> stripJust (T.pack (show e))
        | otherwise -> T.pack (show e)
showElement (UnboxedColumn _ c) i = case c VU.!? i of
    Nothing -> error $ "Column index out of bounds at row " ++ show i
    Just e -> T.pack (show e)

stripJust :: T.Text -> T.Text
stripJust = fromMaybe "null" . T.stripPrefix "Just "
