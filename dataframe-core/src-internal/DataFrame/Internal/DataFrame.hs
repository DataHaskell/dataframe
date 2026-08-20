{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.DataFrame (
    DataFrame (..),
    forceDataFrame,
    GroupedDataFrame (.., Grouped),
    TruncateConfig (..),
    defaultTruncateConfig,
    ellipsisText,
    toMarkdown,
    toMarkdown',
    asText,
    asTextWith,
    pickColumns,
    insertAt,
    truncateCell,
    empty,
    columnNames,
    insertColumn,
    fromNamedColumns,
    getColumn,
    unsafeGetColumn,
    null,
    toCsv,
    toCsv',
    toSeparated,
    getRowAsText,
    showElement,
    stripJust,
) where

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
import DataFrame.Internal.PackedText (packedIndexText)
import Text.Printf
import Type.Reflection (Typeable, eqTypeRep, typeRep, pattern App)
import Prelude hiding (null)

data DataFrame = DataFrame
    { columns :: V.Vector Column
    -- ^ Column-oriented storage: the frame is a vector of columns.
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

Laziness contract: every field except 'valueIndices' and 'groupRepRows' is
computed eagerly by the grouping paths. 'valueIndices' may be a lazy thunk (the
low-cardinality direct grouping defers the O(n) stable placement pass until a
consumer — grouped median/top-k gathers, set ops, the interpreter's
group-slicing — actually demands the permutation); forcing it always yields the
unique stable counting-sort permutation of 'rowToGroup', so WHAT it evaluates to
is independent of when it is forced. 'groupRepRows' is the per-group
representative row (the first original row of each group, in canonical group
order); aggregation uses it to materialize the key columns without demanding
'valueIndices'. It may also be a thunk; its value always equals
@VU.map (valueIndices !) (VU.init offsets)@.
-}
data GroupedDataFrame = GroupedInternal
    { fullDataframe :: DataFrame
    , groupedColumns :: [T.Text]
    , valueIndices :: VU.Vector Int
    -- ^ Rows sorted by group id (stable); possibly an unevaluated thunk.
    , offsets :: VU.Vector Int
    , rowToGroup :: VU.Vector Int
    {- ^ rowToGroup[i] = group index for row i.  Length n (one per row).
    Built once in 'groupBy'; reused by every aggregation.
    -}
    , groupRepRows :: VU.Vector Int
    -- ^ First original row of each group, length nGroups. See laziness note.
    }

{- | The historical five-field view of 'GroupedDataFrame'. Matching ignores
'groupRepRows'; building derives it lazily from @valueIndices@/@offsets@ (the
thunk only forces them if something actually reads the representative rows).
The direct grouping paths construct 'GroupedInternal' directly instead so the
representative rows never demand the placement pass.
-}
pattern Grouped ::
    DataFrame ->
    [T.Text] ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    GroupedDataFrame
pattern Grouped df cols vis offs rtg <- GroupedInternal df cols vis offs rtg _
    where
        Grouped df cols vis offs rtg =
            GroupedInternal
                df
                cols
                vis
                offs
                rtg
                (VU.map (vis VU.!) (VU.init offs))

{-# COMPLETE Grouped #-}

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

{- | Configures how a 'DataFrame' is rendered as text: 'maxRows' caps rendered
rows, 'maxColumns' collapses middle columns past the limit into an ellipsis, and
'maxCellWidth' truncates long cells. A non-positive field means \"no limit\".
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
        getType (PackedText Nothing _) = T.pack $ show (typeRep @T.Text)
        getType (PackedText (Just _) _) = T.pack $ showMaybeType @T.Text
        getType c@(MergedColumn _ _) = getType (mergedHead c)

        get :: Maybe Column -> V.Vector T.Text
        get (Just c@(MergedColumn _ _)) = get (Just (materializeMerged c))
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
        get (Just c@(PackedText _ _)) = get (Just (materializePacked c))
        get Nothing = V.empty
     in showTable
            fmt
            (map clipCell finalHeaders)
            (map clipCell finalTypes)
            (map (V.map clipCell) finalCols)

{- | Decide which columns survive horizontal truncation and where to splice the
ellipsis column. Splits with the extra column on the left for odd 'maxColumns';
inserts the ellipsis only when it actually saves space.
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

{- | Retrieve a column by name, throwing 'ColumnsNotFoundException' if it does not
exist. Unsafe version of 'getColumn'; use when the column is certain to exist.
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
showElement (MergedColumn a b) i =
    showElement
        (materializeMerged (MergedColumn (sliceColumn i 1 a) (sliceColumn i 1 b)))
        0
showElement (BoxedColumn bm (c :: V.Vector a)) i = case bm of
    Just b | not (bitmapTestBit b i) -> "null"
    _ -> case c V.!? i of
        Nothing -> error $ "Column index out of bounds at row " ++ show i
        Just e
            | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) -> e
            | App t1 t2 <- typeRep @a
            , Just HRefl <- eqTypeRep t1 (typeRep @Maybe) ->
                case testEquality t2 (typeRep @T.Text) of
                    Just Refl -> fromMaybe "null" e
                    Nothing -> stripJust (T.pack (show e))
            | otherwise -> T.pack (show e)
showElement (UnboxedColumn bm c) i = case bm of
    Just b | not (bitmapTestBit b i) -> "null"
    _ -> case c VU.!? i of
        Nothing -> error $ "Column index out of bounds at row " ++ show i
        Just e -> T.pack (show e)
showElement (PackedText bm p) i = case bm of
    Just b | not (bitmapTestBit b i) -> "null"
    _ -> packedIndexText p i

stripJust :: T.Text -> T.Text
stripJust = fromMaybe "null" . T.stripPrefix "Just "
