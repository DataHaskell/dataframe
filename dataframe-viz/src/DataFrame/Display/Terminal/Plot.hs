{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

{- |
A plotly-express-style one-shot plotting API for terminal output.

Each chart kind has a small record spec (with sensible defaults) and a
single render function. Customise plots by record-update on the spec:

@
bar ((mkBar "Sex") { y = Just "Survived" }) titanic
bar ((mkBar "ocean_proximity")
      { y = Just "median_house_value", agg = Mean }) housing
@
-}
module DataFrame.Display.Terminal.Plot (
    -- * Aggregation
    Agg (..),

    -- * Bar charts
    Bar (..),
    mkBar,
    bar,

    -- * Histograms
    Histogram (..),
    mkHistogram,
    histogram,

    -- * Scatter plots
    Scatter (..),
    mkScatter,
    scatter,

    -- * Line charts
    Line (..),
    mkLine,
    line,

    -- * Pie charts
    Pie (..),
    mkPie,
    pie,

    -- * Box plots
    Box (..),
    mkBox,
    box,

    -- * Whole-frame plots
    heatmap,
    correlationMatrix,

    -- * Deprecated legacy entry points

    {- |
    These shims delegate to the parametrized API. They are kept for
    one release to ease migration; prefer the spec-based functions
    above for new code.
    -}
    plotHistogram,
    plotScatter,
    plotBars,
    plotLines,
    plotPie,
    plotBoxPlots,
    plotStackedBars,
    plotHeatmap,
    extractNumericColumn,
    extractStringColumn,
) where

import Control.Monad (forM)
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Text.IO as T
import GHC.Stack (HasCallStack)

import DataFrame.Display.Internal.Common (
    Agg (..),
    aggLabel,
    aggregateByGroup,
    extractNumericColumn,
    extractStringColumn,
    groupWithOther,
    groupWithOtherForPie,
    isNumericColumn,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), columnNames)
import Granite (Plot, defPlot)
import qualified Granite

-- ---------------------------------------------------------------------------
-- Bar
-- ---------------------------------------------------------------------------

{- | Bar-chart specification.

@x@ is required; everything else has a default. When @y@ is @Nothing@ the
chart counts rows per group; otherwise @y@ is aggregated using @agg@.
-}
data Bar = Bar
    { x :: T.Text
    -- ^ Grouping (categorical) column.
    , y :: Maybe T.Text
    -- ^ Value column; @Nothing@ counts rows.
    , agg :: Agg
    -- ^ Aggregation applied to @y@. Defaults to 'Sum'.
    , topN :: Maybe Int
    -- ^ Optional cap on the number of bars (rest folded into "Other").
    , title :: Maybe T.Text
    -- ^ Chart title; auto-generated if @Nothing@.
    , plot :: Plot
    -- ^ Granite plot configuration (size, palette, ticks, etc.).
    }

-- | Smart constructor: spec with sensible defaults, given the @x@ column.
mkBar :: T.Text -> Bar
mkBar c =
    Bar
        { x = c
        , y = Nothing
        , agg = Sum
        , topN = Nothing
        , title = Nothing
        , plot = defPlot
        }

-- | Render a 'Bar' spec to stdout.
bar :: (HasCallStack) => Bar -> DataFrame -> IO ()
bar spec df = do
    let effectiveAgg = case spec.y of
            Nothing -> Count
            Just _ -> spec.agg
        rows = aggregateByGroup effectiveAgg spec.x spec.y df
        rows' = maybe rows (`groupWithOther` rows) spec.topN
        t = case spec.title of
            Just s -> s
            Nothing -> autoTitle effectiveAgg spec.y spec.x
        cfg = spec.plot{Granite.plotTitle = t}
    T.putStrLn $ Granite.bars rows' cfg

-- ---------------------------------------------------------------------------
-- Histogram
-- ---------------------------------------------------------------------------

-- | Histogram specification for a numeric column.
data Histogram = Histogram
    { x :: T.Text
    -- ^ Numeric column to bin.
    , bins :: Int
    -- ^ Number of bins (default 30).
    , title :: Maybe T.Text
    , plot :: Plot
    }

mkHistogram :: T.Text -> Histogram
mkHistogram c =
    Histogram
        { x = c
        , bins = 30
        , title = Nothing
        , plot = defPlot
        }

histogram :: (HasCallStack) => Histogram -> DataFrame -> IO ()
histogram spec df = do
    let values = extractNumericColumn spec.x df
        (lo, hi) = if null values then (0, 1) else (minimum values, maximum values)
        t = case spec.title of
            Just s -> s
            Nothing -> "histogram of " <> spec.x
        cfg = spec.plot{Granite.plotTitle = t}
    T.putStrLn $ Granite.histogram (Granite.bins spec.bins lo hi) values cfg

-- ---------------------------------------------------------------------------
-- Scatter
-- ---------------------------------------------------------------------------

-- | Scatter-plot specification.
data Scatter = Scatter
    { x :: T.Text
    , y :: T.Text
    , color :: Maybe T.Text
    -- ^ Optional grouping column; produces a separate series per value.
    , title :: Maybe T.Text
    , plot :: Plot
    }

mkScatter :: T.Text -> T.Text -> Scatter
mkScatter xc yc =
    Scatter
        { x = xc
        , y = yc
        , color = Nothing
        , title = Nothing
        , plot = defPlot
        }

scatter :: (HasCallStack) => Scatter -> DataFrame -> IO ()
scatter spec df = do
    let t = case spec.title of
            Just s -> s
            Nothing -> spec.x <> " vs " <> spec.y
        cfg = spec.plot{Granite.plotTitle = t}
        xVals = extractNumericColumn spec.x df
        yVals = extractNumericColumn spec.y df
    case spec.color of
        Nothing ->
            T.putStrLn $ Granite.scatter [(spec.x <> " vs " <> spec.y, zip xVals yVals)] cfg
        Just grp -> do
            let groupVals = extractStringColumn grp df
                triples = zip3 groupVals xVals yVals
                uniqGroups = L.nub groupVals
                series =
                    [ (g, [(xv, yv) | (gv, xv, yv) <- triples, gv == g])
                    | g <- uniqGroups
                    ]
            T.putStrLn $ Granite.scatter series cfg

-- ---------------------------------------------------------------------------
-- Line
-- ---------------------------------------------------------------------------

-- | Line-chart specification: one x column, one or more y series.
data Line = Line
    { x :: T.Text
    , y :: [T.Text]
    , title :: Maybe T.Text
    , plot :: Plot
    }

mkLine :: T.Text -> [T.Text] -> Line
mkLine xc ys =
    Line
        { x = xc
        , y = ys
        , title = Nothing
        , plot = defPlot
        }

line :: (HasCallStack) => Line -> DataFrame -> IO ()
line spec df = do
    let t = case spec.title of
            Just s -> s
            Nothing -> case spec.y of
                [single] -> single <> " over " <> spec.x
                _ -> T.intercalate ", " spec.y <> " over " <> spec.x
        cfg = spec.plot{Granite.plotTitle = t}
        xVals = extractNumericColumn spec.x df
    seriesData <- forM spec.y $ \col -> do
        let values = extractNumericColumn col df
        return (col, zip xVals values)
    T.putStrLn $ Granite.lineGraph seriesData cfg

-- ---------------------------------------------------------------------------
-- Pie
-- ---------------------------------------------------------------------------

-- | Pie-chart specification.
data Pie = Pie
    { values :: T.Text
    {- ^ Slice-defining column. For 'Count', categorical; for the
    numeric aggs, the value column.
    -}
    , names :: Maybe T.Text
    {- ^ Optional label column. If @Nothing@, slices are labelled by
    the value column directly (for 'Count') or by row index.
    -}
    , agg :: Agg
    , topN :: Maybe Int
    , title :: Maybe T.Text
    , plot :: Plot
    }

mkPie :: T.Text -> Pie
mkPie c =
    Pie
        { values = c
        , names = Nothing
        , agg = Count
        , topN = Just 8
        , title = Nothing
        , plot = defPlot
        }

pie :: (HasCallStack) => Pie -> DataFrame -> IO ()
pie spec df = do
    let vCol = spec.values
        mNames = spec.names
        a = spec.agg
        t = case spec.title of
            Just s -> s
            Nothing -> case mNames of
                Just n -> aggLabel a <> "(" <> vCol <> ") by " <> n
                Nothing -> aggLabel a <> " of " <> vCol
        cfg = spec.plot{Granite.plotTitle = t}
        rows = case (a, mNames) of
            (Count, Nothing) -> aggregateByGroup Count vCol Nothing df
            (Count, Just n) -> aggregateByGroup Count n Nothing df
            (_, Nothing) ->
                let xs = extractNumericColumn vCol df
                 in zip [T.pack ("Item " ++ show i) | i <- [1 .. length xs :: Int]] xs
            (_, Just n) -> aggregateByGroup a n (Just vCol) df
        rows' = maybe rows (`groupWithOtherForPie` rows) spec.topN
    T.putStrLn $ Granite.pie rows' cfg

-- ---------------------------------------------------------------------------
-- Box
-- ---------------------------------------------------------------------------

-- | Box-plot specification across one or more numeric columns.
data Box = Box
    { y :: [T.Text]
    , title :: Maybe T.Text
    , plot :: Plot
    }

mkBox :: [T.Text] -> Box
mkBox ys =
    Box
        { y = ys
        , title = Nothing
        , plot = defPlot
        }

box :: (HasCallStack) => Box -> DataFrame -> IO ()
box spec df = do
    let t = case spec.title of
            Just s -> s
            Nothing -> "box plot of " <> T.intercalate ", " spec.y
        cfg = spec.plot{Granite.plotTitle = t}
    boxData <- forM spec.y $ \col -> return (col, extractNumericColumn col df)
    T.putStrLn $ Granite.boxPlot boxData cfg

-- ---------------------------------------------------------------------------
-- Whole-frame plots
-- ---------------------------------------------------------------------------

{- | Heatmap of all numeric columns in the dataframe; each numeric column
becomes a column of the matrix.
-}
heatmap :: (HasCallStack) => DataFrame -> IO ()
heatmap df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
        matrix = map (`extractNumericColumn` df) numericCols
    T.putStrLn $ Granite.heatmap matrix defPlot

-- | Pearson correlation heatmap across all numeric columns.
correlationMatrix :: (HasCallStack) => DataFrame -> IO ()
correlationMatrix df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
        cols = map (`extractNumericColumn` df) numericCols
        correlations =
            [ [corr xs ys | ys <- cols]
            | xs <- cols
            ]
    print (zip [(0 :: Int) ..] numericCols)
    T.putStrLn $
        Granite.heatmap correlations (defPlot{Granite.plotTitle = "correlation matrix"})
  where
    corr xs ys =
        let n = fromIntegral $ length xs
            meanX = sum xs / n
            meanY = sum ys / n
            covXY = sum [(a - meanX) * (b - meanY) | (a, b) <- zip xs ys] / n
            stdX = sqrt $ sum [(a - meanX) ^ (2 :: Int) | a <- xs] / n
            stdY = sqrt $ sum [(b - meanY) ^ (2 :: Int) | b <- ys] / n
         in covXY / (stdX * stdY)

-- ---------------------------------------------------------------------------
-- Title helper
-- ---------------------------------------------------------------------------

autoTitle :: Agg -> Maybe T.Text -> T.Text -> T.Text
autoTitle Count _ groupCol = "count by " <> groupCol
autoTitle a (Just yCol) groupCol = aggLabel a <> "(" <> yCol <> ") by " <> groupCol
autoTitle a Nothing groupCol = aggLabel a <> " by " <> groupCol

-- ---------------------------------------------------------------------------
-- Deprecated legacy entry points
-- ---------------------------------------------------------------------------

-- | Use 'histogram' with 'mkHistogram' instead.
plotHistogram :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotHistogram c = histogram (mkHistogram c)
{-# DEPRECATED plotHistogram "use 'histogram (mkHistogram col)' instead" #-}

-- | Use 'scatter' with 'mkScatter' instead.
plotScatter :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO ()
plotScatter xc yc = scatter (mkScatter xc yc)
{-# DEPRECATED plotScatter "use 'scatter (mkScatter xCol yCol)' instead" #-}

-- | Use 'bar' with 'mkBar' instead.
plotBars :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotBars c = bar (mkBar c)
{-# DEPRECATED plotBars "use 'bar (mkBar col)' instead" #-}

-- | Use 'line' with 'mkLine' instead.
plotLines :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO ()
plotLines xc ys = line (mkLine xc ys)
{-# DEPRECATED plotLines "use 'line (mkLine xCol yCols)' instead" #-}

-- | Use 'pie' with 'mkPie' instead.
plotPie :: (HasCallStack) => T.Text -> Maybe T.Text -> DataFrame -> IO ()
plotPie c mLabel =
    let s = mkPie c
     in pie s{names = mLabel}
{-# DEPRECATED plotPie "use 'pie (mkPie col)' instead" #-}

-- | Use 'box' with 'mkBox' instead.
plotBoxPlots :: (HasCallStack) => [T.Text] -> DataFrame -> IO ()
plotBoxPlots ys = box (mkBox ys)
{-# DEPRECATED plotBoxPlots "use 'box (mkBox cols)' instead" #-}

{- | Stacked-bar plot: one bar per category, segments from the listed value
columns. Kept as a direct passthrough to granite — has no spec record
because no aggregation is involved (segments are summed per category).
-}
plotStackedBars :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO ()
plotStackedBars categoryCol valueColumns df = do
    let categories = extractStringColumn categoryCol df
        uniqueCategories = L.nub categories
    stackData <- forM uniqueCategories $ \cat -> do
        let indices = [i | (i, c) <- zip [0 ..] categories, c == cat]
        seriesData <- forM valueColumns $ \col -> do
            let allValues = extractNumericColumn col df
                vs = [allValues !! i | i <- indices, i < length allValues]
            return (col, sum vs)
        return (cat, seriesData)
    T.putStrLn $ Granite.stackedBars stackData defPlot

-- | Use 'heatmap' instead.
plotHeatmap :: (HasCallStack) => DataFrame -> IO ()
plotHeatmap = heatmap
{-# DEPRECATED plotHeatmap "use 'heatmap' instead" #-}
