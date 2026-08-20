{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

{- |
A plotly-express-style one-shot plotting API for HTML output: the string-keyed
convenience tier emitting interactive Vega-Lite charts. For the composable
expression-based grammar see "DataFrame.Display.Web.Chart" and its @.Typed@.
-}
module DataFrame.Display.Web.Plot (
    -- * Aggregation
    Agg (..),

    -- * Output
    showInDefaultBrowser,

    -- * Layout
    Size (..),
    defaultSize,

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
    allHistograms,

    -- * Deprecated legacy entry points
    plotHistogram,
    plotScatter,
    plotBars,
    plotLines,
    plotPie,
    plotBoxPlots,
) where

import Control.Monad (forM, void)
import Data.Char (chr)
import qualified Data.List as L
import qualified Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO.Utf8 as T
import GHC.Stack (HasCallStack)
import Numeric (showFFloat)
import System.Directory (getHomeDirectory)
import System.Info (os)
import System.Process (
    CreateProcess,
    StdStream (NoStream),
    createProcess,
    proc,
    shell,
    std_err,
    std_in,
    std_out,
    waitForProcess,
 )
import System.Random (newStdGen, randomRs)

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
import DataFrame.Display.Internal.VegaLite (
    Channel (Color, Theta, X, Y),
    FieldType (..),
    ResolvedField,
    VLSpec (..),
    chanEnc,
    emptySpec,
    numField,
    specHtml,
    textField,
 )
import qualified DataFrame.Display.Internal.VegaLite as VL
import DataFrame.Internal.DataFrame (DataFrame, columnNames)

-- ---------------------------------------------------------------------------
-- Layout
-- ---------------------------------------------------------------------------

-- | Display dimensions for the chart, in pixels.
data Size = Size {width :: Int, height :: Int}

defaultSize :: Size
defaultSize = Size 600 400

generateChartId :: IO T.Text
generateChartId = do
    gen <- newStdGen
    let randomWords =
            filter
                (\c -> c `elem` ([49 .. 57] ++ [65 .. 90] ++ [97 .. 122]))
                (take 64 (randomRs (49, 126) gen :: [Int]))
    return $ "chart_" <> T.pack (map chr randomWords)

-- | Assemble a chart HTML snippet from resolved data fields and a spec.
renderSpec :: Size -> [ResolvedField] -> VLSpec -> IO String
renderSpec sz fields spec = do
    chartId <- generateChartId
    let spec' = spec{vlWidth = sz.width, vlHeight = sz.height}
    return $ T.unpack $ specHtml chartId fields spec'

-- | Pin a channel's zero anchor explicitly, on or off.
anchorZero :: Bool -> VL.ChannelEnc -> VL.ChannelEnc
anchorZero b e = e{VL.ceScale = VL.defaultScale{VL.scaleZero = Just b}}

-- ---------------------------------------------------------------------------
-- Bar
-- ---------------------------------------------------------------------------

data Bar = Bar
    { x :: T.Text
    , y :: Maybe T.Text
    , agg :: Agg
    , topN :: Maybe Int
    , title :: Maybe T.Text
    , size :: Size
    }

mkBar :: T.Text -> Bar
mkBar c = Bar c Nothing Sum Nothing Nothing defaultSize

bar :: (HasCallStack) => Bar -> DataFrame -> IO String
bar spec df = do
    let effectiveAgg = case spec.y of
            Nothing -> Count
            Just _ -> spec.agg
        rows = aggregateByGroup effectiveAgg spec.x spec.y df
        rows' = maybe rows (`groupWithOther` rows) spec.topN
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> autoTitle effectiveAgg spec.y spec.x
        legendLabel = case spec.y of
            Nothing -> "count"
            Just yCol -> aggLabel effectiveAgg <> "(" <> yCol <> ")"
        (labels, values) = unzip rows'
        fields =
            [ textField spec.x labels
            , numField legendLabel values
            ]
        vlSpec =
            (emptySpec VL.Bar)
                { vlEncodings =
                    [ chanEnc X spec.x Nominal
                    , chanEnc Y legendLabel Quantitative
                    ]
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Histogram
-- ---------------------------------------------------------------------------

data Histogram = Histogram
    { x :: T.Text
    , bins :: Int
    , title :: Maybe T.Text
    , size :: Size
    }

mkHistogram :: T.Text -> Histogram
mkHistogram c = Histogram c 30 Nothing defaultSize

histogram :: (HasCallStack) => Histogram -> DataFrame -> IO String
histogram spec df = do
    let values = extractNumericColumn spec.x df
        (lo, hi) = if null values then (0, 1) else (minimum values, maximum values)
        binWidth = (hi - lo) / fromIntegral spec.bins
        binStarts = [lo + fromIntegral i * binWidth | i <- [0 .. spec.bins - 1]]
        countBin b = length [v | v <- values, v >= b && v < b + binWidth]
        counts = map (fromIntegral . countBin) binStarts
        precision = max 0 $ ceiling (negate $ logBase 10 (max 1e-12 binWidth))
        binLabels =
            [T.pack (showFFloat (Just precision) b "") | b <- binStarts]
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> "histogram of " <> spec.x
        fields =
            [ textField spec.x binLabels
            , numField "count" counts
            ]
        vlSpec =
            (emptySpec VL.Bar)
                { vlEncodings =
                    [ (chanEnc X spec.x Ordinal){VL.ceSort = Just VL.DataOrder}
                    , chanEnc Y "count" Quantitative
                    ]
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Scatter
-- ---------------------------------------------------------------------------

{- | 'includeZero' anchors both axes at zero. Off by default so the axes fit
the data — otherwise points far from the origin (e.g. geographic coordinates)
get squashed against the chart edge.
-}
data Scatter = Scatter
    { x :: T.Text
    , y :: T.Text
    , color :: Maybe T.Text
    , title :: Maybe T.Text
    , size :: Size
    , includeZero :: Bool
    }

mkScatter :: T.Text -> T.Text -> Scatter
mkScatter xc yc = Scatter xc yc Nothing Nothing defaultSize False

scatter :: (HasCallStack) => Scatter -> DataFrame -> IO String
scatter spec df = do
    let chartTitle = case spec.title of
            Just s -> s
            Nothing -> spec.x <> " vs " <> spec.y
        xVals = extractNumericColumn spec.x df
        yVals = extractNumericColumn spec.y df
        baseFields = [numField spec.x xVals, numField spec.y yVals]
        baseEncs =
            map
                (anchorZero spec.includeZero)
                [chanEnc X spec.x Quantitative, chanEnc Y spec.y Quantitative]
        (fields, encs) = case spec.color of
            Nothing -> (baseFields, baseEncs)
            Just grp ->
                ( baseFields ++ [textField grp (extractStringColumn grp df)]
                , baseEncs ++ [chanEnc Color grp Nominal]
                )
        vlSpec =
            (emptySpec VL.Point)
                { vlEncodings = encs
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Line
-- ---------------------------------------------------------------------------

{- | 'includeZero' anchors the axes at zero (Vega-Lite's own default). Off
by default so the axes fit the data — a line is position-encoded, so a
forced zero baseline only squashes series far from the origin.
-}
data Line = Line
    { x :: T.Text
    , y :: [T.Text]
    , title :: Maybe T.Text
    , size :: Size
    , includeZero :: Bool
    }

mkLine :: T.Text -> [T.Text] -> Line
mkLine xc ys = Line xc ys Nothing defaultSize False

line :: (HasCallStack) => Line -> DataFrame -> IO String
line spec df = do
    let chartTitle = case spec.title of
            Just s -> s
            Nothing -> case spec.y of
                [single] -> single <> " over " <> spec.x
                _ -> T.intercalate ", " spec.y <> " over " <> spec.x
        xVals = extractNumericColumn spec.x df
        perSeries =
            [ (col, zip xVals (extractNumericColumn col df))
            | col <- spec.y
            ]
        xsLong = concat [[xv | (xv, _) <- pts] | (_, pts) <- perSeries]
        valsLong = concat [[v | (_, v) <- pts] | (_, pts) <- perSeries]
        seriesLong = concat [replicate (length pts) col | (col, pts) <- perSeries]
        fields =
            [ numField spec.x xsLong
            , numField "value" valsLong
            , textField "series" seriesLong
            ]
        vlSpec =
            (emptySpec VL.Line)
                { vlEncodings =
                    [ anchorZero spec.includeZero (chanEnc X spec.x Quantitative)
                    , anchorZero spec.includeZero (chanEnc Y "value" Quantitative)
                    , chanEnc Color "series" Nominal
                    ]
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Pie
-- ---------------------------------------------------------------------------

data Pie = Pie
    { values :: T.Text
    , names :: Maybe T.Text
    , agg :: Agg
    , topN :: Maybe Int
    , title :: Maybe T.Text
    , size :: Size
    }

mkPie :: T.Text -> Pie
mkPie c = Pie c Nothing Count (Just 8) Nothing defaultSize

pie :: (HasCallStack) => Pie -> DataFrame -> IO String
pie spec df = do
    let vCol = spec.values
        mNames = spec.names
        a = spec.agg
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> case mNames of
                Just n -> aggLabel a <> "(" <> vCol <> ") by " <> n
                Nothing -> aggLabel a <> " of " <> vCol
        rows = case (a, mNames) of
            (Count, Nothing) -> aggregateByGroup Count vCol Nothing df
            (Count, Just n) -> aggregateByGroup Count n Nothing df
            (_, Nothing) ->
                let xs = extractNumericColumn vCol df
                 in zip [T.pack ("Item " ++ show i) | i <- [1 .. length xs :: Int]] xs
            (_, Just n) -> aggregateByGroup a n (Just vCol) df
        rows' = maybe rows (`groupWithOtherForPie` rows) spec.topN
        (labels, values) = unzip rows'
        catName = Data.Maybe.fromMaybe "category" mNames
        fields =
            [ textField catName labels
            , numField "value" values
            ]
        vlSpec =
            (emptySpec VL.Arc)
                { vlEncodings =
                    [ chanEnc Theta "value" Quantitative
                    , chanEnc Color catName Nominal
                    ]
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Box
-- ---------------------------------------------------------------------------

data Box = Box
    { y :: [T.Text]
    , title :: Maybe T.Text
    , size :: Size
    }

mkBox :: [T.Text] -> Box
mkBox ys = Box ys Nothing defaultSize

box :: (HasCallStack) => Box -> DataFrame -> IO String
box spec df = do
    let series = [(col, extractNumericColumn col df) | col <- spec.y]
        variableVals = concat [replicate (length vs) col | (col, vs) <- series]
        valueVals = concat [vs | (_, vs) <- series]
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> "box plot of " <> T.intercalate ", " spec.y
        fields =
            [ textField "variable" variableVals
            , numField "value" valueVals
            ]
        vlSpec =
            (emptySpec VL.Boxplot)
                { vlEncodings =
                    [ chanEnc X "variable" Nominal
                    , chanEnc Y "value" Quantitative
                    ]
                , vlTitle = Just chartTitle
                }
    renderSpec spec.size fields vlSpec

-- ---------------------------------------------------------------------------
-- Whole-frame helpers
-- ---------------------------------------------------------------------------

{- | Concatenate a histogram for every numeric column in the frame. Useful
as a one-shot exploratory summary.
-}
allHistograms :: (HasCallStack) => DataFrame -> IO String
allHistograms df = do
    let cols = filter (isNumericColumn df) (columnNames df)
    xs <- forM cols $ \c -> histogram (mkHistogram c) df
    return $ L.intercalate "\n" xs

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

plotHistogram :: (HasCallStack) => T.Text -> DataFrame -> IO String
plotHistogram c = histogram (mkHistogram c)
{-# DEPRECATED plotHistogram "use 'histogram (mkHistogram col)' instead" #-}

plotScatter :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO String
plotScatter xc yc = scatter (mkScatter xc yc)
{-# DEPRECATED plotScatter "use 'scatter (mkScatter xCol yCol)' instead" #-}

plotBars :: (HasCallStack) => T.Text -> DataFrame -> IO String
plotBars c = bar (mkBar c)
{-# DEPRECATED plotBars "use 'bar (mkBar col)' instead" #-}

plotLines :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO String
plotLines xc ys = line (mkLine xc ys)
{-# DEPRECATED plotLines "use 'line (mkLine xCol yCols)' instead" #-}

plotPie :: (HasCallStack) => T.Text -> Maybe T.Text -> DataFrame -> IO String
plotPie c mLabel =
    let s = mkPie c
     in pie s{names = mLabel}
{-# DEPRECATED plotPie "use 'pie (mkPie col)' instead" #-}

plotBoxPlots :: (HasCallStack) => [T.Text] -> DataFrame -> IO String
plotBoxPlots ys = box (mkBox ys)
{-# DEPRECATED plotBoxPlots "use 'box (mkBox cols)' instead" #-}

-- ---------------------------------------------------------------------------
-- Browser launcher
-- ---------------------------------------------------------------------------

-- | Windows launches via a shell: 'start' is a cmd builtin.
showInDefaultBrowser :: String -> IO ()
showInDefaultBrowser p = do
    plotId <- generateChartId
    home <- getHomeDirectory
    let path = "plot-" <> T.unpack plotId <> ".html"
        fullPath =
            if os == "mingw32"
                then home <> "\\" <> path
                else home <> "/" <> path
    putStr "Saving plot to: "
    putStrLn fullPath
    T.writeFile fullPath (T.pack p)
    case os of
        "mingw32" -> launchSilently (shell ("start \"\" \"" <> fullPath <> "\""))
        "darwin" -> launchSilently (proc "open" [fullPath])
        _ -> launchSilently (proc "xdg-open" [fullPath])

launchSilently :: CreateProcess -> IO ()
launchSilently cp = do
    (_, _, _, ph) <-
        createProcess
            cp
                { std_in = NoStream
                , std_out = NoStream
                , std_err = NoStream
                }
    void (waitForProcess ph)
