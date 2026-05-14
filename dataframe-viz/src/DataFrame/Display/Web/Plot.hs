{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NoFieldSelectors #-}

{- |
A plotly-express-style one-shot plotting API for HTML output. Mirrors
'DataFrame.Display.Terminal.Plot' but emits embeddable Chart.js HTML.
-}
module DataFrame.Display.Web.Plot (
    -- * Aggregation
    Agg (..),

    -- * Output
    HtmlPlot (..),
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
import qualified Data.Text as T
import qualified Data.Text.IO as T
import GHC.Stack (HasCallStack)
import Numeric (showFFloat)
import System.Directory (getHomeDirectory)
import System.Info (os)
import System.Process (
    StdStream (NoStream),
    createProcess,
    proc,
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
import DataFrame.Internal.DataFrame (DataFrame, columnNames)

-- ---------------------------------------------------------------------------
-- Output container + layout
-- ---------------------------------------------------------------------------

-- | A snippet of HTML containing a Chart.js plot.
newtype HtmlPlot = HtmlPlot T.Text deriving (Show)

-- | Display dimensions for the HTML canvas, in pixels.
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

wrapInHTML :: T.Text -> T.Text -> Size -> T.Text
wrapInHTML chartId content sz =
    T.concat
        [ "<canvas id=\""
        , chartId
        , "\" style=\"width:100%;max-width:"
        , T.pack (show sz.width)
        , "px;height:"
        , T.pack (show sz.height)
        , "px\"></canvas>\n"
        , "<script src=\"https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.9.4/Chart.min.js\"></script>\n"
        , "<script>\n"
        , content
        , "\n</script>\n"
        ]

palette :: [T.Text]
palette =
    [ "rgb(255, 99, 132)"
    , "rgb(54, 162, 235)"
    , "rgb(255, 206, 86)"
    , "rgb(75, 192, 192)"
    , "rgb(153, 102, 255)"
    , "rgb(255, 159, 64)"
    , "rgb(201, 203, 207)"
    , "rgb(255, 99, 71)"
    , "rgb(60, 179, 113)"
    , "rgb(238, 130, 238)"
    ]

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

bar :: (HasCallStack) => Bar -> DataFrame -> IO HtmlPlot
bar spec df = do
    chartId <- generateChartId
    let effectiveAgg = case spec.y of
            Nothing -> Count
            Just _ -> spec.agg
        rows = aggregateByGroup effectiveAgg spec.x spec.y df
        rows' = maybe rows (`groupWithOther` rows) spec.topN
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> autoTitle effectiveAgg spec.y spec.x
        labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- rows']
        dataPoints = T.intercalate "," [T.pack (show v) | (_, v) <- rows']
        legendLabel = case spec.y of
            Nothing -> "count"
            Just yCol -> aggLabel effectiveAgg <> "(" <> yCol <> ")"
        jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"bar\",\n  data: {\n    labels: ["
                , labels
                , "],\n    datasets: [{\n      label: \""
                , legendLabel
                , "\",\n      data: ["
                , dataPoints
                , "],\n      backgroundColor: \"rgba(54, 162, 235, 0.6)\",\n      borderColor: \"rgba(54, 162, 235, 1)\",\n      borderWidth: 1\n    }]\n  },\n"
                , "  options: {\n    title: { display: true, text: \""
                , chartTitle
                , "\" },\n    scales: { yAxes: [{ ticks: { beginAtZero: true } }] }\n  }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size

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

histogram :: (HasCallStack) => Histogram -> DataFrame -> IO HtmlPlot
histogram spec df = do
    chartId <- generateChartId
    let values = extractNumericColumn spec.x df
        (lo, hi) = if null values then (0, 1) else (minimum values, maximum values)
        binWidth = (hi - lo) / fromIntegral spec.bins
        binStarts = [lo + fromIntegral i * binWidth | i <- [0 .. spec.bins - 1]]
        countBin b = length [v | v <- values, v >= b && v < b + binWidth]
        counts = map countBin binStarts
        precision = max 0 $ ceiling (negate $ logBase 10 (max 1e-12 binWidth))
        labels =
            T.intercalate
                ","
                [ "\"" <> T.pack (showFFloat (Just precision) b "") <> "\""
                | b <- binStarts
                ]
        dataPoints = T.intercalate "," [T.pack (show c) | c <- counts]
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> "histogram of " <> spec.x
        jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"bar\",\n  data: {\n    labels: ["
                , labels
                , "],\n    datasets: [{\n      label: \""
                , spec.x
                , "\",\n      data: ["
                , dataPoints
                , "],\n      backgroundColor: \"rgba(75, 192, 192, 0.6)\",\n      borderColor: \"rgba(75, 192, 192, 1)\",\n      borderWidth: 1\n    }]\n  },\n"
                , "  options: {\n    title: { display: true, text: \""
                , chartTitle
                , "\" },\n    scales: { yAxes: [{ ticks: { beginAtZero: true } }] }\n  }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size

-- ---------------------------------------------------------------------------
-- Scatter
-- ---------------------------------------------------------------------------

data Scatter = Scatter
    { x :: T.Text
    , y :: T.Text
    , color :: Maybe T.Text
    , title :: Maybe T.Text
    , size :: Size
    }

mkScatter :: T.Text -> T.Text -> Scatter
mkScatter xc yc = Scatter xc yc Nothing Nothing defaultSize

scatter :: (HasCallStack) => Scatter -> DataFrame -> IO HtmlPlot
scatter spec df = do
    chartId <- generateChartId
    let chartTitle = case spec.title of
            Just s -> s
            Nothing -> spec.x <> " vs " <> spec.y
        xVals = extractNumericColumn spec.x df
        yVals = extractNumericColumn spec.y df
    datasets <- case spec.color of
        Nothing ->
            return $
                T.singleton '\n'
                    <> renderSeries chartTitle (head palette) (zip xVals yVals)
        Just grp -> do
            let groupVals = extractStringColumn grp df
                triples = zip3 groupVals xVals yVals
                uniq = L.nub groupVals
                ds =
                    [ renderSeries g col [(xv, yv) | (gv, xv, yv) <- triples, gv == g]
                    | (g, col) <- zip uniq (cycle palette)
                    ]
            return $ T.intercalate ",\n" ds
    let jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"scatter\",\n  data: {\n    datasets: ["
                , datasets
                , "\n    ]\n  },\n  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      xAxes: [{ scaleLabel: { display: true, labelString: \""
                , spec.x
                , "\" } }],\n      yAxes: [{ scaleLabel: { display: true, labelString: \""
                , spec.y
                , "\" } }]\n    }\n  }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size
  where
    renderSeries label col pts =
        let body =
                T.intercalate
                    ","
                    [ "{x:" <> T.pack (show xv) <> ", y:" <> T.pack (show yv) <> "}" | (xv, yv) <- pts
                    ]
         in T.concat
                [ "    {\n      label: \""
                , label
                , "\",\n      data: ["
                , body
                , "],\n      pointRadius: 4,\n      pointBackgroundColor: \""
                , col
                , "\"\n    }"
                ]

-- ---------------------------------------------------------------------------
-- Line
-- ---------------------------------------------------------------------------

data Line = Line
    { x :: T.Text
    , y :: [T.Text]
    , title :: Maybe T.Text
    , size :: Size
    }

mkLine :: T.Text -> [T.Text] -> Line
mkLine xc ys = Line xc ys Nothing defaultSize

line :: (HasCallStack) => Line -> DataFrame -> IO HtmlPlot
line spec df = do
    chartId <- generateChartId
    let chartTitle = case spec.title of
            Just s -> s
            Nothing -> case spec.y of
                [single] -> single <> " over " <> spec.x
                _ -> T.intercalate ", " spec.y <> " over " <> spec.x
        xValues = extractNumericColumn spec.x df
        xLabels = T.intercalate "," [T.pack (show v) | v <- xValues]
    datasets <- forM (zip spec.y (cycle palette)) $ \(col, c) -> do
        let values = extractNumericColumn col df
            body = T.intercalate "," [T.pack (show v) | v <- values]
        return $
            T.concat
                [ "    {\n      label: \""
                , col
                , "\",\n      data: ["
                , body
                , "],\n      fill: false,\n      borderColor: \""
                , c
                , "\",\n      tension: 0.1\n    }"
                ]
    let datasetsStr = T.intercalate ",\n" datasets
        jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"line\",\n  data: {\n    labels: ["
                , xLabels
                , "],\n    datasets: [\n"
                , datasetsStr
                , "\n    ]\n  },\n  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: { xAxes: [{ scaleLabel: { display: true, labelString: \""
                , spec.x
                , "\" } }] }\n  }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size

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

pie :: (HasCallStack) => Pie -> DataFrame -> IO HtmlPlot
pie spec df = do
    chartId <- generateChartId
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
        labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- rows']
        dataPoints = T.intercalate "," [T.pack (show v) | (_, v) <- rows']
        colors =
            T.intercalate
                ","
                ["\"" <> c <> "\"" | c <- take (length rows') palette]
        jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"pie\",\n  data: {\n    labels: ["
                , labels
                , "],\n    datasets: [{\n      data: ["
                , dataPoints
                , "],\n      backgroundColor: ["
                , colors
                , "]\n    }]\n  },\n  options: { title: { display: true, text: \""
                , chartTitle
                , "\" } }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size

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

box :: (HasCallStack) => Box -> DataFrame -> IO HtmlPlot
box spec df = do
    chartId <- generateChartId
    boxData <- forM spec.y $ \col -> do
        let vs = extractNumericColumn col df
            sorted = L.sort vs
            n = max 1 (length vs)
            median = sorted !! (n `div` 2)
        return (col, median)
    let labels = T.intercalate "," ["\"" <> col <> "\"" | (col, _) <- boxData]
        medians = T.intercalate "," [T.pack (show med) | (_, med) <- boxData]
        chartTitle = case spec.title of
            Just s -> s
            Nothing -> "box plot of " <> T.intercalate ", " spec.y
        jsCode =
            T.concat
                [ "setTimeout(function() { new Chart(\""
                , chartId
                , "\", {\n  type: \"bar\",\n  data: {\n    labels: ["
                , labels
                , "],\n    datasets: [{\n      label: \"Median\",\n      data: ["
                , medians
                , "],\n      backgroundColor: \"rgba(75, 192, 192, 0.6)\",\n      borderColor: \"rgba(75, 192, 192, 1)\",\n      borderWidth: 1\n    }]\n  },\n"
                , "  options: { title: { display: true, text: \""
                , chartTitle
                , " (showing medians)\" }, scales: { yAxes: [{ ticks: { beginAtZero: true } }] } }\n})}, 100);"
                ]
    return $ HtmlPlot $ wrapInHTML chartId jsCode spec.size

-- ---------------------------------------------------------------------------
-- Whole-frame helpers
-- ---------------------------------------------------------------------------

{- | Concatenate a histogram for every numeric column in the frame. Useful
as a one-shot exploratory summary.
-}
allHistograms :: (HasCallStack) => DataFrame -> IO HtmlPlot
allHistograms df = do
    let cols = filter (isNumericColumn df) (columnNames df)
    xs <- forM cols $ \c -> histogram (mkHistogram c) df
    let allPlots = L.foldl' (\acc (HtmlPlot contents) -> acc <> "\n" <> contents) "" xs
    return (HtmlPlot allPlots)

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

plotHistogram :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotHistogram c = histogram (mkHistogram c)
{-# DEPRECATED plotHistogram "use 'histogram (mkHistogram col)' instead" #-}

plotScatter :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO HtmlPlot
plotScatter xc yc = scatter (mkScatter xc yc)
{-# DEPRECATED plotScatter "use 'scatter (mkScatter xCol yCol)' instead" #-}

plotBars :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotBars c = bar (mkBar c)
{-# DEPRECATED plotBars "use 'bar (mkBar col)' instead" #-}

plotLines :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO HtmlPlot
plotLines xc ys = line (mkLine xc ys)
{-# DEPRECATED plotLines "use 'line (mkLine xCol yCols)' instead" #-}

plotPie :: (HasCallStack) => T.Text -> Maybe T.Text -> DataFrame -> IO HtmlPlot
plotPie c mLabel =
    let s = mkPie c
     in pie s{names = mLabel}
{-# DEPRECATED plotPie "use 'pie (mkPie col)' instead" #-}

plotBoxPlots :: (HasCallStack) => [T.Text] -> DataFrame -> IO HtmlPlot
plotBoxPlots ys = box (mkBox ys)
{-# DEPRECATED plotBoxPlots "use 'box (mkBox cols)' instead" #-}

-- ---------------------------------------------------------------------------
-- Browser launcher
-- ---------------------------------------------------------------------------

showInDefaultBrowser :: HtmlPlot -> IO ()
showInDefaultBrowser (HtmlPlot p) = do
    plotId <- generateChartId
    home <- getHomeDirectory
    let path = "plot-" <> T.unpack plotId <> ".html"
        fullPath =
            if os == "mingw32"
                then home <> "\\" <> path
                else home <> "/" <> path
    putStr "Saving plot to: "
    putStrLn fullPath
    T.writeFile fullPath p
    case os of
        "mingw32" -> openFileSilently "start" fullPath
        "darwin" -> openFileSilently "open" fullPath
        _ -> openFileSilently "xdg-open" fullPath

openFileSilently :: FilePath -> FilePath -> IO ()
openFileSilently program path = do
    (_, _, _, ph) <-
        createProcess
            (proc program [path])
                { std_in = NoStream
                , std_out = NoStream
                , std_err = NoStream
                }
    void (waitForProcess ph)
