{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

{- |
A composable, expression-based grammar of graphics over the untyped 'Expr'.
Charts are built by piping combinators and rendered to interactive Vega-Lite.

@
import qualified DataFrame.Display.Web.Chart as Plt
import DataFrame.Functions (col)

Plt.showChart
  ( Plt.chart df
      |> Plt.mark Plt.Point
      |> Plt.enc Plt.X (col \@Double "age")
      |> Plt.enc Plt.Y (col \@Double "fare")
      |> Plt.enc Plt.Color (col \@Text "class")
  )
@

The element type of each encoded expression determines the Vega-Lite field
type (numeric → quantitative, date/time → temporal, otherwise nominal). A bare
column expression encodes that column; any other expression is materialised
with the core interpreter and inlined under the channel's name.

For the schema-checked typed variant see "DataFrame.Display.Web.Chart.Typed".
-}
module DataFrame.Display.Web.Chart (
    -- * Chart
    Chart,
    chart,

    -- * Re-exported spec vocabulary
    Mark (..),
    Channel (..),
    FieldType (..),
    Agg (..),

    -- * Building blocks
    mark,
    enc,
    encAs,
    aggregateOn,
    binX,
    binY,
    facet,
    row,
    column,
    layer,
    regression,
    density,
    logScale,
    includeZero,
    title,
    size,

    -- * Rendering
    toVegaSpec,
    toHtml,
    showChart,

    -- * One-shot convenience plots
    scatter,
    bar,
    histogram,
    line,
    pie,
    box,
) where

import Data.Aeson (Value)
import Data.Function ((&))
import qualified Data.Text as T

import DataFrame.Display.Internal.Common (Agg (..))
import DataFrame.Display.Internal.VegaLite (
    Channel (..),
    ChannelEnc (..),
    FieldType (..),
    Mark (..),
    ResolvedField (..),
    ScaleSpec (..),
    ScaleType (..),
    Transform (DensityT, RegressionT),
    VLSpec (..),
    chanEnc,
    channelName,
    emptySpec,
    resolveField,
    rowCountWarning,
    specHtml,
    specToValue,
 )
import DataFrame.Display.Web.Plot (showInDefaultBrowser)
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)

-- ---------------------------------------------------------------------------
-- Chart builder
-- ---------------------------------------------------------------------------

{- | An in-progress chart: the source frame, a mark, channel encodings, the
data fields to inline, transforms, and (for layered charts) sub-layers.
-}
data Chart = Chart
    { chDf :: DataFrame
    , chMark :: Mark
    , chEncs :: [ChannelEnc]
    , chFields :: [ResolvedField]
    , chTransforms :: [Transform]
    , chTitle :: Maybe T.Text
    , chW :: Int
    , chH :: Int
    , chLayers :: [Chart]
    }

-- | Start a chart from a frame. Defaults to a point mark at 600x400.
chart :: DataFrame -> Chart
chart df = Chart df Point [] [] [] Nothing 600 400 []

-- | Set the mark type.
mark :: Mark -> Chart -> Chart
mark m c = c{chMark = m}

{- | Encode an expression on a channel. The field type is inferred from the
expression's element type; use 'encAs' to override.
-}
enc :: (Columnable a) => Channel -> Expr a -> Chart -> Chart
enc ch e c =
    let rf = resolveField (chDf c) (channelName ch) e
        ce = chanEnc ch (rfName rf) (rfType rf)
     in c{chEncs = setChannel ce (chEncs c), chFields = chFields c ++ [rf]}

-- | Like 'enc' but force the Vega-Lite field type (e.g. 'Temporal', 'Ordinal').
encAs :: (Columnable a) => Channel -> Expr a -> FieldType -> Chart -> Chart
encAs ch e ft c =
    let rf = resolveField (chDf c) (channelName ch) e
        ce = chanEnc ch (rfName rf) ft
     in c{chEncs = setChannel ce (chEncs c), chFields = chFields c ++ [rf]}

{- | Apply a declarative aggregation to a channel (computed by Vega-Lite). For
'Count' the field is dropped, matching Vega-Lite's fieldless count.
-}
aggregateOn :: Channel -> Agg -> Chart -> Chart
aggregateOn ch a =
    withChannel
        ch
        ( \e ->
            e
                { ceAggregate = Just (aggVegaName a)
                , ceField = if a == Count then "" else ceField e
                }
        )

-- | Bin the X (resp. Y) channel (Vega-Lite @bin@ transform).
binX, binY :: Chart -> Chart
binX = withChannel X (\e -> e{ceBin = True})
binY = withChannel Y (\e -> e{ceBin = True})

{- | Put a channel on a log scale. Like all channel modifiers, apply it
after 'enc' on the same channel ('enc' replaces the whole encoding).
-}
logScale :: Channel -> Chart -> Chart
logScale ch = withScale ch (\s -> s{scaleType = Just LogS})

{- | Anchor (@True@) or release (@False@) a channel's scale at zero.
Vega-Lite includes zero on quantitative scales by default, which squashes
data far from the origin (e.g. geographic coordinates) against the chart
edge; @includeZero X False@ lets the axis fit the data instead. Apply it
after 'enc' on the same channel ('enc' replaces the whole encoding).
-}
includeZero :: Channel -> Bool -> Chart -> Chart
includeZero ch b = withScale ch (\s -> s{scaleZero = Just b})

-- | Facet into small multiples by a column (alias for 'column').
facet :: (Columnable a) => Expr a -> Chart -> Chart
facet = column

-- | Facet across columns / down rows.
column, row :: (Columnable a) => Expr a -> Chart -> Chart
column = enc Column
row = enc Row

-- | Set the chart title.
title :: T.Text -> Chart -> Chart
title t c = c{chTitle = Just t}

-- | Set the chart size in pixels.
size :: Int -> Int -> Chart -> Chart
size w h c = c{chW = w, chH = h}

{- | Overlay several charts that share data into a single layered chart. The
title and size of the first layer are used for the container.
-}
layer :: [Chart] -> Chart
layer [] = error "DataFrame.Display.Web.Chart.layer: empty layer list"
layer cs@(c0 : _) =
    c0
        { chLayers = map (\c -> c{chLayers = []}) cs
        , chFields = concatMap chFields cs
        }

{- | Add a regression (least-squares) line over the chart, fitting @y@ on @x@.
Produces a layered chart: the original marks plus a fitted line.
-}
regression :: (Columnable a, Columnable b) => Expr a -> Expr b -> Chart -> Chart
regression xE yE c =
    let rfx = resolveField (chDf c) "x" xE
        rfy = resolveField (chDf c) "y" yE
        withData = c{chFields = chFields c ++ [rfx, rfy]}
        points = withData{chLayers = []}
        lineLayer =
            withData
                { chMark = Line
                , chEncs =
                    [ chanEnc X (rfName rfx) Quantitative
                    , chanEnc Y (rfName rfy) Quantitative
                    ]
                , chTransforms = [RegressionT (rfName rfy) (rfName rfx)]
                , chLayers = []
                }
     in withData{chLayers = [points, lineLayer]}

{- | Kernel-density estimate of an expression, drawn as an area. Replaces the
chart's mark and encodings with the density curve (Vega-Lite @density@).
-}
density :: (Columnable a) => Expr a -> Chart -> Chart
density e c =
    let rf = resolveField (chDf c) "value" e
     in c
            { chFields = chFields c ++ [rf]
            , chMark = Area
            , chTransforms = chTransforms c ++ [DensityT (rfName rf)]
            , chEncs =
                [ chanEnc X "value" Quantitative
                , chanEnc Y "density" Quantitative
                ]
            }

-- ---------------------------------------------------------------------------
-- Rendering
-- ---------------------------------------------------------------------------

-- | The Vega-Lite spec as an aeson 'Value' (escape hatch for advanced use / hvega).
toVegaSpec :: Chart -> Value
toVegaSpec c = specToValue (allFields c) (toVLSpec c)

-- | A self-contained HTML snippet embedding the chart.
toHtml :: Chart -> String
toHtml c = T.unpack (specHtml "vis" (allFields c) (toVLSpec c))

-- | Render the chart to a temp file and open it in the default browser.
showChart :: Chart -> IO ()
showChart c = do
    rowCountWarning (allFields c)
    showInDefaultBrowser (toHtml c)

-- ---------------------------------------------------------------------------
-- One-shot convenience plots (the simple single-line path)
-- ---------------------------------------------------------------------------

-- | Scatter plot of two expressions. Axes fit the data (no zero anchor).
scatter ::
    (Columnable a, Columnable b) => Expr a -> Expr b -> DataFrame -> IO ()
scatter xE yE df =
    showChart
        ( chart df
            & mark Point
            & enc X xE
            & enc Y yE
            & includeZero X False
            & includeZero Y False
        )

-- | Count of rows per category, as bars.
bar :: (Columnable a) => Expr a -> DataFrame -> IO ()
bar xE df = showChart (chart df & mark Bar & enc X xE & aggregateOn Y Count)

-- | Histogram of a numeric expression (Vega-Lite binning + count).
histogram :: (Columnable a) => Expr a -> DataFrame -> IO ()
histogram xE df =
    showChart (chart df & mark Bar & enc X xE & binX & aggregateOn Y Count)

-- | Line chart of @y@ over @x@. Axes fit the data (no zero anchor).
line :: (Columnable a, Columnable b) => Expr a -> Expr b -> DataFrame -> IO ()
line xE yE df =
    showChart
        ( chart df
            & mark Line
            & enc X xE
            & enc Y yE
            & includeZero X False
            & includeZero Y False
        )

-- | Pie chart counting rows per category.
pie :: (Columnable a) => Expr a -> DataFrame -> IO ()
pie cE df = showChart (chart df & mark Arc & enc Color cE & aggregateOn Theta Count)

-- | Box-and-whisker plot of a numeric expression.
box :: (Columnable a) => Expr a -> DataFrame -> IO ()
box yE df = showChart (chart df & mark Boxplot & enc Y yE)

-- ---------------------------------------------------------------------------
-- Internals
-- ---------------------------------------------------------------------------

-- | Replace any existing encoding on the same channel (last write wins).
setChannel :: ChannelEnc -> [ChannelEnc] -> [ChannelEnc]
setChannel ce encs = filter ((/= ceChannel ce) . ceChannel) encs ++ [ce]

-- | Modify the encoding on a channel, creating a fieldless one if absent.
withChannel :: Channel -> (ChannelEnc -> ChannelEnc) -> Chart -> Chart
withChannel ch f c =
    let encs = chEncs c
     in if any ((== ch) . ceChannel) encs
            then c{chEncs = map (\e -> if ceChannel e == ch then f e else e) encs}
            else c{chEncs = encs ++ [f (chanEnc ch "" Quantitative)]}

-- | Modify a channel's scale spec (creating the encoding if absent).
withScale :: Channel -> (ScaleSpec -> ScaleSpec) -> Chart -> Chart
withScale ch f = withChannel ch (\e -> e{ceScale = f (ceScale e)})

aggVegaName :: Agg -> T.Text
aggVegaName a = case a of
    Count -> "count"
    Sum -> "sum"
    Mean -> "mean"
    Median -> "median"
    Min -> "min"
    Max -> "max"

allFields :: Chart -> [ResolvedField]
allFields c = chFields c ++ concatMap allFields (chLayers c)

toVLSpec :: Chart -> VLSpec
toVLSpec c
    | null (chLayers c) =
        (emptySpec (chMark c))
            { vlEncodings = chEncs c
            , vlTransforms = chTransforms c
            , vlTitle = chTitle c
            , vlWidth = chW c
            , vlHeight = chH c
            }
    | otherwise =
        (emptySpec (chMark c))
            { vlLayers = map toVLSpec (chLayers c)
            , vlTitle = chTitle c
            , vlWidth = chW c
            , vlHeight = chH c
            }
