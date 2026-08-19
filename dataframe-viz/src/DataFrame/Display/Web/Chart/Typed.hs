{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}

{- |
The schema-checked, typed-expression analogue of "DataFrame.Display.Web.Chart".
Charts are built over a 'TypedDataFrame' using typed expressions ('TExpr',
@#col@), so column references are checked against the schema at compile time
and the field type follows from the column's Haskell type.

@
{\-\# LANGUAGE OverloadedLabels \#-\}
import qualified DataFrame.Display.Web.Chart.Typed as Plt

-- one-liner
Plt.scatter #age #fare tdf

-- composable grammar
Plt.showChart
  ( Plt.chart tdf
      |> Plt.mark Plt.Boxplot
      |> Plt.enc Plt.X #region
      |> Plt.enc Plt.Y #value
      |> Plt.enc Plt.Color #grp
      |> Plt.facet #grp
  )
@

Every combinator delegates to "DataFrame.Display.Web.Chart" by unwrapping the
typed expression ('unTExpr') and frame ('unTDF'); the @cols@ phantom is erased
at the boundary.
-}
module DataFrame.Display.Web.Chart.Typed (
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
import Data.Kind (Type)
import qualified Data.Text as T

import DataFrame.Display.Internal.Common (Agg (..))
import DataFrame.Display.Internal.VegaLite (
    Channel (..),
    FieldType (..),
    Mark (..),
 )
import qualified DataFrame.Display.Web.Chart as C
import DataFrame.Internal.Column (Columnable)
import DataFrame.Typed.Types (TExpr (..), TypedDataFrame (..))
import GHC.TypeLits (Symbol)

-- | A typed chart: a phantom-@cols@ wrapper over the untyped builder.
newtype Chart (cols :: [(Symbol, Type)]) = Chart C.Chart

-- | Start a typed chart from a typed frame.
chart :: TypedDataFrame cols -> Chart cols
chart tdf = Chart (C.chart (unTDF tdf))

-- | Set the mark type.
mark :: Mark -> Chart cols -> Chart cols
mark m (Chart c) = Chart (C.mark m c)

-- | Encode a typed expression on a channel (field type inferred from its type).
enc :: (Columnable a) => Channel -> TExpr cols a -> Chart cols -> Chart cols
enc ch te (Chart c) = Chart (C.enc ch (unTExpr te) c)

-- | Like 'enc' but force the Vega-Lite field type.
encAs ::
    (Columnable a) =>
    Channel -> TExpr cols a -> FieldType -> Chart cols -> Chart cols
encAs ch te ft (Chart c) = Chart (C.encAs ch (unTExpr te) ft c)

-- | Apply a declarative aggregation to a channel.
aggregateOn :: Channel -> Agg -> Chart cols -> Chart cols
aggregateOn ch a (Chart c) = Chart (C.aggregateOn ch a c)

-- | Bin the X (resp. Y) channel.
binX, binY :: Chart cols -> Chart cols
binX (Chart c) = Chart (C.binX c)
binY (Chart c) = Chart (C.binY c)

-- | Put a channel on a log scale.
logScale :: Channel -> Chart cols -> Chart cols
logScale ch (Chart c) = Chart (C.logScale ch c)

-- | Anchor (@True@) or release (@False@) a channel's scale at zero.
includeZero :: Channel -> Bool -> Chart cols -> Chart cols
includeZero ch b (Chart c) = Chart (C.includeZero ch b c)

-- | Facet into small multiples by a column (alias for 'column').
facet :: (Columnable a) => TExpr cols a -> Chart cols -> Chart cols
facet = column

-- | Facet across columns / down rows.
column, row :: (Columnable a) => TExpr cols a -> Chart cols -> Chart cols
column te (Chart c) = Chart (C.column (unTExpr te) c)
row te (Chart c) = Chart (C.row (unTExpr te) c)

-- | Set the chart title.
title :: T.Text -> Chart cols -> Chart cols
title t (Chart c) = Chart (C.title t c)

-- | Set the chart size in pixels.
size :: Int -> Int -> Chart cols -> Chart cols
size w h (Chart c) = Chart (C.size w h c)

-- | Overlay several charts sharing data into a single layered chart.
layer :: [Chart cols] -> Chart cols
layer cs = Chart (C.layer [c | Chart c <- cs])

-- | Add a least-squares regression line fitting @y@ on @x@.
regression ::
    (Columnable a, Columnable b) =>
    TExpr cols a -> TExpr cols b -> Chart cols -> Chart cols
regression xE yE (Chart c) = Chart (C.regression (unTExpr xE) (unTExpr yE) c)

-- | Kernel-density estimate of an expression, drawn as an area.
density :: (Columnable a) => TExpr cols a -> Chart cols -> Chart cols
density e (Chart c) = Chart (C.density (unTExpr e) c)

-- | The Vega-Lite spec as an aeson 'Value' (escape hatch for advanced use / hvega).
toVegaSpec :: Chart cols -> Value
toVegaSpec (Chart c) = C.toVegaSpec c

-- | A self-contained HTML snippet embedding the chart.
toHtml :: Chart cols -> String
toHtml (Chart c) = C.toHtml c

-- | Render the chart to a temp file and open it in the default browser.
showChart :: Chart cols -> IO ()
showChart (Chart c) = C.showChart c

-- | Scatter plot of two typed expressions.
scatter ::
    (Columnable a, Columnable b) =>
    TExpr cols a -> TExpr cols b -> TypedDataFrame cols -> IO ()
scatter xE yE tdf = C.scatter (unTExpr xE) (unTExpr yE) (unTDF tdf)

-- | Count of rows per category, as bars.
bar :: (Columnable a) => TExpr cols a -> TypedDataFrame cols -> IO ()
bar xE tdf = C.bar (unTExpr xE) (unTDF tdf)

-- | Histogram of a numeric expression.
histogram :: (Columnable a) => TExpr cols a -> TypedDataFrame cols -> IO ()
histogram xE tdf = C.histogram (unTExpr xE) (unTDF tdf)

-- | Line chart of @y@ over @x@.
line ::
    (Columnable a, Columnable b) =>
    TExpr cols a -> TExpr cols b -> TypedDataFrame cols -> IO ()
line xE yE tdf = C.line (unTExpr xE) (unTExpr yE) (unTDF tdf)

-- | Pie chart counting rows per category.
pie :: (Columnable a) => TExpr cols a -> TypedDataFrame cols -> IO ()
pie cE tdf = C.pie (unTExpr cE) (unTDF tdf)

-- | Box-and-whisker plot of a numeric expression.
box :: (Columnable a) => TExpr cols a -> TypedDataFrame cols -> IO ()
box yE tdf = C.box (unTExpr yE) (unTDF tdf)
