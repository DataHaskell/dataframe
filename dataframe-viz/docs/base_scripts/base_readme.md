<!--
  This file is the runnable scripths source for dataframe-viz's README.
  Every ```haskell block executes in order in one shared session against a
  small in-memory DataFrame. Regenerate the rendered README with:

      scripths docs/base_scripts/base_readme.md -o README.md

  Run it from THIS directory (dataframe-viz/). The `-- cabal: packages:`
  directive in the first block points at the sibling packages in this monorepo
  so the notebook builds against the local working tree (not Hackage). Blocks
  tagged ```text are illustrative (browser-opening or terminal output) and are
  NOT run.
-->

# dataframe-viz

Plotting for the [`dataframe`](https://hackage.haskell.org/package/dataframe) ecosystem. Two
backends share one API shape:

- **Terminal** (`DataFrame.Display.Terminal.Plot`) draws straight to the console (built on
  [`granite`](https://hackage.haskell.org/package/granite)).
- **Web** emits an interactive **Vega-Lite v5** spec rendered in the browser via `vega-embed` —
  a composable grammar of graphics (facet, layer, regression, density, colour/size encodings)
  driven by **expressions**, untyped or typed.

> **This README is a runnable [scripths](https://github.com/DataHaskell/scripths) notebook.**
> Every Haskell block runs top-to-bottom in one shared session. Reproduce every output below with
> `scripths docs/base_scripts/base_readme.md -o README.md` run from `dataframe-viz/`.

## Setup

Charts emit a Vega-Lite spec; in the REPL `showInDefaultBrowser` / `showChart` write it to a temp
file and open it. To keep the output here small we print the spec *without* its inlined data via a
tiny `grammar` helper, against this in-memory frame. The `packages:` directive builds against the
local `dataframe-core` / `dataframe-operations` / `dataframe-viz` working trees:

```haskell
-- cabal: build-depends: text, aeson
-- cabal: packages: ../../../dataframe-core, ../../../dataframe-parsing
-- cabal: packages: ../../../dataframe-operations, ../../../dataframe-viz
-- cabal: default-extensions: OverloadedStrings, TypeApplications, OverloadedLabels
-- cabal: default-extensions: DataKinds, TypeOperators, FlexibleContexts
import DataFrame.Internal.DataFrame (DataFrame, fromNamedColumns)
import DataFrame.Internal.Column (fromList)
import DataFrame.Operators ((|>))
import qualified DataFrame.Functions as F
import DataFrame.Typed.Types (Column, TypedDataFrame)
import DataFrame.Typed.Freeze (freeze)
import qualified DataFrame.Typed.Expr as TE
import Data.Text (Text)

import qualified DataFrame.Display.Web.Plot as Plot
import qualified DataFrame.Display.Web.Chart as Chart
import qualified DataFrame.Display.Web.Chart.Typed as TPlot

import Data.Aeson (Value (Object))
import Data.Aeson.Text (encodeToLazyText)
import qualified Data.Aeson.KeyMap as KM
import qualified Data.Text.Lazy as TL

df = fromNamedColumns
    [ ("income", fromList [1.5, 2.0, 3.1, 4.2, 5.0, 2.2, 3.3, 1.1 :: Double])
    , ("value",  fromList [100, 150, 200, 250, 300, 180, 220, 90 :: Double])
    , ("region", fromList (["INLAND","NEAR BAY","INLAND","NEAR OCEAN","ISLAND","INLAND","NEAR BAY","INLAND"] :: [Text]))
    ]

income = F.col Double "income"
value  = F.col Double "value"
region = F.col Text   "region"

-- show a Vega-Lite spec without its (verbose) inlined data.
-- Returns String so scripths prints it raw rather than show-escaped.
grammar v = case v of
    Object o -> TL.unpack (encodeToLazyText (Object (KM.delete "data" o)))
    _        -> TL.unpack (encodeToLazyText v)
```

## Terminal plots

Terminal plots render to stdout, so they're shown here rather than run:

```text
import qualified DataFrame.Display.Terminal.Plot as T
T.scatter (T.mkScatter "income" "value") df
T.histogram (T.mkHistogram "income") df
```

## Web plots

Three tiers, all compiling to the same Vega-Lite spec:

| Module | Keyed by | Use |
|--------|----------|-----|
| `DataFrame.Display.Web.Plot` | string column names | quick one-liners; returns an HTML `String` |
| `DataFrame.Display.Web.Chart` | untyped `Expr` | composable grammar |
| `DataFrame.Display.Web.Chart.Typed` | typed `TExpr` / `TypedDataFrame` | same grammar, `#column` checked against the schema at compile time |

Vocabulary (re-exported from all three):

- **Marks** — `Bar Line Point Area Boxplot Arc Rule Tick`.
- **Channels** — `X Y Color Size Shape Opacity Theta Column Row Tooltip Order`.
- **Field types** — `Quantitative Nominal Ordinal Temporal`, inferred from the expression's element type:

| Haskell type | field type |
|--------------|-----------|
| `Int`, `Double`, `Float`, `Word`, … | `Quantitative` |
| `Text`, `String`, `Bool`, `Char` | `Nominal` |
| `Day`, `UTCTime`, `LocalTime`, … | `Temporal` |
| `Maybe a` | as `a` |

Override the inferred type with `encAs`. Aggregations (`aggregateOn`): `Count Sum Mean Median Min Max`.

In the REPL or a notebook you render straight to the browser (these aren't run here):

```text
Plot.scatter (Plot.mkScatter "income" "value") df >>= Plot.showInDefaultBrowser
Chart.showChart (Chart.chart df |> Chart.mark Chart.Point
                                |> Chart.enc Chart.X income
                                |> Chart.enc Chart.Y value)
```

### Untyped grammar (`Expr`)

Build a chart by piping combinators onto `chart df`. A scatter with a categorical colour encoding —
`income :: Expr Double` becomes `quantitative`, `region :: Expr Text` becomes `nominal`:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Point
        |> Chart.enc Chart.X income
        |> Chart.enc Chart.Y value
        |> Chart.enc Chart.Color region))
```

Map more columns onto `Size` / `Opacity` / `Shape` / `Tooltip`:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Point
        |> Chart.enc Chart.X income
        |> Chart.enc Chart.Y value
        |> Chart.enc Chart.Size value
        |> Chart.enc Chart.Opacity income))
```

`aggregateOn` applies a Vega-Lite aggregate to a channel. Sum `value` by `region`, coloured by region:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Bar
        |> Chart.enc Chart.X region
        |> Chart.enc Chart.Y value
        |> Chart.aggregateOn Chart.Y Chart.Sum
        |> Chart.enc Chart.Color region))
```

A histogram is a binned `X` with a counted `Y` — binning and counting are Vega-Lite transforms:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Bar
        |> Chart.enc Chart.X income
        |> Chart.binX
        |> Chart.aggregateOn Chart.Y Chart.Count))
```

A line:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Line
        |> Chart.enc Chart.X income
        |> Chart.enc Chart.Y value))
```

`encAs` forces a field type; `logScale` puts a channel on a log scale:

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Point
        |> Chart.encAs Chart.X income Chart.Ordinal
        |> Chart.enc Chart.Y value
        |> Chart.logScale Chart.Y))
```

The medium is expressions, not just column names. A non-column expression is evaluated and inlined
under the channel's name (here `y`):

```haskell
grammar (Chart.toVegaSpec
    (Chart.chart df
        |> Chart.mark Chart.Point
        |> Chart.enc Chart.X income
        |> Chart.enc Chart.Y (value + income)))
```

`regression` overlays a least-squares line (a second layer) and `facet` splits into small multiples:

```haskell
grammar (Chart.toVegaSpec
    (Chart.regression income value
        (Chart.chart df
            |> Chart.mark Chart.Point
            |> Chart.enc Chart.X income
            |> Chart.enc Chart.Y value
            |> Chart.facet region)))
```

`density` draws a kernel-density estimate as an area:

```haskell
grammar (Chart.toVegaSpec
    (Chart.density income (Chart.chart df |> Chart.mark Chart.Area)))
```

`layer` overlays charts that share data:

```haskell
grammar (Chart.toVegaSpec
    (Chart.layer
        [ Chart.chart df |> Chart.mark Chart.Point |> Chart.enc Chart.X income |> Chart.enc Chart.Y value
        , Chart.chart df |> Chart.mark Chart.Line  |> Chart.enc Chart.X income |> Chart.enc Chart.Y value
        ]))
```

`title` and `size` set the chart title and pixel dimensions.

### Typed grammar (`TExpr`)

`DataFrame.Display.Web.Chart.Typed` mirrors every combinator above, over a `TypedDataFrame`, so
`#region` / `#value` are checked against the schema at compile time — a typo or wrong type is a
compile error, and the field type still follows from the column's Haskell type. `box` draws a real
box-and-whisker (quartiles, 1.5×IQR whiskers, outliers), not a bar of medians:

```haskell
type Cols = '[ Column "income" Double, Column "value" Double, Column "region" Text ]

case freeze @Cols df of
    Nothing  -> "schema mismatch"
    Just tdf -> grammar (TPlot.toVegaSpec
        (TPlot.chart tdf
            |> TPlot.mark TPlot.Boxplot
            |> TPlot.enc TPlot.X #region
            |> TPlot.enc TPlot.Y #value))
```

A typed one-liner mirrors the string tier, but the labels must exist in the schema (not run here):

```text
TPlot.scatter #income #value tdf
```

### Rendering

Every tier produces the same outputs:

- `toVegaSpec :: Chart -> Value` — the Vega-Lite spec as an aeson `Value`. Escape hatch for advanced
  use, or hand-off to [`hvega`](https://hackage.haskell.org/package/hvega), which speaks the same spec.
- `toHtml :: Chart -> String` — a self-contained HTML snippet (CDN `vega-embed`, data inlined, so it
  renders from a `file://` URL).
- `showChart :: Chart -> IO ()` — write the HTML to a temp file and open the browser.
- `showInDefaultBrowser :: String -> IO ()` — open an HTML `String` (the string tier returns these).

Frames over ~5,000 rows print a stderr warning, since the data is inlined into the spec.

### String tier (one-shots)

`DataFrame.Display.Web.Plot` is the quick path; each call returns an HTML `String` (not run here):

```text
Plot.bar       (Plot.mkBar "region")              df   -- count rows per region
Plot.histogram (Plot.mkHistogram "income")        df
Plot.scatter   (Plot.mkScatter "income" "value")  df
Plot.line      (Plot.mkLine "income" ["value"])   df
Plot.pie       (Plot.mkPie "region")              df
Plot.box       (Plot.mkBox ["income", "value"])   df
```

Override defaults with record syntax on the spec: `Bar` has `y`, `agg`, `topN`, `title`, `size`;
`Histogram` has `bins`; `Scatter` has `color`; `Pie` has `names`, `agg`, `topN`; `Box` / `Line` take
a list of columns. E.g. `bar (mkBar "region") { y = Just "value", agg = Sum, topN = Just 5 } df`.

## Install

```
build-depends: dataframe-viz
```

The plotting modules are also re-exported from the umbrella `dataframe` package
(`DataFrame.Display.Web.Plot`, `DataFrame.Display.Web.Chart`, `DataFrame.Display.Web.Chart.Typed`).
