{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- |
Internal Vega-Lite spec model plus aeson encoding, shared by the web plot
backends. Not part of the public API.

The encoding medium is the untyped 'Expr'. A channel encoding resolves to a
'ResolvedField' carrying the field name, the Vega-Lite field type (derived from
the expression's Haskell element type) and the column values to inline.
-}
module DataFrame.Display.Internal.VegaLite (
    -- * Spec model
    Mark (..),
    Channel (..),
    FieldType (..),
    ChannelEnc (..),
    Transform (..),
    VLSpec (..),
    emptySpec,
    chanEnc,
    channelName,

    -- * Resolving expressions to fields
    ResolvedField (..),
    fieldTypeOf,
    resolveField,
    textField,
    numField,

    -- * Encoding to JSON / HTML
    specToValue,
    inlineRows,
    specHtml,
    rowCountWarning,
) where

import qualified Control.Monad
import Data.Aeson (ToJSON (toJSON), Value (Null), object, (.=))
import qualified Data.Aeson.Key as K
import Data.Aeson.Text (encodeToLazyText)
import qualified Data.List as L
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Vector as V
import System.IO (hPutStrLn, stderr)
import Type.Reflection (TypeRep, tyConName, typeRep, typeRepTyCon, pattern App)

import DataFrame.Internal.Column (Column, Columnable, unwrapTypedColumn)
import DataFrame.Internal.DataFrame (DataFrame, getColumn)
import DataFrame.Internal.Expression (Expr (Col))
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.Display.Internal.Common (columnToDoubles, columnToStrings)

-- ---------------------------------------------------------------------------
-- Spec model
-- ---------------------------------------------------------------------------

data Mark = Bar | Line | Point | Area | Boxplot | Arc | Rule | Tick
    deriving (Eq, Show)

markName :: Mark -> T.Text
markName m = case m of
    Bar -> "bar"
    Line -> "line"
    Point -> "point"
    Area -> "area"
    Boxplot -> "boxplot"
    Arc -> "arc"
    Rule -> "rule"
    Tick -> "tick"

data Channel
    = X
    | Y
    | Color
    | Size
    | Shape
    | Column
    | Row
    | Opacity
    | Theta
    | Tooltip
    | Order
    deriving (Eq, Show)

channelName :: Channel -> T.Text
channelName c = case c of
    X -> "x"
    Y -> "y"
    Color -> "color"
    Size -> "size"
    Shape -> "shape"
    Column -> "column"
    Row -> "row"
    Opacity -> "opacity"
    Theta -> "theta"
    Tooltip -> "tooltip"
    Order -> "order"

data FieldType = Quantitative | Nominal | Ordinal | Temporal
    deriving (Eq, Show)

fieldTypeName :: FieldType -> T.Text
fieldTypeName t = case t of
    Quantitative -> "quantitative"
    Nominal -> "nominal"
    Ordinal -> "ordinal"
    Temporal -> "temporal"

{- | A single channel encoding. An empty 'ceField' with a 'ceAggregate' of
@count@ produces a fieldless count aggregation, as Vega-Lite expects.
-}
data ChannelEnc = ChannelEnc
    { ceChannel :: Channel
    , ceField :: T.Text
    , ceType :: FieldType
    , ceAggregate :: Maybe T.Text
    , ceBin :: Bool
    , ceLogScale :: Bool
    }

-- | A bare channel encoding with no aggregation, binning, or log scale.
chanEnc :: Channel -> T.Text -> FieldType -> ChannelEnc
chanEnc ch fld ft = ChannelEnc ch fld ft Nothing False False

data Transform
    = -- | Fit @regression(yField) on xField@ (used inside a layer).
      RegressionT T.Text T.Text
    | -- | Kernel-density estimate of a field.
      DensityT T.Text
    deriving (Eq, Show)

{- | A Vega-Lite spec. When 'vlLayers' is non-empty the spec is a layered
container: the top level carries the shared data and each layer carries its own
mark/encoding/transform.
-}
data VLSpec = VLSpec
    { vlMark :: Mark
    , vlEncodings :: [ChannelEnc]
    , vlTransforms :: [Transform]
    , vlTitle :: Maybe T.Text
    , vlWidth :: Int
    , vlHeight :: Int
    , vlLayers :: [VLSpec]
    }

emptySpec :: Mark -> VLSpec
emptySpec m = VLSpec m [] [] Nothing 600 400 []

-- ---------------------------------------------------------------------------
-- Field-type inference from the expression's element type
-- ---------------------------------------------------------------------------

{- | Derive the Vega-Lite field type from a Haskell type: numeric →
'Quantitative', date/time → 'Temporal', everything else → 'Nominal'. @Maybe a@
classifies as its inner type.
-}
fieldTypeOf :: forall a -> (Columnable a) => FieldType
fieldTypeOf a = classify (typeRep @a)

classify :: forall k (x :: k). TypeRep x -> FieldType
classify tr
    | nm `elem` quantNames = Quantitative
    | nm `elem` temporalNames = Temporal
    | nm == "Maybe" = case tr of
        App _ arg -> classify arg
        _ -> Nominal
    | otherwise = Nominal
  where
    nm = tyConName (typeRepTyCon tr)
    quantNames =
        [ "Int"
        , "Int8"
        , "Int16"
        , "Int32"
        , "Int64"
        , "Word"
        , "Word8"
        , "Word16"
        , "Word32"
        , "Word64"
        , "Integer"
        , "Natural"
        , "Double"
        , "Float"
        , "Scientific"
        ]
    temporalNames =
        ["Day", "UTCTime", "LocalTime", "ZonedTime", "TimeOfDay"]

-- ---------------------------------------------------------------------------
-- Resolving an expression to a field + values
-- ---------------------------------------------------------------------------

data ResolvedField = ResolvedField
    { rfName :: T.Text
    , rfType :: FieldType
    , rfValues :: [Value]
    }

{- | Resolve an expression against a frame. A bare @Col name@ reuses the named
column; any other expression is materialised with the core interpreter and
stored under the given fallback name.
-}
resolveField ::
    forall a. (Columnable a) => DataFrame -> T.Text -> Expr a -> ResolvedField
resolveField df fallbackName expr =
    let ft = fieldTypeOf a
        (name, col) = case expr of
            Col cname -> (cname, lookupCol cname)
            _ -> (fallbackName, materialiseExpr df expr)
     in ResolvedField name ft (columnToValues ft col)
  where
    lookupCol cname = case getColumn cname df of
        Just c -> c
        Nothing ->
            error $ "DataFrame.Display.Web: column not found: " <> T.unpack cname

materialiseExpr :: (Columnable a) => DataFrame -> Expr a -> Column
materialiseExpr df expr = case interpret df expr of
    Right tc -> unwrapTypedColumn tc
    Left err ->
        error $ "DataFrame.Display.Web: could not evaluate expression: " <> show err

columnToValues :: FieldType -> Column -> [Value]
columnToValues Quantitative col = map toJSON (columnToDoubles col)
columnToValues _ col = map (toJSON :: T.Text -> Value) (columnToStrings col)

-- | A nominal field built directly from text values (for pre-computed data).
textField :: T.Text -> [T.Text] -> ResolvedField
textField name vals = ResolvedField name Nominal (map (toJSON :: T.Text -> Value) vals)

-- | A quantitative field built directly from numeric values (for pre-computed data).
numField :: T.Text -> [Double] -> ResolvedField
numField name vals = ResolvedField name Quantitative (map toJSON vals)

-- ---------------------------------------------------------------------------
-- Encoding to JSON
-- ---------------------------------------------------------------------------

schemaUrl :: T.Text
schemaUrl = "https://vega.github.io/schema/vega-lite/v5.json"

specToValue :: [ResolvedField] -> VLSpec -> Value
specToValue fields spec
    | null (vlLayers spec) =
        object $
            commonPairs ++ unitPairs spec
    | otherwise =
        object $
            commonPairs ++ [("layer", toJSON (map (object . unitPairs) (vlLayers spec)))]
  where
    commonPairs =
        catMaybes
            [ Just ("$schema" .= schemaUrl)
            , fmap ("title" .=) (vlTitle spec)
            , Just ("width" .= vlWidth spec)
            , Just ("height" .= vlHeight spec)
            , Just ("data" .= object ["values" .= inlineRows fields])
            ]

-- | The mark/encoding/transform pairs of a unit spec (no data — that is shared).
unitPairs :: VLSpec -> [(K.Key, Value)]
unitPairs spec =
    catMaybes
        [ if null (vlTransforms spec)
            then Nothing
            else Just ("transform" .= map transformValue (vlTransforms spec))
        , Just ("mark" .= markValue (vlMark spec))
        , Just ("encoding" .= encodingValue (vlEncodings spec))
        ]

markValue :: Mark -> Value
markValue m = object ["type" .= markName m, "tooltip" .= True]

encodingValue :: [ChannelEnc] -> Value
encodingValue encs =
    object [K.fromText (channelName (ceChannel e)) .= channelValue e | e <- encs]

channelValue :: ChannelEnc -> Value
channelValue e =
    object $
        catMaybes
            [ if T.null (ceField e) then Nothing else Just ("field" .= ceField e)
            , Just ("type" .= fieldTypeName (ceType e))
            , fmap ("aggregate" .=) (ceAggregate e)
            , if ceBin e then Just ("bin" .= True) else Nothing
            , if ceLogScale e
                then Just ("scale" .= object ["type" .= ("log" :: T.Text)])
                else Nothing
            ]

transformValue :: Transform -> Value
transformValue (RegressionT yField xField) =
    object ["regression" .= yField, "on" .= xField]
transformValue (DensityT field) =
    object ["density" .= field]

-- | Build the @data.values@ array of row objects, deduplicating fields by name.
inlineRows :: [ResolvedField] -> Value
inlineRows fields =
    let uniq = L.nubBy (\a b -> rfName a == rfName b) fields
        vecs = [(rfName f, V.fromList (rfValues f)) | f <- uniq]
        n = maximum (0 : map (V.length . snd) vecs)
        row i = object [K.fromText nm .= fromMaybe Null (vs V.!? i) | (nm, vs) <- vecs]
     in toJSON [row i | i <- [0 .. n - 1]]

-- ---------------------------------------------------------------------------
-- HTML embedding (vega-embed via CDN)
-- ---------------------------------------------------------------------------

{- | Render a spec to a self-contained HTML snippet that loads vega/vega-lite/
vega-embed from a CDN and embeds the chart. Data is inlined, so the snippet
renders correctly even from a @file://@ URL.
-}
specHtml :: T.Text -> [ResolvedField] -> VLSpec -> T.Text
specHtml chartId fields spec =
    let specJson = TL.toStrict (encodeToLazyText (specToValue fields spec))
     in T.concat
            [ "<div id=\""
            , chartId
            , "\"></div>\n"
            , "<script src=\"https://cdn.jsdelivr.net/npm/vega@5\"></script>\n"
            , "<script src=\"https://cdn.jsdelivr.net/npm/vega-lite@5\"></script>\n"
            , "<script src=\"https://cdn.jsdelivr.net/npm/vega-embed@6\"></script>\n"
            , "<script>vegaEmbed('#"
            , chartId
            , "', "
            , specJson
            , ");</script>\n"
            ]

{- | Warn on stderr when a large number of rows is being inlined, which bloats
the spec and can slow the browser.
-}
rowCountWarning :: [ResolvedField] -> IO ()
rowCountWarning fields = do
    let n = maximum (0 : map (length . rfValues) fields)
    Control.Monad.when (n > 5000) $
        hPutStrLn stderr $
            "DataFrame.Display.Web: inlining "
                ++ show n
                ++ " rows into the plot spec; consider filtering or aggregating first."
