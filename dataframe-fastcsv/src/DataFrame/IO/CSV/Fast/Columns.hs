{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

{- | Column-level dispatch for the fast CSV reader: resolve each column's
target type up front into a 'ColumnPlan' (schema hit -> typed pass directly;
miss -> sample classification + typed fallback chain; EitherRead \/ exotic
schema types -> the legacy Text pipeline). Plans are chunk-runnable so the
parallel reader (WS-E2) can execute them over row sub-ranges.
-}
module DataFrame.IO.CSV.Fast.Columns (
    ColumnEnv (..),
    ColumnPlan (..),
    Step,
    TargetType,
    ChunkOutcome (..),
    buildColumn,
    planColumn,
    runChainChunk,
    runSchemaChunk,
    runStep,
    legacyColumn,
    allNullColumn,
    finishMode,
    schemaError,
) where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Vector as V

import Control.Exception (ErrorCall (..), throwIO)
import Data.Maybe (isJust)
import Data.Time (Day)
import Data.Type.Equality (testEquality)
import Type.Reflection (typeRep)

import DataFrame.IO.CSV (
    ReadOptions (..),
    schemaTypeMap,
    shouldInferFromSample,
    typeInferenceSampleSize,
 )
import DataFrame.IO.CSV.Fast.Passes
import DataFrame.IO.CSV.Fast.Slice (extractField)
import DataFrame.Internal.Column (Column, ensureOptional, fromVector)
import DataFrame.Internal.Schema (SchemaType (..))
import DataFrame.Operations.Typing (
    ParseOptions (..),
    ParsingAssumption (..),
    SafeReadMode (..),
    effectiveSafeRead,
    isNullishOrMissing,
    makeParsingAssumption,
    parseFromExamples,
 )

-- | One step of a type-fallback chain (always ends in 'StepText').
data Step = StepBool | StepInt | StepDouble | StepDate | StepText

-- | Run one chain step over the env's row range as a single typed pass.
runStep :: ColumnEnv -> NullSpec -> Int -> Bool -> Step -> IO PassResult
runStep env nspec col nullOnFail step = case step of
    StepBool -> boolPass env nspec col nullOnFail
    StepInt -> intPass env nspec col nullOnFail
    StepDouble -> doublePass env nspec col nullOnFail
    StepDate -> datePass env nspec col nullOnFail
    StepText -> Right . PassText <$> textPass env nspec col

-- | The documented fallback chain for each inference assumption.
chainFor :: ParsingAssumption -> [Step]
chainFor BoolAssumption = [StepBool, StepText]
chainFor IntAssumption = [StepInt, StepDouble, StepText]
chainFor DoubleAssumption = [StepDouble, StepText]
chainFor DateAssumption = [StepDate, StepText]
chainFor TextAssumption = [StepText]
chainFor NoAssumption = [StepBool, StepInt, StepDouble, StepDate, StepText]

-- | Schema target types served by the direct typed passes.
data TargetType = TInt | TDouble | TText | TBool | TDate

targetName :: TargetType -> String
targetName TInt = "Int"
targetName TDouble = "Double"
targetName TText = "Text"
targetName TBool = "Bool"
targetName TDate = "Day"

stepForTarget :: TargetType -> Step
stepForTarget TInt = StepInt
stepForTarget TDouble = StepDouble
stepForTarget TText = StepText
stepForTarget TBool = StepBool
stepForTarget TDate = StepDate

supportedType :: SchemaType -> Maybe TargetType
supportedType (SType (_ :: P.Proxy a))
    | isJust (testEquality (typeRep @a) (typeRep @Int)) = Just TInt
    | isJust (testEquality (typeRep @a) (typeRep @Double)) = Just TDouble
    | isJust (testEquality (typeRep @a) (typeRep @T.Text)) = Just TText
    | isJust (testEquality (typeRep @a) (typeRep @Bool)) = Just TBool
    | isJust (testEquality (typeRep @a) (typeRep @Day)) = Just TDate
    | otherwise = Nothing

{- | How to build one column, resolved once per read (before any chunk
fan-out). 'PlanLegacy' carries the \"re-apply 'parseWithTypes'\" flag.
-}
data ColumnPlan
    = {- | Inference: fallback chain; the 'Bool' asks for the all-null
      pre-check ('NoAssumption' only).
      -}
      PlanChain !SafeReadMode !NullSpec ![Step] !Bool
    | PlanSchema !SafeReadMode !NullSpec !TargetType
    | PlanLegacy !SafeReadMode !Bool

{- | Resolve a column's plan. For inferred columns the sample is always
classified against the global (full-range) env, so chunked and sequential
reads share one assumption.
-}
planColumn :: ColumnEnv -> T.Text -> Int -> ColumnPlan
planColumn env name col = case mode of
    EitherRead -> PlanLegacy mode (isJust schemaEntry)
    _ -> case schemaEntry of
        Nothing -> chainPlan env mode col
        Just st -> case supportedType st of
            Just t -> PlanSchema mode (nullSpecFor opts mode) t
            Nothing -> PlanLegacy mode True
  where
    opts = ceOpts env
    mode = effectiveSafeRead (safeRead opts) (safeReadOverrides opts) name
    schemaEntry = M.lookup name (schemaTypeMap (typeSpec opts))

-- | Classify a row sample with the existing assumption logic.
chainPlan :: ColumnEnv -> SafeReadMode -> Int -> ColumnPlan
chainPlan env mode col =
    PlanChain mode (nullSpecFor opts mode) (chainFor assumption) noAssumption
  where
    opts = ceOpts env
    ts = typeSpec opts
    sampleN =
        if shouldInferFromSample ts then typeInferenceSampleSize ts else 0
    isNullT = case mode of
        NoSafeRead -> T.null
        _ -> isNullishOrMissing (missingIndicators opts)
    sample =
        V.generate
            (min sampleN (ceNumRow env))
            (\i -> extractField (ceCtx env) (rowAt env i) col)
    classified =
        V.map (\t -> if isNullT t then Nothing else Just t) sample
    assumption = makeParsingAssumption (dateFormat opts) classified
    noAssumption = case assumption of
        NoAssumption -> True
        _ -> False

{- | What a chain plan produced for one row range: every cell null (only
reported when the all-null pre-check is on), or the index of the first
chain step whose pass succeeded plus its column.
-}
data ChunkOutcome = AllNullChunk | Resolved !Int !PassCol

-- | Run a fallback chain over the env's row range.
runChainChunk ::
    ColumnEnv -> NullSpec -> [Step] -> Bool -> Int -> IO ChunkOutcome
runChainChunk env nspec steps checkAllNull col = do
    allNull <- if checkAllNull then allNullPass env nspec col else pure False
    if allNull then pure AllNullChunk else go 0 steps
  where
    go i (s : rest) = do
        r <- runStep env nspec col False s
        case r of
            Right c -> pure (Resolved i c)
            Left _ -> go (i + 1) rest
    go _ [] = error "fastcsv: type-fallback chain exhausted (StepText must succeed)"

{- | Run a schema-typed pass over the env's row range. @rowOff@ maps a
chunk-local failure index to the global data-row index.
-}
runSchemaChunk ::
    ColumnEnv ->
    NullSpec ->
    SafeReadMode ->
    TargetType ->
    Int ->
    Int ->
    IO (Either Int PassCol)
runSchemaChunk env nspec mode t col rowOff = do
    r <- runStep env nspec col (mode == MaybeRead) (stepForTarget t)
    pure (either (Left . (+ rowOff)) Right r)

-- | The error a schema column raises when a cell fails under 'NoSafeRead'.
schemaError :: T.Text -> TargetType -> Int -> ErrorCall
schemaError name t i =
    ErrorCall $
        "fastcsv: schema column "
            <> show name
            <> ": row "
            <> show i
            <> " does not parse as "
            <> targetName t

-- | The all-null column an all-null range resolves to (Maybe Text).
allNullColumn :: Int -> Column
allNullColumn n = fromVector (V.replicate n (Nothing :: Maybe T.Text))

-- | 'MaybeRead' columns keep their Maybe-ness even when fully valid.
finishMode :: SafeReadMode -> Column -> Column
finishMode MaybeRead = ensureOptional
finishMode _ = id

{- | Build one column sequentially (single chunk over the full row range).
Returns the column plus a flag telling the caller to re-apply the legacy
schema conversion ('parseWithTypes') afterwards — only set for schema'd
columns the typed passes cannot serve.
-}
buildColumn :: ColumnEnv -> T.Text -> Int -> IO (Column, Bool)
buildColumn env name col = case planColumn env name col of
    PlanLegacy mode pwt -> (,pwt) <$> legacyColumn env mode col
    PlanSchema mode nspec t -> do
        r <- runSchemaChunk env nspec mode t col 0
        c <- either (throwIO . schemaError name t) (pure . passColumn) r
        pure (finishMode mode c, False)
    PlanChain mode nspec steps checkAllNull -> do
        oc <- runChainChunk env nspec steps checkAllNull col
        let c = case oc of
                AllNullChunk -> allNullColumn (ceNumRow env)
                Resolved _ c' -> passColumn c'
        pure (finishMode mode c, False)

{- | The original Text-materializing pipeline, kept for 'EitherRead' and
exotic schema types (cold paths).
-}
legacyColumn :: ColumnEnv -> SafeReadMode -> Int -> IO Column
legacyColumn env mode col = do
    let opts = ceOpts env
        ts = typeSpec opts
        parseOpts =
            ParseOptions
                { missingValues = missingIndicators opts
                , sampleSize =
                    if shouldInferFromSample ts
                        then typeInferenceSampleSize ts
                        else 0
                , parseSafe = mode
                , parseSafeOverrides = []
                , parseDateFormat = dateFormat opts
                }
        cells =
            V.generate
                (ceNumRow env)
                (\i -> extractField (ceCtx env) (rowAt env i) col)
    pure (parseFromExamples parseOpts cells)
