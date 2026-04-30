{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Typing where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Applicative (asum)
import Control.Monad (join)
import Control.Monad.ST (runST)
import Data.Maybe (fromMaybe)
import qualified Data.Proxy as P
import Data.Time
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    Columnable,
    bitmapTestBit,
    ensureOptional,
    finalizeParseResult,
    fromVector,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema
import DataFrame.Operations.Core
import Text.Read
import Type.Reflection

type DateFormat = String

{- | How parse failures are surfaced in the resulting column.

* 'NoSafeRead' — strict parsing: failures throw (via 'read').
* 'MaybeRead' — failures become 'Nothing'; columns are wrapped as @Maybe a@.
* 'EitherRead' — failures become @Left rawText@; columns are wrapped as
  @Either Text a@, preserving the original input so callers can inspect it.
-}
data SafeReadMode
    = NoSafeRead
    | MaybeRead
    | EitherRead
    deriving (Eq, Show, Read)

-- | Options controlling how text columns are parsed into typed values.
data ParseOptions = ParseOptions
    { missingValues :: [T.Text]
    -- ^ Values to treat as @Nothing@ when the effective mode is 'MaybeRead'.
    , sampleSize :: Int
    -- ^ Number of rows to inspect when inferring a column's type (0 = all rows).
    , parseSafe :: SafeReadMode
    {- ^ Default 'SafeReadMode' applied to every column that does not have an
    entry in 'parseSafeOverrides'. 'NoSafeRead' only treats empty strings as
    missing; 'MaybeRead' additionally treats 'missingValues' and nullish
    strings as @Nothing@; 'EitherRead' wraps the resulting column as
    @Either Text a@ with the raw input preserved on failure.
    -}
    , parseSafeOverrides :: [(T.Text, SafeReadMode)]
    {- ^ Per-column overrides. When a column name is present here, its value
    takes precedence over 'parseSafe'. Typical use: strict IDs
    (@NoSafeRead@) alongside lenient fields (@MaybeRead@/@EitherRead@).
    -}
    , parseDateFormat :: DateFormat
    -- ^ Date format string as accepted by "Data.Time.Format" (e.g. @\"%Y-%m-%d\"@).
    }

{- | Sensible out-of-the-box parse options: infer from the first 100 rows,
  treat common nullish strings as missing, and expect ISO 8601 dates.
-}
defaultParseOptions :: ParseOptions
defaultParseOptions =
    ParseOptions
        { missingValues = []
        , sampleSize = 100
        , parseSafe = MaybeRead
        , parseSafeOverrides = []
        , parseDateFormat = "%Y-%m-%d"
        }

{- | Resolve a column's effective 'SafeReadMode': the override if present,
otherwise the default.
-}
effectiveSafeRead ::
    SafeReadMode -> [(T.Text, SafeReadMode)] -> T.Text -> SafeReadMode
effectiveSafeRead def overrides name = fromMaybe def (lookup name overrides)

parseDefaults :: ParseOptions -> DataFrame -> DataFrame
parseDefaults opts df = df{columns = V.imap forCol (columns df)}
  where
    -- Index -> column name: reverse the columnIndices map once.
    nameAt =
        let inverted = M.fromList [(i, n) | (n, i) <- M.toList (columnIndices df)]
         in \i -> M.findWithDefault "" i inverted
    forCol i col =
        let mode =
                effectiveSafeRead
                    (parseSafe opts)
                    (parseSafeOverrides opts)
                    (nameAt i)
         in parseDefault opts{parseSafe = mode, parseSafeOverrides = []} col

parseDefault :: ParseOptions -> Column -> Column
parseDefault opts (BoxedColumn Nothing (c :: V.Vector a)) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl -> parseFromExamples opts (V.map T.pack c)
            Nothing -> BoxedColumn Nothing c
        Just Refl -> parseFromExamples opts c
parseDefault opts (BoxedColumn (Just bm) (c :: V.Vector a)) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl ->
                parseFromExamples
                    opts
                    (V.imap (\i x -> if bitmapTestBit bm i then T.pack x else "") c)
            Nothing -> BoxedColumn (Just bm) c
        Just Refl ->
            parseFromExamples opts (V.imap (\i x -> if bitmapTestBit bm i then x else "") c)
parseDefault _ column = column

parseFromExamples :: ParseOptions -> V.Vector T.Text -> Column
parseFromExamples opts cols =
    let isNull = case parseSafe opts of
            NoSafeRead -> T.null
            _ -> isNullishOrMissing (missingValues opts)
        -- `examples` is small (≤ sampleSize, default 100), so the
        -- Maybe-wrap allocation here is ignorable.  The full-column
        -- equivalent (`asMaybeText = V.map ... cols`) has been removed:
        -- handlers now walk `cols` directly with `isNull`.
        examples = V.map (classify isNull) (V.take (sampleSize opts) cols)
        dfmt = parseDateFormat opts
        assumption = makeParsingAssumption dfmt examples
     in case parseSafe opts of
            EitherRead -> handleEitherAssumption dfmt assumption cols
            mode ->
                let result = case assumption of
                        BoolAssumption -> handleBoolAssumption isNull cols
                        IntAssumption -> handleIntAssumption isNull cols
                        DoubleAssumption -> handleDoubleAssumption isNull cols
                        TextAssumption -> handleTextAssumption isNull cols
                        DateAssumption -> handleDateAssumption dfmt isNull cols
                        NoAssumption -> handleNoAssumption dfmt isNull cols
                 in if mode == MaybeRead then ensureOptional result else result
  where
    classify p t = if p t then Nothing else Just t

{- | For 'EitherRead' mode: take the chosen parsing assumption and produce an
@Either Text a@ column. Successful parses become @Right@; any row that fails
to parse as the chosen type (including null/missing cells) becomes @Left@
carrying the raw input text verbatim.
-}
handleEitherAssumption ::
    DateFormat -> ParsingAssumption -> V.Vector T.Text -> Column
handleEitherAssumption dfmt assumption raw = case assumption of
    BoolAssumption -> fromVector (V.map (toEither readBool) raw)
    IntAssumption -> fromVector (V.map (toEither readInt) raw)
    DoubleAssumption -> fromVector (V.map (toEither readDouble) raw)
    DateAssumption -> fromVector (V.map (toEither (parseTimeOpt dfmt)) raw)
    -- TextAssumption and NoAssumption degenerate to Either Text Text; treat
    -- empty strings as Left "" so the convention (Left = missing/failure) stays
    -- consistent across column types.
    TextAssumption -> fromVector (V.map textToEither raw)
    NoAssumption -> fromVector (V.map textToEither raw)
  where
    toEither :: (T.Text -> Maybe a) -> T.Text -> Either T.Text a
    toEither p t = maybe (Left t) Right (p t)

    textToEither :: T.Text -> Either T.Text T.Text
    textToEither t = if T.null t then Left t else Right t

parseUnboxedColumnWithPred ::
    forall src a.
    (VU.Unbox a) =>
    a ->
    (src -> Bool) ->
    (src -> Maybe a) ->
    V.Vector src ->
    Maybe (Maybe Bitmap, VU.Vector a)
parseUnboxedColumnWithPred nullValue isNull parser vec = runST $ do
    let n = V.length vec
    values <- VUM.unsafeNew n
    vmask <- VUM.unsafeNew n
    let go !i !anyNull
            | i >= n = finalizeParseResult values vmask anyNull
            | otherwise =
                let !src = V.unsafeIndex vec i
                 in if isNull src
                        then do
                            VUM.unsafeWrite vmask i 0
                            VUM.unsafeWrite values i nullValue
                            go (i + 1) True
                        else case parser src of
                            Just v -> do
                                VUM.unsafeWrite vmask i 1
                                VUM.unsafeWrite values i v
                                go (i + 1) anyNull
                            Nothing -> return Nothing
    go 0 False
{-# INLINE parseUnboxedColumnWithPred #-}

-- | Wrap a successful 'parseUnboxedColumnWithPred' result as a 'Column'.
unboxedOrFallback ::
    (Columnable a, VU.Unbox a) =>
    Maybe (Maybe Bitmap, VU.Vector a) ->
    Column ->
    Column
unboxedOrFallback (Just (mbm, vec)) _ = UnboxedColumn mbm vec
unboxedOrFallback Nothing fallback = fallback

handleBoolAssumption :: (T.Text -> Bool) -> V.Vector T.Text -> Column
handleBoolAssumption isNull cols =
    unboxedOrFallback
        (parseUnboxedColumnWithPred False isNull readBool cols)
        (handleTextAssumption isNull cols)

handleIntAssumption :: (T.Text -> Bool) -> V.Vector T.Text -> Column
handleIntAssumption isNull cols =
    case parseUnboxedColumnWithPred 0 isNull readInt cols of
        Just (mbm, vec) -> UnboxedColumn mbm vec
        Nothing ->
            unboxedOrFallback
                (parseUnboxedColumnWithPred 0 isNull readDouble cols)
                (handleTextAssumption isNull cols)

handleDoubleAssumption :: (T.Text -> Bool) -> V.Vector T.Text -> Column
handleDoubleAssumption isNull cols =
    unboxedOrFallback
        (parseUnboxedColumnWithPred 0 isNull readDouble cols)
        (handleTextAssumption isNull cols)

{- | Text columns: no parse, just null-marking.  When the whole column
is non-null we return a plain 'V.Vector T.Text'; otherwise we emit a
@V.Vector (Maybe T.Text)@ the same shape the old code produced.
-}
handleTextAssumption :: (T.Text -> Bool) -> V.Vector T.Text -> Column
handleTextAssumption isNull cols
    | V.any isNull cols =
        fromVector
            (V.map (\t -> if isNull t then Nothing else Just t) cols)
    | otherwise = fromVector cols

{- | Date: single parse pass, boxed because 'Day' is not unboxable.
Bails to 'handleTextAssumption' the moment a non-null cell fails to
parse as a 'Day'.  Still avoids the outer @V.Vector (Maybe T.Text)@
allocation — we walk @cols@ directly with @isNull@.
-}
handleDateAssumption ::
    DateFormat -> (T.Text -> Bool) -> V.Vector T.Text -> Column
handleDateAssumption dateFormat isNull cols =
    case parseBoxedMaybeColumn isNull (parseTimeOpt dateFormat) cols of
        Just (anyNull, vec)
            -- `vec :: V.Vector (Maybe Day)`.  If no nulls, strip the
            -- outer 'Maybe' (every cell is guaranteed 'Just') so the
            -- column type stays 'Day' rather than becoming 'Maybe Day'.
            | anyNull -> fromVector vec
            | otherwise -> fromVector (V.mapMaybe id vec)
        Nothing -> handleTextAssumption isNull cols

parseBoxedMaybeColumn ::
    (T.Text -> Bool) ->
    (T.Text -> Maybe a) ->
    V.Vector T.Text ->
    Maybe (Bool, V.Vector (Maybe a))
parseBoxedMaybeColumn isNull parser cols = runST $ do
    let n = V.length cols
    out <- VM.new n
    let loop !i !anyNull
            | i >= n = do
                frozen <- V.unsafeFreeze out
                return (Just (anyNull, frozen))
            | otherwise =
                let !t = V.unsafeIndex cols i
                 in if isNull t
                        then do
                            VM.unsafeWrite out i Nothing
                            loop (i + 1) True
                        else case parser t of
                            Just v -> do
                                VM.unsafeWrite out i (Just v)
                                loop (i + 1) anyNull
                            Nothing -> return Nothing
    loop 0 False

handleNoAssumption ::
    DateFormat -> (T.Text -> Bool) -> V.Vector T.Text -> Column
handleNoAssumption dateFormat isNull cols
    -- Only reached when the 100-row sample was all-null.  Try each
    -- concrete type in turn; fall back to Text otherwise.
    | V.all isNull cols =
        fromVector (V.map (const (Nothing :: Maybe T.Text)) cols)
    | Just (mbm, vec) <- parseUnboxedColumnWithPred False isNull readBool cols =
        UnboxedColumn mbm vec
    | Just (mbm, vec) <- parseUnboxedColumnWithPred 0 isNull readInt cols =
        UnboxedColumn mbm vec
    | Just (mbm, vec) <- parseUnboxedColumnWithPred 0 isNull readDouble cols =
        UnboxedColumn mbm vec
    | otherwise = case parseBoxedMaybeColumn isNull (parseTimeOpt dateFormat) cols of
        Just (anyNull, vec)
            -- `vec :: V.Vector (Maybe Day)`.  If no nulls, strip the
            -- outer 'Maybe' (every cell is guaranteed 'Just') so the
            -- column type stays 'Day' rather than becoming 'Maybe Day'.
            | anyNull -> fromVector vec
            | otherwise -> fromVector (V.mapMaybe id vec)
        Nothing -> handleTextAssumption isNull cols

{- | Predicate matching what 'parseSafe == NoSafeRead' previously used:
only empty strings are treated as missing.

We still expose 'convertNullish' \/ 'convertOnlyEmpty' below because
other parts of the library reference them, but neither is used by
'parseFromExamples' any longer.
-}
isNullishOrMissing :: [T.Text] -> T.Text -> Bool
isNullishOrMissing missing v = isNullish v || v `elem` missing

convertNullish :: [T.Text] -> T.Text -> Maybe T.Text
convertNullish missing v = if isNullish v || v `elem` missing then Nothing else Just v

convertOnlyEmpty :: T.Text -> Maybe T.Text
convertOnlyEmpty v = if v == "" then Nothing else Just v

parseTimeOpt :: DateFormat -> T.Text -> Maybe Day
parseTimeOpt dateFormat s =
    parseTimeM {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

unsafeParseTime :: DateFormat -> T.Text -> Day
unsafeParseTime dateFormat s =
    parseTimeOrError {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

hasNullValues :: (Eq a) => V.Vector (Maybe a) -> Bool
hasNullValues = V.any (== Nothing)

vecSameConstructor :: V.Vector (Maybe a) -> V.Vector (Maybe b) -> Bool
vecSameConstructor xs ys = (V.length xs == V.length ys) && V.and (V.zipWith hasSameConstructor xs ys)
  where
    hasSameConstructor :: Maybe a -> Maybe b -> Bool
    hasSameConstructor (Just _) (Just _) = True
    hasSameConstructor Nothing Nothing = True
    hasSameConstructor _ _ = False

makeParsingAssumption ::
    DateFormat -> V.Vector (Maybe T.Text) -> ParsingAssumption
makeParsingAssumption dateFormat asMaybeText
    -- All the examples are "NA", "Null", "", so we can't make any shortcut
    -- assumptions and just have to go the long way.
    | V.all (== Nothing) asMaybeText = NoAssumption
    -- After accounting for nulls, parsing for Ints and Doubles results in the
    -- same corresponding positions of Justs and Nothings, so we assume
    -- that the best way to parse is Int
    | vecSameConstructor asMaybeText asMaybeBool = BoolAssumption
    | vecSameConstructor asMaybeText asMaybeInt
        && vecSameConstructor asMaybeText asMaybeDouble =
        IntAssumption
    -- After accounting for nulls, the previous condition fails, so some (or none) can be parsed as Ints
    -- and some can be parsed as Doubles, so we make the assumpotion of doubles.
    | vecSameConstructor asMaybeText asMaybeDouble = DoubleAssumption
    -- After accounting for nulls, parsing for Dates results in the same corresponding
    -- positions of Justs and Nothings, so we assume that the best way to parse is Date.
    | vecSameConstructor asMaybeText asMaybeDate = DateAssumption
    | otherwise = TextAssumption
  where
    asMaybeBool = V.map (>>= readBool) asMaybeText
    asMaybeInt = V.map (>>= readInt) asMaybeText
    asMaybeDouble = V.map (>>= readDouble) asMaybeText
    asMaybeDate = V.map (>>= parseTimeOpt dateFormat) asMaybeText

data ParsingAssumption
    = BoolAssumption
    | IntAssumption
    | DoubleAssumption
    | DateAssumption
    | NoAssumption
    | TextAssumption

{- | Re-type columns of a 'DataFrame' according to the supplied schema map.
The caller provides a @resolveMode@ function that maps a column name to its
'SafeReadMode' — typically built from a global default plus an overrides map
via 'effectiveSafeRead'.
-}
parseWithTypes ::
    (T.Text -> SafeReadMode) ->
    M.Map T.Text SchemaType ->
    DataFrame ->
    DataFrame
parseWithTypes resolveMode ts df
    | M.null ts = df
    | otherwise =
        M.foldrWithKey
            (\k v d -> insertColumn k (asType (resolveMode k) v (unsafeGetColumn k d)) d)
            df
            ts
  where
    -- \| Re-parse a plain (non-Maybe, non-Either) target type according to the
    -- 'SafeReadMode'. @toStr@ converts column elements to a 'String' ready for
    -- 'Read'.
    plainType ::
        forall a b.
        (Columnable a, Read a) =>
        SafeReadMode -> V.Vector b -> (b -> String) -> Column
    plainType mode col toStr = case mode of
        NoSafeRead -> fromVector (V.map ((read @a) . toStr) col)
        MaybeRead -> fromVector (V.map ((readMaybe @a) . toStr) col)
        EitherRead -> fromVector (V.map ((readEitherRaw @a) . toStr) col)

    asType :: SafeReadMode -> SchemaType -> Column -> Column
    asType mode (SType (_ :: P.Proxy a)) c@(BoxedColumn _ (col :: V.Vector b)) = case typeRep @a of
        App t1 _t2 -> case eqTypeRep t1 (typeRep @Maybe) of
            Just HRefl -> case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> c
                Nothing -> case testEquality (typeRep @T.Text) (typeRep @b) of
                    Just Refl -> fromVector (V.map (join . (readAsMaybe @a) . T.unpack) col)
                    Nothing -> fromVector (V.map (join . (readAsMaybe @a) . show) col)
            Nothing -> case t1 of
                App t1' _t2' -> case eqTypeRep t1' (typeRep @Either) of
                    Just HRefl -> case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl -> c
                        Nothing -> case testEquality (typeRep @T.Text) (typeRep @b) of
                            Just Refl -> fromVector (V.map ((readAsEither @a) . T.unpack) col)
                            Nothing -> fromVector (V.map ((readAsEither @a) . show) col)
                    Nothing -> case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl -> c
                        Nothing -> case testEquality (typeRep @T.Text) (typeRep @b) of
                            Just Refl -> plainType @a mode col T.unpack
                            Nothing -> plainType @a mode col show
                _ -> c
        _ -> case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> c
            Nothing -> case testEquality (typeRep @T.Text) (typeRep @b) of
                Just Refl -> plainType @a mode col T.unpack
                Nothing -> plainType @a mode col show
    asType _ _ c = c

readAsMaybe :: (Read a) => String -> Maybe a
readAsMaybe s
    | null s = Nothing
    | otherwise = readMaybe $ "Just " <> s

readAsEither :: (Read a) => String -> a
readAsEither v = case asum [readMaybe $ "Left " <> s, readMaybe $ "Right " <> s] of
    Nothing -> error $ "Couldn't read value: " <> s
    Just v' -> v'
  where
    s = if null v then "\"\"" else v

{- | Try 'readMaybe'; on failure return @Left raw@ where @raw@ is the original
input text. Used by 'parseWithTypes' under 'EitherRead'.
-}
readEitherRaw :: forall a. (Read a) => String -> Either T.Text a
readEitherRaw s = case readMaybe s of
    Just v -> Right v
    Nothing -> Left (T.pack s)
