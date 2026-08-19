{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Single-pass sampled inference lattice (Round-2 S2). One walk over
the sampled cells maintains a candidate mask {Bool, Int, Double, Date}
cleared by attempting the WS-B byte parsers; the assumption is the
highest-priority surviving candidate. Shared by every reader (audit T1),
together with the Int -> Double prefix promotion that replaces the
full-column retry chain (audit T2).
-}
module DataFrame.Operations.Inference (
    DateFormat,
    ParsingAssumption (..),
    makeParsingAssumption,
    makeParsingAssumptionBytes,
    byteStringDateParser,
    parseTimeOpt,
    promoteIntColumn,
    promoteIntColumnIndexed,
    readIntStrict,
) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad.ST (runST)
import Data.Maybe (isJust)
import Data.Time (Day, defaultTimeLocale, parseTimeM)
import DataFrame.Internal.Column (Column (..), finalizeParseResult)
import DataFrame.Internal.Parsing (readByteStringDate, readInt)
import DataFrame.Internal.Parsing.Fast (
    parseBoolField,
    parseDateField,
    parseDoubleField,
    parseIntField,
 )

type DateFormat = String

data ParsingAssumption
    = BoolAssumption
    | IntAssumption
    | DoubleAssumption
    | DateAssumption
    | NoAssumption
    | TextAssumption
    deriving (Eq, Show)

parseTimeOpt :: DateFormat -> T.Text -> Maybe Day
parseTimeOpt dateFormat s =
    parseTimeM {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

{- | The default @%Y-%m-%d@ format takes the WS-B byte-level fast path;
custom formats keep the reference 'readByteStringDate' parser.
-}
byteStringDateParser :: DateFormat -> BS.ByteString -> Maybe Day
byteStringDateParser "%Y-%m-%d" = parseDateField
byteStringDateParser fmt = readByteStringDate fmt
{-# INLINE byteStringDateParser #-}

{- | 'DataFrame.Internal.Parsing.readInt' that rejects overflow instead of
wrapping. Fields of <= 18 chars cannot overflow and keep the Text-level
parse; longer (rare) fields take the exact byte-level parser, so an
overflowing cell demotes\/promotes instead of silently wrapping.
-}
readIntStrict :: T.Text -> Maybe Int
readIntStrict t
    | T.length t <= 18 = readInt t
    | otherwise = parseIntField (TE.encodeUtf8 t)
{-# INLINE readIntStrict #-}

{- | Candidate-mask priority, reproducing the documented fallback order:
an all-null sample makes no assumption; Int wins only when the Double
mask agrees (so mixed Int\/Double samples classify as Double).
-}
pickAssumption ::
    Bool -> Bool -> Bool -> Bool -> Bool -> ParsingAssumption
pickAssumption seen b i d dt
    | not seen = NoAssumption
    | b = BoolAssumption
    | i && d = IntAssumption
    | d = DoubleAssumption
    | dt = DateAssumption
    | otherwise = TextAssumption

{- | Classify a sample of decoded 'T.Text' cells ('Nothing' = null).
Bool\/Int\/Double candidates are tested with the WS-B byte parsers on
the UTF-8 bytes (strip-tolerant, overflow-rejecting); the Date
candidate keeps 'parseTimeOpt' so custom formats behave exactly as
before. The walk exits early once every candidate is cleared.
-}
makeParsingAssumption ::
    DateFormat -> V.Vector (Maybe T.Text) -> ParsingAssumption
makeParsingAssumption dfmt cells = go 0 False True True True True
  where
    n = V.length cells
    go !i !seen !b !int !d !dt
        | i >= n || (seen && not (b || int || d || dt)) =
            pickAssumption seen b int d dt
        | otherwise = case V.unsafeIndex cells i of
            Nothing -> go (i + 1) seen b int d dt
            Just t ->
                let bs = TE.encodeUtf8 t
                 in go
                        (i + 1)
                        True
                        (b && isJust (parseBoolField bs))
                        (int && isJust (parseIntField bs))
                        (d && isJust (parseDoubleField bs))
                        (dt && isJust (parseTimeOpt dfmt t))

-- | Byte-level twin of 'makeParsingAssumption' for raw field bytes.
makeParsingAssumptionBytes ::
    DateFormat -> V.Vector (Maybe BS.ByteString) -> ParsingAssumption
makeParsingAssumptionBytes dfmt cells = go 0 False True True True True
  where
    n = V.length cells
    dateP = byteStringDateParser dfmt
    go !i !seen !b !int !d !dt
        | i >= n || (seen && not (b || int || d || dt)) =
            pickAssumption seen b int d dt
        | otherwise = case V.unsafeIndex cells i of
            Nothing -> go (i + 1) seen b int d dt
            Just bs ->
                go
                    (i + 1)
                    True
                    (b && isJust (parseBoolField bs))
                    (int && isJust (parseIntField bs))
                    (d && isJust (parseDoubleField bs))
                    (dt && isJust (dateP bs))

{- | Fused Int pass with in-place promotion (audit T2): on the first
non-null cell that fails Int but parses as Double, the built Int prefix
is converted by a vector map and the pass continues as Double over the
retained raw cells. @Nothing@ = some cell parses as neither (the caller
demotes the column to Text). Null slots hold sentinels (0 \/ 0.0)
guarded by the bitmap, exactly like the unpromoted passes.
-}
promoteIntColumn ::
    forall src.
    (Int -> src -> Bool) ->
    (src -> Maybe Int) ->
    (src -> Maybe Double) ->
    V.Vector src ->
    Maybe Column
promoteIntColumn isNullAt parseI parseD vec =
    promoteIntColumnIndexed
        (V.length vec)
        (\i -> isNullAt i (V.unsafeIndex vec i))
        (parseI . V.unsafeIndex vec)
        (parseD . V.unsafeIndex vec)
{-# INLINE promoteIntColumn #-}

{- | 'promoteIntColumn' over row indices instead of a materialized cell
vector (used by readers that keep cells as buffer offsets).
-}
promoteIntColumnIndexed ::
    Int ->
    (Int -> Bool) ->
    (Int -> Maybe Int) ->
    (Int -> Maybe Double) ->
    Maybe Column
promoteIntColumnIndexed n isNullAt parseI parseD = runST $ do
    values <- VUM.unsafeNew n
    vmask <- VUM.unsafeNew n
    let intLoop !i !anyNull
            | i >= n =
                fmap (uncurry UnboxedColumn)
                    <$> finalizeParseResult values vmask anyNull
            | isNullAt i = do
                VUM.unsafeWrite vmask i 0
                VUM.unsafeWrite values i 0
                intLoop (i + 1) True
            | otherwise = case parseI i of
                Just v -> do
                    VUM.unsafeWrite vmask i 1
                    VUM.unsafeWrite values i v
                    intLoop (i + 1) anyNull
                Nothing -> case parseD i of
                    Just dv -> promote i anyNull dv
                    Nothing -> return Nothing

        -- Switch to Double at row i: convert the Int prefix, then
        -- continue parsing the remaining cells as Double.
        promote !i !anyNull !dv = do
            dvalues <- VUM.unsafeNew n
            let copy !j
                    | j >= i = return ()
                    | otherwise = do
                        x <- VUM.unsafeRead values j
                        VUM.unsafeWrite dvalues j (fromIntegral x)
                        copy (j + 1)
            copy 0
            VUM.unsafeWrite vmask i 1
            VUM.unsafeWrite dvalues i dv
            dblLoop dvalues (i + 1) anyNull

        dblLoop dvalues !i !anyNull
            | i >= n =
                fmap (uncurry UnboxedColumn)
                    <$> finalizeParseResult dvalues vmask anyNull
            | isNullAt i = do
                VUM.unsafeWrite vmask i 0
                VUM.unsafeWrite dvalues i 0
                dblLoop dvalues (i + 1) True
            | otherwise = case parseD i of
                Just dv -> do
                    VUM.unsafeWrite vmask i 1
                    VUM.unsafeWrite dvalues i dv
                    dblLoop dvalues (i + 1) anyNull
                Nothing -> return Nothing
    intLoop 0 False
{-# INLINE promoteIntColumnIndexed #-}
