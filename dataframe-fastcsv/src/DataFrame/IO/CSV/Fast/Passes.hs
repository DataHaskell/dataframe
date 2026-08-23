{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | The typed column passes (Round-2 WS-E1): each pass walks one column's
delimiter slices and appends parsed values straight into a WS-C builder —
no per-field 'Data.Text.Text' and no deferred re-parse. Text bytes are
memcpy'd into a shared buffer, only for Text-typed columns.
-}
module DataFrame.IO.CSV.Fast.Passes (
    ColumnEnv (..),
    NullSpec (..),
    PassCol (..),
    PassResult,
    passColumn,
    nullSpecFor,
    isNullSlice,
    isNullText,
    rowAt,
    intPass,
    doublePass,
    boolPass,
    textPass,
    datePass,
    allNullPass,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Control.Monad.ST (stToIO)
import Data.Word (Word8)
import Foreign.Ptr (Ptr, nullPtr, plusPtr)
import GHC.Exts (Int (..), Int#, isTrue#, (+#), (>=#))

import DataFrame.IO.CSV (ReadOptions (..))
import DataFrame.IO.CSV.Fast.Index (quote)
import DataFrame.IO.CSV.Fast.Slice
import DataFrame.Internal.Column (Column, Columnable, fromVector)
import DataFrame.Internal.Column.Builder (
    ColumnBuilder (..),
    TextChunk,
    appendNum,
    appendText,
    appendTextSliceFromPtr,
    freezeTextChunk,
    mergeTextChunks,
    newNumBuilder,
    newTextBuilder,
 )
import DataFrame.Internal.Parsing.Fast (
    isMissingField,
    isMissingFieldIn,
    isMissingFieldSlice,
    parseBoolFieldSlice,
    parseDateFieldSlice,
    parseDoubleFieldSlice,
    parseIntFieldSlice,
 )
import DataFrame.Operations.Typing (
    SafeReadMode (..),
    isNullishOrMissing,
    parseTimeOpt,
 )

-- | Per-read context shared by every column pass.
data ColumnEnv = ColumnEnv
    { ceCtx :: !FieldCtx
    , ceRows :: !(VS.Vector Int)
    -- ^ Row indices (into row-ends) of the data rows, blanks skipped.
    , ceNumRow :: !Int
    , cePtr :: !(Ptr Word8)
    -- ^ Base pointer of the file content (valid for the whole read).
    , ceTextHint :: !Int
    -- ^ Initial byte capacity for a Text column builder.
    , ceOpts :: !ReadOptions
    }

-- | Byte-level null test: mode-dependent (audit T3, behaviors list #1).
data NullSpec
    = -- | 'NoSafeRead': only the empty field is null.
      NullEmpty
    | -- | 'MaybeRead': canonical missing set plus custom extras.
      NullMissing ![BS.ByteString] ![T.Text]

nullSpecFor :: ReadOptions -> SafeReadMode -> NullSpec
nullSpecFor _ NoSafeRead = NullEmpty
nullSpecFor opts _ =
    NullMissing
        (filter (not . isMissingField) (map TE.encodeUtf8 toks))
        toks
  where
    toks = missingIndicators opts

{-# INLINE isNullSlice #-}
isNullSlice :: NullSpec -> BS.ByteString -> Int -> Int -> Bool
isNullSlice NullEmpty _ s e = e <= s
isNullSlice (NullMissing extras _) bs s e =
    isMissingFieldSlice bs s e
        || (not (null extras) && isMissingFieldIn extras (sliceBS bs s e))

-- | Text-level twin of 'isNullSlice' for the decode-and-strip trim path.
isNullText :: NullSpec -> T.Text -> Bool
isNullText NullEmpty t = T.null t
isNullText (NullMissing _ toks) t = isNullishOrMissing toks t

{- | What a pass produced: a finished 'Column', or a raw text chunk that the
merger splices at the byte level (no per-chunk 'T.Text' materialization).
-}
data PassCol = PassFull !Column | PassText !TextChunk

-- | Finish one 'PassCol' on its own (the single-chunk \/ sequential case).
passColumn :: PassCol -> Column
passColumn (PassFull c) = c
passColumn (PassText tc) = mergeTextChunks [tc]

-- | Result of a fallible pass: @Left rowIndex@ on the first parse failure.
type PassResult = Either Int PassCol

rowAt :: ColumnEnv -> Int -> Int
rowAt env = VS.unsafeIndex (ceRows env)
{-# INLINE rowAt #-}

{- | One pass appending unboxed values; @nullOnFail@ turns parse failures
into nulls (schema + 'MaybeRead') instead of aborting (inference demotion).
-}
{-# INLINE unboxedPass #-}
unboxedPass ::
    (Columnable a, VU.Unbox a) =>
    ColumnEnv ->
    NullSpec ->
    Int ->
    Bool ->
    a ->
    (BS.ByteString -> Int -> Int -> Maybe a) ->
    IO PassResult
unboxedPass env nspec col nullOnFail nullVal parser = do
    let ctx = ceCtx env
        bs = fcBS ctx
        !(I# n) = ceNumRow env
    b <- stToIO (newNumBuilder nullVal (ceNumRow env))
    -- Loop counter is Int#: join points are not worker/wrapper'd, so a
    -- boxed counter would allocate on every iteration.
    let go :: Int# -> IO PassResult
        go i
            | isTrue# (i >=# n) =
                Right . PassFull <$> stToIO (freezeBuilder b)
            | otherwise =
                withParseSlice ctx (rowAt env (I# i)) col $ \s e ->
                    if isNullSlice nspec bs s e
                        then stToIO (appendNull b) >> go (i +# 1#)
                        else case parser bs s e of
                            Just v -> stToIO (appendNum b v) >> go (i +# 1#)
                            Nothing
                                | nullOnFail ->
                                    stToIO (appendNull b) >> go (i +# 1#)
                                | otherwise -> pure (Left (I# i))
    go 0#

intPass :: ColumnEnv -> NullSpec -> Int -> Bool -> IO PassResult
intPass env nspec col nullOnFail =
    unboxedPass env nspec col nullOnFail (0 :: Int) parseIntFieldSlice

doublePass :: ColumnEnv -> NullSpec -> Int -> Bool -> IO PassResult
doublePass env nspec col nullOnFail =
    unboxedPass env nspec col nullOnFail (0 :: Double) parseDoubleFieldSlice

boolPass :: ColumnEnv -> NullSpec -> Int -> Bool -> IO PassResult
boolPass env nspec col nullOnFail =
    unboxedPass env nspec col nullOnFail False parseBoolFieldSlice

{- | Text pass (never fails): raw bytes are memcpy'd into the shared
builder buffer; only quoted fields containing a doubled quote, and the
legacy trim knob, take the decode path. Returns the raw chunk; 'T.Text'
values materialize once, when the merger splices the chunks.
-}
textPass :: ColumnEnv -> NullSpec -> Int -> IO TextChunk
textPass env nspec col = do
    let ctx = ceCtx env
        bs = fcBS ctx
        ptr = cePtr env
        !(I# n) = ceNumRow env
    b <- stToIO (newTextBuilder (ceNumRow env) (ceTextHint env))
    let go :: Int# -> IO TextChunk
        go i
            | isTrue# (i >=# n) = stToIO (freezeTextChunk b)
            | otherwise =
                withFieldSlice ctx (rowAt env (I# i)) col $ \s e q ->
                    if fcTrim ctx && not q
                        then do
                            let t = T.strip (decodeSlice bs s e)
                            if isNullText nspec t
                                then stToIO (appendNull b)
                                else stToIO (appendText b t)
                            go (i +# 1#)
                        else
                            if isNullSlice nspec bs s e
                                then stToIO (appendNull b) >> go (i +# 1#)
                                else do
                                    hasQ <-
                                        if q
                                            then containsQuote ptr s e
                                            else pure False
                                    if hasQ
                                        then
                                            stToIO . appendText b $
                                                unescapeDoubledQuotes
                                                    (decodeSlice bs s e)
                                        else
                                            stToIO $
                                                appendTextSliceFromPtr
                                                    b
                                                    (ptr `plusPtr` s)
                                                    (e - s)
                                    go (i +# 1#)
    go 0#

-- | memchr for a doubled-quote candidate inside a quoted field's content.
containsQuote :: Ptr Word8 -> Int -> Int -> IO Bool
containsQuote ptr s e = do
    p <- BSI.memchr (ptr `plusPtr` s) quote (fromIntegral (e - s))
    pure (p /= nullPtr)
{-# INLINE containsQuote #-}

-- | Date pass: boxed (Day is not unboxable), mirrors @handleDateAssumption@.
datePass :: ColumnEnv -> NullSpec -> Int -> Bool -> IO PassResult
datePass env nspec col nullOnFail = do
    let ctx = ceCtx env
        bs = fcBS ctx
        n = ceNumRow env
        dfmt = dateFormat (ceOpts env)
        parseAt s e
            | dfmt == "%Y-%m-%d" = parseDateFieldSlice bs s e
            | otherwise = parseTimeOpt dfmt (decodeSlice bs s e)
    out <- VM.new n
    let go !i !anyNull
            | i >= n = do
                vec <- V.unsafeFreeze out
                pure . Right . PassFull $
                    if anyNull
                        then fromVector vec
                        else fromVector (V.mapMaybe id vec)
            | otherwise =
                withParseSlice ctx (rowAt env i) col $ \s e ->
                    if isNullSlice nspec bs s e
                        then VM.unsafeWrite out i Nothing >> go (i + 1) True
                        else case parseAt s e of
                            Just d ->
                                VM.unsafeWrite out i (Just d) >> go (i + 1) anyNull
                            Nothing
                                | nullOnFail ->
                                    VM.unsafeWrite out i Nothing >> go (i + 1) True
                                | otherwise -> pure (Left i)
    go (0 :: Int) False

-- | Is every data cell of the column null? (@handleNoAssumption@ head case.)
allNullPass :: ColumnEnv -> NullSpec -> Int -> IO Bool
allNullPass env nspec col = go 0
  where
    ctx = ceCtx env
    bs = fcBS ctx
    n = ceNumRow env
    go !i
        | i >= n = pure True
        | otherwise =
            withParseSlice ctx (rowAt env i) col $ \s e ->
                if isNullSlice nspec bs s e then go (i + 1) else pure False
