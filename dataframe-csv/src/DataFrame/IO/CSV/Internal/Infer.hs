{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Column inference over raw field bytes for the default CSV reader:
classify a bounded sample with the shared lattice
("DataFrame.Operations.Inference"), then build the column in one fused
pass with in-place Int -> Double promotion (audit T2). Cells arrive as
'Cells' — unboxed offsets into the input buffer plus a builder validity
vector — so the passes parse slices without materializing cell values.
-}
module DataFrame.IO.CSV.Internal.Infer (
    inferColumnFromBS,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.IntMap.Strict as IM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad.ST (runST, stToIO)
import Data.Time (Day)
import Data.Word (Word8)
import DataFrame.IO.CSV.Internal.Options
import DataFrame.IO.CSV.Internal.Sink (Cells (..), cellAt, withCell)
import DataFrame.Internal.Column
import DataFrame.Internal.ColumnBuilder
import DataFrame.Internal.Parsing
import DataFrame.Internal.Parsing.Fast (
    parseBoolFieldSlice,
    parseDateFieldSlice,
    parseDoubleFieldSlice,
    parseIntFieldSlice,
 )
import DataFrame.Operations.Typing
import Foreign.Ptr (Ptr, castPtr, plusPtr)

inferColumnFromBS :: SafeReadMode -> ReadOptions -> Cells -> IO Column
inferColumnFromBS mode opts cells =
    let sampleN = let n = typeInferenceSampleSize (typeSpec opts) in if n == 0 then 100 else n
        dfmt = dateFormat opts
        -- The sample IS still Maybe-wrapped; it's bounded at 100 rows
        -- by default, so the allocation is ignorable.
        samples = V.generate (min sampleN (cLen cells)) $ \i ->
            if validAt cells i then Just (cellAt cells i) else Nothing
        assumption = makeParsingAssumptionBytes dfmt samples
        orText = maybe (textColumn cells) pure
     in case mode of
            EitherRead -> pure (handleBSEither dfmt assumption cells)
            _ -> case assumption of
                IntAssumption -> orText (handleBSInt cells)
                DoubleAssumption -> orText (handleBSDouble cells)
                BoolAssumption -> orText (handleBSBool cells)
                DateAssumption -> orText (handleBSDate dfmt cells)
                TextAssumption -> textColumn cells
                NoAssumption -> handleBSNo dfmt cells

validAt :: Cells -> Int -> Bool
validAt cells i = VU.unsafeIndex (cValid cells) i /= 0
{-# INLINE validAt #-}

{- | @%Y-%m-%d@ takes the WS-B slice fast path; custom formats keep the
reference parser over a materialized cell.
-}
dateSliceParser :: String -> Cells -> Int -> Maybe Day
dateSliceParser "%Y-%m-%d" cells i = withCell cells i parseDateFieldSlice
dateSliceParser fmt cells i = readByteStringDate fmt (cellAt cells i)
{-# INLINE dateSliceParser #-}

{- | 'EitherRead' wrap for the ByteString inference path: produce an
@Either Text a@ column. Raw input bytes are preserved verbatim; rows that were
marked invalid by the builder (e.g. empty cells before EitherRead disabled
missing-indicator detection) become @Left \"\"@.
-}
handleBSEither :: String -> ParsingAssumption -> Cells -> Column
handleBSEither dfmt assumption cells = case assumption of
    BoolAssumption -> wrap readByteStringBool
    IntAssumption -> wrap readByteStringInt
    DoubleAssumption -> wrap readByteStringDouble
    DateAssumption -> wrap (readByteStringDate dfmt)
    -- Text / No assumption: column is Either Text Text; empty cells become
    -- Left "" to keep the "Left means missing/failure" convention.
    TextAssumption -> fromVector (V.generate (cLen cells) textEither)
    NoAssumption -> fromVector (V.generate (cLen cells) textEither)
  where
    wrap ::
        forall a. (Columnable a) => (BS.ByteString -> Maybe a) -> Column
    wrap p = fromVector (V.generate (cLen cells) (toEither p))

    toEither ::
        forall a. (BS.ByteString -> Maybe a) -> Int -> Either T.Text a
    toEither p i
        | not (validAt cells i) = Left (TE.decodeUtf8Lenient bs)
        | otherwise = case p bs of
            Just v -> Right v
            Nothing -> Left (TE.decodeUtf8Lenient bs)
      where
        bs = cellAt cells i

    textEither :: Int -> Either T.Text T.Text
    textEither i =
        let t = TE.decodeUtf8Lenient (cellAt cells i)
         in if not (validAt cells i) || T.null t then Left t else Right t

handleBSBool :: Cells -> Maybe Column
handleBSBool cells =
    uncurry UnboxedColumn
        <$> parseUnboxedCells False (\i -> withCell cells i parseBoolFieldSlice) cells

{- | Int columns: one fused pass with in-place Int -> Double promotion
('promoteIntColumnIndexed', audit T2) instead of the full-column Double
re-parse; both parsers are the WS-B slice parsers (bit-exact with the
old @readByteString*@ pair).
-}
handleBSInt :: Cells -> Maybe Column
handleBSInt cells =
    promoteIntColumnIndexed
        (cLen cells)
        (not . validAt cells)
        (\i -> withCell cells i parseIntFieldSlice)
        (\i -> withCell cells i parseDoubleFieldSlice)

handleBSDouble :: Cells -> Maybe Column
handleBSDouble cells =
    uncurry UnboxedColumn
        <$> parseUnboxedCells 0 (\i -> withCell cells i parseDoubleFieldSlice) cells

-- Dates are boxed ('Day' isn't 'VU.Unbox'), so fuse into a V.Vector
-- (Maybe Day) in one pass.  Bails on the first non-null cell that
-- fails to parse.
handleBSDate :: String -> Cells -> Maybe Column
handleBSDate dfmt cells =
    case parseBoxedMaybeCells (dateSliceParser dfmt cells) cells of
        Just (anyNull, out)
            | anyNull -> Just (fromVector out)
            | otherwise -> Just (fromVector (V.mapMaybe id out))
        Nothing -> Nothing

{- | Text columns build through the WS-C 'TextBuilder': one shared byte
array + offsets, 'Data.Text.Text' values sliced off it at freeze (UTF-8
validated once, lenient per-field decode only when invalid).
-}
textColumn :: Cells -> IO Column
textColumn cells = do
    let n = cLen cells
    b <- stToIO (newTextBuilder n (n * 8))
    BSU.unsafeUseAsCStringLen (cBS cells) $ \(p, _) -> do
        let base = castPtr p :: Ptr Word8
            go !i
                | i >= n = pure ()
                | otherwise = do
                    let s = VU.unsafeIndex (cOffs cells) (2 * i)
                        e = VU.unsafeIndex (cOffs cells) (2 * i + 1)
                    if not (validAt cells i)
                        then stToIO (appendNull b)
                        else
                            if s >= 0
                                then stToIO (appendTextSliceFromPtr b (base `plusPtr` s) (e - s))
                                else
                                    let cell = cEsc cells IM.! e
                                     in BSU.unsafeUseAsCStringLen cell $ \(q, l) ->
                                            stToIO
                                                (appendTextSliceFromPtr b (castPtr q) l)
                    go (i + 1)
        go 0
    stToIO (freezeBuilder b)

handleBSNo :: String -> Cells -> IO Column
handleBSNo dfmt cells
    | VU.all (== 0) (cValid cells) =
        pure (fromVector (V.replicate (cLen cells) (Nothing :: Maybe T.Text)))
    | Just (mbm, out) <-
        parseUnboxedCells False (\i -> withCell cells i parseBoolFieldSlice) cells =
        pure (UnboxedColumn mbm out)
    | Just (mbm, out) <-
        parseUnboxedCells 0 (\i -> withCell cells i parseIntFieldSlice) cells =
        pure (UnboxedColumn mbm out)
    | Just (mbm, out) <-
        parseUnboxedCells 0 (\i -> withCell cells i parseDoubleFieldSlice) cells =
        pure (UnboxedColumn mbm out)
    | otherwise = maybe (textColumn cells) pure (handleBSDate dfmt cells)

-- Boxed counterpart to 'parseUnboxedCells' for types that aren't
-- 'VU.Unbox' (e.g. 'Day').  Same one-pass + early-bail shape.
parseBoxedMaybeCells ::
    (Int -> Maybe a) -> Cells -> Maybe (Bool, V.Vector (Maybe a))
parseBoxedMaybeCells parser cells = runST $ do
    let n = cLen cells
    out <- VM.new n
    let loop !i !anyNull
            | i >= n = do
                frozen <- V.unsafeFreeze out
                return (Just (anyNull, frozen))
            | not (validAt cells i) = do
                VM.unsafeWrite out i Nothing
                loop (i + 1) True
            | otherwise = case parser i of
                Just v -> do
                    VM.unsafeWrite out i (Just v)
                    loop (i + 1) anyNull
                Nothing -> return Nothing
    loop 0 False

{- | One-pass fused parse into a typed unboxed column, reading
nullability from the builder's validity vector. Returns @Just@ only
when every non-null cell parses; the first unparseable cell
short-circuits so the caller can fall back to the next assumption.
-}
parseUnboxedCells ::
    forall a.
    (VU.Unbox a) =>
    a ->
    (Int -> Maybe a) ->
    Cells ->
    Maybe (Maybe Bitmap, VU.Vector a)
parseUnboxedCells nullValue parser cells = runST $ do
    let n = cLen cells
    values <- VUM.unsafeNew n
    vmask <- VUM.unsafeNew n
    let go !i !anyNull
            | i >= n = finalizeParseResult values vmask anyNull
            | not (validAt cells i) = do
                VUM.unsafeWrite vmask i 0
                VUM.unsafeWrite values i nullValue
                go (i + 1) True
            | otherwise = case parser i of
                Just v -> do
                    VUM.unsafeWrite vmask i 1
                    VUM.unsafeWrite values i v
                    go (i + 1) anyNull
                Nothing -> return Nothing
    go 0 False
{-# INLINE parseUnboxedCells #-}
