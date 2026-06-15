{-# LANGUAGE BangPatterns #-}

{- | Per-column sinks for the default CSV reader. Typed columns (explicit
schema) parse straight from the input slice into the WS-C builders;
inferred and EitherRead columns record unboxed @(start, end)@ offsets
into the input buffer (plus a cold side map for quote-unescaped cells)
for the inference pass ("DataFrame.IO.CSV.Internal.Infer").
-}
module DataFrame.IO.CSV.Internal.Sink (
    Env (..),
    MissingMode (..),
    Sink (..),
    SliceCol,
    Cells (..),
    cellAt,
    withCell,
    newSliceCol,
    feedSink,
    nullSink,
    freezeCells,
    sliceBS,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.IntMap.Strict as IM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad.ST (RealWorld, stToIO)
import Data.IORef
import Data.Word (Word8)
import DataFrame.IO.CSV.Internal.Scanner
import DataFrame.Internal.ColumnBuilder
import DataFrame.Internal.Parsing.Fast (
    isMissingFieldSlice,
    parseDoubleFieldSlice,
    parseIntFieldSlice,
 )
import Foreign.Ptr (Ptr, castPtr, plusPtr)

{- | How missing tokens are detected (audit D2: canonical list goes
through the WS-B memcmp fast path; custom lists decode and compare).
-}
data MissingMode = MissNone | MissCanonical | MissCustom

-- | Read-time environment shared by every sink.
data Env = Env
    { envBS :: !BS.ByteString
    -- ^ Whole input (BOM-stripped when the caller stripped it).
    , envPtr :: !(Ptr Word8)
    -- ^ Base pointer of 'envBS' (valid for the whole decode).
    , envMode :: !MissingMode
    , envTexts :: ![T.Text]
    -- ^ The effective missing-indicator list ('Data.Text' level).
    }

data Sink
    = SinkInt !(IntBuilder RealWorld)
    | SinkDouble !(DoubleBuilder RealWorld)
    | SinkText !(TextBuilder RealWorld)
    | SinkBS !SliceCol

{- | Growable cell store for inferred columns: per cell a @(start, end)@
offset pair into the input (already Char8-stripped) and a 0\/1 validity
byte. Quote-unescaped cells (cold) live in an 'IM.IntMap' keyed by row;
their offset pair is @(-1, row)@.
-}
data SliceCol = SliceCol
    { scOffs :: !(IORef (VUM.IOVector Int))
    , scValid :: !(IORef (VUM.IOVector Word8))
    , scEsc :: !(IORef (IM.IntMap BS.ByteString))
    }

-- | Frozen 'SliceCol' plus the buffer it points into.
data Cells = Cells
    { cBS :: !BS.ByteString
    , cOffs :: !(VU.Vector Int)
    , cEsc :: !(IM.IntMap BS.ByteString)
    , cValid :: !(VU.Vector Word8)
    , cLen :: !Int
    }

{- | Resolve cell @i@ as a slice and continue (no 'BS.ByteString' header
is allocated for the common in-buffer case).
-}
withCell :: Cells -> Int -> (BS.ByteString -> Int -> Int -> r) -> r
withCell c !i k =
    let s = VU.unsafeIndex (cOffs c) (2 * i)
        e = VU.unsafeIndex (cOffs c) (2 * i + 1)
     in if s >= 0
            then k (cBS c) s e
            else let cell = cEsc c IM.! e in k cell 0 (BS.length cell)
{-# INLINE withCell #-}

-- | Cell @i@ as a (zero-copy) 'BS.ByteString'.
cellAt :: Cells -> Int -> BS.ByteString
cellAt c i = withCell c i sliceBS
{-# INLINE cellAt #-}

newSliceCol :: Int -> IO SliceCol
newSliceCol hint = do
    let cap = max 16 hint
    offs <- VUM.unsafeNew (2 * cap)
    val <- VUM.unsafeNew cap
    SliceCol <$> newIORef offs <*> newIORef val <*> newIORef IM.empty

appendSliceCol :: SliceCol -> Int -> Int -> Int -> Word8 -> IO ()
appendSliceCol (SliceCol oRef vRef _) !row !s !e !ok = do
    offs <- readIORef oRef
    offs' <-
        if 2 * row < VUM.length offs
            then pure offs
            else do
                o <- VUM.unsafeGrow offs (VUM.length offs)
                writeIORef oRef o
                pure o
    VUM.unsafeWrite offs' (2 * row) s
    VUM.unsafeWrite offs' (2 * row + 1) e
    val <- readIORef vRef
    val' <-
        if row < VUM.length val
            then pure val
            else do
                v <- VUM.unsafeGrow val (VUM.length val)
                writeIORef vRef v
                pure v
    VUM.unsafeWrite val' row ok
{-# INLINE appendSliceCol #-}

-- | Freeze the first @n@ cells (the kept row count).
freezeCells :: BS.ByteString -> SliceCol -> Int -> IO Cells
freezeCells bs (SliceCol oRef vRef eRef) n = do
    offs <- readIORef oRef
    val <- readIORef vRef
    esc <- readIORef eRef
    Cells bs
        <$> VU.freeze (VUM.slice 0 (2 * n) offs)
        <*> pure esc
        <*> VU.freeze (VUM.slice 0 n val)
        <*> pure n

-- | O(1) sub-'BS.ByteString' of a buffer (no copy).
sliceBS :: BS.ByteString -> Int -> Int -> BS.ByteString
sliceBS bs s e = BSU.unsafeTake (e - s) (BSU.unsafeDrop s bs)
{-# INLINE sliceBS #-}

{- | Feed one field slice @[s, e)@ of the input into a sink. @unesc@
fields (quoted with embedded @\"\"@) are unescaped into a fresh strict
'BS.ByteString' first (cold path).
-}
feedSink :: Env -> Sink -> Int -> Int -> Int -> Bool -> IO ()
feedSink env sink !row !s !e !unesc = case sink of
    SinkBS sc
        | unesc -> do
            let fresh = unescapeQuotes (envBS env) s e
                stripped = withStripC8 fresh 0 (BS.length fresh) (sliceBS fresh)
            modifyIORef' (scEsc sc) (IM.insert row stripped)
            appendSliceCol sc row (-1) row (missingOk env stripped)
        | otherwise -> withStripC8 (envBS env) s e $ \s' e' ->
            let ok = case envMode env of
                    MissNone -> 1
                    MissCanonical ->
                        if isMissingFieldSlice (envBS env) s' e' then 0 else 1
                    MissCustom -> missingOk env (sliceBS (envBS env) s' e')
             in appendSliceCol sc row s' e' ok
    _
        | unesc ->
            let fresh = unescapeQuotes (envBS env) s e
             in BSU.unsafeUseAsCStringLen fresh $ \(p, l) ->
                    feedTyped env sink fresh (castPtr p) 0 l
        | otherwise -> feedTyped env sink (envBS env) (envPtr env) s e
{-# INLINE feedSink #-}

-- | Validity byte for an already-stripped cell.
missingOk :: Env -> BS.ByteString -> Word8
missingOk env cell = case envMode env of
    MissNone -> 1
    MissCanonical -> if isMissingFieldSlice cell 0 (BS.length cell) then 0 else 1
    MissCustom -> if TE.decodeUtf8Lenient cell `elem` envTexts env then 0 else 1

feedTyped ::
    Env -> Sink -> BS.ByteString -> Ptr Word8 -> Int -> Int -> IO ()
feedTyped env sink bs !ptr !s !e = case sink of
    SinkInt b -> case parseIntFieldSlice bs s e of
        Just v -> stToIO (appendInt b v)
        Nothing -> stToIO (appendNull b)
    SinkDouble b -> case parseDoubleFieldSlice bs s e of
        Just v -> stToIO (appendDouble b v)
        Nothing -> stToIO (appendNull b)
    SinkText b -> appendTextCell env b bs ptr s e
    SinkBS _ -> error "feedTyped: SinkBS handled by feedSink"
{-# INLINE feedTyped #-}

{- | Text cells reproduce @T.strip . decodeUtf8Lenient@ plus the
missing-indicator test. Fast path: ASCII-edge slices append raw bytes
(the shared-buffer freeze decodes leniently per field); anything with
non-ASCII edges or a custom missing list goes through real 'T.strip'.
-}
appendTextCell ::
    Env ->
    TextBuilder RealWorld ->
    BS.ByteString ->
    Ptr Word8 ->
    Int ->
    Int ->
    IO ()
appendTextCell env b bs !ptr !s !e =
    withStripAscii bs s e $ \s' e' ->
        if s' < e'
            && (BSU.unsafeIndex bs s' >= 0x80 || BSU.unsafeIndex bs (e' - 1) >= 0x80)
            then slow
            else case envMode env of
                MissNone -> fast s' e'
                MissCanonical ->
                    if isMissingFieldSlice bs s' e'
                        then stToIO (appendNull b)
                        else fast s' e'
                MissCustom -> slow
  where
    fast s' e' = stToIO (appendTextSliceFromPtr b (ptr `plusPtr` s') (e' - s'))
    slow = do
        let t = T.strip (TE.decodeUtf8Lenient (sliceBS bs s e))
            missing = case envMode env of
                MissNone -> False
                _ -> t `elem` envTexts env
        if missing then stToIO (appendNull b) else stToIO (appendText b t)
{-# INLINE appendTextCell #-}

-- | Append a null cell (pad-with-null for short rows, audit D6).
nullSink :: Sink -> Int -> IO ()
nullSink sink !row = case sink of
    SinkInt b -> stToIO (appendNull b)
    SinkDouble b -> stToIO (appendNull b)
    SinkText b -> stToIO (appendNull b)
    SinkBS sc -> appendSliceCol sc row 0 0 0
{-# INLINE nullSink #-}
