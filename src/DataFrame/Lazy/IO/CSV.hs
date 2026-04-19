{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Lazy.IO.CSV where

import qualified Data.ByteString as BS
import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TextEncoding
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (forM_, unless, when, zipWithM_)
import Data.Attoparsec.Text (IResult (..), parseWith)
import Data.Char (intToDigit)
import Data.IORef
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality))
import Data.Word (Word8)
import DataFrame.Internal.Column (
    Column (..),
    MutableColumn (..),
    columnLength,
    ensureOptional,
    freezeColumn',
    freezeColumnEither,
    writeColumn,
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema (Schema, SchemaType (..), elements)
import DataFrame.Operations.Typing (SafeReadMode (..), effectiveSafeRead)
import System.IO
import Type.Reflection
import Prelude hiding (takeWhile)

-- | Record for CSV read options.
data ReadOptions = ReadOptions
    { hasHeader :: Bool
    , inferTypes :: Bool
    , safeRead :: SafeReadMode
    {- ^ Default 'SafeReadMode' for columns without an entry in
    'safeReadOverrides'.
    -}
    , safeReadOverrides :: [(T.Text, SafeReadMode)]
    -- ^ Per-column 'SafeReadMode' overrides; takes precedence over 'safeRead'.
    , rowRange :: !(Maybe (Int, Int)) -- (start, length)
    , seekPos :: !(Maybe Integer)
    , totalRows :: !(Maybe Int)
    , leftOver :: !T.Text
    , rowsRead :: !Int
    }

{- | By default we assume the file has a header and we infer types on read.
'safeRead' starts as 'NoSafeRead' — set it to 'MaybeRead' to wrap columns as
@Maybe a@, or 'EitherRead' to wrap as @Either Text a@ preserving the raw text
of any rows that fail to parse. Use 'safeReadOverrides' to pick a different
mode for specific columns.
-}
defaultOptions :: ReadOptions
defaultOptions =
    ReadOptions
        { hasHeader = True
        , inferTypes = True
        , safeRead = NoSafeRead
        , safeReadOverrides = []
        , rowRange = Nothing
        , seekPos = Nothing
        , totalRows = Nothing
        , leftOver = ""
        , rowsRead = 0
        }

{- | Reads a CSV file from the given path.
Note this file stores intermediate temporary files
while converting the CSV from a row to a columnar format.
-}
readCsv :: FilePath -> IO DataFrame
readCsv path = fst <$> readSeparated ',' defaultOptions path

{- | Reads a tab separated file from the given path.
Note this file stores intermediate temporary files
while converting the CSV from a row to a columnar format.
-}
readTsv :: FilePath -> IO DataFrame
readTsv path = fst <$> readSeparated '\t' defaultOptions path

-- | Reads a character separated file into a dataframe using mutable vectors.
readSeparated ::
    Char -> ReadOptions -> FilePath -> IO (DataFrame, (Integer, T.Text, Int))
readSeparated c opts path = do
    totalRows' <- case totalRows opts of
        Nothing ->
            countRows c path >>= \total -> if hasHeader opts then return (total - 1) else return total
        Just n -> if hasHeader opts then return (n - 1) else return n
    let (_, len') = case rowRange opts of
            Nothing -> (0, totalRows')
            Just (start, len'') -> (start, min len'' (totalRows' - rowsRead opts))
    withFile path ReadMode $ \handle -> do
        firstRow <- fmap T.strip . parseSep c <$> TIO.hGetLine handle
        let columnNames =
                if hasHeader opts
                    then fmap (T.filter (/= '\"')) firstRow
                    else fmap (T.singleton . intToDigit) [0 .. (length firstRow - 1)]
        -- If there was no header rewind the file cursor.
        unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

        currPos <- hTell handle
        when (isJust $ seekPos opts) $
            hSeek handle AbsoluteSeek (fromMaybe currPos (seekPos opts))

        -- Initialize mutable vectors for each column
        let numColumns = length columnNames
        let numRows = len'
        -- Use this row to infer the types of the rest of the column.
        (dataRow, remainder) <- readSingleLine c (leftOver opts) handle

        -- This array will track the indices of all null values for each column.
        nullIndices <- VM.unsafeNew numColumns
        VM.set nullIndices []
        mutableCols <- VM.unsafeNew numColumns
        getInitialDataVectors numRows mutableCols dataRow

        -- Read rows into the mutable vectors
        (unconsumed, r) <-
            fillColumns numRows c mutableCols nullIndices remainder handle

        -- Freeze the mutable vectors into immutable ones
        nulls' <- V.unsafeFreeze nullIndices
        let !columnNamesV = V.fromList columnNames
        cols <-
            V.mapM
                (freezeColumn columnNamesV mutableCols nulls' opts)
                (V.generate numColumns id)
        pos <- hTell handle

        return
            ( DataFrame
                { columns = cols
                , columnIndices = M.fromList (zip columnNames [0 ..])
                , dataframeDimensions = (maybe 0 columnLength (cols V.!? 0), V.length cols)
                , derivingExpressions = M.empty
                }
            , (pos, unconsumed, r + 1)
            )
{-# INLINE readSeparated #-}

getInitialDataVectors :: Int -> VM.IOVector MutableColumn -> [T.Text] -> IO ()
getInitialDataVectors n mCol xs = do
    forM_ (zip [0 ..] xs) $ \(i, x) -> do
        col <- case inferValueType x of
            "Int" ->
                MUnboxedColumn
                    <$> ( (VUM.unsafeNew n :: IO (VUM.IOVector Int)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readInt x) >> return c
                        )
            "Double" ->
                MUnboxedColumn
                    <$> ( (VUM.unsafeNew n :: IO (VUM.IOVector Double)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readDouble x) >> return c
                        )
            _ ->
                MBoxedColumn
                    <$> ( (VM.unsafeNew n :: IO (VM.IOVector T.Text)) >>= \c -> VM.unsafeWrite c 0 x >> return c
                        )
        VM.unsafeWrite mCol i col
{-# INLINE getInitialDataVectors #-}

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns ::
    Int ->
    Char ->
    VM.IOVector MutableColumn ->
    VM.IOVector [(Int, T.Text)] ->
    T.Text ->
    Handle ->
    IO (T.Text, Int)
fillColumns n c mutableCols nullIndices unused handle = do
    input <- newIORef unused
    rowsRead' <- newIORef (0 :: Int)
    forM_ [1 .. (n - 1)] $ \i -> do
        atEOF <- hIsEOF handle
        input' <- readIORef input
        unless (atEOF && input' == mempty) $ do
            parseWith (TIO.hGetChunk handle) (parseRow c) input' >>= \case
                Fail _unconsumed ctx er -> do
                    erpos <- hTell handle
                    fail $
                        "Failed to parse CSV file around "
                            <> show erpos
                            <> " byte; due: "
                            <> show er
                            <> "; context: "
                            <> show ctx
                Partial _ -> do
                    fail "Partial handler is called"
                Done (unconsumed :: T.Text) (row :: [T.Text]) -> do
                    writeIORef input unconsumed
                    modifyIORef rowsRead' (+ 1)
                    zipWithM_ (writeValue mutableCols nullIndices i) [0 ..] row
    l <- readIORef input
    r <- readIORef rowsRead'
    pure (l, r)
{-# INLINE fillColumns #-}

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue ::
    VM.IOVector MutableColumn ->
    VM.IOVector [(Int, T.Text)] ->
    Int ->
    Int ->
    T.Text ->
    IO ()
writeValue mutableCols nullIndices count colIndex value = do
    col <- VM.unsafeRead mutableCols colIndex
    res <- writeColumn count value col
    let modify val = VM.unsafeModify nullIndices ((count, val) :) colIndex
    either modify (const (return ())) res
{-# INLINE writeValue #-}

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn ::
    V.Vector T.Text ->
    VM.IOVector MutableColumn ->
    V.Vector [(Int, T.Text)] ->
    ReadOptions ->
    Int ->
    IO Column
freezeColumn colNames mutableCols nulls opts colIndex = do
    col <- VM.unsafeRead mutableCols colIndex
    let colNulls = nulls V.! colIndex
        mode =
            effectiveSafeRead
                (safeRead opts)
                (safeReadOverrides opts)
                (colNames V.! colIndex)
    case mode of
        EitherRead -> freezeColumnEither colNulls col
        MaybeRead -> do
            frozen <- freezeColumn' colNulls col
            return $! ensureOptional frozen
        NoSafeRead -> freezeColumn' colNulls col
{-# INLINE freezeColumn #-}

-- ---------------------------------------------------------------------------
-- Streaming scan API
-- ---------------------------------------------------------------------------

{- | Open a CSV/separated file for streaming, returning an open handle
(positioned just after the header line) and the column specification
for the schema columns that appear in the file header.

The caller is responsible for closing the handle when done.
-}
openCsvStream ::
    Char ->
    Schema ->
    FilePath ->
    IO (Handle, [(Int, T.Text, SchemaType)])
openCsvStream sep schema path = do
    handle <- openFile path ReadMode
    hSetBuffering handle (BlockBuffering (Just (8 * 1024 * 1024)))
    headerLine <- TIO.hGetLine handle
    let headerCols = fmap (T.filter (/= '"') . T.strip) (parseSep sep headerLine)
    let schemaMap = elements schema
    let colSpec =
            [ (idx, name, stype)
            | (idx, name) <- zip [0 ..] headerCols
            , Just stype <- [M.lookup name schemaMap]
            ]
    when (null colSpec) $
        hClose handle
            >> fail
                ("openCsvStream: none of the schema columns appear in the header of " <> path)
    return (handle, colSpec)

{- | Read up to @batchSz@ rows from the open handle, returning a batch
'DataFrame' and the unconsumed leftover text.  Returns 'Nothing' when
the handle is at EOF and there is no leftover input.

The caller must pass the leftover returned by the previous call (use @""@
for the first call).
-}
readBatch ::
    Char ->
    [(Int, T.Text, SchemaType)] ->
    Int ->
    BS.ByteString ->
    Handle ->
    IO (Maybe (DataFrame, BS.ByteString))
readBatch sep colSpec batchSz leftover handle = do
    let sepByte = fromIntegral (fromEnum sep) :: Word8
        numCols = length colSpec
        -- Read in 8 MB chunks; only the partial-line tail is copied on refill.
        chunkSize = 8 * 1024 * 1024
    nullsArr <- VM.unsafeNew numCols
    VM.set nullsArr []
    mCols <- VM.unsafeNew numCols
    forM_ (zip [0 ..] colSpec) $ \(ci, (_, _, st)) ->
        VM.unsafeWrite mCols ci =<< makeCol batchSz st
    -- buf holds unprocessed bytes; refilled on demand when no newline is found.
    bufRef <- newIORef leftover
    -- Row-by-row scan. When the buffer has no unquoted newline, fetch another chunk.
    -- The copy on refill is only the partial-line tail (≤ one row ≈ few hundred bytes).
    let loop !rowIdx = do
            remaining <- readIORef bufRef
            if rowIdx >= batchSz
                then return (rowIdx, remaining)
                else case findUnquotedNewline remaining of
                    Nothing -> do
                        chunk <- BS.hGet handle chunkSize
                        if BS.null chunk
                            then return (rowIdx, remaining) -- EOF
                            else writeIORef bufRef (remaining <> chunk) >> loop rowIdx
                    Just nlIdx -> do
                        let line = BS.take nlIdx remaining
                            rest' = BS.drop (nlIdx + 1) remaining
                            line' =
                                if not (BS.null line) && BS.last line == 0x0D
                                    then BS.init line
                                    else line
                        writeIORef bufRef rest'
                        forM_ (zip [0 ..] colSpec) $ \(ci, (fi, _, _)) -> do
                            let fieldBs = getNthFieldBs sepByte fi line'
                            col <- VM.unsafeRead mCols ci
                            res <- writeColumnBs rowIdx fieldBs col
                            case res of
                                Left nv -> VM.unsafeModify nullsArr ((rowIdx, nv) :) ci
                                Right _ -> return ()
                        loop (rowIdx + 1)
    (completeRows, newLeftover) <- loop 0
    if completeRows == 0
        then return Nothing
        else do
            forM_ [0 .. numCols - 1] $ \ci -> do
                col <- VM.unsafeRead mCols ci
                VM.unsafeWrite mCols ci (sliceCol completeRows col)
            nullsVec <- V.unsafeFreeze nullsArr
            cols <- V.generateM numCols $ \ci -> do
                col <- VM.unsafeRead mCols ci
                freezeColumn' (nullsVec V.! ci) col
            let colNames = [name | (_, name, _) <- colSpec]
            return $
                Just
                    ( DataFrame
                        { columns = cols
                        , columnIndices = M.fromList (zip colNames [0 ..])
                        , dataframeDimensions = (completeRows, numCols)
                        , derivingExpressions = M.empty
                        }
                    , newLeftover
                    )

{- | Write a 'ByteString' field value directly into a mutable column,
parsing numerics without an intermediate 'T.Text' allocation.
-}
writeColumnBs ::
    Int -> BS.ByteString -> MutableColumn -> IO (Either T.Text Bool)
writeColumnBs i bs (MBoxedColumn (col :: VM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl ->
            let val = TextEncoding.decodeUtf8Lenient bs
             in VM.unsafeWrite col i val >> return (Right True)
        Nothing -> return (Left (TextEncoding.decodeUtf8Lenient bs))
writeColumnBs i bs (MUnboxedColumn (col :: VUM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> case readByteStringDouble bs of
            Just v -> VUM.unsafeWrite col i v >> return (Right True)
            Nothing -> VUM.unsafeWrite col i 0 >> return (Left (TextEncoding.decodeUtf8Lenient bs))
        Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> case readByteStringInt bs of
                Just v -> VUM.unsafeWrite col i v >> return (Right True)
                Nothing -> VUM.unsafeWrite col i 0 >> return (Left (TextEncoding.decodeUtf8Lenient bs))
            Nothing -> return (Left (TextEncoding.decodeUtf8Lenient bs))
{-# INLINE writeColumnBs #-}

{- | Extracts the Nth field (0-indexed), respecting double quotes and stripping them.
Fast path: uses memchr-based 'BS.break' when no quotes are present in the line.
Slow path: quote-aware character-by-character scan.
-}
getNthFieldBs :: Word8 -> Int -> BS.ByteString -> BS.ByteString
getNthFieldBs sep targetIdx bs
    | not (BS.any (== 0x22) bs) = skipFast targetIdx bs
    | otherwise = go 0 0 False 0
  where
    -- Fast path: skip fields using elemIndex (memchr); avoids pair allocation.
    skipFast k s =
        case BS.elemIndex sep s of
            Nothing -> if k == 0 then s else BS.empty
            Just i ->
                if k == 0
                    then BS.take i s
                    else skipFast (k - 1) (BS.drop (i + 1) s)

    -- Slow path: quote-aware scan.
    quoteChar = 0x22 :: Word8
    len = BS.length bs
    go !idx !start !inQ !pos
        | pos >= len =
            if idx == targetIdx then extract start pos else BS.empty
        | otherwise =
            let c = BS.index bs pos
             in if c == quoteChar
                    then go idx start (not inQ) (pos + 1)
                    else
                        if c == sep && not inQ
                            then
                                if idx == targetIdx
                                    then extract start pos
                                    else go (idx + 1) (pos + 1) False (pos + 1)
                            else go idx start inQ (pos + 1)

    extract s e =
        let fieldVal = BS.take (e - s) (BS.drop s bs)
         in if BS.length fieldVal >= 2
                && BS.head fieldVal == quoteChar
                && BS.last fieldVal == quoteChar
                then BS.init (BS.tail fieldVal)
                else fieldVal
{-# INLINE getNthFieldBs #-}

-- | Allocate a fresh 'MutableColumn' for @n@ slots based on a 'SchemaType'.
makeCol :: Int -> SchemaType -> IO MutableColumn
makeCol n (SType (_ :: P.Proxy a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> MUnboxedColumn <$> (VUM.unsafeNew n :: IO (VUM.IOVector Int))
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> MUnboxedColumn <$> (VUM.unsafeNew n :: IO (VUM.IOVector Double))
            Nothing -> MBoxedColumn <$> (VM.unsafeNew n :: IO (VM.IOVector T.Text))

-- | Slice a 'MutableColumn' to @n@ elements (no-copy view).
sliceCol :: Int -> MutableColumn -> MutableColumn
sliceCol n (MBoxedColumn col) = MBoxedColumn (VM.take n col)
sliceCol n (MUnboxedColumn col) = MUnboxedColumn (VUM.take n col)

{- | Finds the index of the next unquoted newline (0x0A).
Fast path: uses memchr (SIMD) and falls back to a quote-aware linear scan
only if a double-quote appears before the candidate newline.
-}
findUnquotedNewline :: BS.ByteString -> Maybe Int
findUnquotedNewline bs =
    case BS.elemIndex 0x0A bs of
        Nothing -> Nothing
        Just nlPos
            -- No quote before the newline → safe to use this position.
            -- Check with elemIndex to avoid allocating a ByteString slice.
            | maybe True (>= nlPos) (BS.elemIndex 0x22 bs) -> Just nlPos
            -- Quote present → may be a newline inside a quoted field; scan carefully.
            | otherwise -> slowScan 0 False
  where
    len = BS.length bs
    slowScan !pos !inQ
        | pos >= len = Nothing
        | otherwise =
            let c = BS.index bs pos
             in if c == 0x22
                    then slowScan (pos + 1) (not inQ)
                    else
                        if c == 0x0A && not inQ
                            then Just pos
                            else slowScan (pos + 1) inQ
{-# INLINE findUnquotedNewline #-}
