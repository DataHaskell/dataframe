{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.CSV where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Data.Csv.Streaming (Records (..))
import qualified Data.Csv.Streaming as CsvStream

import Control.DeepSeq
import Control.Exception (SomeException, catch)
import Control.Monad
import Data.Char
import qualified Data.Csv as Csv
import Data.Either
import Data.Functor
import Data.IORef
import Data.Maybe
import Data.Type.Equality (TestEquality (testEquality))
import Data.Word (Word8)
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), toSeparated)
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema
import DataFrame.Operations.Typing
import System.IO
import Type.Reflection
import Prelude hiding (concat, takeWhile)

chunkSize :: Int
chunkSize = 16_384

data PagedVector a = PagedVector
    { pvChunks :: !(IORef [V.Vector a])
    -- ^ Finished chunks (reverse order)
    , pvActive :: !(IORef (VM.IOVector a))
    -- ^ Current mutable chunk
    , pvCount :: !(IORef Int)
    -- ^ Items written in current chunk
    }

data PagedUnboxedVector a = PagedUnboxedVector
    { puvChunks :: !(IORef [VU.Vector a])
    , puvActive :: !(IORef (VUM.IOVector a))
    , puvCount :: !(IORef Int)
    }

data BuilderColumn
    = BuilderInt !(PagedUnboxedVector Int) !(PagedUnboxedVector Word8)
    | BuilderDouble !(PagedUnboxedVector Double) !(PagedUnboxedVector Word8)
    | BuilderText !(PagedVector T.Text) !(PagedUnboxedVector Word8)
    | BuilderBS !(PagedVector BS.ByteString) !(PagedUnboxedVector Word8)

newPagedVector :: IO (PagedVector a)
newPagedVector = do
    active <- VM.unsafeNew chunkSize
    PagedVector <$> newIORef [] <*> newIORef active <*> newIORef 0

newPagedUnboxedVector :: (VUM.Unbox a) => IO (PagedUnboxedVector a)
newPagedUnboxedVector = do
    active <- VUM.unsafeNew chunkSize
    PagedUnboxedVector <$> newIORef [] <*> newIORef active <*> newIORef 0

appendPagedVector :: PagedVector a -> a -> IO ()
appendPagedVector (PagedVector chunksRef activeRef countRef) !val = do
    count <- readIORef countRef
    active <- readIORef activeRef

    if count < chunkSize
        then do
            VM.unsafeWrite active count val
            writeIORef countRef $! count + 1
        else do
            frozen <- V.unsafeFreeze active
            modifyIORef' chunksRef (frozen :)

            newActive <- VM.unsafeNew chunkSize
            VM.unsafeWrite newActive 0 val

            writeIORef activeRef newActive
            writeIORef countRef 1
{-# INLINE appendPagedVector #-}

appendPagedUnboxedVector :: (VUM.Unbox a) => PagedUnboxedVector a -> a -> IO ()
appendPagedUnboxedVector (PagedUnboxedVector chunksRef activeRef countRef) !val = do
    count <- readIORef countRef
    active <- readIORef activeRef

    if count < chunkSize
        then do
            VUM.unsafeWrite active count val
            writeIORef countRef $! count + 1
        else do
            frozen <- VU.unsafeFreeze active
            modifyIORef' chunksRef (frozen :)

            newActive <- VUM.unsafeNew chunkSize
            VUM.unsafeWrite newActive 0 val

            writeIORef activeRef newActive
            writeIORef countRef 1
{-# INLINE appendPagedUnboxedVector #-}

freezePagedVector :: PagedVector a -> IO (V.Vector a)
freezePagedVector (PagedVector chunksRef activeRef countRef) = do
    count <- readIORef countRef
    active <- readIORef activeRef
    chunks <- readIORef chunksRef

    writeIORef chunksRef [] -- release chunk references
    let frozenChunks = reverse chunks
        totalLen = count + sum (map V.length frozenChunks)

    mv <- VM.unsafeNew totalLen

    let copyChunk !offset chunk = do
            V.copy (VM.slice offset (V.length chunk) mv) chunk
            pure (offset + V.length chunk)

    offset <- foldM copyChunk 0 frozenChunks
    VM.copy (VM.slice offset count mv) (VM.slice 0 count active)

    V.unsafeFreeze mv

freezePagedUnboxedVector ::
    (VUM.Unbox a) => PagedUnboxedVector a -> IO (VU.Vector a)
freezePagedUnboxedVector (PagedUnboxedVector chunksRef activeRef countRef) = do
    count <- readIORef countRef
    active <- readIORef activeRef
    chunks <- readIORef chunksRef

    writeIORef chunksRef [] -- release chunk references
    let frozenChunks = reverse chunks
        totalLen = count + sum (map VU.length frozenChunks)

    mv <- VUM.unsafeNew totalLen

    let copyChunk !offset chunk = do
            VU.copy (VUM.slice offset (VU.length chunk) mv) chunk
            pure (offset + VU.length chunk)

    offset <- foldM copyChunk 0 frozenChunks
    VUM.copy (VUM.slice offset count mv) (VUM.slice 0 count active)

    VU.unsafeFreeze mv

-- | STANDARD CONFIG TYPES
data HeaderSpec = NoHeader | UseFirstRow | ProvideNames [T.Text]
    deriving (Eq, Show)

data TypeSpec
    = InferFromSample Int
    | SpecifyTypes [(T.Text, SchemaType)] TypeSpec
    | NoInference

-- | CSV read parameters.
data ReadOptions = ReadOptions
    { headerSpec :: HeaderSpec
    -- ^ Where to get the headers from. (default: UseFirstRow)
    , typeSpec :: TypeSpec
    -- ^ Whether/how to infer types. (default: InferFromSample 100)
    , safeRead :: SafeReadMode
    {- ^ Default 'SafeReadMode' for columns without an entry in
    'safeReadOverrides'. (default: 'NoSafeRead')
    -}
    , safeReadOverrides :: [(T.Text, SafeReadMode)]
    -- ^ Per-column 'SafeReadMode' overrides; takes precedence over 'safeRead'.
    , dateFormat :: String
    {- ^ Format of date fields as recognized by the Data.Time.Format module.

    __Examples:__

    @
    > parseTimeM True defaultTimeLocale "%Y/%-m/%-d" "2010/3/04" :: Maybe Day
    Just 2010-03-04
    > parseTimeM True defaultTimeLocale "%d/%-m/%-Y" "04/3/2010" :: Maybe Day
    Just 2010-03-04
    @
    -}
    , columnSeparator :: Char
    -- ^ Character that separates column values.
    , numColumns :: Maybe Int
    -- ^ Number of columns to read.
    , missingIndicators :: [T.Text]
    -- ^ Values that should be read as `Nothing`.
    }

shouldInferFromSample :: TypeSpec -> Bool
shouldInferFromSample (InferFromSample _) = True
shouldInferFromSample (SpecifyTypes _ fallback) = shouldInferFromSample fallback
shouldInferFromSample _ = False

schemaTypeMap :: TypeSpec -> M.Map T.Text SchemaType
schemaTypeMap (SpecifyTypes xs _) = M.fromList xs
schemaTypeMap _ = M.empty

typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize (SpecifyTypes _ fallback) = typeInferenceSampleSize fallback
typeInferenceSampleSize _ = 0

defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = NoSafeRead
        , safeReadOverrides = []
        , dateFormat = "%Y-%m-%d"
        , columnSeparator = ','
        , numColumns = Nothing
        , missingIndicators =
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
        }

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsv ".\/data\/taxi.csv"

@
-}
readCsv :: FilePath -> IO DataFrame
readCsv = readSeparated defaultReadOptions

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsvWithOpts ".\/data\/taxi.csv" (D.defaultReadOptions { dateFormat = "%d/%-m/%-Y" })

@
-}
readCsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
readCsvWithOpts = readSeparated

{- | Read TSV (tab separated) file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readTsv ".\/data\/taxi.tsv"

@
-}
readTsv :: FilePath -> IO DataFrame
readTsv = readSeparated (defaultReadOptions{columnSeparator = '\t'})

{- | Read text file with specified delimiter into a dataframe.

==== __Example__
@
ghci> D.readSeparated (D.defaultReadOptions { columnSeparator = ';' }) ".\/data\/taxi.txt"

@
-}
readSeparated :: ReadOptions -> FilePath -> IO DataFrame
readSeparated opts !path = do
    let stripUtf8Bom bs = fromMaybe bs (BL.stripPrefix "\xEF\xBB\xBF" bs)
    csvData <- stripUtf8Bom <$> BL.readFile path
    fmap force (decodeSeparated opts csvData)

decodeSeparated :: ReadOptions -> BL.ByteString -> IO DataFrame
decodeSeparated !opts csvData = do
    let sep = columnSeparator opts
    let decodeOpts = Csv.defaultDecodeOptions{Csv.decDelimiter = fromIntegral (ord sep)}
    let stream = CsvStream.decodeWith decodeOpts Csv.NoHeader csvData

    let peekStream (Cons (Right row) rest) = return (row, rest)
        peekStream (Cons (Left err) _) = error $ "Error parsing CSV header: " ++ err
        peekStream (Nil Nothing _) = error "Empty CSV file"
        peekStream (Nil (Just err) _) = error err

    (firstRowRaw, dataStream) <- peekStream stream

    let (columnNames, rowsToProcess) = case headerSpec opts of
            NoHeader ->
                ( map (T.pack . show) [0 .. V.length firstRowRaw - 1]
                , Cons (Right firstRowRaw) dataStream
                )
            UseFirstRow ->
                ( map (T.strip . TE.decodeUtf8Lenient . BL.toStrict) (V.toList firstRowRaw)
                , dataStream
                )
            ProvideNames ns ->
                ( ns ++ drop (length ns) (map (T.pack . show) [0 .. V.length firstRowRaw - 1])
                , Cons (Right firstRowRaw) dataStream
                )

    (sampleRow, _) <- peekStream rowsToProcess
    builderCols <- initializeColumns columnNames (V.toList sampleRow) opts
    let !builderColsV = V.fromList builderCols
    let colNamesV = V.fromList columnNames
        resolveMode =
            effectiveSafeRead
                (safeRead opts)
                (safeReadOverrides opts)
        -- If ANY column is EitherRead we keep every raw cell (including
        -- "N/A" etc.) verbatim; otherwise the missing-indicator list applies.
        anyEither =
            any (\n -> resolveMode n == EitherRead) columnNames
        missing = if anyEither then [] else missingIndicators opts
    processStream missing rowsToProcess builderColsV (numColumns opts)

    frozenCols <-
        V.zipWithM
            (\name bc -> finalizeBuilderColumn (resolveMode name) opts bc)
            colNamesV
            builderColsV
    let numRows = maybe 0 columnLength (frozenCols V.!? 0)

    let df =
            DataFrame
                frozenCols
                (M.fromList (zip columnNames [0 ..]))
                (numRows, V.length frozenCols)
                M.empty -- TODO give typed column references
    pure $ parseWithTypes resolveMode (schemaTypeMap (typeSpec opts)) df

initializeColumns ::
    [T.Text] -> [BL.ByteString] -> ReadOptions -> IO [BuilderColumn]
initializeColumns names _row opts = zipWithM initColumn names (map lookupType names)
  where
    typeMap = schemaTypeMap (typeSpec opts)
    -- Return Nothing for columns that should be inferred from BS
    shouldInfer = case typeSpec opts of
        InferFromSample _ -> True
        SpecifyTypes _ fallback -> shouldInferFromSample fallback
        NoInference -> False
    lookupType name = M.lookup name typeMap
    resolveMode =
        effectiveSafeRead (safeRead opts) (safeReadOverrides opts)
    initColumn :: T.Text -> Maybe SchemaType -> IO BuilderColumn
    initColumn name _ | resolveMode name == EitherRead = do
        validityRef <- newPagedUnboxedVector
        BuilderBS <$> newPagedVector <*> pure validityRef
    initColumn _ Nothing | shouldInfer = do
        validityRef <- newPagedUnboxedVector
        BuilderBS <$> newPagedVector <*> pure validityRef
    initColumn _ mtype = do
        validityRef <- newPagedUnboxedVector
        let t = fromMaybe (schemaType @T.Text) mtype
        case t of
            SType (_ :: P.Proxy a) -> case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> BuilderInt <$> newPagedUnboxedVector <*> pure validityRef
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> BuilderDouble <$> newPagedUnboxedVector <*> pure validityRef
                    Nothing -> BuilderText <$> newPagedVector <*> pure validityRef

processStream ::
    [T.Text] ->
    CsvStream.Records (V.Vector BL.ByteString) ->
    V.Vector BuilderColumn ->
    Maybe Int ->
    IO ()
processStream _ _ _ (Just 0) = return ()
processStream missing (Cons (Right row) rest) cols n =
    processRow missing row cols
        >> processStream missing rest cols (fmap (flip (-) 1) n)
processStream _missing (Cons (Left err) _) _ _ = error ("CSV Parse Error: " ++ err)
processStream _missing (Nil _ _) _ _ = return ()

processRow ::
    [T.Text] -> V.Vector BL.ByteString -> V.Vector BuilderColumn -> IO ()
processRow missing !vals !cols = V.zipWithM_ processValue vals cols
  where
    processValue !bs !col = do
        let !bs' = BL.toStrict bs
        case col of
            BuilderInt gv valid -> case readByteStringInt bs' of
                Just !i -> appendPagedUnboxedVector gv i >> appendPagedUnboxedVector valid 1
                Nothing -> appendPagedUnboxedVector gv 0 >> appendPagedUnboxedVector valid 0
            BuilderDouble gv valid -> case readByteStringDouble bs' of
                Just !d -> appendPagedUnboxedVector gv d >> appendPagedUnboxedVector valid 1
                Nothing -> appendPagedUnboxedVector gv 0.0 >> appendPagedUnboxedVector valid 0
            BuilderText gv valid -> do
                let !val = T.strip (TE.decodeUtf8Lenient bs')
                appendPagedVector gv val
                appendPagedUnboxedVector valid (if val `elem` missing then 0 else 1)
            BuilderBS gv valid -> do
                let !bs'' = C.strip bs'
                appendPagedVector gv bs''
                appendPagedUnboxedVector
                    valid
                    (if TE.decodeUtf8Lenient bs'' `elem` missing then 0 else 1)

freezeBuilderColumn :: BuilderColumn -> IO Column
freezeBuilderColumn (BuilderInt gv validRef) = do
    vec <- freezePagedUnboxedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $! UnboxedColumn Nothing vec
        else constructOptional vec valid
freezeBuilderColumn (BuilderDouble gv validRef) = do
    vec <- freezePagedUnboxedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $! UnboxedColumn Nothing vec
        else constructOptional vec valid
freezeBuilderColumn (BuilderText gv validRef) = do
    vec <- freezePagedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $! BoxedColumn Nothing vec
        else constructOptionalBoxed vec valid
freezeBuilderColumn (BuilderBS _ _) =
    error
        "freezeBuilderColumn: BuilderBS must be finalized via finalizeBuilderColumn"

finalizeBuilderColumn ::
    SafeReadMode -> ReadOptions -> BuilderColumn -> IO Column
finalizeBuilderColumn mode opts bc = do
    col <- case bc of
        BuilderBS gv validRef -> do
            vec <- freezePagedVector gv
            valid <- freezePagedUnboxedVector validRef
            return $! inferColumnFromBS mode opts vec valid
        _ -> freezeBuilderColumn bc
    return $! case mode of
        NoSafeRead -> col
        MaybeRead -> ensureOptional col
        EitherRead -> col

inferColumnFromBS ::
    SafeReadMode ->
    ReadOptions ->
    V.Vector BS.ByteString ->
    VU.Vector Word8 ->
    Column
inferColumnFromBS mode opts vec valid =
    let sampleN = let n = typeInferenceSampleSize (typeSpec opts) in if n == 0 then 100 else n
        dfmt = dateFormat opts
        asMaybeFull = V.generate (V.length vec) $ \i ->
            if valid VU.! i == 1 then Just (vec V.! i) else Nothing
        samples = V.take sampleN asMaybeFull
        assumption = makeParsingAssumptionBS dfmt samples
     in case mode of
            EitherRead -> handleBSEither dfmt assumption vec valid
            _ -> case assumption of
                IntAssumption -> handleBSInt dfmt asMaybeFull
                DoubleAssumption -> handleBSDouble asMaybeFull
                BoolAssumption -> handleBSBool asMaybeFull
                DateAssumption -> handleBSDate dfmt asMaybeFull
                TextAssumption -> handleBSText asMaybeFull
                NoAssumption -> handleBSNo dfmt asMaybeFull

{- | 'EitherRead' wrap for the ByteString inference path: produce an
@Either Text a@ column. Raw input bytes are preserved verbatim; rows that were
marked invalid by the builder (e.g. empty cells before EitherRead disabled
missing-indicator detection) become @Left \"\"@.
-}
handleBSEither ::
    String ->
    ParsingAssumption ->
    V.Vector BS.ByteString ->
    VU.Vector Word8 ->
    Column
handleBSEither dfmt assumption vec valid = case assumption of
    BoolAssumption -> wrap readByteStringBool
    IntAssumption -> wrap readByteStringInt
    DoubleAssumption -> wrap readByteStringDouble
    DateAssumption -> wrap (readByteStringDate dfmt)
    -- Text / No assumption: column is Either Text Text; empty cells become
    -- Left "" to keep the "Left means missing/failure" convention.
    TextAssumption -> fromVector (V.imap textEither vec)
    NoAssumption -> fromVector (V.imap textEither vec)
  where
    wrap ::
        forall a. (Columnable a) => (BS.ByteString -> Maybe a) -> Column
    wrap p = fromVector (V.imap (toEither p) vec)

    toEither ::
        forall a.
        (BS.ByteString -> Maybe a) ->
        Int ->
        BS.ByteString ->
        Either T.Text a
    toEither p i bs
        | valid VU.! i == 0 = Left (TE.decodeUtf8Lenient bs)
        | otherwise = case p bs of
            Just v -> Right v
            Nothing -> Left (TE.decodeUtf8Lenient bs)

    textEither :: Int -> BS.ByteString -> Either T.Text T.Text
    textEither i bs =
        let t = TE.decodeUtf8Lenient bs
         in if valid VU.! i == 0 || T.null t then Left t else Right t

makeParsingAssumptionBS ::
    String -> V.Vector (Maybe BS.ByteString) -> ParsingAssumption
makeParsingAssumptionBS dfmt asMaybe
    | V.all (== Nothing) asMaybe = NoAssumption
    | vecSameConstructor asMaybe asMaybeBool = BoolAssumption
    | vecSameConstructor asMaybe asMaybeInt
        && vecSameConstructor asMaybe asMaybeDouble =
        IntAssumption
    | vecSameConstructor asMaybe asMaybeDouble = DoubleAssumption
    | vecSameConstructor asMaybe asMaybeDate = DateAssumption
    | otherwise = TextAssumption
  where
    asMaybeBool = V.map (>>= readByteStringBool) asMaybe
    asMaybeInt = V.map (>>= readByteStringInt) asMaybe
    asMaybeDouble = V.map (>>= readByteStringDouble) asMaybe
    asMaybeDate = V.map (>>= readByteStringDate dfmt) asMaybe

handleBSBool :: V.Vector (Maybe BS.ByteString) -> Column
handleBSBool asMaybe
    | parsableAsBool =
        maybe (fromVector asMaybeBool) fromVector (sequenceA asMaybeBool)
    | otherwise = handleBSText asMaybe
  where
    asMaybeBool = V.map (>>= readByteStringBool) asMaybe
    parsableAsBool = vecSameConstructor asMaybe asMaybeBool

handleBSInt :: String -> V.Vector (Maybe BS.ByteString) -> Column
handleBSInt _dfmt asMaybe
    | parsableAsInt =
        maybe (fromVector asMaybeInt) fromVector (sequenceA asMaybeInt)
    | parsableAsDouble =
        maybe (fromVector asMaybeDouble) fromVector (sequenceA asMaybeDouble)
    | otherwise = handleBSText asMaybe
  where
    asMaybeInt = V.map (>>= readByteStringInt) asMaybe
    asMaybeDouble = V.map (>>= readByteStringDouble) asMaybe
    parsableAsInt =
        vecSameConstructor asMaybe asMaybeInt
            && vecSameConstructor asMaybe asMaybeDouble
    parsableAsDouble = vecSameConstructor asMaybe asMaybeDouble

handleBSDouble :: V.Vector (Maybe BS.ByteString) -> Column
handleBSDouble asMaybe
    | parsableAsDouble =
        maybe (fromVector asMaybeDouble) fromVector (sequenceA asMaybeDouble)
    | otherwise = handleBSText asMaybe
  where
    asMaybeDouble = V.map (>>= readByteStringDouble) asMaybe
    parsableAsDouble = vecSameConstructor asMaybe asMaybeDouble

handleBSDate :: String -> V.Vector (Maybe BS.ByteString) -> Column
handleBSDate dfmt asMaybe
    | parsableAsDate =
        maybe (fromVector asMaybeDate) fromVector (sequenceA asMaybeDate)
    | otherwise = handleBSText asMaybe
  where
    asMaybeDate = V.map (>>= readByteStringDate dfmt) asMaybe
    parsableAsDate = vecSameConstructor asMaybe asMaybeDate

handleBSText :: V.Vector (Maybe BS.ByteString) -> Column
handleBSText asMaybe =
    let asMaybeText = V.map (fmap TE.decodeUtf8Lenient) asMaybe
     in maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)

handleBSNo :: String -> V.Vector (Maybe BS.ByteString) -> Column
handleBSNo dfmt asMaybe
    | V.all (== Nothing) asMaybe =
        fromVector (V.map (const (Nothing :: Maybe T.Text)) asMaybe)
    | parsableAsBool =
        maybe (fromVector asMaybeBool) fromVector (sequenceA asMaybeBool)
    | parsableAsInt =
        maybe (fromVector asMaybeInt) fromVector (sequenceA asMaybeInt)
    | parsableAsDouble =
        maybe (fromVector asMaybeDouble) fromVector (sequenceA asMaybeDouble)
    | parsableAsDate =
        maybe (fromVector asMaybeDate) fromVector (sequenceA asMaybeDate)
    | otherwise = handleBSText asMaybe
  where
    asMaybeBool = V.map (>>= readByteStringBool) asMaybe
    asMaybeInt = V.map (>>= readByteStringInt) asMaybe
    asMaybeDouble = V.map (>>= readByteStringDouble) asMaybe
    asMaybeDate = V.map (>>= readByteStringDate dfmt) asMaybe
    parsableAsBool = vecSameConstructor asMaybe asMaybeBool
    parsableAsInt =
        vecSameConstructor asMaybe asMaybeInt
            && vecSameConstructor asMaybe asMaybeDouble
    parsableAsDouble = vecSameConstructor asMaybe asMaybeDouble
    parsableAsDate = vecSameConstructor asMaybe asMaybeDate

constructOptional ::
    (VU.Unbox a, Columnable a) => VU.Vector a -> VU.Vector Word8 -> IO Column
constructOptional vec valid = do
    let bm = buildBitmapFromValid valid
    pure $ UnboxedColumn (Just bm) vec

constructOptionalBoxed :: V.Vector T.Text -> VU.Vector Word8 -> IO Column
constructOptionalBoxed vec valid = do
    let bm = buildBitmapFromValid valid
    pure $ BoxedColumn (Just bm) vec

writeCsv :: FilePath -> DataFrame -> IO ()
writeCsv = writeSeparated ','

writeTsv :: FilePath -> DataFrame -> IO ()
writeTsv = writeSeparated '\t'

writeSeparated ::
    -- | Separator
    Char ->
    -- | Path to write to
    FilePath ->
    DataFrame ->
    IO ()
writeSeparated c filepath df = TIO.writeFile filepath (toSeparated c df)

-- | Parse a CSV string into a DataFrame using default options.
fromCsv :: String -> IO (Either String DataFrame)
fromCsv s = do
    let bs = BL.fromStrict (TE.encodeUtf8 (T.pack s))
    (Right <$> decodeSeparated defaultReadOptions bs)
        `catch` (\(e :: SomeException) -> pure (Left (show e)))

-- | Parse a lazy 'ByteString' containing CSV data into a DataFrame using default options.
fromCsvBytes :: BL.ByteString -> IO DataFrame
fromCsvBytes = decodeSeparated defaultReadOptions

stripQuotes :: T.Text -> T.Text
stripQuotes txt =
    case T.uncons txt of
        Just ('"', rest) ->
            case T.unsnoc rest of
                Just (middle, '"') -> middle
                _ -> txt
        _ -> txt
