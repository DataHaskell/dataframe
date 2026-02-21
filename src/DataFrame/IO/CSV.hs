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

import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
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

import Control.Monad
import Data.Char
import qualified Data.Csv as Csv
import Data.Either
import Data.Function (on)
import Data.Functor
import Data.IORef
import Data.Maybe
import Data.Type.Equality (TestEquality (testEquality))
import Data.Word (Word8)
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..))
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

    lastChunk <- V.unsafeFreeze (VM.slice 0 count active)

    return $! V.concat (reverse (lastChunk : chunks))

freezePagedUnboxedVector ::
    (VUM.Unbox a) => PagedUnboxedVector a -> IO (VU.Vector a)
freezePagedUnboxedVector (PagedUnboxedVector chunksRef activeRef countRef) = do
    count <- readIORef countRef
    active <- readIORef activeRef
    chunks <- readIORef chunksRef

    lastChunk <- VU.unsafeFreeze (VUM.slice 0 count active)
    return $! VU.concat (reverse (lastChunk : chunks))

-- | STANDARD CONFIG TYPES
data HeaderSpec = NoHeader | UseFirstRow | ProvideNames [T.Text]
    deriving (Eq, Show)

data TypeSpec = InferFromSample Int | SpecifyTypes [SchemaType] | NoInference

-- | CSV read parameters.
data ReadOptions = ReadOptions
    { headerSpec :: HeaderSpec
    -- ^ Where to get the headers from. (default: UseFirstRow)
    , typeSpec :: TypeSpec
    -- ^ Whether/how to infer types. (default: InferFromSample 100)
    , safeRead :: Bool
    -- ^ Whether to partially parse values into `Maybe`/`Either`. (default: True)
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
shouldInferFromSample _ = False

schemaTypes :: TypeSpec -> [SchemaType]
schemaTypes (SpecifyTypes xs) = xs
schemaTypes _ = []

typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize _ = 0

defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = True
        , dateFormat = "%Y-%m-%d"
        , columnSeparator = ','
        , numColumns = Nothing
        , missingIndicators = []
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
    decodeSeparated opts csvData

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
    builderCols <- initializeColumns (V.toList sampleRow) opts
    processStream
        (missingIndicators opts)
        rowsToProcess
        builderCols
        (numColumns opts)

    frozenCols <- V.fromList <$> mapM freezeBuilderColumn builderCols
    let numRows = maybe 0 columnLength (frozenCols V.!? 0)

    let df =
            DataFrame
                frozenCols
                (M.fromList (zip columnNames [0 ..]))
                (numRows, V.length frozenCols)
                M.empty -- TODO give typed column references
    return $
        if shouldInferFromSample (typeSpec opts)
            then
                parseDefaults
                    (missingIndicators opts)
                    (typeInferenceSampleSize (typeSpec opts))
                    (safeRead opts)
                    (dateFormat opts)
                    df
            else
                if not (null (schemaTypes (typeSpec opts)))
                    then parseWithTypes (schemaTypes (typeSpec opts)) df
                    else df

initializeColumns :: [BL.ByteString] -> ReadOptions -> IO [BuilderColumn]
initializeColumns row opts = case typeSpec opts of
    NoInference -> zipWithM initColumn row (expandTypes [])
    InferFromSample _ -> zipWithM initColumn row (expandTypes [])
    SpecifyTypes ts -> zipWithM initColumn row (expandTypes ts)
  where
    expandTypes xs = xs ++ replicate (length row - length xs) (schemaType @T.Text)
    initColumn :: BL.ByteString -> SchemaType -> IO BuilderColumn
    initColumn _ t = do
        validityRef <- newPagedUnboxedVector
        case t of
            SType (_ :: P.Proxy a) -> case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> BuilderInt <$> newPagedUnboxedVector <*> pure validityRef
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> BuilderDouble <$> newPagedUnboxedVector <*> pure validityRef
                    Nothing -> BuilderText <$> newPagedVector <*> pure validityRef

processStream ::
    [T.Text] ->
    CsvStream.Records (V.Vector BL.ByteString) ->
    [BuilderColumn] ->
    Maybe Int ->
    IO ()
processStream _ _ _ (Just 0) = return ()
processStream missing (Cons (Right row) rest) cols n =
    processRow missing row cols
        >> processStream missing rest cols (fmap (flip (-) 1) n)
processStream missing (Cons (Left err) _) _ _ = error ("CSV Parse Error: " ++ err)
processStream missing (Nil _ _) _ _ = return ()

processRow :: [T.Text] -> V.Vector BL.ByteString -> [BuilderColumn] -> IO ()
processRow missing !vals !cols = V.zipWithM_ processValue vals (V.fromList cols)
  where
    processValue !bs !col = do
        let bs' = BL.toStrict bs
        case col of
            BuilderInt gv valid -> case readByteStringInt bs' of
                Just !i -> appendPagedUnboxedVector gv i >> appendPagedUnboxedVector valid 1
                Nothing -> appendPagedUnboxedVector gv 0 >> appendPagedUnboxedVector valid 0
            BuilderDouble gv valid -> case readByteStringDouble bs' of
                Just !d -> appendPagedUnboxedVector gv d >> appendPagedUnboxedVector valid 1
                Nothing -> appendPagedUnboxedVector gv 0.0 >> appendPagedUnboxedVector valid 0
            BuilderText gv valid -> do
                let !val = T.strip (TE.decodeUtf8Lenient bs')
                if isNullish val || val `elem` missing
                    then appendPagedVector gv T.empty >> appendPagedUnboxedVector valid 0
                    else appendPagedVector gv val >> appendPagedUnboxedVector valid 1

freezeBuilderColumn :: BuilderColumn -> IO Column
freezeBuilderColumn (BuilderInt gv validRef) = do
    vec <- freezePagedUnboxedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $ UnboxedColumn vec
        else constructOptional vec valid
freezeBuilderColumn (BuilderDouble gv validRef) = do
    vec <- freezePagedUnboxedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $ UnboxedColumn vec
        else constructOptional vec valid
freezeBuilderColumn (BuilderText gv validRef) = do
    vec <- freezePagedVector gv
    valid <- freezePagedUnboxedVector validRef
    if VU.all (== 1) valid
        then return $ BoxedColumn vec
        else constructOptionalBoxed vec valid

constructOptional ::
    (VU.Unbox a, Columnable a) => VU.Vector a -> VU.Vector Word8 -> IO Column
constructOptional vec valid = do
    let size = VU.length vec
    mvec <- VM.new size
    forM_ [0 .. size - 1] $ \i ->
        if (valid VU.! i) == 0
            then VM.write mvec i Nothing
            else VM.write mvec i (Just (vec VU.! i))
    OptionalColumn <$> V.freeze mvec

constructOptionalBoxed :: V.Vector T.Text -> VU.Vector Word8 -> IO Column
constructOptionalBoxed vec valid = do
    let size = V.length vec
    mvec <- VM.new size
    forM_ [0 .. size - 1] $ \i ->
        if (valid VU.! i) == 0
            then VM.write mvec i Nothing
            else VM.write mvec i (Just (vec V.! i))
    OptionalColumn <$> V.freeze mvec

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
writeSeparated c filepath df = withFile filepath WriteMode $ \handle -> do
    let (rows, _) = dataframeDimensions df
    let headers = map fst (L.sortBy (compare `on` snd) (M.toList (columnIndices df)))
    TIO.hPutStrLn handle (T.intercalate "," headers)
    forM_ [0 .. (rows - 1)] $ \i -> do
        let row = getRowAsText df i
        TIO.hPutStrLn handle (T.intercalate "," row)

getRowAsText :: DataFrame -> Int -> [T.Text]
getRowAsText df i = V.ifoldr go [] (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    go k (BoxedColumn (c :: V.Vector a)) acc = case c V.!? i of
        Just e -> textRep : acc
          where
            textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> e
                Nothing -> case typeRep @a of
                    App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
                        Just HRefl -> case testEquality t2 (typeRep @T.Text) of
                            Just Refl -> fromMaybe "null" e
                            Nothing -> (fromOptional . T.pack . show) e
                              where
                                fromOptional s
                                    | T.isPrefixOf "Just " s = T.drop (T.length "Just ") s
                                    | otherwise = "null"
                        Nothing -> (T.pack . show) e
                    _ -> (T.pack . show) e
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i
    go k (UnboxedColumn c) acc = case c VU.!? i of
        Just e -> T.pack (show e) : acc
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i
    go k (OptionalColumn (c :: V.Vector (Maybe a))) acc = case c V.!? i of
        Just e -> case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> fromMaybe T.empty e : acc
            Nothing -> maybe T.empty (T.pack . show) e : acc
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i

stripQuotes :: T.Text -> T.Text
stripQuotes txt =
    case T.uncons txt of
        Just ('"', rest) ->
            case T.unsnoc rest of
                Just (middle, '"') -> middle
                _ -> txt
        _ -> txt
