{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet where

import Control.Exception (throw)
import Control.Monad
import qualified Data.ByteString as BSO
import Data.Either
import Data.IORef
import Data.Int
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import DataFrame.Errors (DataFrameException (ColumnNotFoundException))
import DataFrame.Internal.Binary (littleEndianWord32)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr, getColumns)
import qualified DataFrame.Operations.Core as DI
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as DS
import System.FilePath.Glob (glob)

import DataFrame.IO.Parquet.Dictionary
import DataFrame.IO.Parquet.Levels
import DataFrame.IO.Parquet.Page
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types
import System.Directory (doesDirectoryExist)

import qualified Data.Vector.Unboxed as VU
import DataFrame.IO.Parquet.Seeking
import System.FilePath ((</>))
import System.IO (IOMode (ReadMode))

-- Options -----------------------------------------------------------------

{- | Options for reading Parquet data.

These options are applied in this order:

1. predicate filtering
2. column projection
3. row range

Column selection for @selectedColumns@ uses leaf column names only.
-}
data ParquetReadOptions = ParquetReadOptions
    { selectedColumns :: Maybe [T.Text]
    {- ^ Columns to keep in the final dataframe. If set, only these columns are returned.
    Predicate-referenced columns are read automatically when needed and projected out after filtering.
    -}
    , predicate :: Maybe (Expr Bool)
    -- ^ Optional row filter expression applied before projection.
    , rowRange :: Maybe (Int, Int)
    -- ^ Optional row slice @(start, end)@ with start-inclusive/end-exclusive semantics.
    }
    deriving (Eq, Show)

{- | Default Parquet read options.

Equivalent to:

@
ParquetReadOptions
    { selectedColumns = Nothing
    , predicate = Nothing
    , rowRange = Nothing
    }
@
-}
defaultParquetReadOptions :: ParquetReadOptions
defaultParquetReadOptions =
    ParquetReadOptions
        { selectedColumns = Nothing
        , predicate = Nothing
        , rowRange = Nothing
        }

-- Public API --------------------------------------------------------------

{- | Read a parquet file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readParquet ".\/data\/mtcars.parquet"
@
-}
readParquet :: FilePath -> IO DataFrame
readParquet = readParquetWithOpts defaultParquetReadOptions

{- | Read a Parquet file using explicit read options.

==== __Example__
@
ghci> D.readParquetWithOpts
ghci|   (D.defaultParquetReadOptions{D.selectedColumns = Just ["id"], D.rowRange = Just (0, 10)})
ghci|   "./tests/data/alltypes_plain.parquet"
@

When @selectedColumns@ is set and @predicate@ references other columns, those predicate columns
are auto-included for decoding, then projected back to the requested output columns.
-}

{- | Strip Parquet encoding artifact names (REPEATED wrappers and their single
  list-element children) from a raw column path, leaving user-visible names.
-}
cleanColPath :: [SNode] -> [String] -> [String]
cleanColPath nodes path = go nodes path False
  where
    go _ [] _ = []
    go ns (p : ps) skipThis =
        case L.find (\n -> sName n == p) ns of
            Nothing -> []
            Just n
                | sRep n == REPEATED && not (null (sChildren n)) ->
                    let skipChildren = length (sChildren n) == 1
                     in go (sChildren n) ps skipChildren
                | skipThis ->
                    go (sChildren n) ps False
                | null (sChildren n) ->
                    [p]
                | otherwise ->
                    p : go (sChildren n) ps False

readParquetWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetWithOpts = _readParquetWithOpts Nothing

-- | Internal function to pass testing parameters
_readParquetWithOpts ::
    ForceNonSeekable -> ParquetReadOptions -> FilePath -> IO DataFrame
_readParquetWithOpts extraConfig opts path = withFileBufferedOrSeekable extraConfig path ReadMode $ \file -> do
    fileMetadata <- readMetadataFromHandle file
    let columnPaths = getColumnPaths (drop 1 $ schema fileMetadata)
    let columnNames = map fst columnPaths
    let leafNames = map (last . T.splitOn ".") columnNames
    let availableSelectedColumns = L.nub leafNames
    let predicateColumns = maybe [] (L.nub . getColumns) (predicate opts)
    let selectedColumnsForRead = case selectedColumns opts of
            Nothing -> Nothing
            Just selected -> Just (L.nub (selected ++ predicateColumns))
    let selectedColumnSet = S.fromList <$> selectedColumnsForRead
    let shouldReadColumn colName _ =
            case selectedColumnSet of
                Nothing -> True
                Just selected -> colName `S.member` selected

    case selectedColumnsForRead of
        Nothing -> pure ()
        Just requested ->
            let missing = requested L.\\ availableSelectedColumns
             in unless
                    (L.null missing)
                    ( throw
                        ( ColumnNotFoundException
                            (T.pack $ show missing)
                            "readParquetWithOpts"
                            availableSelectedColumns
                        )
                    )

    let totalRows = sum (map (fromIntegral . rowGroupNumRows) (rowGroups fileMetadata)) :: Int
    colMutMap <- newIORef (M.empty :: M.Map T.Text DI.MutableColumn)
    colOffMap <- newIORef (M.empty :: M.Map T.Text Int)
    lTypeMap <- newIORef (M.empty :: M.Map T.Text LogicalType)

    let schemaElements = schema fileMetadata
    let sNodes = parseAll (drop 1 schemaElements)
    let getTypeLength :: [String] -> Maybe Int32
        getTypeLength path = findTypeLength schemaElements path 0
          where
            findTypeLength [] _ _ = Nothing
            findTypeLength (s : ss) targetPath depth
                | map T.unpack (pathToElement s ss depth) == targetPath
                    && elementType s == STRING
                    && typeLength s > 0 =
                    Just (typeLength s)
                | otherwise =
                    findTypeLength ss targetPath (if numChildren s > 0 then depth + 1 else depth)

            pathToElement _ _ _ = []

    forM_ (rowGroups fileMetadata) $ \rowGroup -> do
        forM_ (zip (rowGroupColumns rowGroup) [0 ..]) $ \(colChunk, colIdx) -> do
            let metadata = columnMetaData colChunk
            let colPath = columnPathInSchema metadata
            let cleanPath = cleanColPath sNodes colPath
            let colLeafName =
                    if null cleanPath
                        then T.pack $ "col_" ++ show colIdx
                        else T.pack $ last cleanPath
            let colFullName =
                    if null cleanPath
                        then colLeafName
                        else T.intercalate "." $ map T.pack cleanPath

            when (shouldReadColumn colLeafName colPath) $ do
                let colDataPageOffset = columnDataPageOffset metadata
                let colDictionaryPageOffset = columnDictionaryPageOffset metadata
                let colStart =
                        if colDictionaryPageOffset > 0 && colDataPageOffset > colDictionaryPageOffset
                            then colDictionaryPageOffset
                            else colDataPageOffset
                let colLength = columnTotalCompressedSize metadata

                columnBytes <-
                    seekAndReadBytes
                        (Just (AbsoluteSeek, fromIntegral colStart))
                        (fromIntegral colLength)
                        file

                pages <- readAllPages (columnCodec metadata) columnBytes

                let maybeTypeLength =
                        if columnType metadata == PFIXED_LEN_BYTE_ARRAY
                            then getTypeLength colPath
                            else Nothing

                let primaryEncoding = maybe EPLAIN fst (L.uncons (columnEncodings metadata))

                let schemaTail = drop 1 (schema fileMetadata)
                let (maxDef, maxRep) = levelsForPath schemaTail colPath
                let lType =
                        maybe
                            LOGICAL_TYPE_UNKNOWN
                            logicalType
                            (findLeafSchema schemaTail colPath)
                column <-
                    processColumnPages
                        (maxDef, maxRep)
                        pages
                        (columnType metadata)
                        primaryEncoding
                        maybeTypeLength
                        lType

                mutMapSnap <- readIORef colMutMap
                case M.lookup colFullName mutMapSnap of
                    Nothing -> do
                        mc <- DI.newMutableColumn totalRows column
                        DI.copyIntoMutableColumn mc 0 column
                        modifyIORef' colMutMap (M.insert colFullName mc)
                        modifyIORef' colOffMap (M.insert colFullName (DI.columnLength column))
                    Just mc -> do
                        off <- (M.! colFullName) <$> readIORef colOffMap
                        DI.copyIntoMutableColumn mc off column
                        modifyIORef' colOffMap (M.adjust (+ DI.columnLength column) colFullName)
                modifyIORef' lTypeMap (M.insert colFullName lType)

    finalMutMap <- readIORef colMutMap
    finalColMap <-
        M.traverseWithKey (\_ mc -> DI.freezeMutableColumn mc) finalMutMap
    finalLTypeMap <- readIORef lTypeMap
    let orderedColumns =
            map
                ( \name ->
                    ( name
                    , applyLogicalType (finalLTypeMap M.! name) $ finalColMap M.! name
                    )
                )
                (filter (`M.member` finalColMap) columnNames)

    pure $ applyReadOptions opts (DI.fromNamedColumns orderedColumns)

{- | Read Parquet files from a directory or glob path.

This is equivalent to calling 'readParquetFilesWithOpts' with 'defaultParquetReadOptions'.
-}
readParquetFiles :: FilePath -> IO DataFrame
readParquetFiles = readParquetFilesWithOpts defaultParquetReadOptions

{- | Read multiple Parquet files (directory or glob) using explicit options.

If @path@ is a directory, all non-directory entries are read.
If @path@ is a glob, matching files are read.

For multi-file reads, @rowRange@ is applied once after concatenation (global range semantics).

==== __Example__
@
ghci> D.readParquetFilesWithOpts
ghci|   (D.defaultParquetReadOptions{D.selectedColumns = Just ["id"], D.rowRange = Just (0, 5)})
ghci|   "./tests/data/alltypes_plain*.parquet"
@
-}
readParquetFilesWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetFilesWithOpts opts path = do
    isDir <- doesDirectoryExist path

    let pat = if isDir then path </> "*.parquet" else path

    matches <- glob pat

    files <- filterM (fmap not . doesDirectoryExist) matches

    case files of
        [] ->
            error $
                "readParquetFiles: no parquet files found for " ++ path
        _ -> do
            let optsWithoutRowRange = opts{rowRange = Nothing}
            dfs <- mapM (readParquetWithOpts optsWithoutRowRange) files
            pure (applyRowRange opts (mconcat dfs))

-- Options application -----------------------------------------------------

applyRowRange :: ParquetReadOptions -> DataFrame -> DataFrame
applyRowRange opts df =
    maybe df (`DS.range` df) (rowRange opts)

applySelectedColumns :: ParquetReadOptions -> DataFrame -> DataFrame
applySelectedColumns opts df =
    maybe df (`DS.select` df) (selectedColumns opts)

applyPredicate :: ParquetReadOptions -> DataFrame -> DataFrame
applyPredicate opts df =
    maybe df (`DS.filterWhere` df) (predicate opts)

applyReadOptions :: ParquetReadOptions -> DataFrame -> DataFrame
applyReadOptions opts =
    applyRowRange opts
        . applySelectedColumns opts
        . applyPredicate opts

-- File and metadata parsing -----------------------------------------------

-- | read the file in memory at once, parse magicString and return the entire file ByteString
readMetadataFromPath :: FilePath -> IO (FileMetadata, BSO.ByteString)
readMetadataFromPath path = do
    contents <- BSO.readFile path
    let (size, magicString) = readMetadataSizeFromFooter contents
    when (magicString /= "PAR1") $ error "Invalid Parquet file"
    meta <- readMetadata contents size
    pure (meta, contents)

-- | read from the end of the file, parse magicString and return the entire file ByteString
readMetadataFromHandle :: FileBufferedOrSeekable -> IO FileMetadata
readMetadataFromHandle sh = do
    footerBs <- readLastBytes (fromIntegral footerSize) sh
    let (size, magicString) = readMetadataSizeFromFooterSlice footerBs
    when (magicString /= "PAR1") $ error "Invalid Parquet file"
    readMetadataByHandleMetaSize sh size

-- | Takes the last 8 bit of the file to parse metadata size and magic string
readMetadataSizeFromFooterSlice :: BSO.ByteString -> (Int, BSO.ByteString)
readMetadataSizeFromFooterSlice contents =
    let
        size = fromIntegral (littleEndianWord32 contents)
        magicString = BSO.take 4 (BSO.drop 4 contents)
     in
        (size, magicString)

readMetadataSizeFromFooter :: BSO.ByteString -> (Int, BSO.ByteString)
readMetadataSizeFromFooter = readMetadataSizeFromFooterSlice . BSO.takeEnd 8

-- Schema navigation -------------------------------------------------------

getColumnPaths :: [SchemaElement] -> [(T.Text, Int)]
getColumnPaths schemaElements =
    let nodes = parseAll schemaElements
     in go nodes 0 [] False
  where
    go [] _ _ _ = []
    go (n : ns) idx path skipThis
        | null (sChildren n) =
            let newPath = if skipThis then path else path ++ [T.pack (sName n)]
                fullPath = T.intercalate "." newPath
             in (fullPath, idx) : go ns (idx + 1) path skipThis
        | sRep n == REPEATED =
            let skipChildren = length (sChildren n) == 1
                childLeaves = go (sChildren n) idx path skipChildren
             in childLeaves ++ go ns (idx + length childLeaves) path skipThis
        | skipThis =
            let childLeaves = go (sChildren n) idx path False
             in childLeaves ++ go ns (idx + length childLeaves) path skipThis
        | otherwise =
            let subPath = path ++ [T.pack (sName n)]
                childLeaves = go (sChildren n) idx subPath False
             in childLeaves ++ go ns (idx + length childLeaves) path skipThis

findLeafSchema :: [SchemaElement] -> [String] -> Maybe SchemaElement
findLeafSchema elems path =
    case go (parseAll elems) path of
        Just node -> L.find (\e -> T.unpack (elementName e) == sName node) elems
        Nothing -> Nothing
  where
    go [] _ = Nothing
    go _ [] = Nothing
    go nodes [p] = L.find (\n -> sName n == p) nodes
    go nodes (p : ps) = L.find (\n -> sName n == p) nodes >>= \n -> go (sChildren n) ps

-- Page decoding -----------------------------------------------------------

processColumnPages ::
    (Int, Int) ->
    [Page] ->
    ParquetType ->
    ParquetEncoding ->
    Maybe Int32 ->
    LogicalType ->
    IO DI.Column
processColumnPages (maxDef, maxRep) pages pType _ maybeTypeLength lType = do
    let dictPages = filter isDictionaryPage pages
    let dataPages = filter isDataPage pages

    let dictValsM =
            case dictPages of
                [] -> Nothing
                (dictPage : _) ->
                    case pageTypeHeader (pageHeader dictPage) of
                        DictionaryPageHeader{..} ->
                            let countForBools =
                                    if pType == PBOOLEAN
                                        then Just dictionaryPageHeaderNumValues
                                        else maybeTypeLength
                             in Just (readDictVals pType (pageBytes dictPage) countForBools)
                        _ -> Nothing

    cols <- forM dataPages $ \page -> do
        let bs0 = pageBytes page
        case pageTypeHeader (pageHeader page) of
            DataPageHeader{..} -> do
                let n = fromIntegral dataPageHeaderNumValues
                    (defLvls, repLvls, afterLvls) = readLevelsV1 n maxDef maxRep bs0
                    nPresent = length (filter (== maxDef) defLvls)
                decodePageData
                    dictValsM
                    (maxDef, maxRep)
                    pType
                    maybeTypeLength
                    dataPageHeaderEncoding
                    defLvls
                    repLvls
                    nPresent
                    afterLvls
                    "v1"
            DataPageHeaderV2{..} -> do
                let n = fromIntegral dataPageHeaderV2NumValues
                    (defLvls, repLvls, afterLvls) =
                        readLevelsV2
                            n
                            maxDef
                            maxRep
                            definitionLevelByteLength
                            repetitionLevelByteLength
                            bs0
                    nPresent
                        | dataPageHeaderV2NumNulls > 0 =
                            fromIntegral (dataPageHeaderV2NumValues - dataPageHeaderV2NumNulls)
                        | otherwise = length (filter (== maxDef) defLvls)
                decodePageData
                    dictValsM
                    (maxDef, maxRep)
                    pType
                    maybeTypeLength
                    dataPageHeaderV2Encoding
                    defLvls
                    repLvls
                    nPresent
                    afterLvls
                    "v2"

            -- Cannot happen as these are filtered out by isDataPage above
            DictionaryPageHeader{} -> error "processColumnPages: impossible DictionaryPageHeader"
            INDEX_PAGE_HEADER -> error "processColumnPages: impossible INDEX_PAGE_HEADER"
            PAGE_TYPE_HEADER_UNKNOWN -> error "processColumnPages: impossible PAGE_TYPE_HEADER_UNKNOWN"
    pure $ DI.concatManyColumns cols

decodePageData ::
    Maybe DictVals ->
    (Int, Int) ->
    ParquetType ->
    Maybe Int32 ->
    ParquetEncoding ->
    [Int] ->
    [Int] ->
    Int ->
    BSO.ByteString ->
    String ->
    IO DI.Column
decodePageData dictValsM (maxDef, maxRep) pType maybeTypeLength encoding defLvls repLvls nPresent afterLvls versionLabel =
    case encoding of
        EPLAIN ->
            case pType of
                PBOOLEAN ->
                    let (vals, _) = readNBool nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepBool maxRep maxDef repLvls defLvls vals
                                else toMaybeBool maxDef defLvls vals
                PINT32
                    | maxDef == 0
                    , maxRep == 0 ->
                        pure $ DI.fromUnboxedVector (readNInt32Vec nPresent afterLvls)
                PINT32 ->
                    let (vals, _) = readNInt32 nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepInt32 maxRep maxDef repLvls defLvls vals
                                else toMaybeInt32 maxDef defLvls vals
                PINT64
                    | maxDef == 0
                    , maxRep == 0 ->
                        pure $ DI.fromUnboxedVector (readNInt64Vec nPresent afterLvls)
                PINT64 ->
                    let (vals, _) = readNInt64 nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepInt64 maxRep maxDef repLvls defLvls vals
                                else toMaybeInt64 maxDef defLvls vals
                PINT96 ->
                    let (vals, _) = readNInt96Times nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepUTCTime maxRep maxDef repLvls defLvls vals
                                else toMaybeUTCTime maxDef defLvls vals
                PFLOAT
                    | maxDef == 0
                    , maxRep == 0 ->
                        pure $ DI.fromUnboxedVector (readNFloatVec nPresent afterLvls)
                PFLOAT ->
                    let (vals, _) = readNFloat nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepFloat maxRep maxDef repLvls defLvls vals
                                else toMaybeFloat maxDef defLvls vals
                PDOUBLE
                    | maxDef == 0
                    , maxRep == 0 ->
                        pure $ DI.fromUnboxedVector (readNDoubleVec nPresent afterLvls)
                PDOUBLE ->
                    let (vals, _) = readNDouble nPresent afterLvls
                     in pure $
                            if maxRep > 0
                                then stitchForRepDouble maxRep maxDef repLvls defLvls vals
                                else toMaybeDouble maxDef defLvls vals
                PBYTE_ARRAY ->
                    let (raws, _) = readNByteArrays nPresent afterLvls
                        texts = map decodeUtf8Lenient raws
                     in pure $
                            if maxRep > 0
                                then stitchForRepText maxRep maxDef repLvls defLvls texts
                                else toMaybeText maxDef defLvls texts
                PFIXED_LEN_BYTE_ARRAY ->
                    case maybeTypeLength of
                        Just len ->
                            let (raws, _) = splitFixed nPresent (fromIntegral len) afterLvls
                                texts = map decodeUtf8Lenient raws
                             in pure $
                                    if maxRep > 0
                                        then stitchForRepText maxRep maxDef repLvls defLvls texts
                                        else toMaybeText maxDef defLvls texts
                        Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type length"
                PARQUET_TYPE_UNKNOWN -> error "Cannot read unknown Parquet type"
        ERLE_DICTIONARY -> decodeDictV1 dictValsM maxDef maxRep repLvls defLvls nPresent afterLvls
        EPLAIN_DICTIONARY -> decodeDictV1 dictValsM maxDef maxRep repLvls defLvls nPresent afterLvls
        other -> error ("Unsupported " ++ versionLabel ++ " encoding: " ++ show other)

-- Logical type conversion -------------------------------------------------

applyLogicalType :: LogicalType -> DI.Column -> DI.Column
applyLogicalType (TimestampType _ unit) col =
    fromRight col $
        DI.mapColumn
            (microsecondsToUTCTime . (* (1_000_000 `div` unitDivisor unit)))
            col
applyLogicalType (DecimalType precision scale) col
    | precision <= 9 = case DI.toVector @Int32 @VU.Vector col of
        Right xs ->
            DI.fromUnboxedVector $
                VU.map (\raw -> fromIntegral @Int32 @Double raw / 10 ^ scale) xs
        Left _ -> col
    | precision <= 18 = case DI.toVector @Int64 @VU.Vector col of
        Right xs ->
            DI.fromUnboxedVector $
                VU.map (\raw -> fromIntegral @Int64 @Double raw / 10 ^ scale) xs
        Left _ -> col
    | otherwise = col
applyLogicalType _ col = col

microsecondsToUTCTime :: Int64 -> UTCTime
microsecondsToUTCTime us =
    posixSecondsToUTCTime (fromIntegral us / 1_000_000)

unitDivisor :: TimeUnit -> Int64
unitDivisor MILLISECONDS = 1_000
unitDivisor MICROSECONDS = 1_000_000
unitDivisor NANOSECONDS = 1_000_000_000
unitDivisor TIME_UNIT_UNKNOWN = 1

applyScale :: Int32 -> Int32 -> Double
applyScale scale rawValue =
    fromIntegral rawValue / (10 ^ scale)
