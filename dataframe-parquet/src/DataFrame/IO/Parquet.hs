{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet where

import Control.Exception (throw)
import Control.Monad
import Control.Monad.IO.Class (MonadIO (..))
import Data.Bits (Bits (shiftL), (.|.))
import qualified Data.ByteString as BS
import Data.Either (fromRight)
import Data.Functor ((<&>))
import Data.Int (Int32, Int64)
#if !MIN_VERSION_base(4,20,0)
import Data.List (foldl', transpose)
#else
import Data.List (transpose)
#endif
import qualified Data.List as L
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Time (UTCTime)
import Data.Time.Clock.POSIX (posixSecondsToUTCTime)
import qualified Data.Vector as Vector
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors (DataFrameException (ColumnsNotFoundException))
import DataFrame.IO.Parquet.Page (
    PageDecoder,
    UnboxedPageDecoder,
    boolDecoder,
    byteArrayDecoder,
    doubleDecoder,
    fixedLenByteArrayDecoder,
    floatDecoder,
    int32Decoder,
    int64Decoder,
    int96Decoder,
    readPages,
 )
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable,
    ForceNonSeekable,
    withFileBufferedOrSeekable,
 )
import DataFrame.IO.Parquet.Thrift (
    ColumnChunk (..),
    DecimalType (..),
    FileMetadata (..),
    LogicalType (..),
    RowGroup (..),
    ThriftType (..),
    TimeUnit (..),
    TimestampType (..),
    unField,
 )
import DataFrame.IO.Parquet.Utils (
    ColumnDescription (..),
    foldNonNullable,
    foldNonNullableUnboxed,
    foldNullable,
    foldNullableUnboxed,
    foldRepeated,
    foldRepeatedUnboxed,
    generateColumnDescriptions,
    getColumnNames,
 )
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    ReaderIO (runReaderIO),
 )
import DataFrame.Internal.Column (Column, Columnable)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Expression (Expr, getColumns)
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as DS
import qualified Pinch
import qualified Streamly.Data.Stream as Stream
import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import System.IO (IOMode (ReadMode))

-- Options -----------------------------------------------------------------

{- | Options for reading Parquet data.

These options are applied in this order:

1. predicate filtering
2. column projection
3. row range
4. safe column promotion

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
    , safeColumns :: Bool
    -- ^ When True, every column is promoted to OptionalColumn after read, regardless of nullability in the schema.
    }
    deriving (Show)

{- | Default Parquet read options.

Equivalent to:

@
ParquetReadOptions
    { selectedColumns = Nothing
    , predicate = Nothing
    , rowRange = Nothing
    , safeColumns = False
    }
@
-}
defaultParquetReadOptions :: ParquetReadOptions
defaultParquetReadOptions =
    ParquetReadOptions
        { selectedColumns = Nothing
        , predicate = Nothing
        , rowRange = Nothing
        , safeColumns = False
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
readParquetWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetWithOpts = _readParquetWithOpts Nothing

-- | Internal entry point used by tests to force non-seekable mode.
_readParquetWithOpts ::
    ForceNonSeekable -> ParquetReadOptions -> FilePath -> IO DataFrame
_readParquetWithOpts extraConfig opts path =
    withFileBufferedOrSeekable extraConfig path ReadMode $ \file ->
        runReaderIO (parseParquetWithOpts opts) file

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

-- Core parsing pipeline ---------------------------------------------------

{- | Parse a Parquet file via the 'RandomAccess' handle, applying all
read options. This is the central parsing entry point used by
'_readParquetWithOpts'.
-}
parseParquetWithOpts ::
    (RandomAccess m, MonadIO m) =>
    ParquetReadOptions ->
    m DataFrame
parseParquetWithOpts opts = do
    metadata <- parseFileMetadata

    let schemaElems = unField metadata.schema
        allNames = getColumnNames (drop 1 schemaElems)
        leafNames = L.nub (map (last . T.splitOn ".") allNames)
        predicateColumns = maybe [] (L.nub . getColumns) (predicate opts)
        selectedColumnsForRead = case selectedColumns opts of
            Nothing -> Nothing
            Just selected -> Just (L.nub (selected ++ predicateColumns))

    -- TODO: When selectedColumnsForRead is Just, pass the set of required
    -- column indices into the chunk parsers so that RandomAccess reads are
    -- skipped for columns not in the selection, rather than decoding all
    -- columns and projecting afterward.

    -- TODO: When rowRange is set, compute cumulative row offsets from
    -- rg_num_rows in each RowGroup and skip any group whose row interval does
    -- not overlap the requested range, avoiding all decoding for those groups.

    -- TODO: When predicate is set, inspect cmd_statistics min/max values for
    -- predicate-referenced columns in each RowGroup and skip groups where
    -- statistics prove the predicate cannot be satisfied.

    -- Validate selected columns
    case selectedColumnsForRead of
        Nothing -> pure ()
        Just requested ->
            let missing = requested L.\\ leafNames
             in unless (L.null missing) $
                    liftIO $
                        throw
                            ( ColumnsNotFoundException
                                missing
                                "readParquetWithOpts"
                                leafNames
                            )

    let descriptions = generateColumnDescriptions schemaElems
        chunks = columnChunksForAll metadata
        nCols = length chunks
        nDescs = length descriptions

    unless (nCols == nDescs) $
        error $
            "Column count mismatch: got "
                <> show nCols
                <> " columns but schema implied "
                <> show nDescs
                <> " columns"

    -- Some files omit the top-level num_rows field; fall back to summing row-group counts.
    let topLevelRows = fromIntegral . unField $ metadata.num_rows :: Int
        rgRows =
            sum $ map (fromIntegral . unField . rg_num_rows) (unField metadata.row_groups) ::
                Int
        vectorLength = if topLevelRows > 0 then topLevelRows else rgRows

    rawCols <- zipWithM (parseColumnChunks vectorLength) chunks descriptions

    let finalCols = zipWith applyDescLogicalType descriptions rawCols
        indices = Map.fromList $ zip allNames [0 ..]
        dimensions = (vectorLength, length finalCols)

    let df =
            DataFrame
                (Vector.fromListN (length finalCols) finalCols)
                indices
                dimensions
                Map.empty

    return (applyReadOptions opts df)

{- | Parse the file-level Thrift metadata from the Parquet file footer.
Validates the trailing 4-byte magic marker (\"PAR1\") before decoding.
-}
parseFileMetadata :: (RandomAccess m) => m FileMetadata
parseFileMetadata = do
    footerBytes <- readSuffix 8
    let magic = BS.drop 4 footerBytes
    when (magic /= "PAR1") $
        error
            ( "Not a valid Parquet file: expected magic bytes \"PAR1\", got "
                ++ show magic
            )
    let size = getMetadataSize footerBytes
    rawMetadata <- readSuffix (size + 8) <&> BS.take size
    case Pinch.decode Pinch.compactProtocol rawMetadata of
        Left e -> error $ "Failed to parse Parquet metadata: " ++ show e
        Right metadata -> return metadata
  where
    getMetadataSize footer =
        let sizes :: [Int]
            sizes = map (fromIntegral . BS.index footer) [0 .. 3]
         in foldl' (.|.) 0 $ zipWith shiftL sizes [0, 8 .. 24]

-- | Read the file metadata from a Parquet file at the given path.
readMetadataFromPath :: FilePath -> IO FileMetadata
readMetadataFromPath path =
    withFileBufferedOrSeekable Nothing path ReadMode $
        runReaderIO parseFileMetadata

-- | Read only the file metadata from an open 'FileBufferedOrSeekable' handle.
readMetadataFromHandle :: FileBufferedOrSeekable -> IO FileMetadata
readMetadataFromHandle = runReaderIO parseFileMetadata

-- | Collect column chunks per column (transposed across all row groups).
columnChunksForAll :: FileMetadata -> [[ColumnChunk]]
columnChunksForAll =
    transpose . map (unField . rg_columns) . unField . row_groups

-- | Dispatch a column's chunks to the correct decoder path.
parseColumnChunks ::
    (RandomAccess m, MonadIO m) =>
    Int ->
    [ColumnChunk] ->
    ColumnDescription ->
    m Column
parseColumnChunks totalRows chunks description
    | description.maxRepetitionLevel == 0 && description.maxDefinitionLevel == 0 =
        getNonNullableColumn totalRows description chunks
    | description.maxRepetitionLevel == 0 =
        getNullableColumn totalRows description chunks
    | otherwise =
        getRepeatedColumn description chunks

-- | Decode a required (non-nullable, non-repeated) column.
getNonNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    Int ->
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNonNullableColumn totalRows description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> go byteArrayDecoder
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    go ::
        forall a.
        (Columnable a) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldNonNullable totalRows $
            (\(vs, _, _) -> vs)
                <$> Stream.unfoldEach (readPages description decoder) (Stream.fromList chunks)

    unboxedGo ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldNonNullableUnboxed totalRows $
            (\(vs, _, _) -> vs)
                <$> Stream.unfoldEach
                    (readPages description decoder)
                    (Stream.fromList chunks)

-- | Decode an optional (nullable) column.
getNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    Int ->
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNullableColumn totalRows description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> go byteArrayDecoder
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    maxDef :: Int
    maxDef = fromIntegral description.maxDefinitionLevel

    go ::
        forall a.
        (Columnable a) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldNullable maxDef totalRows $
            (\(vs, ds, _) -> (vs, ds))
                <$> Stream.unfoldEach (readPages description decoder) (Stream.fromList chunks)
    unboxedGo ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldNullableUnboxed maxDef totalRows $
            (\(vs, ds, _) -> (vs, ds))
                <$> Stream.unfoldEach
                    (readPages description decoder)
                    (Stream.fromList chunks)

-- | Decode a repeated (list/nested) column.
getRepeatedColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getRepeatedColumn description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> go byteArrayDecoder
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    maxRep :: Int
    maxRep = fromIntegral description.maxRepetitionLevel
    maxDef :: Int
    maxDef = fromIntegral description.maxDefinitionLevel

    go ::
        forall a.
        ( Columnable a
        , Columnable (Maybe [Maybe a])
        , Columnable (Maybe [Maybe [Maybe a]])
        , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
        ) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldRepeated maxRep maxDef $
            Stream.unfoldEach (readPages description decoder) (Stream.fromList chunks)

    unboxedGo ::
        forall a.
        ( VU.Unbox a
        , Columnable a
        , Columnable (Maybe [Maybe a])
        , Columnable (Maybe [Maybe [Maybe a]])
        , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
        ) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldRepeatedUnboxed maxRep maxDef $
            Stream.unfoldEach
                (readPages description decoder)
                (Stream.fromList chunks)

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

applySafeRead :: ParquetReadOptions -> DataFrame -> DataFrame
applySafeRead opts df
    | safeColumns opts = df{columns = Vector.map DI.ensureOptional (columns df)}
    | otherwise = df

applyReadOptions :: ParquetReadOptions -> DataFrame -> DataFrame
applyReadOptions opts =
    applySafeRead opts
        . applyRowRange opts
        . applySelectedColumns opts
        . applyPredicate opts

-- Logical type conversion -------------------------------------------------

{- | Apply a column-description's logical type annotation to convert raw
decoded values (e.g. millisecond integers → 'UTCTime').
-}
applyDescLogicalType :: ColumnDescription -> DI.Column -> DI.Column
applyDescLogicalType desc = applyLogicalType (colLogicalType desc)

applyLogicalType :: Maybe LogicalType -> DI.Column -> DI.Column
applyLogicalType (Just (LT_TIMESTAMP f)) col =
    let ts = unField f
        unit = unField ts.timestamp_unit
        divisor = case unit of
            MILLIS _ -> 1_000
            MICROS _ -> 1_000_000
            NANOS _ -> 1_000_000_000
     in fromRight col $
            DI.mapColumn
                (microsecondsToUTCTime . (* (1_000_000 `div` divisor)))
                col
applyLogicalType (Just (LT_DECIMAL f)) col =
    let dt = unField f
        scale = unField dt.decimal_scale
        precision = unField dt.decimal_precision
     in if precision <= 9
            then case DI.toVector @Int32 @VU.Vector col of
                Right xs ->
                    DI.fromUnboxedVector $
                        VU.map (\raw -> fromIntegral @Int32 @Double raw / 10 ^ scale) xs
                Left _ -> col
            else
                if precision <= 18
                    then case DI.toVector @Int64 @VU.Vector col of
                        Right xs ->
                            DI.fromUnboxedVector $
                                VU.map (\raw -> fromIntegral @Int64 @Double raw / 10 ^ scale) xs
                        Left _ -> col
                    else col
applyLogicalType _ col = col

microsecondsToUTCTime :: Int64 -> UTCTime
microsecondsToUTCTime us =
    posixSecondsToUTCTime (fromIntegral us / 1_000_000)
