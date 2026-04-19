{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Unstable.Parquet (readParquetUnstable) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.Bits (Bits (shiftL), (.|.))
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import Data.List (foldl', transpose)
import qualified Data.Map as Map
import Data.Maybe (isNothing)
import Data.Text (Text)
import qualified Data.Vector as Vector
import DataFrame.IO.Parquet.Seeking (withFileBufferedOrSeekable)
import DataFrame.IO.Unstable.Parquet.Page (
    PageDecoder,
    boolDecoder,
    byteArrayDecoder,
    doubleDecoder,
    fixedLenByteArrayDecoder,
    floatDecoder,
    int32Decoder,
    int64Decoder,
    int96Decoder,
    nonNullableChunk,
    nullableChunk,
    repeatedChunk,
 )
import DataFrame.IO.Unstable.Parquet.Thrift (
    ColumnChunk (..),
    FileMetadata (..),
    RowGroup (..),
    SchemaElement (..),
    ThriftType (..),
    unField,
 )
import DataFrame.IO.Unstable.Parquet.Utils (
    ColumnDescription (..),
    foldNonNullable,
    foldNullable,
    foldRepeated,
    generateColumnDescriptions,
    getColumnNames,
 )
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    ReaderIO (runReaderIO),
 )
import DataFrame.Internal.Column (Column, Columnable)
import DataFrame.Internal.DataFrame (DataFrame (..))
import qualified Pinch
import qualified Streamly.Data.Stream as Stream
import qualified System.IO as IO

readParquetUnstable :: FilePath -> IO DataFrame
readParquetUnstable filepath = withFileBufferedOrSeekable Nothing filepath IO.ReadMode $ \handle -> do
    runReaderIO parseParquet handle

parseParquet :: (RandomAccess m, MonadIO m) => m DataFrame
parseParquet = do
    metadata <- parseFileMetadata
    let vectorLength = fromIntegral . unField $ metadata.num_rows :: Int
        columnActions = parseColumns metadata
    columnList <- sequence columnActions
    let columns = Vector.fromListN (length columnList) columnList
        columnNames :: [Text]
        columnNames = getColumnNames (drop 1 $ unField metadata.schema)
        columnIndices = Map.fromList $ zip columnNames [0 ..]
        dataframeDimensions = (vectorLength, length columnActions)
    return $ DataFrame columns columnIndices dataframeDimensions Map.empty

parseFileMetadata ::
    (RandomAccess m) => m FileMetadata
parseFileMetadata = do
    footerOffset <- readSuffix 8
    let size = getMetadataSize footerOffset
    rawMetadata <- readSuffix (size + 8) <&> BS.take size
    case Pinch.decode Pinch.compactProtocol rawMetadata of
        Left e -> error $ show e
        Right metadata -> return metadata
  where
    getMetadataSize footer =
        let sizes :: [Int]
            sizes = map (fromIntegral . BS.index footer) [0 .. 3]
         in foldl' (.|.) 0 $ zipWith shiftL sizes [0, 8 .. 24]

parseColumns :: (RandomAccess m, MonadIO m) => FileMetadata -> [m Column]
parseColumns metadata =
    let columnDescriptions = generateColumnDescriptions $ unField $ schema metadata
        colChunks = columnChunks metadata
        _numColumns = length colChunks
        _numDescs = length columnDescriptions
     in if _numColumns /= _numDescs
            then
                error $
                    "Column count mismatch: got "
                        <> show _numColumns
                        <> " columns but the schema implied "
                        <> show _numDescs
                        <> " columns"
            else zipWith parse colChunks columnDescriptions
  where
    -- One list of ColumnChunks per column (across all row groups).
    columnChunks :: FileMetadata -> [[ColumnChunk]]
    columnChunks =
        transpose
            . map (unField . rg_columns)
            . unField
            . row_groups

    parse ::
        (RandomAccess m, MonadIO m) =>
        [ColumnChunk] ->
        ColumnDescription ->
        m Column
    parse chunks description
        | description.maxRepetitionLevel == 0 && description.maxDefinitionLevel == 0 =
            getNonNullableColumn description chunks
        | description.maxRepetitionLevel == 0 =
            getNullableColumn description chunks
        | otherwise = getRepeatedColumn description chunks

getNonNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNonNullableColumn description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> go boolDecoder
        Just (INT32 _) -> go int32Decoder
        Just (INT64 _) -> go int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> go floatDecoder
        Just (DOUBLE _) -> go doubleDecoder
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
        foldNonNullable $
            Stream.mapM (nonNullableChunk description decoder) (Stream.fromList chunks)

getNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNullableColumn description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> go boolDecoder
        Just (INT32 _) -> go int32Decoder
        Just (INT64 _) -> go int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> go floatDecoder
        Just (DOUBLE _) -> go doubleDecoder
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
        foldNullable maxDef $
            Stream.mapM (nullableChunk description decoder) (Stream.fromList chunks)

getRepeatedColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getRepeatedColumn description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> go boolDecoder
        Just (INT32 _) -> go int32Decoder
        Just (INT64 _) -> go int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> go floatDecoder
        Just (DOUBLE _) -> go doubleDecoder
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
            Stream.mapM (repeatedChunk description decoder) (Stream.fromList chunks)
