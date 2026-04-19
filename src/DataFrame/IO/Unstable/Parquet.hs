{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RankNTypes #-}

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
import DataFrame.IO.Unstable.Parquet.Page (
    boolReader,
    doubleReader,
    floatReader,
    int32Reader,
    int64Reader,
    int96Reader,
    nonNullableStream,
 )
import DataFrame.IO.Unstable.Parquet.Thrift (
    ColumnChunk (..),
    FileMetadata (..),
    RowGroup (..),
    SchemaElement (..),
    unField,
 )
import DataFrame.IO.Unstable.Parquet.Utils (
    ColumnDescription,
    foldColumns,
    generateColumnDescriptions,
 )
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    ReaderIO (runReaderIO),
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import qualified Pinch
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream
import Streamly.Data.Unfold (Unfold)
import Streamly.Internal.Data.Unfold ()
import qualified System.IO as IO

readParquetUnstable :: FilePath -> IO DataFrame
readParquetUnstable filepath = IO.withFile filepath IO.ReadMode $ \handle -> do
    runReaderIO parseParquet handle

parseParquet :: (RandomAccess r, MonadIO r) => r DataFrame
parseParquet = do
    metadata <- parseFileMetadata
    let vectorLength = fromIntegral . unField $ metadata.num_rows :: Int
        columnStreams = parseColumns metadata
    columnList <- mapM (foldColumns vectorLength) columnStreams
    let columns = Vector.fromListN (length columnList) columnList
        columnNames :: [Text]
        columnNames =
            map (unField . name)
                . filter
                    ( \se ->
                        (isNothing $ unField $ num_children se)
                            || unField se.num_children == Just 0
                    )
                $ unField metadata.schema
        columnIndices = Map.fromList $ zip columnNames [0 ..]
        dataframeDimensions = (vectorLength, length columnStreams)
    return $ DataFrame columns columnIndices dataframeDimensions Map.empty

parseFileMetadata ::
    (RandomAccess r) => r FileMetadata
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

parseColumns :: (RandomAccess r, MonadIO r) => FileMetadata -> [Stream r a]
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
    columnChunks :: (RandomAccess r) => FileMetadata -> [Stream r ColumnChunk]
    columnChunks =
        map Stream.fromList
            . transpose
            . map (unField . rg_columns)
            . unField
            . row_groups
    getColumnUnfold description
        | description.maxRepetitionLevel == 0 && description.maxDefinitionLevel == 0 =
            getNonNullableUnfold description
        | description.maxRepetitionLevel == 0 = error "TODO: implement nullable stream"
        | otherwise = error "TODO: implement maxRep > 0"
    parse ::
        (RandomAccess m, MonadIO m) =>
        Stream m ColumnChunk -> ColumnDescription -> Stream m a
    parse columnChunkStream description = case getColumnUnfold description of
        (ColumnUnfold columnUnfold) -> Stream.unfoldEach columnUnfold columnChunkStream

data ColumnUnfold where
    ColumnUnfold ::
        (RandomAccess m, MonadIO m) =>
        (forall a. Unfold m ColumnChunk a) -> ColumnUnfold

getNonNullableUnfold :: ColumnDescription -> ColumnUnfold
getNonNullableUnfold description = case description.colElementType of
    0 -> ColumnUnfold $ stream boolReader
    1 -> ColumnUnfold $ stream int32Reader
    2 -> ColumnUnfold $ stream int64Reader
    3 -> ColumnUnfold $ stream int96Reader
    4 -> ColumnUnfold $ stream floatReader
    5 -> ColumnUnfold $ stream doubleReader
    6 -> ColumnUnfold $ stream byteArrayReader
    7 -> case description.typeLength of
        Nothing -> error "FIXED_LEN_BYTE_ARRAY Requires type_length to be set"
        Just tl -> ColumnUnfold $ stream (fixedLenByteArrayReader tl)
    _ -> error "Unknown Parquet Type"
  where
    stream = nonNullableStream description
