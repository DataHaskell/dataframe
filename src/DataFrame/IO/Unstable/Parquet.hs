{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}

module DataFrame.IO.Unstable.Parquet (readParquetUnstable) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.Bits (Bits (shiftL), (.|.))
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import Data.List (foldl', transpose)
import qualified Data.Map as Map
import Data.Maybe (fromJust, fromMaybe, isNothing)
import Data.Text (Text)
import qualified Data.Vector as Vector
import DataFrame.IO.Parquet.Dictionary (readDictVals)
import DataFrame.IO.Parquet.Page (decompressData)
import DataFrame.IO.Parquet.Types (DictVals)
import DataFrame.IO.Unstable.Parquet.PageParser (parsePage)
import DataFrame.IO.Unstable.Parquet.Thrift (
    ColumnChunk (..),
    ColumnMetaData (..),
    CompressionCodec (..),
    DictionaryPageHeader (..),
    FileMetadata (..),
    PageHeader (..),
    RowGroup (..),
    SchemaElement (..),
    pinchCompressionToParquetCompression,
    pinchThriftTypeToParquetType,
    unField,
 )
import DataFrame.IO.Unstable.Parquet.Utils (
    ColumnDescription,
    PageDescription (PageDescription),
    foldColumns,
    generateColumnDescriptions,
 )
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    Range (Range),
    ReaderIO (runReaderIO),
 )
import DataFrame.Internal.Column (Column)
import DataFrame.Internal.DataFrame (DataFrame (..))
import Pinch (decodeWithLeftovers)
import qualified Pinch
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream
import Streamly.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as Unfold
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

parseColumns :: (RandomAccess r, MonadIO r) => FileMetadata -> [Stream r Column]
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

    parse ::
        (RandomAccess r, MonadIO r) =>
        Stream r ColumnChunk -> ColumnDescription -> Stream r Column
    parse columnChunkStream description = Stream.unfoldEach (parseColumnChunk description) columnChunkStream

data ColumnChunkState
    = ColumnChunkState
    { remainingBytes :: !BS.ByteString
    , codec :: !CompressionCodec
    , dictionary :: !(Maybe DictVals)
    , parquetType :: !Int
    }

parseColumnChunk ::
    (RandomAccess r, MonadIO r) => ColumnDescription -> Unfold r ColumnChunk Column
parseColumnChunk description = Unfold.Unfold step inject
  where
    inject :: (RandomAccess r) => ColumnChunk -> r ColumnChunkState
    inject columnChunk = do
        let columnMetadata = fromJust $ unField $ cc_meta_data columnChunk
            dataOffset = unField $ cmd_data_page_offset columnMetadata
            dictOffset = fromMaybe dataOffset (unField $ cmd_dictionary_page_offset columnMetadata)
            startOffset = min dataOffset dictOffset
            compressedSize = unField $ cmd_total_compressed_size columnMetadata
            chunkCodec = unField $ cmd_codec columnMetadata
            parquetType = fromEnum $ pinchThriftTypeToParquetType (unField $ cmd_type columnMetadata)
            range = Range (fromIntegral startOffset) (fromIntegral compressedSize)

        rawBytes <- readBytes range
        return $ ColumnChunkState rawBytes chunkCodec Nothing parquetType

    step ::
        (RandomAccess r, MonadIO r) =>
        ColumnChunkState -> r (Unfold.Step ColumnChunkState Column)
    step (ColumnChunkState remaining chunkCodec dict parquetType) = do
        if BS.null remaining
            then return Unfold.Stop
            else case parsePageHeader remaining of
                Left e -> error $ show e
                Right (remainder, header) -> do
                    let compressedPageSize = fromIntegral $ unField $ ph_compressed_page_size header
                        (pageData, rest) = BS.splitAt compressedPageSize remainder
                    uncompressedData <-
                        liftIO $
                            decompressData (pinchCompressionToParquetCompression chunkCodec) pageData

                    case unField $ ph_dictionary_page_header header of
                        Just dictHeader -> do
                            {-
                               The dictionary page must be placed at the first position of the column chunk
                               if it is partly or completely dictionary encoded. At most one dictionary page
                               can be placed in a column chunk.
                               This allows us to maintain the parsed DictVals for the chunk and pass it along
                               to subsequent data pages.
                               https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L698C1-L712C2
                            -}
                            let numValues = fromIntegral $ unField $ diph_num_values dictHeader
                                newDict = readDictVals (toEnum parquetType) uncompressedData (Just numValues)
                            step (ColumnChunkState rest chunkCodec (Just newDict) parquetType)
                        Nothing -> do
                            -- It's a data page. Yield it.
                            column <-
                                parsePage
                                    description
                                    (PageDescription uncompressedData header chunkCodec dict parquetType)
                            return $ Unfold.Yield column (ColumnChunkState rest chunkCodec dict parquetType)

parsePageHeader :: BS.ByteString -> Either String (BS.ByteString, PageHeader)
parsePageHeader bytes = case decodeWithLeftovers Pinch.compactProtocol bytes of
    Left e -> Left e
    Right header -> Right header
