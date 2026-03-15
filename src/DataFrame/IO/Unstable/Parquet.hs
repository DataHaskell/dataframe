
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE GADTs #-}

module DataFrame.IO.Unstable.Parquet (readParquet) where

import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), ReaderIO (runReaderIO), Range (Range))
import qualified System.IO as IO
import DataFrame.IO.Unstable.Parquet.Thrift (
  FileMetadata (..),
  ColumnChunk (..),
  RowGroup (..),
  ColumnMetaData(..),
  PageHeader(..),
  DictionaryPageHeader(..),
  CompressionCodec(..),
  unField, pinchCompressionToParquetCompression
  , pinchThriftTypeToParquetType
  )
import DataFrame.IO.Unstable.Parquet.Utils (ColumnDescription, generateColumnDescriptions)
import DataFrame.IO.Parquet.Types (DictVals)
import DataFrame.IO.Parquet.Dictionary (readDictVals)
import DataFrame.IO.Parquet.Page (decompressData)
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import qualified Pinch
import Data.Bits (Bits(shiftL), (.|.))
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream
import Streamly.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as Unfold
import Control.Monad.IO.Class (MonadIO(..))
import DataFrame.IO.Unstable.Parquet.PageParser (parsePage)
import DataFrame.Internal.Column (Columnable)
import Data.List (transpose)
import Data.Maybe (fromMaybe, fromJust)
import Type.Reflection (Typeable)
import Pinch (decodeWithLeftovers)

readParquet filepath = IO.withFile filepath IO.ReadMode $ \handle -> do
  fileMetadata <- runReaderIO parseFileMetadata handle
  print fileMetadata

data ColumnStream r where
  ColumnStream :: forall a r. (Columnable a) => Stream r a -> ColumnStream r

doTheThing :: (RandomAccess r, MonadIO r) => r [ColumnStream r]
doTheThing = do
  metadata <- parseFileMetadata
  return (parseColumns metadata)

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

parseColumns :: (RandomAccess r, MonadIO r) => FileMetadata -> [ColumnStream r]
parseColumns metadata = 
  let columnDescriptions = generateColumnDescriptions $ unField $ schema metadata
      colChunks = columnChunks metadata
      _numColumns = length colChunks
      _numDescs = length columnDescriptions
  in if _numColumns /= _numDescs
       then error $ "Column count mismatch: got " 
                  <> show _numColumns
                  <> " columns but the schema implied "
                  <> show _numDescs
                  <> " columns"
       else zipWith parse colChunks columnDescriptions
  where
    columnChunks :: (RandomAccess r) => FileMetadata -> [Stream r ColumnChunk]
    columnChunks = map (Stream.fromList) . transpose . map (unField . rg_columns) . unField . row_groups
    
    parse :: (RandomAccess r, MonadIO r) => Stream r ColumnChunk -> ColumnDescription -> ColumnStream r
    parse columnChunkStream description = ColumnStream $ 
      Stream.unfoldEach (parsePage description) $ Stream.unfoldEach parseColumnChunk columnChunkStream 

data ColumnChunkState
  = ColumnChunkState
  { remainingBytes :: !BS.ByteString
  , codec :: !CompressionCodec
  , dictionary :: !(Maybe DictVals)
  , parquetType :: !Int
  }

parseColumnChunk :: (RandomAccess r, MonadIO r) => Unfold r ColumnChunk (BS.ByteString, PageHeader, CompressionCodec, Maybe DictVals, Int)
parseColumnChunk = Unfold.Unfold step inject
  where
    inject :: (RandomAccess r) => ColumnChunk -> r ColumnChunkState
    inject columnChunk = do
      let columnMetadata = fromJust $ unField $ cc_meta_data columnChunk
          dataOffset = unField $ cmd_data_page_offset columnMetadata
          dictOffset = fromMaybe dataOffset (unField $ cmd_dictionary_page_offset columnMetadata)
          startOffset = min dataOffset dictOffset
          compressedSize = unField $ cmd_total_compressed_size columnMetadata
          c = unField $ cmd_codec columnMetadata
          pType =  fromEnum $ pinchThriftTypeToParquetType (unField $ cmd_type columnMetadata)
          range = Range (fromIntegral startOffset) (fromIntegral compressedSize)
     
      rawBytes <- readBytes range
      return $ ColumnChunkState rawBytes c Nothing pType

    step :: (RandomAccess r, MonadIO r) => ColumnChunkState -> r (Unfold.Step ColumnChunkState (BS.ByteString, PageHeader, CompressionCodec, Maybe DictVals, Int))
    step (ColumnChunkState remaining c dict pType) = do
      if BS.null remaining
        then return Unfold.Stop
        else case parsePageHeader remaining of
          Left e -> error $ show e
          Right (remainder, header) -> do
            let compressedPageSize = fromIntegral $ unField $ ph_compressed_page_size header
                (pageData, rest) = BS.splitAt compressedPageSize remainder
            uncompressedData <- liftIO $ decompressData (pinchCompressionToParquetCompression c) pageData
            
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
                    newDict = readDictVals (toEnum pType) uncompressedData (Just numValues)
                step (ColumnChunkState rest c (Just newDict) pType)
              Nothing -> do
                -- It's a data page. Yield it.
                return $ Unfold.Yield (uncompressedData, header, c, dict, pType) (ColumnChunkState rest c dict pType)

parsePageHeader :: BS.ByteString -> Either String (BS.ByteString, PageHeader)
parsePageHeader bytes = case decodeWithLeftovers Pinch.compactProtocol bytes of
  Left e -> Left e
  Right header -> Right header


