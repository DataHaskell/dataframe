
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ExplicitForAll #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Unstable.Parquet (readParquet) where

import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), mmapFileVector, ReaderIO (runReaderIO), Range (Range))
import DataFrame.IO.Unstable.Parquet.Thrift (
  FileMetadata (..),
  ColumnChunk (..),
  RowGroup (..),
  ColumnMetaData(..),
  PageHeader(..),
  unField, 
  )
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import qualified Pinch
import Data.Bits (Bits(shiftL), (.|.))
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream
import Streamly.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as Unfold
import DataFrame.Internal.Column (Columnable)
import Data.List (transpose)
import Data.Kind (Type)
import Data.Maybe (fromJust)
import Pinch (decodeWithLeftovers)

readParquet filepath = do
  file <- mmapFileVector filepath
  fileMetadata <- runReaderIO parseFileMetadata file
  print fileMetadata

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

parseColumns :: (RandomAccess r, Columnable a) => FileMetadata -> [Stream r a]
parseColumns metadata = map parse (columnChunks metadata)
  where
    columnChunks :: forall (m :: Type -> Type) a. Applicative m => FileMetadata -> [Stream m ColumnChunk]
    columnChunks = map (Stream.fromList) . transpose . map (unField . rg_columns) . unField . row_groups
    parse columnChunkStream = Stream.unfoldEach parseColumnChunk columnChunkStream 

data ColumnChunkState r a
  = ColumnChunkState
  { remainingBytes :: BS.ByteString
  , currentValueStream :: Stream r a
  }

parseColumnChunk :: (RandomAccess r, Columnable a) => Unfold r ColumnChunk a
parseColumnChunk = Unfold.Unfold step inject
  where
    inject :: (RandomAccess r, Columnable a) => ColumnChunk -> r (ColumnChunkState r a)
    inject columnChunk = do
      -- Regarding the usage of fromJust:
      -- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L997
      --    Note: while marked as optional, this field is in fact required by most major
      --    Parquet implementations. As such, writers MUST populate this field.
      let columnMetadata = fromJust $ unField columnChunk.cc_meta_data
          dataOffset =  unField columnMetadata.cmd_data_page_offset
          compressedSize = unField columnMetadata.cmd_total_compressed_size
          range = Range (fromIntegral dataOffset) (fromIntegral compressedSize)
          
      -- We must handle all the things, of course, but for now:
      rawBytes <- readBytes range
      case parsePage rawBytes of
        Nothing -> return $ ColumnChunkState rawBytes Stream.nil
        Just (stream, remainder) -> return $ ColumnChunkState remainder stream
    step :: (RandomAccess r, Columnable a) => ColumnChunkState r a -> r (Unfold.Step (ColumnChunkState r a) a)
    step columnChunkState = do
      maybeA <- Stream.uncons columnChunkState.currentValueStream
      case maybeA of
        Nothing -> do
          case parsePage columnChunkState.remainingBytes of
            Nothing -> return Unfold.Stop
            Just (newStream, remainder) -> return . Unfold.Skip $ ColumnChunkState remainder newStream
        Just (a, newStream) -> return $ Unfold.Yield a (columnChunkState{currentValueStream = newStream})


parsePage :: (RandomAccess r, Columnable a) => BS.ByteString -> Maybe (Stream r a, BS.ByteString)
parsePage rawBytes = readPage pageHeader remainder
  where
    readPage :: (RandomAccess r, Columnable a) => PageHeader -> BS.ByteString -> Maybe (Stream r a, BS.ByteString)
    readPage = undefined -- I'm still figuring this out
    (remainder, pageHeader) = readPageHeader rawBytes
    readPageHeader :: BS.ByteString -> (BS.ByteString, PageHeader)
    readPageHeader bytes = case decodeWithLeftovers Pinch.compactProtocol bytes of
      Left e -> error e
      Right header -> header
