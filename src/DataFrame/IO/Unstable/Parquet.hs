module DataFrame.IO.Unstable.Parquet (readParquet) where

import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), mmapFileVector, ReaderIO (runReaderIO))
import DataFrame.IO.Unstable.Parquet.Thrift (FileMetadata (..))
import qualified Data.ByteString as BS
import Data.Functor ((<&>))
import qualified Pinch
import Data.Bits (Bits(shiftL), (.|.))

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
