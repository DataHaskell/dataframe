module DataFrame.IO.Unstable.Parquet.Decompress where

import DataFrame.IO.Unstable.Parquet.Thrift (CompressionCodec (..))
import qualified Data.ByteString as BS
import qualified Data.ByteString as LB
import Data.ByteString.Internal (toForeignPtr, createAndTrim)
import qualified Codec.Compression.Zstd.Base as Zstd
import qualified Codec.Compression.GZip as GZip
import qualified Snappy
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)

decompressData :: Int -> CompressionCodec -> BS.ByteString -> IO BS.ByteString
decompressData uncompressedSize codec compressed = case codec of
    (ZSTD _) -> createAndTrim uncompressedSize $ \dstPtr ->
      let (srcFP, offset, compressedSize) = toForeignPtr compressed
      in withForeignPtr srcFP $ \srcPtr -> do
        result <- Zstd.decompress
                    dstPtr
                    uncompressedSize
                    (srcPtr `plusPtr`offset)
                    compressedSize
        case result of
          Left e -> error $ "ZSTD error: " <> e
          Right actualSize -> return actualSize
    (SNAPPY _) -> case Snappy.decompress compressed of
        Left e -> error (show e)
        Right res -> pure res
    (UNCOMPRESSED _) -> pure compressed
    (GZIP _) -> pure (LB.toStrict (GZip.decompress (BS.fromStrict compressed)))
    other -> error ("Unsupported compression type: " <> show other)

