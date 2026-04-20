module DataFrame.IO.Parquet.Decompress where

import qualified Codec.Compression.GZip as GZip
import qualified Codec.Compression.Zstd.Base as Zstd
import qualified Data.ByteString as BS
import qualified Data.ByteString as LB
import Data.ByteString.Internal (createAndTrim, toForeignPtr)
import DataFrame.IO.Parquet.Thrift (CompressionCodec (..))
import Foreign.ForeignPtr (withForeignPtr)
import Foreign.Ptr (plusPtr)
import qualified Snappy

decompressData :: Int -> CompressionCodec -> BS.ByteString -> IO BS.ByteString
decompressData uncompressedSize codec compressed = case codec of
    (ZSTD _) -> createAndTrim uncompressedSize $ \dstPtr ->
        let (srcFP, offset, compressedSize) = toForeignPtr compressed
         in withForeignPtr srcFP $ \srcPtr -> do
                result <-
                    Zstd.decompress
                        dstPtr
                        uncompressedSize
                        (srcPtr `plusPtr` offset)
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
