{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Unstable.Parquet.Dictionary (DictVals (..), readDictVals, decodeRLEBitPackedHybrid) where

import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Int (Int32, Int64)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time (UTCTime)
import qualified Data.Vector as V
import Data.Word
import DataFrame.IO.Parquet.Binary (readUVarInt)
import DataFrame.IO.Unstable.Parquet.Thrift (ThriftType (..))
import DataFrame.IO.Unstable.Parquet.Time (int96ToUTCTime)
import DataFrame.Internal.Binary (
    littleEndianInt32,
    littleEndianWord32,
    littleEndianWord64,
 )
import GHC.Float

data DictVals
    = DBool (V.Vector Bool)
    | DInt32 (V.Vector Int32)
    | DInt64 (V.Vector Int64)
    | DInt96 (V.Vector UTCTime)
    | DFloat (V.Vector Float)
    | DDouble (V.Vector Double)
    | DText (V.Vector T.Text)
    deriving (Show, Eq)

readDictVals :: ThriftType -> BS.ByteString -> Maybe Int32 -> DictVals
readDictVals (BOOLEAN _) bs (Just count) = DBool (V.fromList (take (fromIntegral count) $ readPageBool bs))
readDictVals (INT32 _) bs _ = DInt32 (V.fromList (readPageInt32 bs))
readDictVals (INT64 _) bs _ = DInt64 (V.fromList (readPageInt64 bs))
readDictVals (INT96 _) bs _ = DInt96 (V.fromList (readPageInt96Times bs))
readDictVals (FLOAT _) bs _ = DFloat (V.fromList (readPageFloat bs))
readDictVals (DOUBLE _) bs _ = DDouble (V.fromList (readPageWord64 bs))
readDictVals (BYTE_ARRAY _) bs _ = DText (V.fromList (readPageBytes bs))
readDictVals (FIXED_LEN_BYTE_ARRAY _) bs (Just len) = DText (V.fromList (readPageFixedBytes bs (fromIntegral len)))
readDictVals t _ _ = error $ "Unsupported dictionary type: " ++ show t

readPageInt32 :: BS.ByteString -> [Int32]
readPageInt32 xs
    | BS.null xs = []
    | otherwise = littleEndianInt32 (BS.take 4 xs) : readPageInt32 (BS.drop 4 xs)

readPageWord64 :: BS.ByteString -> [Double]
readPageWord64 xs
    | BS.null xs = []
    | otherwise =
        castWord64ToDouble (littleEndianWord64 (BS.take 8 xs))
            : readPageWord64 (BS.drop 8 xs)

readPageBytes :: BS.ByteString -> [T.Text]
readPageBytes xs
    | BS.null xs = []
    | otherwise =
        let lenBytes = fromIntegral (littleEndianInt32 $ BS.take 4 xs)
            totalBytesRead = lenBytes + 4
         in decodeUtf8Lenient (BS.take lenBytes (BS.drop 4 xs))
                : readPageBytes (BS.drop totalBytesRead xs)

readPageBool :: BS.ByteString -> [Bool]
readPageBool bs =
    concatMap (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7]) (BS.unpack bs)

readPageInt64 :: BS.ByteString -> [Int64]
readPageInt64 xs
    | BS.null xs = []
    | otherwise =
        fromIntegral (littleEndianWord64 (BS.take 8 xs)) : readPageInt64 (BS.drop 8 xs)

readPageFloat :: BS.ByteString -> [Float]
readPageFloat xs
    | BS.null xs = []
    | otherwise =
        castWord32ToFloat (littleEndianWord32 (BS.take 4 xs))
            : readPageFloat (BS.drop 4 xs)

readNInt96Times :: Int -> BS.ByteString -> ([UTCTime], BS.ByteString)
readNInt96Times 0 bs = ([], bs)
readNInt96Times k bs =
    let timestamp96 = BS.take 12 bs
        utcTime = int96ToUTCTime timestamp96
        bs' = BS.drop 12 bs
        (times, rest) = readNInt96Times (k - 1) bs'
     in (utcTime : times, rest)

readPageInt96Times :: BS.ByteString -> [UTCTime]
readPageInt96Times bs
    | BS.null bs = []
    | otherwise =
        let (times, _) = readNInt96Times (BS.length bs `div` 12) bs
         in times

readPageFixedBytes :: BS.ByteString -> Int -> [T.Text]
readPageFixedBytes xs len
    | BS.null xs = []
    | otherwise =
        decodeUtf8Lenient (BS.take len xs) : readPageFixedBytes (BS.drop len xs) len

unpackBitPacked :: Int -> Int -> BS.ByteString -> ([Word32], BS.ByteString)
unpackBitPacked bw count bs
    | count <= 0 = ([], bs)
    | BS.null bs = ([], bs)
    | otherwise =
        let totalBytes = (bw * count + 7) `div` 8
            chunk = BS.take totalBytes bs
            rest = BS.drop totalBytes bs
         in (extractBits bw count chunk, rest)

-- | LSB-first bit accumulator: reads each byte once with no intermediate ByteString allocation.
extractBits :: Int -> Int -> BS.ByteString -> [Word32]
extractBits bw count bs = go 0 (0 :: Word64) 0 count
  where
    !mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1 :: Word64
    !len = BS.length bs
    go !byteIdx !acc !accBits !remaining
        | remaining <= 0 = []
        | accBits >= bw =
            fromIntegral (acc .&. mask)
                : go byteIdx (acc `shiftR` bw) (accBits - bw) (remaining - 1)
        | byteIdx >= len = []
        | otherwise =
            let b = fromIntegral (BSU.unsafeIndex bs byteIdx) :: Word64
             in go (byteIdx + 1) (acc .|. (b `shiftL` accBits)) (accBits + 8) remaining

decodeRLEBitPackedHybrid :: Int -> BS.ByteString -> ([Word32], BS.ByteString)
decodeRLEBitPackedHybrid bitWidth bs
    | bitWidth == 0 = ([0], bs)
    | BS.null bs = ([], bs)
    | otherwise =
        -- readUVarInt is evaluated here, inside the guard that has already
        -- confirmed bs is non-empty.  Keeping it in a where clause would cause
        -- it to be forced before the BS.null guard under {-# LANGUAGE Strict #-}.
        let (hdr64, afterHdr) = readUVarInt bs
            isPacked = (hdr64 .&. 1) == 1
         in if isPacked
                then
                    let groups = fromIntegral (hdr64 `shiftR` 1) :: Int
                        totalVals = groups * 8
                     in unpackBitPacked bitWidth totalVals afterHdr
                else
                    let mask = if bitWidth == 32 then maxBound else (1 `shiftL` bitWidth) - 1
                        runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                        nBytes = (bitWidth + 7) `div` 8 :: Int
                        word32 = littleEndianWord32 (BS.take 4 afterHdr)
                        value = word32 .&. mask
                     in (replicate runLen value, BS.drop nBytes afterHdr)
