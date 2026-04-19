{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Page where

import qualified Codec.Compression.GZip as GZip
import qualified Codec.Compression.Zstd.Streaming as Zstd
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import Data.Int
import Data.Maybe (fromMaybe)
import qualified Data.Vector.Unboxed as VU
import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types
import DataFrame.Internal.Binary (
    littleEndianInt32,
    littleEndianWord32,
    littleEndianWord64,
 )
import GHC.Float
import qualified Snappy

isDataPage :: Page -> Bool
isDataPage page = case pageTypeHeader (pageHeader page) of
    DataPageHeader{} -> True
    DataPageHeaderV2{} -> True
    _ -> False

isDictionaryPage :: Page -> Bool
isDictionaryPage page = case pageTypeHeader (pageHeader page) of
    DictionaryPageHeader{} -> True
    _ -> False

decompressData :: CompressionCodec -> BS.ByteString -> IO BS.ByteString
decompressData codec compressed = case codec of
    ZSTD -> do
        result <- Zstd.decompress
        drainZstd result compressed []
      where
        drainZstd (Zstd.Consume f) input acc = do
            result <- f input
            drainZstd result BS.empty acc
        drainZstd (Zstd.Produce chunk next) _ acc = do
            result <- next
            drainZstd result BS.empty (chunk : acc)
        drainZstd (Zstd.Done final) _ acc =
            pure $ BS.concat (reverse (final : acc))
        drainZstd (Zstd.Error msg msg2) _ _ =
            error ("ZSTD error: " ++ msg ++ " " ++ msg2)
    SNAPPY -> case Snappy.decompress compressed of
        Left e -> error (show e)
        Right res -> pure res
    UNCOMPRESSED -> pure compressed
    GZIP -> pure (LB.toStrict (GZip.decompress (BS.fromStrict compressed)))
    other -> error ("Unsupported compression type: " ++ show other)

readPage :: CompressionCodec -> BS.ByteString -> IO (Maybe Page, BS.ByteString)
readPage c columnBytes =
    if BS.null columnBytes
        then pure (Nothing, BS.empty)
        else do
            let (hdr, remainder) = readPageHeader emptyPageHeader columnBytes 0

            let compressed = BS.take (fromIntegral $ compressedPageSize hdr) remainder

            fullData <- decompressData c compressed

            pure
                ( Just $ Page hdr fullData
                , BS.drop (fromIntegral $ compressedPageSize hdr) remainder
                )

readPageHeader ::
    PageHeader -> BS.ByteString -> Int16 -> (PageHeader, BS.ByteString)
readPageHeader hdr xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (remainder, _elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (pType, remainder') = readInt32FromBytes remainder
                             in
                                readPageHeader
                                    (hdr{pageHeaderPageType = pageTypeFromInt pType})
                                    remainder'
                                    identifier
                        2 ->
                            let
                                (parsedUncompressedPageSize, remainder') = readInt32FromBytes remainder
                             in
                                readPageHeader
                                    (hdr{uncompressedPageSize = parsedUncompressedPageSize})
                                    remainder'
                                    identifier
                        3 ->
                            let
                                (parsedCompressedPageSize, remainder') = readInt32FromBytes remainder
                             in
                                readPageHeader
                                    (hdr{compressedPageSize = parsedCompressedPageSize})
                                    remainder'
                                    identifier
                        4 ->
                            let
                                (crc, remainder') = readInt32FromBytes remainder
                             in
                                readPageHeader (hdr{pageHeaderCrcChecksum = crc}) remainder' identifier
                        5 ->
                            let
                                (dataPageHeader, remainder') = readPageTypeHeader emptyDataPageHeader remainder 0
                             in
                                readPageHeader (hdr{pageTypeHeader = dataPageHeader}) remainder' identifier
                        6 -> error "Index page header not supported"
                        7 ->
                            let
                                (dictionaryPageHeader, remainder') = readPageTypeHeader emptyDictionaryPageHeader remainder 0
                             in
                                readPageHeader
                                    (hdr{pageTypeHeader = dictionaryPageHeader})
                                    remainder'
                                    identifier
                        8 ->
                            let
                                (dataPageHeaderV2, remainder') = readPageTypeHeader emptyDataPageHeaderV2 remainder 0
                             in
                                readPageHeader (hdr{pageTypeHeader = dataPageHeaderV2}) remainder' identifier
                        n -> error $ "Unknown page header field " ++ show n

readPageTypeHeader ::
    PageTypeHeader -> BS.ByteString -> Int16 -> (PageTypeHeader, BS.ByteString)
readPageTypeHeader INDEX_PAGE_HEADER _ _ = error "readPageTypeHeader: unsupported INDEX_PAGE_HEADER"
readPageTypeHeader PAGE_TYPE_HEADER_UNKNOWN _ _ = error "readPageTypeHeader: unsupported PAGE_TYPE_HEADER_UNKNOWN"
readPageTypeHeader hdr@(DictionaryPageHeader{}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (remainder, _elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageHeaderNumValues = numValues})
                                    remainder'
                                    identifier
                        2 ->
                            let
                                (enc, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageHeaderEncoding = parquetEncodingFromInt enc})
                                    remainder'
                                    identifier
                        3 ->
                            let
                                isSorted = fromMaybe (error "readPageTypeHeader: not enough bytes") (remainder BS.!? 0)
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageIsSorted = isSorted == compactBooleanTrue})
                                    -- TODO(mchavinda): The bool logic here is a little tricky.
                                    -- If the field is a bool then you can get the value
                                    -- from the byte (and you don't have to drop a field).
                                    -- But in other cases you do.
                                    -- This might become a problem later but in the mean
                                    -- time I'm not dropping (this assumes this is the common case).
                                    remainder
                                    identifier
                        n ->
                            error $ "readPageTypeHeader: unsupported identifier " ++ show n
readPageTypeHeader hdr@(DataPageHeader{}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (remainder, _elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderNumValues = numValues})
                                    remainder'
                                    identifier
                        2 ->
                            let
                                (enc, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderEncoding = parquetEncodingFromInt enc})
                                    remainder'
                                    identifier
                        3 ->
                            let
                                (enc, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{definitionLevelEncoding = parquetEncodingFromInt enc})
                                    remainder'
                                    identifier
                        4 ->
                            let
                                (enc, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{repetitionLevelEncoding = parquetEncodingFromInt enc})
                                    remainder'
                                    identifier
                        5 ->
                            let
                                (stats, remainder') = readStatisticsFromBytes emptyColumnStatistics remainder 0
                             in
                                readPageTypeHeader (hdr{dataPageHeaderStatistics = stats}) remainder' identifier
                        n -> error $ show n
readPageTypeHeader hdr@(DataPageHeaderV2{}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (remainder, _elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2NumValues = numValues})
                                    remainder'
                                    identifier
                        2 ->
                            let
                                (numNulls, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2NumNulls = numNulls})
                                    remainder'
                                    identifier
                        3 ->
                            let
                                (parsedNumRows, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2NumRows = parsedNumRows})
                                    remainder'
                                    identifier
                        4 ->
                            let
                                (enc, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2Encoding = parquetEncodingFromInt enc})
                                    remainder'
                                    identifier
                        5 ->
                            let
                                (n, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader (hdr{definitionLevelByteLength = n}) remainder' identifier
                        6 ->
                            let
                                (n, remainder') = readInt32FromBytes remainder
                             in
                                readPageTypeHeader (hdr{repetitionLevelByteLength = n}) remainder' identifier
                        7 ->
                            let
                                (isCompressed, remainder') = case BS.uncons remainder of
                                    Just (b, bytes) -> ((b .&. 0x0f) == compactBooleanTrue, bytes)
                                    Nothing -> (True, BS.empty)
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2IsCompressed = isCompressed})
                                    remainder'
                                    identifier
                        8 ->
                            let
                                (stats, remainder') = readStatisticsFromBytes emptyColumnStatistics remainder 0
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2Statistics = stats})
                                    remainder'
                                    identifier
                        n -> error $ show n

readField' :: BS.ByteString -> Int16 -> Maybe (BS.ByteString, TType, Int16)
readField' bs lastFieldId = case BS.uncons bs of
    Nothing -> Nothing
    Just (x, xs) ->
        if x .&. 0x0f == 0
            then Nothing
            else
                let modifier = fromIntegral ((x .&. 0xf0) `shiftR` 4) :: Int16
                    (identifier, remainder) =
                        if modifier == 0
                            then readIntFromBytes @Int16 xs
                            else (lastFieldId + modifier, xs)
                    elemType = toTType (x .&. 0x0f)
                 in Just (remainder, elemType, identifier)

readAllPages :: CompressionCodec -> BS.ByteString -> IO [Page]
readAllPages codec bytes = go bytes []
  where
    go bs acc =
        if BS.null bs
            then return (reverse acc)
            else do
                (maybePage, remainderaining) <- readPage codec bs
                case maybePage of
                    Nothing -> return (reverse acc)
                    Just page -> go remainderaining (page : acc)

-- | Read n Int32 values directly into an unboxed vector (no intermediate list).
readNInt32Vec :: Int -> BS.ByteString -> VU.Vector Int32
readNInt32Vec n bs = VU.generate n (\i -> littleEndianInt32 (BS.drop (4 * i) bs))

-- | Read n Int64 values directly into an unboxed vector.
readNInt64Vec :: Int -> BS.ByteString -> VU.Vector Int64
readNInt64Vec n bs = VU.generate n (\i -> fromIntegral (littleEndianWord64 (BS.drop (8 * i) bs)))

-- | Read n Float values directly into an unboxed vector.
readNFloatVec :: Int -> BS.ByteString -> VU.Vector Float
readNFloatVec n bs =
    VU.generate
        n
        (\i -> castWord32ToFloat (littleEndianWord32 (BS.drop (4 * i) bs)))

-- | Read n Double values directly into an unboxed vector.
readNDoubleVec :: Int -> BS.ByteString -> VU.Vector Double
readNDoubleVec n bs =
    VU.generate
        n
        (\i -> castWord64ToDouble (littleEndianWord64 (BS.drop (8 * i) bs)))

readNInt32 :: Int -> BS.ByteString -> ([Int32], BS.ByteString)
readNInt32 0 bs = ([], bs)
readNInt32 k bs =
    let x = littleEndianInt32 (BS.take 4 bs)
        bs' = BS.drop 4 bs
        (xs, rest) = readNInt32 (k - 1) bs'
     in (x : xs, rest)

readNDouble :: Int -> BS.ByteString -> ([Double], BS.ByteString)
readNDouble 0 bs = ([], bs)
readNDouble k bs =
    let x = castWord64ToDouble (littleEndianWord64 (BS.take 8 bs))
        bs' = BS.drop 8 bs
        (xs, rest) = readNDouble (k - 1) bs'
     in (x : xs, rest)

readNByteArrays :: Int -> BS.ByteString -> ([BS.ByteString], BS.ByteString)
readNByteArrays 0 bs = ([], bs)
readNByteArrays k bs =
    let len = fromIntegral (littleEndianInt32 (BS.take 4 bs)) :: Int
        body = BS.take len (BS.drop 4 bs)
        bs' = BS.drop (4 + len) bs
        (xs, rest) = readNByteArrays (k - 1) bs'
     in (body : xs, rest)

readNBool :: Int -> BS.ByteString -> ([Bool], BS.ByteString)
readNBool 0 bs = ([], bs)
readNBool count bs =
    let totalBytes = (count + 7) `div` 8
        chunk = BS.take totalBytes bs
        rest = BS.drop totalBytes bs
        bits =
            concatMap
                (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7])
                (BS.unpack chunk)
        bools = take count bits
     in (bools, rest)

readNInt64 :: Int -> BS.ByteString -> ([Int64], BS.ByteString)
readNInt64 0 bs = ([], bs)
readNInt64 k bs =
    let x = fromIntegral (littleEndianWord64 (BS.take 8 bs))
        bs' = BS.drop 8 bs
        (xs, rest) = readNInt64 (k - 1) bs'
     in (x : xs, rest)

readNFloat :: Int -> BS.ByteString -> ([Float], BS.ByteString)
readNFloat 0 bs = ([], bs)
readNFloat k bs =
    let x = castWord32ToFloat (littleEndianWord32 (BS.take 4 bs))
        bs' = BS.drop 4 bs
        (xs, rest) = readNFloat (k - 1) bs'
     in (x : xs, rest)

splitFixed :: Int -> Int -> BS.ByteString -> ([BS.ByteString], BS.ByteString)
splitFixed 0 _ bs = ([], bs)
splitFixed k len bs =
    let body = BS.take len bs
        bs' = BS.drop len bs
        (xs, rest) = splitFixed (k - 1) len bs'
     in (body : xs, rest)

readStatisticsFromBytes ::
    ColumnStatistics -> BS.ByteString -> Int16 -> (ColumnStatistics, BS.ByteString)
readStatisticsFromBytes cs xs lastFieldId =
    let
        fieldContents = readField' xs lastFieldId
     in
        case fieldContents of
            Nothing -> (cs, BS.drop 1 xs)
            Just (remainder, _elemType, identifier) -> case identifier of
                1 ->
                    let
                        (maxInBytes, remainder') = readByteStringFromBytes remainder
                     in
                        readStatisticsFromBytes (cs{columnMax = maxInBytes}) remainder' identifier
                2 ->
                    let
                        (minInBytes, remainder') = readByteStringFromBytes remainder
                     in
                        readStatisticsFromBytes (cs{columnMin = minInBytes}) remainder' identifier
                3 ->
                    let
                        (nullCount, remainder') = readIntFromBytes @Int64 remainder
                     in
                        readStatisticsFromBytes (cs{columnNullCount = nullCount}) remainder' identifier
                4 ->
                    let
                        (distinctCount, remainder') = readIntFromBytes @Int64 remainder
                     in
                        readStatisticsFromBytes
                            (cs{columnDistictCount = distinctCount})
                            remainder'
                            identifier
                5 ->
                    let
                        (maxInBytes, remainder') = readByteStringFromBytes remainder
                     in
                        readStatisticsFromBytes (cs{columnMaxValue = maxInBytes}) remainder' identifier
                6 ->
                    let
                        (minInBytes, remainder') = readByteStringFromBytes remainder
                     in
                        readStatisticsFromBytes (cs{columnMinValue = minInBytes}) remainder' identifier
                7 ->
                    case BS.uncons remainder of
                        Nothing ->
                            error "readStatisticsFromBytes: not enough bytes"
                        Just (isMaxValueExact, remainder') ->
                            readStatisticsFromBytes
                                (cs{isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue})
                                remainder'
                                identifier
                8 ->
                    case BS.uncons remainder of
                        Nothing ->
                            error "readStatisticsFromBytes: not enough bytes"
                        Just (isMinValueExact, remainder') ->
                            readStatisticsFromBytes
                                (cs{isColumnMinValueExact = isMinValueExact == compactBooleanTrue})
                                remainder'
                                identifier
                n -> error $ show n
