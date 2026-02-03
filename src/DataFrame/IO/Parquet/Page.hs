{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Page where

import qualified Codec.Compression.GZip as GZip
import Codec.Compression.Zstd.Streaming
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LB
import Data.Int
import Data.Maybe (fromMaybe)
import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types
import GHC.Float
import qualified Snappy

isDataPage :: Page -> Bool
isDataPage page = case pageTypeHeader (pageHeader page) of
    DataPageHeader{..} -> True
    DataPageHeaderV2{..} -> True
    _ -> False

isDictionaryPage :: Page -> Bool
isDictionaryPage page = case pageTypeHeader (pageHeader page) of
    DictionaryPageHeader{..} -> True
    _ -> False

readPage :: CompressionCodec -> BS.ByteString -> IO (Maybe Page, BS.ByteString)
readPage c columnBytes =
    if BS.null columnBytes
        then pure (Nothing, BS.empty)
        else do
            let (hdr, rem) = readPageHeader emptyPageHeader columnBytes 0

            let compressed = BS.take (fromIntegral $ compressedPageSize hdr) rem

            fullData <- case c of
                ZSTD -> do
                    Consume dFunc <- decompress
                    Consume dFunc' <- dFunc compressed
                    Done res <- dFunc' BS.empty
                    pure res
                SNAPPY -> case Snappy.decompress compressed of
                    Left e -> error (show e)
                    Right res -> pure res
                UNCOMPRESSED -> pure compressed
                GZIP -> pure (LB.toStrict (GZip.decompress (BS.fromStrict compressed)))
                other -> error ("Unsupported compression type: " ++ show other)
            pure
                ( Just $ Page hdr fullData
                , BS.drop (fromIntegral $ compressedPageSize hdr) rem
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
                    Just (rem, elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (pType, rem') = readInt32FromBytes rem
                             in
                                readPageHeader (hdr{pageHeaderPageType = pageTypeFromInt pType}) rem' identifier
                        2 ->
                            let
                                (uncompressedPageSize, rem') = readInt32FromBytes rem
                             in
                                readPageHeader
                                    (hdr{uncompressedPageSize = uncompressedPageSize})
                                    rem'
                                    identifier
                        3 ->
                            let
                                (compressedPageSize, rem') = readInt32FromBytes rem
                             in
                                readPageHeader (hdr{compressedPageSize = compressedPageSize}) rem' identifier
                        4 ->
                            let
                                (crc, rem') = readInt32FromBytes rem
                             in
                                readPageHeader (hdr{pageHeaderCrcChecksum = crc}) rem' identifier
                        5 ->
                            let
                                (dataPageHeader, rem') = readPageTypeHeader emptyDataPageHeader rem 0
                             in
                                readPageHeader (hdr{pageTypeHeader = dataPageHeader}) rem' identifier
                        6 -> error "Index page header not supported"
                        7 ->
                            let
                                (dictionaryPageHeader, rem') = readPageTypeHeader emptyDictionaryPageHeader rem 0
                             in
                                readPageHeader (hdr{pageTypeHeader = dictionaryPageHeader}) rem' identifier
                        8 ->
                            let
                                (dataPageHeaderV2, rem') = readPageTypeHeader emptyDataPageHeaderV2 rem 0
                             in
                                readPageHeader (hdr{pageTypeHeader = dataPageHeaderV2}) rem' identifier
                        n -> error $ "Unknown page header field " ++ show n

readPageTypeHeader ::
    PageTypeHeader -> BS.ByteString -> Int16 -> (PageTypeHeader, BS.ByteString)
readPageTypeHeader INDEX_PAGE_HEADER _ _ = error "readPageTypeHeader: unsupported INDEX_PAGE_HEADER"
readPageTypeHeader PAGE_TYPE_HEADER_UNKNOWN _ _ = error "readPageTypeHeader: unsupported PAGE_TYPE_HEADER_UNKNOWN"
readPageTypeHeader hdr@(DictionaryPageHeader{..}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (rem, elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageHeaderNumValues = numValues})
                                    rem'
                                    identifier
                        2 ->
                            let
                                (enc, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageHeaderEncoding = parquetEncodingFromInt enc})
                                    rem'
                                    identifier
                        3 ->
                            let
                                isSorted = fromMaybe (error "readPageTypeHeader: not enough bytes") (rem BS.!? 0)
                             in
                                readPageTypeHeader
                                    (hdr{dictionaryPageIsSorted = isSorted == compactBooleanTrue})
                                    -- TODO(mchavinda): The bool logic here is a little tricky.
                                    -- If the field is a bool then you can get the value
                                    -- from the byte (and you don't have to drop a field).
                                    -- But in other cases you do.
                                    -- This might become a problem later but in the mean
                                    -- time I'm not dropping (this assumes this is the common case).
                                    rem
                                    identifier
                        n ->
                            error $ "readPageTypeHeader: unsupported identifier " ++ show n
readPageTypeHeader hdr@(DataPageHeader{..}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (rem, elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{dataPageHeaderNumValues = numValues}) rem' identifier
                        2 ->
                            let
                                (enc, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderEncoding = parquetEncodingFromInt enc})
                                    rem'
                                    identifier
                        3 ->
                            let
                                (enc, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{definitionLevelEncoding = parquetEncodingFromInt enc})
                                    rem'
                                    identifier
                        4 ->
                            let
                                (enc, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{repetitionLevelEncoding = parquetEncodingFromInt enc})
                                    rem'
                                    identifier
                        5 ->
                            let
                                (stats, rem') = readStatisticsFromBytes emptyColumnStatistics rem 0
                             in
                                readPageTypeHeader (hdr{dataPageHeaderStatistics = stats}) rem' identifier
                        n -> error $ show n
readPageTypeHeader hdr@(DataPageHeaderV2{..}) xs lastFieldId =
    if BS.null xs
        then (hdr, BS.empty)
        else
            let
                fieldContents = readField' xs lastFieldId
             in
                case fieldContents of
                    Nothing -> (hdr, BS.drop 1 xs)
                    Just (rem, elemType, identifier) -> case identifier of
                        1 ->
                            let
                                (numValues, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{dataPageHeaderV2NumValues = numValues}) rem' identifier
                        2 ->
                            let
                                (numNulls, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{dataPageHeaderV2NumNulls = numNulls}) rem' identifier
                        3 ->
                            let
                                (numRows, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{dataPageHeaderV2NumRows = numRows}) rem' identifier
                        4 ->
                            let
                                (enc, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2Encoding = parquetEncodingFromInt enc})
                                    rem'
                                    identifier
                        5 ->
                            let
                                (n, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{definitionLevelByteLength = n}) rem' identifier
                        6 ->
                            let
                                (n, rem') = readInt32FromBytes rem
                             in
                                readPageTypeHeader (hdr{repetitionLevelByteLength = n}) rem' identifier
                        7 ->
                            let
                                (isCompressed, rem') = case BS.uncons rem of
                                    Just (b, bytes) -> ((b .&. 0x0f) == compactBooleanTrue, bytes)
                                    Nothing -> (True, BS.empty)
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2IsCompressed = isCompressed})
                                    rem'
                                    identifier
                        8 ->
                            let
                                (stats, rem') = readStatisticsFromBytes emptyColumnStatistics rem 0
                             in
                                readPageTypeHeader
                                    (hdr{dataPageHeaderV2Statistics = stats})
                                    rem'
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
                    (identifier, rem) =
                        if modifier == 0
                            then readIntFromBytes @Int16 xs
                            else (lastFieldId + modifier, xs)
                    elemType = toTType (x .&. 0x0f)
                 in Just (rem, elemType, identifier)

readAllPages :: CompressionCodec -> BS.ByteString -> IO [Page]
readAllPages codec bytes = go bytes []
  where
    go bs acc =
        if BS.null bs
            then return (reverse acc)
            else do
                (maybePage, remaining) <- readPage codec bs
                case maybePage of
                    Nothing -> return (reverse acc)
                    Just page -> go remaining (page : acc)

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
            Just (rem, elemType, identifier) -> case identifier of
                1 ->
                    let
                        (maxInBytes, rem') = readByteStringFromBytes rem
                     in
                        readStatisticsFromBytes (cs{columnMax = maxInBytes}) rem' identifier
                2 ->
                    let
                        (minInBytes, rem') = readByteStringFromBytes rem
                     in
                        readStatisticsFromBytes (cs{columnMin = minInBytes}) rem' identifier
                3 ->
                    let
                        (nullCount, rem') = readIntFromBytes @Int64 rem
                     in
                        readStatisticsFromBytes (cs{columnNullCount = nullCount}) rem' identifier
                4 ->
                    let
                        (distinctCount, rem') = readIntFromBytes @Int64 rem
                     in
                        readStatisticsFromBytes (cs{columnDistictCount = distinctCount}) rem' identifier
                5 ->
                    let
                        (maxInBytes, rem') = readByteStringFromBytes rem
                     in
                        readStatisticsFromBytes (cs{columnMaxValue = maxInBytes}) rem' identifier
                6 ->
                    let
                        (minInBytes, rem') = readByteStringFromBytes rem
                     in
                        readStatisticsFromBytes (cs{columnMinValue = minInBytes}) rem' identifier
                7 ->
                    case BS.uncons rem of
                        Nothing ->
                            error "readStatisticsFromBytes: not enough bytes"
                        Just (isMaxValueExact, rem') ->
                            readStatisticsFromBytes
                                (cs{isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue})
                                rem'
                                identifier
                8 ->
                    case BS.uncons rem of
                        Nothing ->
                            error "readStatisticsFromBytes: not enough bytes"
                        Just (isMinValueExact, rem') ->
                            readStatisticsFromBytes
                                (cs{isColumnMinValueExact = isMinValueExact == compactBooleanTrue})
                                rem'
                                identifier
                n -> error $ show n
