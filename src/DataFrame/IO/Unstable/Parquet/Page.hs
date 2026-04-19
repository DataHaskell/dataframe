{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Unstable.Parquet.Page where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bits
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.Maybe (fromJust, fromMaybe)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Time
import qualified Data.Vector as V
import DataFrame.IO.Parquet.Levels (readLevelsV1, readLevelsV2)
import DataFrame.IO.Parquet.Time (int96ToUTCTime)
import DataFrame.IO.Unstable.Parquet.Decompress (decompressData)
import DataFrame.IO.Unstable.Parquet.Dictionary (
    DictVals (..),
    decodeRLEBitPackedHybrid,
    readDictVals,
 )
import DataFrame.IO.Unstable.Parquet.Thrift (
    ColumnChunk (..),
    ColumnMetaData (..),
    CompressionCodec,
    DataPageHeader (..),
    DataPageHeaderV2 (..),
    DictionaryPageHeader (..),
    Encoding (..),
    PageHeader (..),
    PageType (..),
    ThriftType (..),
    unField,
 )
import DataFrame.IO.Unstable.Parquet.Utils (
    ColumnDescription (..),
 )
import DataFrame.IO.Utils.RandomAccess (
    RandomAccess (..),
    Range (Range),
 )
import DataFrame.Internal.Binary (
    littleEndianInt32,
    littleEndianWord32,
    littleEndianWord64,
 )
import GHC.Float (castWord32ToFloat, castWord64ToDouble)
import Pinch (decodeWithLeftovers)
import qualified Pinch
import Streamly.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as Unfold

newtype ValueReader a = ValueReader {readValue :: BS.ByteString -> (a, ValueReader a, BS.ByteString)}

data ColumnChunkState a
    = ColumnChunkState
    { buffer :: BS.ByteString
    , codec :: CompressionCodec
    , parquetType :: ThriftType
    , pageState :: PageState
    , valueReader :: ValueReader a
    }

data PageState
    = PageState
    { remainingPageBytes :: BS.ByteString
    , currentPageHeader :: PageHeader
    , currentDictionary :: Maybe DictVals
    , repetitionLevels :: [Int]
    , definitionLevels :: [Int]
    }

nonNullableStream ::
    (RandomAccess m, MonadIO m) =>
    ColumnDescription -> (Maybe DictVals -> ValueReader a) -> Unfold m ColumnChunk a
nonNullableStream description makeReader = Unfold.Unfold (step makeReader) (inject makeReader)
  where
    inject ::
        (RandomAccess m, MonadIO m) =>
        (Maybe DictVals -> ValueReader a) -> ColumnChunk -> m (ColumnChunkState a)
    inject mkReader columnChunk = do
        -- according to the spec, columnMetadata MUST be present
        -- https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L997-L998
        let columnMetadata = fromJust $ unField $ columnChunk.cc_meta_data
            columnCodec = unField $ columnMetadata.cmd_codec
            dataOffset = unField $ columnMetadata.cmd_data_page_offset
            offset = fromMaybe dataOffset (unField $ columnMetadata.cmd_dictionary_page_offset)
            compressedSize = unField $ columnMetadata.cmd_total_compressed_size
            range = Range (fromIntegral offset) (fromIntegral compressedSize)
            pType = unField $ columnMetadata.cmd_type
            reader = mkReader Nothing
        rawBytes <- readBytes range
        let dummyPageState = PageState BS.empty undefined Nothing [] [] -- dummy so that we can call goToNextPage for the first page
        nextPage <-
            liftIO $
                goToNextPage description $
                    ColumnChunkState rawBytes columnCodec pType dummyPageState reader
        let initialState = case nextPage of
                Left e -> error $ show e -- TODO figure out what to do instead of just erroring out here
                Right ccs -> ccs
        return initialState
    step ::
        (RandomAccess m, MonadIO m) =>
        (Maybe DictVals -> ValueReader a) ->
        ColumnChunkState a ->
        m (Unfold.Step (ColumnChunkState a) a)
    step mkReader chunkState
        | BS.null chunkState.pageState.remainingPageBytes = do
            nextPage <- liftIO $ goToNextPage description chunkState
            case nextPage of
                Left _ -> return Unfold.Stop -- TODO when we add logging we should log the error here
                Right newState -> return $ Unfold.Skip newState
        | otherwise = do
            let pageheader = chunkState.pageState.currentPageHeader :: PageHeader
            case unField $ pageheader.ph_type of
                DATA_PAGE _ -> case unField pageheader.ph_data_page_header of
                    Nothing -> error "PageType is DATA_PAGE but data_page_header is missing"
                    Just (datapageHeader) -> do
                        case unField datapageHeader.dph_encoding of
                            PLAIN _ ->
                                let (value, newReader, remainder) = readValue chunkState.valueReader chunkState.pageState.remainingPageBytes
                                    newPageState = chunkState.pageState{remainingPageBytes = remainder}
                                 in return $
                                        Unfold.Yield value $
                                            chunkState{pageState = newPageState, valueReader = newReader}
                            PLAIN_DICTIONARY _ -> case chunkState.pageState.currentDictionary of
                                Nothing -> error "Encoding is PLAIN_DICTIONARY but dictionary is missing"
                                Just dictionary ->
                                    let (value, newReader, remainder) = readValue chunkState.valueReader chunkState.pageState.remainingPageBytes
                                        newPageState = chunkState.pageState{remainingPageBytes = remainder}
                                     in return $
                                            Unfold.Yield value $
                                                chunkState{pageState = newPageState, valueReader = newReader}
                            RLE_DICTIONARY _ -> case chunkState.pageState.currentDictionary of
                                Nothing -> error "Encoding is PLAIN_DICTIONARY but dictionary is missing"
                                Just dictionary ->
                                    let (value, newReader, remainder) = readValue chunkState.valueReader chunkState.pageState.remainingPageBytes
                                        newPageState = chunkState.pageState{remainingPageBytes = remainder}
                                     in return $
                                            Unfold.Yield value $
                                                chunkState{pageState = newPageState, valueReader = newReader}
                            other -> error ("Unsupported encoding: " <> show other)
                {-
                   The dictionary page must be placed at the first position of the column chunk
                   if it is partly or completely dictionary encoded. At most one dictionary page
                   can be placed in a column chunk.
                   This allows us to maintain the parsed DictVals for the chunk and pass it along
                   to subsequent data pages.
                   https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L698C1-L712C2
                -}
                DICTIONARY_PAGE _ -> case unField pageheader.ph_dictionary_page_header of
                    Nothing -> error "PageType is DICTIONARY_PAGE but dictionary_page_header is missing"
                    Just (dictHeader) -> do
                        let numValues = fromIntegral $ unField $ dictHeader.diph_num_values
                            pType = chunkState.parquetType
                            newDict = readDictVals pType chunkState.pageState.remainingPageBytes (Just numValues)
                            newPageState =
                                PageState
                                    BS.empty
                                    pageheader
                                    (Just newDict)
                                    []
                                    []
                            newReader = mkReader (Just newDict)
                        return $
                            Unfold.Skip (chunkState{pageState = newPageState, valueReader = newReader})
                INDEX_PAGE _ -> error "INDEX_PAGE Unimplemented"
                DATA_PAGE_V2 _ -> error "DATA_PAGE_V2 TODO"

data PageErrorType
    = FailedToParseHeader T.Text
    | ColumnChunkExhausted
    deriving (Eq, Show)

goToNextPage ::
    ColumnDescription ->
    ColumnChunkState a ->
    IO (Either PageErrorType (ColumnChunkState a))
goToNextPage description chunkState
    | BS.null chunkState.buffer = pure $ Left ColumnChunkExhausted
    | otherwise = case parsePageHeader chunkState.buffer of
        Left e -> pure $ Left $ FailedToParseHeader (T.pack e)
        Right (buffer', pageheader) -> do
            (buffer'', newPageState) <- getNewBufferAndPageState pageheader buffer'
            pure . Right $
                ColumnChunkState
                    buffer''
                    chunkState.codec
                    chunkState.parquetType
                    newPageState
                    chunkState.valueReader
  where
    getNewBufferAndPageState pageheader buffer = do
        let (compressedPageData, buffer') = BS.splitAt compressedPageSize buffer
            compressedPageSize = fromIntegral . unField $ pageheader.ph_compressed_page_size
        (repLevels, defLevels, decompressedPageData) <-
            readLevelsAndDecompress chunkState.codec pageheader compressedPageData
        pure
            (buffer', PageState decompressedPageData pageheader Nothing repLevels defLevels)
    readLevelsAndDecompress ::
        CompressionCodec ->
        PageHeader ->
        BS.ByteString ->
        IO ([Int], [Int], BS.ByteString)
    readLevelsAndDecompress compressionCodec pageheader bs = case unField pageheader.ph_type of
        DATA_PAGE _ -> case unField pageheader.ph_data_page_header of
            Nothing -> error "PageType is DATA_PAGE but data_page_header is missing"
            Just (datapageheader) -> do
                decompressed <- decompressData uncompressedSize compressionCodec bs
                let (ds, rs, rest) =
                        readLevelsV1
                            (fromIntegral $ unField datapageheader.dph_num_values)
                            (fromIntegral description.maxDefinitionLevel)
                            (fromIntegral description.maxRepetitionLevel)
                            decompressed
                return (rs, ds, rest)
        DICTIONARY_PAGE _ -> do
            decompressed <- decompressData uncompressedSize compressionCodec bs
            return ([], [], decompressed)
        INDEX_PAGE _ -> undefined
        DATA_PAGE_V2 _ -> case unField pageheader.ph_data_page_header_v2 of
            Nothing -> error "PageType is DATA_PAGE_V2 but data_page_header_v2 is missing"
            Just (datapageheaderv2) -> do
                let (ds, rs, rest) =
                        readLevelsV2
                            (fromIntegral $ unField datapageheaderv2.dph2_num_values)
                            (fromIntegral description.maxDefinitionLevel)
                            (fromIntegral description.maxRepetitionLevel)
                            (unField datapageheaderv2.dph2_definition_levels_byte_length)
                            (unField datapageheaderv2.dph2_repetition_levels_byte_length)
                            bs
                decompressed <- decompressData uncompressedSize compressionCodec rest
                return (rs, ds, decompressed)
      where
        uncompressedSize = fromIntegral $ unField pageheader.ph_uncompressed_page_size

parsePageHeader :: BS.ByteString -> Either String (BS.ByteString, PageHeader)
parsePageHeader bytes = decodeWithLeftovers Pinch.compactProtocol bytes

-- Readers

genericReader ::
    Maybe DictVals ->
    (BS.ByteString -> (a, BS.ByteString)) ->
    (DictVals -> Int -> a) ->
    ValueReader a
genericReader maybeDict readVal readDictVal = case maybeDict of
    Nothing -> ValueReader f
    Just dictionary -> dictReader dictionary readDictVal
  where
    f bs =
        let (value, bs') = readVal bs
         in (value, ValueReader f, bs')

boolReader :: Maybe DictVals -> ValueReader Bool
boolReader = \case
    Nothing -> ValueReader (f [])
    Just dictionary -> dictReader dictionary dictReaderBool
  where
    f [] bs
        | BS.null bs = error "Cannot read Bools from an empty buffer"
        | otherwise =
            let (valueStack, bs') = readBool bs
             in f valueStack bs'
    f (v : vs) bs = (v, ValueReader (f vs), bs)

int32Reader :: Maybe DictVals -> ValueReader Int32
int32Reader d = genericReader d readInt32 dictReaderInt32

int64Reader :: Maybe DictVals -> ValueReader Int64
int64Reader d = genericReader d readInt64 dictReaderInt64

int96Reader :: Maybe DictVals -> ValueReader UTCTime
int96Reader d = genericReader d readInt96 dictReaderInt96

floatReader :: Maybe DictVals -> ValueReader Float
floatReader d = genericReader d readFloat dictReaderFloat

doubleReader :: Maybe DictVals -> ValueReader Double
doubleReader d = genericReader d readDouble dictReaderDouble

byteArrayReader :: Maybe DictVals -> ValueReader T.Text
byteArrayReader d = genericReader d readByteArray dictReaderText

fixedLenByteArrayReader :: Int -> Maybe DictVals -> ValueReader T.Text
fixedLenByteArrayReader n d = genericReader d (readFixedLenByteArray n) dictReaderText

readBool :: BS.ByteString -> ([Bool], BS.ByteString)
readBool bs = (word8ToBools . BS.take 1 $ bs, BS.drop 1 bs)
  where
    word8ToBools ws =
        concatMap
            (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7])
            (BS.unpack ws)

readInt32 :: BS.ByteString -> (Int32, BS.ByteString)
readInt32 bs = (littleEndianInt32 (BS.take 4 bs), BS.drop 4 bs)

readInt64 :: BS.ByteString -> (Int64, BS.ByteString)
readInt64 bs = (fromIntegral $ littleEndianWord64 (BS.take 8 bs), BS.drop 8 bs)

readInt96 :: BS.ByteString -> (UTCTime, BS.ByteString)
readInt96 bs = (int96ToUTCTime (BS.take 12 bs), BS.drop 12 bs)

readFloat :: BS.ByteString -> (Float, BS.ByteString)
readFloat bs = (castWord32ToFloat . littleEndianWord32 . BS.take 4 $ bs, BS.drop 4 bs)

readDouble :: BS.ByteString -> (Double, BS.ByteString)
readDouble bs = (castWord64ToDouble . littleEndianWord64 . BS.take 8 $ bs, BS.drop 8 bs)

readByteArray :: BS.ByteString -> (T.Text, BS.ByteString)
readByteArray bs = (decodeUtf8Lenient . BS.take len . BS.drop 4 $ bs, BS.drop (len + 4) bs)
  where
    len = fromIntegral . littleEndianInt32 . BS.take 4 $ bs

readFixedLenByteArray :: Int -> BS.ByteString -> (T.Text, BS.ByteString)
readFixedLenByteArray len bs = (decodeUtf8Lenient . BS.take len $ bs, BS.drop len bs)

dictReader :: DictVals -> (DictVals -> Int -> a) -> ValueReader a
dictReader dictionary lookup = ValueReader f
  where
    f input = case BS.uncons input of
        Nothing -> error "Empty Index Buffer"
        Just (w, rest) ->
            let bitWidth = fromIntegral w :: Int
             in go bitWidth [] rest
    go bitWidth [] rest
        | BS.null rest = error "Empty Index Buffer"
        | otherwise = go bitWidth valueStack rest'
      where
        (indices, rest') = decodeRLEBitPackedHybrid bitWidth rest
        valueStack = map ((lookup dictionary) . fromIntegral) indices
    go bitWidth (v : vs) rest = (v, ValueReader f', rest)
      where
        f' input = go bitWidth vs input

dictReaderBool :: DictVals -> Int -> Bool
dictReaderBool (DBool ds) i = ds V.! i
dictReaderBool d _ = error $ "Expected Dictionary of Bools. Got Dictionary of " <> dictType d

dictReaderInt32 :: DictVals -> Int -> Int32
dictReaderInt32 (DInt32 ds) i = ds V.! i
dictReaderInt32 d _ = error $ "Expected Dictionary of Int32. Got Dictionary of " <> dictType d

dictReaderInt64 :: DictVals -> Int -> Int64
dictReaderInt64 (DInt64 ds) i = ds V.! i
dictReaderInt64 d _ = error $ "Expected Dictionary of Int64. Got Dictionary of " <> dictType d

dictReaderInt96 :: DictVals -> Int -> UTCTime
dictReaderInt96 (DInt96 ds) i = ds V.! i
dictReaderInt96 d _ = error $ "Expected Dictionary of Int64. Got Dictionary of " <> dictType d

dictReaderFloat :: DictVals -> Int -> Float
dictReaderFloat (DFloat ds) i = ds V.! i
dictReaderFloat d _ = error $ "Expected Dictionary of Float. Got Dictionary of " <> dictType d

dictReaderDouble :: DictVals -> Int -> Double
dictReaderDouble (DDouble ds) i = ds V.! i
dictReaderDouble d _ = error $ "Expected Dictionary of Double. Got Dictionary of " <> dictType d

dictReaderText :: DictVals -> Int -> T.Text
dictReaderText (DText ds) i = ds V.! i
dictReaderText d _ = error $ "Expected Dictionary of Text. Got Dictionary of " <> dictType d

dictType :: DictVals -> String
dictType (DBool _) = "Booleans"
dictType (DInt32 _) = "Int32"
dictType (DInt64 _) = "Int64"
dictType (DInt96 _) = "Int96"
dictType (DFloat _) = "Float"
dictType (DDouble _) = "Double"
dictType (DText _) = "Text"
