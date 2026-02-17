{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Thrift where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import Data.Char
import Data.IORef
import Data.Int
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import Data.Typeable (Typeable)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word
import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Types
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame, unsafeGetColumn)
import qualified DataFrame.Operations.Core as DI
import Type.Reflection (
    eqTypeRep,
    typeRep,
    (:~~:) (HRefl),
 )

data SchemaElement = SchemaElement
    { elementName :: T.Text
    , elementType :: TType
    , typeLength :: Int32
    , numChildren :: Int32
    , fieldId :: Int32
    , repetitionType :: RepetitionType
    , convertedType :: Int32
    , scale :: Int32
    , precision :: Int32
    , logicalType :: LogicalType
    }
    deriving (Show, Eq)

createParquetSchema :: DataFrame -> [SchemaElement]
createParquetSchema df = schemaDef : map toSchemaElement (DI.columnNames df)
  where
    -- The schema always contains an initial element
    -- indicating the group of fields.
    schemaDef =
        SchemaElement
            { elementName = "schema"
            , elementType = STOP
            , typeLength = 0
            , numChildren = fromIntegral (snd (DI.dimensions df))
            , fieldId = -1
            , repetitionType = UNKNOWN_REPETITION_TYPE
            , convertedType = 0
            , scale = 0
            , precision = 0
            , logicalType = LOGICAL_TYPE_UNKNOWN
            }
    toSchemaElement colName =
        let
            colType :: TType
            colType = case unsafeGetColumn colName df of
                (DI.BoxedColumn (col :: V.Vector a)) -> haskellToTType @a
                (DI.UnboxedColumn (col :: VU.Vector a)) -> haskellToTType @a
                (DI.OptionalColumn (col :: V.Vector (Maybe a))) -> haskellToTType @a
            lType =
                if DI.hasElemType @T.Text (unsafeGetColumn colName df)
                    || DI.hasElemType @(Maybe T.Text) (unsafeGetColumn colName df)
                    then STRING_TYPE
                    else LOGICAL_TYPE_UNKNOWN
         in
            SchemaElement colName colType 0 0 (-1) OPTIONAL 0 0 0 lType

data KeyValue = KeyValue
    { key :: String
    , value :: String
    }
    deriving (Show, Eq)

data FileMetadata = FileMetaData
    { version :: Int32
    , schema :: [SchemaElement]
    , numRows :: Integer
    , rowGroups :: [RowGroup]
    , keyValueMetadata :: [KeyValue]
    , createdBy :: Maybe String
    , columnOrders :: [ColumnOrder]
    , encryptionAlgorithm :: EncryptionAlgorithm
    , footerSigningKeyMetadata :: BS.ByteString
    }
    deriving (Show, Eq)

data TType
    = STOP
    | BOOL
    | BYTE
    | I16
    | I32
    | I64
    | I96
    | FLOAT
    | DOUBLE
    | STRING
    | LIST
    | SET
    | MAP
    | STRUCT
    | UUID
    deriving (Show, Eq)

haskellToTType :: forall a. (Typeable a) => TType
haskellToTType
    | is @Bool = BOOL
    | is @Int8 = BYTE
    | is @Word8 = BYTE
    | is @Int16 = I16
    | is @Word16 = I16
    | is @Int32 = I32
    | is @Word32 = I32
    | is @Int64 = I64
    | is @Word64 = I64
    | is @Float = FLOAT
    | is @Double = DOUBLE
    | is @String = STRING
    | is @T.Text = STRING
    | is @BS.ByteString = STRING
    | otherwise = STOP
  where
    is :: forall x. (Typeable x) => Bool
    is = case eqTypeRep (typeRep @a) (typeRep @x) of
        Just HRefl -> True
        Nothing -> False

defaultMetadata :: FileMetadata
defaultMetadata =
    FileMetaData
        { version = 0
        , schema = []
        , numRows = 0
        , rowGroups = []
        , keyValueMetadata = []
        , createdBy = Nothing
        , columnOrders = []
        , encryptionAlgorithm = ENCRYPTION_ALGORITHM_UNKNOWN
        , footerSigningKeyMetadata = BS.empty
        }

data ColumnMetaData = ColumnMetaData
    { columnType :: ParquetType
    , columnEncodings :: [ParquetEncoding]
    , columnPathInSchema :: [String]
    , columnCodec :: CompressionCodec
    , columnNumValues :: Int64
    , columnTotalUncompressedSize :: Int64
    , columnTotalCompressedSize :: Int64
    , columnKeyValueMetadata :: [KeyValue]
    , columnDataPageOffset :: Int64
    , columnIndexPageOffset :: Int64
    , columnDictionaryPageOffset :: Int64
    , columnStatistics :: ColumnStatistics
    , columnEncodingStats :: [PageEncodingStats]
    , bloomFilterOffset :: Int64
    , bloomFilterLength :: Int32
    , columnSizeStatistics :: SizeStatistics
    , columnGeospatialStatistics :: GeospatialStatistics
    }
    deriving (Show, Eq)

data ColumnChunk = ColumnChunk
    { columnChunkFilePath :: String
    , columnChunkMetadataFileOffset :: Int64
    , columnMetaData :: ColumnMetaData
    , columnChunkOffsetIndexOffset :: Int64
    , columnChunkOffsetIndexLength :: Int32
    , columnChunkColumnIndexOffset :: Int64
    , columnChunkColumnIndexLength :: Int32
    , cryptoMetadata :: ColumnCryptoMetadata
    , encryptedColumnMetadata :: BS.ByteString
    }
    deriving (Show, Eq)

data RowGroup = RowGroup
    { rowGroupColumns :: [ColumnChunk]
    , totalByteSize :: Int64
    , rowGroupNumRows :: Int64
    , rowGroupSortingColumns :: [SortingColumn]
    , fileOffset :: Int64
    , totalCompressedSize :: Int64
    , ordinal :: Int16
    }
    deriving (Show, Eq)

defaultSchemaElement :: SchemaElement
defaultSchemaElement =
    SchemaElement
        ""
        STOP
        0
        0
        (-1)
        UNKNOWN_REPETITION_TYPE
        0
        0
        0
        LOGICAL_TYPE_UNKNOWN

emptyColumnMetadata :: ColumnMetaData
emptyColumnMetadata =
    ColumnMetaData
        PARQUET_TYPE_UNKNOWN
        []
        []
        COMPRESSION_CODEC_UNKNOWN
        0
        0
        0
        []
        0
        0
        0
        emptyColumnStatistics
        []
        0
        0
        emptySizeStatistics
        emptyGeospatialStatistics

emptyColumnChunk :: ColumnChunk
emptyColumnChunk =
    ColumnChunk
        ""
        0
        emptyColumnMetadata
        0
        0
        0
        0
        COLUMN_CRYPTO_METADATA_UNKNOWN
        BS.empty

emptyKeyValue :: KeyValue
emptyKeyValue = KeyValue{key = "", value = ""}

emptyRowGroup :: RowGroup
emptyRowGroup = RowGroup [] 0 0 [] 0 0 0

compactBooleanTrue
    , compactI32
    , compactI64
    , compactDouble
    , compactBinary
    , compactList
    , compactStruct ::
        Word8
compactBooleanTrue = 0x01
compactI32 = 0x05
compactI64 = 0x06
compactDouble = 0x07
compactBinary = 0x08
compactList = 0x09
compactStruct = 0x0C

toTType :: Word8 -> TType
toTType t =
    fromMaybe STOP $
        M.lookup (t .&. 0x0f) $
            M.fromList
                [ (compactBooleanTrue, BOOL)
                , (compactI32, I32)
                , (compactI64, I64)
                , (compactDouble, DOUBLE)
                , (compactBinary, STRING)
                , (compactList, LIST)
                , (compactStruct, STRUCT)
                ]

readField ::
    BS.ByteString -> IORef Int -> Int16 -> IO (Maybe (TType, Int16))
readField buf pos lastFieldId = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return Nothing
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return (lastFieldId + modifier)
            let elemType = toTType (t .&. 0x0f)
            pure $ Just (elemType, identifier)

skipToStructEnd :: BS.ByteString -> IORef Int -> IO ()
skipToStructEnd buf pos = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return ()
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return 0
            let elemType = toTType (t .&. 0x0f)
            skipFieldData elemType buf pos
            skipToStructEnd buf pos

skipFieldData :: TType -> BS.ByteString -> IORef Int -> IO ()
skipFieldData fieldType buf pos = case fieldType of
    BOOL -> return ()
    I32 -> void (readIntFromBuffer @Int32 buf pos)
    I64 -> void (readIntFromBuffer @Int64 buf pos)
    DOUBLE -> void (readIntFromBuffer @Int64 buf pos)
    STRING -> void (readByteString buf pos)
    LIST -> skipList buf pos
    STRUCT -> skipToStructEnd buf pos
    _ -> error $ "Unknown field type" ++ show fieldType

skipList :: BS.ByteString -> IORef Int -> IO ()
skipList buf pos = do
    sizeAndType <- readAndAdvance pos buf
    let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
    let elemType = toTType sizeAndType
    replicateM_ sizeOnly (skipFieldData elemType buf pos)

readMetadata :: BS.ByteString -> Int -> IO FileMetadata
readMetadata contents size = do
    let metadataStartPos = BS.length contents - footerSize - size
    let metadataBytes =
            BS.pack $
                map (BS.index contents) [metadataStartPos .. (metadataStartPos + size - 1)]
    let lastFieldId = 0
    bufferPos <- newIORef (0 :: Int)
    readFileMetaData defaultMetadata metadataBytes bufferPos lastFieldId

readFileMetaData ::
    FileMetadata ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO FileMetadata
readFileMetaData metadata metaDataBuf bufferPos lastFieldId = do
    fieldContents <- readField metaDataBuf bufferPos lastFieldId
    case fieldContents of
        Nothing -> return metadata
        Just (elemType, identifier) -> case identifier of
            1 -> do
                version <- readIntFromBuffer @Int32 metaDataBuf bufferPos
                readFileMetaData
                    (metadata{version = version})
                    metaDataBuf
                    bufferPos
                    identifier
            2 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)
                let _elemType = toTType sizeAndType
                schemaElements <-
                    replicateM
                        listSize
                        (readSchemaElement defaultSchemaElement metaDataBuf bufferPos 0)
                readFileMetaData
                    (metadata{schema = schemaElements})
                    metaDataBuf
                    bufferPos
                    identifier
            3 -> do
                numRows <- readIntFromBuffer @Int64 metaDataBuf bufferPos
                readFileMetaData
                    (metadata{numRows = fromIntegral numRows})
                    metaDataBuf
                    bufferPos
                    identifier
            4 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                -- TODO actually check elemType agrees (also for all the other underscored _elemType in this module)
                let _elemType = toTType sizeAndType
                rowGroups <-
                    replicateM listSize (readRowGroup emptyRowGroup metaDataBuf bufferPos 0)
                readFileMetaData
                    (metadata{rowGroups = rowGroups})
                    metaDataBuf
                    bufferPos
                    identifier
            5 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                let _elemType = toTType sizeAndType
                keyValueMetadata <-
                    replicateM listSize (readKeyValue emptyKeyValue metaDataBuf bufferPos 0)
                readFileMetaData
                    (metadata{keyValueMetadata = keyValueMetadata})
                    metaDataBuf
                    bufferPos
                    identifier
            6 -> do
                createdBy <- readString metaDataBuf bufferPos
                readFileMetaData
                    (metadata{createdBy = Just createdBy})
                    metaDataBuf
                    bufferPos
                    identifier
            7 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                let _elemType = toTType sizeAndType
                columnOrders <- replicateM listSize (readColumnOrder metaDataBuf bufferPos 0)
                readFileMetaData
                    (metadata{columnOrders = columnOrders})
                    metaDataBuf
                    bufferPos
                    identifier
            8 -> do
                encryptionAlgorithm <- readEncryptionAlgorithm metaDataBuf bufferPos 0
                readFileMetaData
                    (metadata{encryptionAlgorithm = encryptionAlgorithm})
                    metaDataBuf
                    bufferPos
                    identifier
            9 -> do
                footerSigningKeyMetadata <- readByteString metaDataBuf bufferPos
                readFileMetaData
                    (metadata{footerSigningKeyMetadata = footerSigningKeyMetadata})
                    metaDataBuf
                    bufferPos
                    identifier
            n -> return $ error $ "UNIMPLEMENTED " ++ show n

readSchemaElement ::
    SchemaElement ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO SchemaElement
readSchemaElement schemaElement buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return schemaElement
        Just (elemType, identifier) -> case identifier of
            1 -> do
                schemaElemType <- toIntegralType <$> readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{elementType = schemaElemType})
                    buf
                    pos
                    identifier
            2 -> do
                typeLength <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{typeLength = typeLength})
                    buf
                    pos
                    identifier
            3 -> do
                fieldRepetitionType <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{repetitionType = repetitionTypeFromInt fieldRepetitionType})
                    buf
                    pos
                    identifier
            4 -> do
                nameSize <- readVarIntFromBuffer @Int buf pos
                if nameSize <= 0
                    then readSchemaElement schemaElement buf pos identifier
                    else do
                        contents <- replicateM nameSize (readAndAdvance pos buf)
                        readSchemaElement
                            (schemaElement{elementName = T.pack (map (chr . fromIntegral) contents)})
                            buf
                            pos
                            identifier
            5 -> do
                numChildren <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{numChildren = numChildren})
                    buf
                    pos
                    identifier
            6 -> do
                convertedType <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{convertedType = convertedType})
                    buf
                    pos
                    identifier
            7 -> do
                scale <- readInt32FromBuffer buf pos
                readSchemaElement (schemaElement{scale = scale}) buf pos identifier
            8 -> do
                precision <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{precision = precision})
                    buf
                    pos
                    identifier
            9 -> do
                fieldId <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{fieldId = fieldId})
                    buf
                    pos
                    identifier
            10 -> do
                logicalType <- readLogicalType LOGICAL_TYPE_UNKNOWN buf pos 0
                readSchemaElement
                    (schemaElement{logicalType = logicalType})
                    buf
                    pos
                    identifier
            n -> error ("Uknown schema element: " ++ show n)

readRowGroup ::
    RowGroup -> BS.ByteString -> IORef Int -> Int16 -> IO RowGroup
readRowGroup r buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return r
        Just (elemType, identifier) -> case identifier of
            1 -> do
                sizeAndType <- readAndAdvance pos buf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int buf pos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)
                let _elemType = toTType sizeAndType
                columnChunks <-
                    replicateM listSize (readColumnChunk emptyColumnChunk buf pos 0)
                readRowGroup (r{rowGroupColumns = columnChunks}) buf pos identifier
            2 -> do
                totalBytes <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{totalByteSize = totalBytes}) buf pos identifier
            3 -> do
                nRows <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{rowGroupNumRows = nRows}) buf pos identifier
            4 -> return r
            5 -> do
                offset <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{fileOffset = offset}) buf pos identifier
            6 -> do
                compressedSize <- readIntFromBuffer @Int64 buf pos
                readRowGroup
                    (r{totalCompressedSize = compressedSize})
                    buf
                    pos
                    identifier
            7 -> do
                ordinal <- readIntFromBuffer @Int16 buf pos
                readRowGroup (r{ordinal = ordinal}) buf pos identifier
            _ -> error $ "Unknown row group field: " ++ show identifier

readColumnChunk ::
    ColumnChunk -> BS.ByteString -> IORef Int -> Int16 -> IO ColumnChunk
readColumnChunk c buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return c
        Just (elemType, identifier) -> case identifier of
            1 -> do
                stringSize <- readVarIntFromBuffer @Int buf pos
                contents <-
                    map (chr . fromIntegral) <$> replicateM stringSize (readAndAdvance pos buf)
                readColumnChunk
                    (c{columnChunkFilePath = contents})
                    buf
                    pos
                    identifier
            2 -> do
                columnChunkMetadataFileOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkMetadataFileOffset = columnChunkMetadataFileOffset})
                    buf
                    pos
                    identifier
            3 -> do
                columnMetadata <- readColumnMetadata emptyColumnMetadata buf pos 0
                readColumnChunk
                    (c{columnMetaData = columnMetadata})
                    buf
                    pos
                    identifier
            4 -> do
                columnOffsetIndexOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkOffsetIndexOffset = columnOffsetIndexOffset})
                    buf
                    pos
                    identifier
            5 -> do
                columnOffsetIndexLength <- readInt32FromBuffer buf pos
                readColumnChunk
                    (c{columnChunkOffsetIndexLength = columnOffsetIndexLength})
                    buf
                    pos
                    identifier
            6 -> do
                columnChunkColumnIndexOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkColumnIndexOffset = columnChunkColumnIndexOffset})
                    buf
                    pos
                    identifier
            7 -> do
                columnChunkColumnIndexLength <- readInt32FromBuffer buf pos
                readColumnChunk
                    (c{columnChunkColumnIndexLength = columnChunkColumnIndexLength})
                    buf
                    pos
                    identifier
            _ -> error "Unknown column chunk"

readColumnMetadata ::
    ColumnMetaData ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO ColumnMetaData
readColumnMetadata cm buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return cm
        Just (elemType, identifier) -> case identifier of
            1 -> do
                cType <- parquetTypeFromInt <$> readInt32FromBuffer buf pos
                readColumnMetadata (cm{columnType = cType}) buf pos identifier
            2 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                encodings <- replicateM sizeOnly (readParquetEncoding buf pos 0)
                readColumnMetadata
                    (cm{columnEncodings = encodings})
                    buf
                    pos
                    identifier
            3 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                paths <- replicateM sizeOnly (readString buf pos)
                readColumnMetadata
                    (cm{columnPathInSchema = paths})
                    buf
                    pos
                    identifier
            4 -> do
                cType <- compressionCodecFromInt <$> readInt32FromBuffer buf pos
                readColumnMetadata (cm{columnCodec = cType}) buf pos identifier
            5 -> do
                numValues <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata (cm{columnNumValues = numValues}) buf pos identifier
            6 -> do
                columnTotalUncompressedSize <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnTotalUncompressedSize = columnTotalUncompressedSize})
                    buf
                    pos
                    identifier
            7 -> do
                columnTotalCompressedSize <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnTotalCompressedSize = columnTotalCompressedSize})
                    buf
                    pos
                    identifier
            8 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                columnKeyValueMetadata <-
                    replicateM sizeOnly (readKeyValue emptyKeyValue buf pos 0)
                readColumnMetadata
                    (cm{columnKeyValueMetadata = columnKeyValueMetadata})
                    buf
                    pos
                    identifier
            9 -> do
                columnDataPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnDataPageOffset = columnDataPageOffset})
                    buf
                    pos
                    identifier
            10 -> do
                columnIndexPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnIndexPageOffset = columnIndexPageOffset})
                    buf
                    pos
                    identifier
            11 -> do
                columnDictionaryPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnDictionaryPageOffset = columnDictionaryPageOffset})
                    buf
                    pos
                    identifier
            12 -> do
                stats <- readStatistics emptyColumnStatistics buf pos 0
                readColumnMetadata (cm{columnStatistics = stats}) buf pos identifier
            13 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                pageEncodingStats <-
                    replicateM sizeOnly (readPageEncodingStats emptyPageEncodingStats buf pos 0)
                readColumnMetadata
                    (cm{columnEncodingStats = pageEncodingStats})
                    buf
                    pos
                    identifier
            14 -> do
                bloomFilterOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{bloomFilterOffset = bloomFilterOffset})
                    buf
                    pos
                    identifier
            15 -> do
                bloomFilterLength <- readInt32FromBuffer buf pos
                readColumnMetadata
                    (cm{bloomFilterLength = bloomFilterLength})
                    buf
                    pos
                    identifier
            16 -> do
                stats <- readSizeStatistics emptySizeStatistics buf pos 0
                readColumnMetadata
                    (cm{columnSizeStatistics = stats})
                    buf
                    pos
                    identifier
            17 -> return $ error "UNIMPLEMENTED"
            _ -> error $ "Unknown column metadata " ++ show identifier

readEncryptionAlgorithm ::
    BS.ByteString -> IORef Int -> Int16 -> IO EncryptionAlgorithm
readEncryptionAlgorithm buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return ENCRYPTION_ALGORITHM_UNKNOWN
        Just (elemType, identifier) -> case identifier of
            1 -> do
                readAesGcmV1
                    ( AesGcmV1
                        { aadPrefix = BS.empty
                        , aadFileUnique = BS.empty
                        , supplyAadPrefix = False
                        }
                    )
                    buf
                    pos
                    0
            2 -> do
                readAesGcmCtrV1
                    ( AesGcmCtrV1
                        { aadPrefix = BS.empty
                        , aadFileUnique = BS.empty
                        , supplyAadPrefix = False
                        }
                    )
                    buf
                    pos
                    0
            n -> return ENCRYPTION_ALGORITHM_UNKNOWN

readColumnOrder ::
    BS.ByteString -> IORef Int -> Int16 -> IO ColumnOrder
readColumnOrder buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return COLUMN_ORDER_UNKNOWN
        Just (elemType, identifier) -> case identifier of
            1 -> do
                -- Read begin struct and stop since this an empty struct.
                replicateM_ 2 (readTypeOrder buf pos 0)
                return TYPE_ORDER
            _ -> return COLUMN_ORDER_UNKNOWN

readAesGcmCtrV1 ::
    EncryptionAlgorithm ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO EncryptionAlgorithm
readAesGcmCtrV1 v@(AesGcmCtrV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return v
        Just (elemType, identifier) -> case identifier of
            1 -> do
                aadPrefix <- readByteString buf pos
                readAesGcmCtrV1 (v{aadPrefix = aadPrefix}) buf pos identifier
            2 -> do
                aadFileUnique <- readByteString buf pos
                readAesGcmCtrV1
                    (v{aadFileUnique = aadFileUnique})
                    buf
                    pos
                    identifier
            3 -> do
                supplyAadPrefix <- readAndAdvance pos buf
                readAesGcmCtrV1
                    (v{supplyAadPrefix = supplyAadPrefix == compactBooleanTrue})
                    buf
                    pos
                    identifier
            _ -> return ENCRYPTION_ALGORITHM_UNKNOWN
readAesGcmCtrV1 _ _ _ _ =
    error "readAesGcmCtrV1 called with non AesGcmCtrV1"

readAesGcmV1 ::
    EncryptionAlgorithm ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO EncryptionAlgorithm
readAesGcmV1 v@(AesGcmV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return v
        Just (elemType, identifier) -> case identifier of
            1 -> do
                aadPrefix <- readByteString buf pos
                readAesGcmV1 (v{aadPrefix = aadPrefix}) buf pos identifier
            2 -> do
                aadFileUnique <- readByteString buf pos
                readAesGcmV1 (v{aadFileUnique = aadFileUnique}) buf pos identifier
            3 -> do
                supplyAadPrefix <- readAndAdvance pos buf
                readAesGcmV1
                    (v{supplyAadPrefix = supplyAadPrefix == compactBooleanTrue})
                    buf
                    pos
                    identifier
            _ -> return ENCRYPTION_ALGORITHM_UNKNOWN
readAesGcmV1 _ _ _ _ =
    error "readAesGcmV1 called with non AesGcmV1"

readTypeOrder ::
    BS.ByteString -> IORef Int -> Int16 -> IO ColumnOrder
readTypeOrder buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return TYPE_ORDER
        Just (elemType, identifier) ->
            if elemType == STOP
                then return TYPE_ORDER
                else readTypeOrder buf pos identifier

readKeyValue ::
    KeyValue -> BS.ByteString -> IORef Int -> Int16 -> IO KeyValue
readKeyValue kv buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return kv
        Just (elemType, identifier) -> case identifier of
            1 -> do
                k <- readString buf pos
                readKeyValue (kv{key = k}) buf pos identifier
            2 -> do
                v <- readString buf pos
                readKeyValue (kv{value = v}) buf pos identifier
            _ -> error "Unknown kv"

readPageEncodingStats ::
    PageEncodingStats ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO PageEncodingStats
readPageEncodingStats pes buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return pes
        Just (elemType, identifier) -> case identifier of
            1 -> do
                pType <- pageTypeFromInt <$> readInt32FromBuffer buf pos
                readPageEncodingStats (pes{pageEncodingPageType = pType}) buf pos identifier
            2 -> do
                pEnc <- parquetEncodingFromInt <$> readInt32FromBuffer buf pos
                readPageEncodingStats (pes{pageEncoding = pEnc}) buf pos identifier
            3 -> do
                encodedCount <- readInt32FromBuffer buf pos
                readPageEncodingStats
                    (pes{pagesWithEncoding = encodedCount})
                    buf
                    pos
                    identifier
            _ -> error "Unknown page encoding stats"

readParquetEncoding ::
    BS.ByteString -> IORef Int -> Int16 -> IO ParquetEncoding
readParquetEncoding buf pos lastFieldId = parquetEncodingFromInt <$> readInt32FromBuffer buf pos

readStatistics ::
    ColumnStatistics ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO ColumnStatistics
readStatistics cs buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return cs
        Just (elemType, identifier) -> case identifier of
            1 -> do
                maxInBytes <- readByteString buf pos
                readStatistics (cs{columnMax = maxInBytes}) buf pos identifier
            2 -> do
                minInBytes <- readByteString buf pos
                readStatistics (cs{columnMin = minInBytes}) buf pos identifier
            3 -> do
                nullCount <- readIntFromBuffer @Int64 buf pos
                readStatistics (cs{columnNullCount = nullCount}) buf pos identifier
            4 -> do
                distinctCount <- readIntFromBuffer @Int64 buf pos
                readStatistics
                    (cs{columnDistictCount = distinctCount})
                    buf
                    pos
                    identifier
            5 -> do
                maxInBytes <- readByteString buf pos
                readStatistics (cs{columnMaxValue = maxInBytes}) buf pos identifier
            6 -> do
                minInBytes <- readByteString buf pos
                readStatistics (cs{columnMinValue = minInBytes}) buf pos identifier
            7 -> do
                isMaxValueExact <- readAndAdvance pos buf
                readStatistics
                    (cs{isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue})
                    buf
                    pos
                    identifier
            8 -> do
                isMinValueExact <- readAndAdvance pos buf
                readStatistics
                    (cs{isColumnMinValueExact = isMinValueExact == compactBooleanTrue})
                    buf
                    pos
                    identifier
            _ -> error "Unknown statistics"

readSizeStatistics ::
    SizeStatistics ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO SizeStatistics
readSizeStatistics ss buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return ss
        Just (elemType, identifier) -> case identifier of
            1 -> do
                unencodedByteArrayDataTypes <- readIntFromBuffer @Int64 buf pos
                readSizeStatistics
                    (ss{unencodedByteArrayDataTypes = unencodedByteArrayDataTypes})
                    buf
                    pos
                    identifier
            2 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                repetitionLevelHistogram <-
                    replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
                readSizeStatistics
                    (ss{repetitionLevelHistogram = repetitionLevelHistogram})
                    buf
                    pos
                    identifier
            3 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                definitionLevelHistogram <-
                    replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
                readSizeStatistics
                    (ss{definitionLevelHistogram = definitionLevelHistogram})
                    buf
                    pos
                    identifier
            _ -> error "Unknown size statistics"

footerSize :: Int
footerSize = 8

toIntegralType :: Int32 -> TType
toIntegralType n
    | n == 0 = BOOL
    | n == 1 = I32
    | n == 2 = I64
    | n == 3 = I96
    | n == 4 = FLOAT
    | n == 5 = DOUBLE
    | n == 6 = STRING
    | n == 7 = STRING
    | otherwise = error ("Unknown type in schema: " ++ show n)

readLogicalType ::
    LogicalType -> BS.ByteString -> IORef Int -> Int16 -> IO LogicalType
readLogicalType logicalType buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> pure logicalType
        Just (elemType, identifier) -> case identifier of
            1 -> do
                -- This is an empty enum and is read as a field.
                _ <- readField buf pos 0
                readLogicalType STRING_TYPE buf pos identifier
            2 -> do
                _ <- readField buf pos 0
                readLogicalType MAP_TYPE buf pos identifier
            3 -> do
                _ <- readField buf pos 0
                readLogicalType LIST_TYPE buf pos identifier
            4 -> do
                _ <- readField buf pos 0
                readLogicalType ENUM_TYPE buf pos identifier
            5 -> do
                decimal <- readDecimalType 0 0 buf pos 0
                readLogicalType decimal buf pos identifier
            6 -> do
                _ <- readField buf pos 0
                readLogicalType DATE_TYPE buf pos identifier
            7 -> do
                time <- readTimeType False MILLISECONDS buf pos 0
                readLogicalType time buf pos identifier
            8 -> do
                timestamp <- readTimestampType False MILLISECONDS buf pos 0
                readLogicalType timestamp buf pos identifier
            -- Apparently reserved for interval types
            9 -> do
                _ <- readField buf pos 0
                readLogicalType LOGICAL_TYPE_UNKNOWN buf pos identifier
            10 -> do
                intType <- readIntType 0 False buf pos 0
                readLogicalType intType buf pos identifier
            11 -> do
                _ <- readField buf pos 0
                readLogicalType LOGICAL_TYPE_UNKNOWN buf pos identifier
            12 -> do
                _ <- readField buf pos 0
                readLogicalType JSON_TYPE buf pos identifier
            13 -> do
                _ <- readField buf pos 0
                readLogicalType BSON_TYPE buf pos identifier
            14 -> do
                _ <- readField buf pos 0
                readLogicalType UUID_TYPE buf pos identifier
            15 -> do
                _ <- readField buf pos 0
                readLogicalType FLOAT16_TYPE buf pos identifier
            16 -> error "Variant fields are unsupported"
            17 -> error "Geometry fields are unsupported"
            18 -> error "Geography fields are unsupported"
            n -> error $ "Unknown logical type field: " ++ show n

readIntType ::
    Int8 ->
    Bool ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO LogicalType
readIntType bitWidth intIsSigned buf pos lastFieldId = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return (IntType bitWidth intIsSigned)
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return (lastFieldId + modifier)

            case identifier of
                1 -> do
                    bitWidth' <- readAndAdvance pos buf
                    readIntType (fromIntegral bitWidth') intIsSigned buf pos identifier
                2 -> do
                    let intIsSigned' = (t .&. 0x0f) == compactBooleanTrue
                    readIntType bitWidth intIsSigned' buf pos identifier
                _ -> error $ "UNKNOWN field ID for IntType: " ++ show identifier

readDecimalType ::
    Int32 ->
    Int32 ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO LogicalType
readDecimalType precision scale buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return (DecimalType precision scale)
        Just (elemType, identifier) -> case identifier of
            1 -> do
                scale' <- readInt32FromBuffer buf pos
                readDecimalType precision scale' buf pos identifier
            2 -> do
                precision' <- readInt32FromBuffer buf pos
                readDecimalType precision' scale buf pos identifier
            _ -> error $ "UNKNOWN field ID for DecimalType" ++ show identifier

readTimeType ::
    Bool ->
    TimeUnit ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO LogicalType
readTimeType isAdjustedToUTC unit buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return (TimeType{isAdjustedToUTC = isAdjustedToUTC, unit = unit})
        Just (elemType, identifier) -> case identifier of
            1 -> do
                isAdjustedToUTC' <- (== compactBooleanTrue) <$> readAndAdvance pos buf
                readTimeType isAdjustedToUTC' unit buf pos identifier
            2 -> do
                unit' <- readUnit TIME_UNIT_UNKNOWN buf pos 0
                readTimeType isAdjustedToUTC unit' buf pos identifier
            _ -> error $ "UNKNOWN field ID for TimeType" ++ show identifier

readTimestampType ::
    Bool ->
    TimeUnit ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    IO LogicalType
readTimestampType isAdjustedToUTC unit buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return (TimestampType isAdjustedToUTC unit)
        Just (elemType, identifier) -> case identifier of
            1 -> do
                isAdjustedToUTC' <- (== compactBooleanTrue) <$> readNoAdvance pos buf
                readTimestampType False unit buf pos identifier
            2 -> do
                _ <- readField buf pos 0
                unit' <- readUnit TIME_UNIT_UNKNOWN buf pos 0
                readTimestampType isAdjustedToUTC unit' buf pos identifier
            _ -> error $ "UNKNOWN field ID for TimestampType" ++ show identifier

readUnit :: TimeUnit -> BS.ByteString -> IORef Int -> Int16 -> IO TimeUnit
readUnit unit buf pos lastFieldId = do
    fieldContents <- readField buf pos lastFieldId
    case fieldContents of
        Nothing -> return unit
        Just (elemType, identifier) -> case identifier of
            1 -> do
                readUnit MILLISECONDS buf pos identifier
            2 -> do
                readUnit MICROSECONDS buf pos identifier
            3 -> do
                readUnit NANOSECONDS buf pos identifier
            n -> error $ "Unknown time unit: " ++ show n
