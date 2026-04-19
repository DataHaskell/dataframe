{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Unstable.Parquet.Page (
    -- Types
    PageDecoder,
    -- Per-type decoders
    boolDecoder,
    int32Decoder,
    int64Decoder,
    int96Decoder,
    floatDecoder,
    doubleDecoder,
    byteArrayDecoder,
    fixedLenByteArrayDecoder,
    -- Chunk processors
    nonNullableChunk,
    nullableChunk,
    repeatedChunk,
) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.Maybe (fromJust, fromMaybe)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Time (UTCTime)
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import DataFrame.IO.Unstable.Parquet.Decompress (decompressData)
import DataFrame.IO.Unstable.Parquet.Dictionary (
    DictVals (..),
    readDictVals,
 )
import DataFrame.IO.Unstable.Parquet.Encoding (decodeDictIndicesV)
import DataFrame.IO.Unstable.Parquet.Levels (readLevelsV1V, readLevelsV2V)
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
import DataFrame.IO.Unstable.Parquet.Time (int96ToUTCTime)
import DataFrame.IO.Unstable.Parquet.Utils (ColumnDescription (..))
import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), Range (Range))
import DataFrame.Internal.Binary (
    littleEndianInt32,
    littleEndianWord32,
    littleEndianWord64,
 )
import GHC.Float (castWord32ToFloat, castWord64ToDouble)
import Pinch (decodeWithLeftovers)
import qualified Pinch
import qualified Streamly.Data.Stream as Stream
import Streamly.Internal.Data.Unfold (Step (..), Unfold, mkUnfoldM)

-- ---------------------------------------------------------------------------
-- Types
-- ---------------------------------------------------------------------------

{- | A type-specific page decoder.
Given the optional dictionary, the page encoding, the number of present
values, and the decompressed value bytes, returns exactly @nPresent@ values.
-}
type PageDecoder a =
    Maybe DictVals -> Encoding -> Int -> BS.ByteString -> VB.Vector a

-- ---------------------------------------------------------------------------
-- Per-type decoders
-- ---------------------------------------------------------------------------

boolDecoder :: PageDecoder Bool
boolDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNBool nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getBool
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getBool
    _ -> error ("boolDecoder: unsupported encoding " ++ show enc)
  where
    getBool (DBool ds) i = ds VB.! i
    getBool d _ = error ("boolDecoder: wrong dict type, got " ++ show d)

int32Decoder :: PageDecoder Int32
int32Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.convert (readNInt32 nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getInt32
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getInt32
    _ -> error ("int32Decoder: unsupported encoding " ++ show enc)
  where
    getInt32 (DInt32 ds) i = ds VB.! i
    getInt32 d _ = error ("int32Decoder: wrong dict type, got " ++ show d)

int64Decoder :: PageDecoder Int64
int64Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.convert (readNInt64 nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getInt64
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getInt64
    _ -> error ("int64Decoder: unsupported encoding " ++ show enc)
  where
    getInt64 (DInt64 ds) i = ds VB.! i
    getInt64 d _ = error ("int64Decoder: wrong dict type, got " ++ show d)

int96Decoder :: PageDecoder UTCTime
int96Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNInt96 nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getInt96
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getInt96
    _ -> error ("int96Decoder: unsupported encoding " ++ show enc)
  where
    getInt96 (DInt96 ds) i = ds VB.! i
    getInt96 d _ = error ("int96Decoder: wrong dict type, got " ++ show d)

floatDecoder :: PageDecoder Float
floatDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.convert (readNFloat nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getFloat
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getFloat
    _ -> error ("floatDecoder: unsupported encoding " ++ show enc)
  where
    getFloat (DFloat ds) i = ds VB.! i
    getFloat d _ = error ("floatDecoder: wrong dict type, got " ++ show d)

doubleDecoder :: PageDecoder Double
doubleDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.convert (readNDouble nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getDouble
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getDouble
    _ -> error ("doubleDecoder: unsupported encoding " ++ show enc)
  where
    getDouble (DDouble ds) i = ds VB.! i
    getDouble d _ = error ("doubleDecoder: wrong dict type, got " ++ show d)

byteArrayDecoder :: PageDecoder T.Text
byteArrayDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNTexts nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    _ -> error ("byteArrayDecoder: unsupported encoding " ++ show enc)
  where
    getText (DText ds) i = ds VB.! i
    getText d _ = error ("byteArrayDecoder: wrong dict type, got " ++ show d)

fixedLenByteArrayDecoder :: Int -> PageDecoder T.Text
fixedLenByteArrayDecoder len mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNFixedTexts len nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    _ -> error ("fixedLenByteArrayDecoder: unsupported encoding " ++ show enc)
  where
    getText (DText ds) i = ds VB.! i
    getText d _ = error ("fixedLenByteArrayDecoder: wrong dict type, got " ++ show d)

{- | Shared dictionary-path helper: decode @nPresent@ RLE/bit-packed indices
and look each one up in the dictionary.
-}
lookupDict ::
    Maybe DictVals ->
    Int ->
    BS.ByteString ->
    (DictVals -> Int -> a) ->
    VB.Vector a
lookupDict mDict nPresent bs f = case mDict of
    Nothing -> error "Dictionary-encoded page but no dictionary page seen"
    Just dict ->
        let (idxs, _) = decodeDictIndicesV nPresent bs
         in VB.generate nPresent (\i -> f dict (VU.unsafeIndex idxs i))

-- ---------------------------------------------------------------------------
-- Chunk processors
-- ---------------------------------------------------------------------------

-- | Process one @ColumnChunk@ into a vector of values (non-nullable path).
nonNullableChunk ::
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    PageDecoder a ->
    ColumnChunk ->
    m (VB.Vector a)
nonNullableChunk description decoder columnChunk = do
    (codec, pType, rawBytes) <- readChunkBytes columnChunk
    pages <-
        liftIO $
            Stream.toList $
                Stream.unfold (readPages description codec pType decoder) rawBytes
    return $ VB.concat [vs | (vs, _, _) <- pages]

{- | Process one @ColumnChunk@ into (values, definition levels) for nullable
columns (@maxDef > 0@, @maxRep == 0@).
-}
nullableChunk ::
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    PageDecoder a ->
    ColumnChunk ->
    m (VB.Vector a, VU.Vector Int)
nullableChunk description decoder columnChunk = do
    (codec, pType, rawBytes) <- readChunkBytes columnChunk
    pages <-
        liftIO $
            Stream.toList $
                Stream.unfold (readPages description codec pType decoder) rawBytes
    return
        ( VB.concat [vs | (vs, _, _) <- pages]
        , VU.concat [ds | (_, ds, _) <- pages]
        )

{- | Process one @ColumnChunk@ into (values, definition levels, repetition
levels) for repeated columns (@maxRep > 0@).
-}
repeatedChunk ::
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    PageDecoder a ->
    ColumnChunk ->
    m (VB.Vector a, VU.Vector Int, VU.Vector Int)
repeatedChunk description decoder columnChunk = do
    (codec, pType, rawBytes) <- readChunkBytes columnChunk
    pages <-
        liftIO $
            Stream.toList $
                Stream.unfold (readPages description codec pType decoder) rawBytes
    return
        ( VB.concat [vs | (vs, _, _) <- pages]
        , VU.concat [ds | (_, ds, _) <- pages]
        , VU.concat [rs | (_, _, rs) <- pages]
        )

-- ---------------------------------------------------------------------------
-- Core page-iteration loop
-- ---------------------------------------------------------------------------

-- | Read the raw (compressed) byte range for a column chunk.
readChunkBytes ::
    (RandomAccess m) =>
    ColumnChunk ->
    m (CompressionCodec, ThriftType, BS.ByteString)
readChunkBytes columnChunk = do
    let meta = fromJust . unField $ columnChunk.cc_meta_data
        codec = unField meta.cmd_codec
        pType = unField meta.cmd_type
        dataOffset = fromIntegral . unField $ meta.cmd_data_page_offset
        dictOffset = fromIntegral <$> unField meta.cmd_dictionary_page_offset
        offset = fromMaybe dataOffset dictOffset
        compLen = fromIntegral . unField $ meta.cmd_total_compressed_size
    rawBytes <- readBytes (Range offset compLen)
    return (codec, pType, rawBytes)

{- | An 'Unfold' over the pages of a column chunk.

Seed: the raw (possibly compressed) bytes starting at the first page.
Yields one @(values, defLevels, repLevels)@ triple per data page.
Dictionary pages are consumed silently and update the running dictionary
that is threaded through the unfold state.

The internal state is @(Maybe DictVals, BS.ByteString)@: current dictionary
and remaining bytes.
-}
readPages ::
    ColumnDescription ->
    CompressionCodec ->
    ThriftType ->
    PageDecoder a ->
    Unfold IO BS.ByteString (VB.Vector a, VU.Vector Int, VU.Vector Int)
readPages description codec pType decoder = mkUnfoldM step inject
  where
    maxDef = fromIntegral description.maxDefinitionLevel :: Int
    maxRep = fromIntegral description.maxRepetitionLevel :: Int

    -- Inject: wrap the raw bytes with an empty dictionary.
    inject bs = return (Nothing, bs)

    step (dict, bs)
        | BS.null bs = return Stop
        | otherwise = case parsePageHeader bs of
            Left e -> error ("readPages: failed to parse page header: " ++ e)
            Right (rest, hdr) -> do
                let compSz = fromIntegral . unField $ hdr.ph_compressed_page_size
                    uncmpSz = fromIntegral . unField $ hdr.ph_uncompressed_page_size
                    (pageData, rest') = BS.splitAt compSz rest
                case unField hdr.ph_type of
                    DICTIONARY_PAGE _ -> do
                        let Just dictHdr = unField hdr.ph_dictionary_page_header
                            numVals = unField dictHdr.diph_num_values
                        decompressed <- decompressData uncmpSz codec pageData
                        let d = readDictVals pType decompressed (Just numVals)
                        return $ Skip (Just d, rest')
                    DATA_PAGE _ -> do
                        let Just dph = unField hdr.ph_data_page_header
                            n = fromIntegral . unField $ dph.dph_num_values
                            enc = unField dph.dph_encoding
                        decompressed <- decompressData uncmpSz codec pageData
                        let (defLvls, repLvls, nPresent, valBytes) =
                                readLevelsV1V n maxDef maxRep decompressed
                            triple = (decoder dict enc nPresent valBytes, defLvls, repLvls)
                        return $ Yield triple (dict, rest')
                    DATA_PAGE_V2 _ -> do
                        let Just dph2 = unField hdr.ph_data_page_header_v2
                            n = fromIntegral . unField $ dph2.dph2_num_values
                            enc = unField dph2.dph2_encoding
                            defLen = unField dph2.dph2_definition_levels_byte_length
                            repLen = unField dph2.dph2_repetition_levels_byte_length
                            -- V2: levels are never compressed; only the value
                            -- payload is (optionally) compressed.
                            isCompressed = fromMaybe True (unField dph2.dph2_is_compressed)
                            (defLvls, repLvls, nPresent, compValBytes) =
                                readLevelsV2V n maxDef maxRep repLen defLen pageData
                        valBytes <-
                            if isCompressed
                                then decompressData uncmpSz codec compValBytes
                                else pure compValBytes
                        let triple = (decoder dict enc nPresent valBytes, defLvls, repLvls)
                        return $ Yield triple (dict, rest')
                    INDEX_PAGE _ -> return $ Skip (dict, rest')

-- ---------------------------------------------------------------------------
-- Page header parsing
-- ---------------------------------------------------------------------------

parsePageHeader :: BS.ByteString -> Either String (BS.ByteString, PageHeader)
parsePageHeader = decodeWithLeftovers Pinch.compactProtocol

-- ---------------------------------------------------------------------------
-- Batch value readers
-- ---------------------------------------------------------------------------

readNBool :: Int -> BS.ByteString -> [Bool]
readNBool count bs =
    let totalBytes = (count + 7) `div` 8
        bits =
            concatMap
                (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7])
                (BS.unpack (BS.take totalBytes bs))
     in take count bits

readNInt32 :: Int -> BS.ByteString -> VU.Vector Int32
readNInt32 n bs = VU.generate n $ \i -> littleEndianInt32 (BS.drop (4 * i) bs)

readNInt64 :: Int -> BS.ByteString -> VU.Vector Int64
readNInt64 n bs = VU.generate n $ \i ->
    fromIntegral (littleEndianWord64 (BS.drop (8 * i) bs))

readNInt96 :: Int -> BS.ByteString -> [UTCTime]
readNInt96 0 _ = []
readNInt96 n bs = int96ToUTCTime (BS.take 12 bs) : readNInt96 (n - 1) (BS.drop 12 bs)

readNFloat :: Int -> BS.ByteString -> VU.Vector Float
readNFloat n bs = VU.generate n $ \i ->
    castWord32ToFloat (littleEndianWord32 (BS.drop (4 * i) bs))

readNDouble :: Int -> BS.ByteString -> VU.Vector Double
readNDouble n bs = VU.generate n $ \i ->
    castWord64ToDouble (littleEndianWord64 (BS.drop (8 * i) bs))

readNTexts :: Int -> BS.ByteString -> [T.Text]
readNTexts 0 _ = []
readNTexts n bs =
    let len = fromIntegral . littleEndianInt32 . BS.take 4 $ bs
        text = decodeUtf8Lenient . BS.take len . BS.drop 4 $ bs
     in text : readNTexts (n - 1) (BS.drop (4 + len) bs)

readNFixedTexts :: Int -> Int -> BS.ByteString -> [T.Text]
readNFixedTexts _ 0 _ = []
readNFixedTexts len n bs =
    decodeUtf8Lenient (BS.take len bs)
        : readNFixedTexts len (n - 1) (BS.drop len bs)
