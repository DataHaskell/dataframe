{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}

{- | Simple column-oriented binary spill format (DFBN).

Layout (all integers little-endian):

@
[magic:       4  bytes] "DFBN"
[num_columns: 4  bytes] Word32
  per column:
    [name_len:  2  bytes] Word16  (byte length of UTF-8 name)
    [name:     name_len bytes]
    [type_tag:  1  byte]  Word8
[num_rows:    8  bytes] Word64

per column data block (order matches schema):
  type_tag 0 (Int):            num_rows × Int64 LE
  type_tag 1 (Double):         num_rows × Double LE (IEEE 754)
  type_tag 2 (Text):           (num_rows+1) × Word32 offsets  ++  payload bytes (UTF-8)
  type_tag 3 (Maybe Int):      ceil(num_rows/8)-byte null bitmap  ++  num_rows × Int64 LE
  type_tag 4 (Maybe Double):   ceil(num_rows/8)-byte null bitmap  ++  num_rows × Double LE
  type_tag 5 (Maybe Text):     ceil(num_rows/8)-byte null bitmap
                                ++  (num_rows+1) × Word32 offsets  ++  payload bytes
@

Null bitmap: bit @i@ of byte @i\/8@ is 1 when row @i@ is non-null.
-}
module DataFrame.Lazy.IO.Binary (
    spillToDisk,
    readSpilled,
    withSpilled,
) where

import Control.Exception (SomeException, bracket, try)
import Control.Monad (foldM, void, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Unboxed as VU

import Data.Bits (setBit, shiftL, testBit, (.|.))
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Word (Word16, Word32, Word64, Word8)
import qualified DataFrame.Internal.Binary as Binary
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (DataFrame (..))
import Foreign (ForeignPtr, castForeignPtr, plusForeignPtr, sizeOf)
import System.Directory (getTemporaryDirectory, removeFile)
import System.IO (IOMode (..), hClose, openTempFile, withFile)
import Type.Reflection (typeRep)

-- ---------------------------------------------------------------------------
-- Type tags
-- ---------------------------------------------------------------------------

tagInt, tagDouble, tagText, tagMaybeInt, tagMaybeDouble, tagMaybeText :: Word8
tagInt = 0
tagDouble = 1
tagText = 2
tagMaybeInt = 3
tagMaybeDouble = 4
tagMaybeText = 5

-- ---------------------------------------------------------------------------
-- Write
-- ---------------------------------------------------------------------------

-- | Serialise a 'DataFrame' to a DFBN binary file.
spillToDisk :: FilePath -> DataFrame -> IO ()
spillToDisk path df =
    withFile path WriteMode $ \h -> BSB.hPutBuilder h (buildDataFrame df)

buildDataFrame :: DataFrame -> BSB.Builder
buildDataFrame df =
    BSB.byteString "DFBN"
        <> BSB.word32LE ncols
        <> foldMap (uncurry buildColumnSchema) (zip names cols)
        <> BSB.word64LE nrows
        <> foldMap (buildColumnData nrowsInt) cols
  where
    names =
        fmap
            fst
            (L.sortBy (\a b -> compare (snd a) (snd b)) (M.toList (columnIndices df)))
    ncols = fromIntegral (length names) :: Word32
    cols = V.toList (columns df)
    nrowsInt = fst (dataframeDimensions df)
    nrows = fromIntegral nrowsInt :: Word64

buildColumnSchema :: T.Text -> Column -> BSB.Builder
buildColumnSchema name col =
    BSB.word16LE nameLen
        <> BSB.byteString nameBytes
        <> BSB.word8 (columnTypeTag col)
  where
    nameBytes = TE.encodeUtf8 name
    nameLen = fromIntegral (BS.length nameBytes) :: Word16

columnTypeTag :: Column -> Word8
columnTypeTag (UnboxedColumn (_ :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> tagInt
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> tagDouble
            Nothing -> error "spillToDisk: unsupported UnboxedColumn element type"
columnTypeTag (BoxedColumn _) = tagText
columnTypeTag (OptionalColumn (_ :: V.Vector (Maybe a))) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> tagMaybeInt
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> tagMaybeDouble
            Nothing -> tagMaybeText

buildColumnData :: Int -> Column -> BSB.Builder
buildColumnData _ (UnboxedColumn (v :: VU.Vector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> buildIntVector v
        Nothing ->
            case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> buildDoubleVector v
                Nothing -> error "spillToDisk: unsupported UnboxedColumn element type"
buildColumnData _ (BoxedColumn (v :: V.Vector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> buildTextVector v
        Nothing -> error "spillToDisk: unsupported BoxedColumn element type"
buildColumnData _ (OptionalColumn (v :: V.Vector (Maybe a))) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl ->
            buildNullBitmap (V.map isJust v)
                <> buildIntVector (VU.convert (V.map (fromMaybe 0) v))
        Nothing ->
            case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl ->
                    buildNullBitmap (V.map isJust v)
                        <> buildDoubleVector (VU.convert (V.map (fromMaybe 0.0) v))
                Nothing ->
                    let showText x = case testEquality (typeRep @a) (typeRep @T.Text) of
                            Just Refl -> x
                            Nothing -> T.pack (show x)
                        texts = V.map (maybe T.empty showText) v
                     in buildNullBitmap (V.map isJust v) <> buildTextVector texts

{- | Bulk-encode an Int vector as 8-byte LE values (native layout on LE platforms).
hPutBuilder flushes synchronously so the underlying ForeignPtr outlives the Builder.
-}
buildIntVector :: VU.Vector Int -> BSB.Builder
buildIntVector v =
    let sv = VU.convert v :: VS.Vector Int
        (fp, n) = VS.unsafeToForeignPtr0 sv
        bs = BSI.fromForeignPtr (castForeignPtr fp) 0 (n * sizeOf (0 :: Int))
     in BSB.byteString bs

-- | Bulk-encode a Double vector as 8-byte LE IEEE 754 values (native layout on LE platforms).
buildDoubleVector :: VU.Vector Double -> BSB.Builder
buildDoubleVector v =
    let sv = VU.convert v :: VS.Vector Double
        (fp, n) = VS.unsafeToForeignPtr0 sv
        bs = BSI.fromForeignPtr (castForeignPtr fp) 0 (n * sizeOf (0 :: Double))
     in BSB.byteString bs

-- | Write a Text vector: (num_rows+1) Word32 offsets followed by UTF-8 payload.
buildTextVector :: V.Vector T.Text -> BSB.Builder
buildTextVector v =
    foldMap BSB.word32LE offsets <> foldMap BSB.byteString encoded
  where
    encoded = V.toList (V.map TE.encodeUtf8 v)
    offsets = scanl (\acc bs -> acc + fromIntegral (BS.length bs)) (0 :: Word32) encoded

-- | Build a null-validity bitmap: 1 bit per row, packed LSB-first into bytes.
buildNullBitmap :: V.Vector Bool -> BSB.Builder
buildNullBitmap valids = foldMap (BSB.word8 . mkByte) [0 .. numBytes - 1]
  where
    n = V.length valids
    numBytes = (n + 7) `div` 8
    mkByte byteIdx =
        foldr
            ( \bit acc ->
                let row = byteIdx * 8 + bit
                 in if row < n && (valids V.! row) then setBit acc bit else acc
            )
            (0 :: Word8)
            [0 .. 7]

-- ---------------------------------------------------------------------------
-- Read
-- ---------------------------------------------------------------------------

-- | @(new_offset, value)@
type ParseResult a = Either String (Int, a)

-- | Deserialise a DFBN binary file into a 'DataFrame'.
readSpilled :: FilePath -> IO DataFrame
readSpilled path = do
    bs <- BS.readFile path
    case parseDataFrame bs 0 of
        Left err -> fail ("readSpilled: " <> err)
        Right (_, df) -> return df

parseDataFrame :: BS.ByteString -> Int -> ParseResult DataFrame
parseDataFrame bs off0 = do
    (off1, magic) <- readBytes bs off0 4
    when (magic /= "DFBN") $ Left "bad magic bytes"
    (off2, ncols) <- readWord32LE bs off1
    let ncolsInt = fromIntegral ncols :: Int
    (off3, schema) <- readN ncolsInt (readColumnSchema bs) off2
    (off4, nrows64) <- readWord64LE bs off3
    let nrows = fromIntegral nrows64 :: Int
    (off5, cols) <-
        foldM
            ( \(o, acc) (_, tag) -> do
                (o', col) <- readColumnData bs o nrows tag
                return (o', acc ++ [col])
            )
            (off4, [])
            schema
    let names = fmap fst schema
    return
        ( off5
        , DataFrame
            { columns = V.fromList cols
            , columnIndices = M.fromList (zip names [0 ..])
            , dataframeDimensions = (nrows, ncolsInt)
            , derivingExpressions = M.empty
            }
        )

readColumnSchema :: BS.ByteString -> Int -> ParseResult (T.Text, Word8)
readColumnSchema bs off = do
    (off1, nameLen) <- readWord16LE bs off
    let nameLenInt = fromIntegral nameLen :: Int
    (off2, nameBytes) <- readBytes bs off1 nameLenInt
    (off3, tag) <- readWord8 bs off2
    return (off3, (TE.decodeUtf8 nameBytes, tag))

readColumnData :: BS.ByteString -> Int -> Int -> Word8 -> ParseResult Column
readColumnData bs off nrows tag
    | tag == tagInt = do
        (off', v) <- readIntColumn bs off nrows
        return (off', UnboxedColumn v)
    | tag == tagDouble = do
        (off', v) <- readDoubleColumn bs off nrows
        return (off', UnboxedColumn v)
    | tag == tagText = do
        (off', v) <- readTextColumn bs off nrows
        return (off', BoxedColumn v)
    | tag == tagMaybeInt = do
        (off1, bitmap) <- readNullBitmap bs off nrows
        (off2, v) <- readIntColumn bs off1 nrows
        let maybes =
                V.fromList
                    (zipWith (\valid x -> if valid then Just x else Nothing) bitmap (VU.toList v)) ::
                    V.Vector (Maybe Int)
        return (off2, OptionalColumn maybes)
    | tag == tagMaybeDouble = do
        (off1, bitmap) <- readNullBitmap bs off nrows
        (off2, v) <- readDoubleColumn bs off1 nrows
        let maybes =
                V.fromList
                    (zipWith (\valid x -> if valid then Just x else Nothing) bitmap (VU.toList v)) ::
                    V.Vector (Maybe Double)
        return (off2, OptionalColumn maybes)
    | tag == tagMaybeText = do
        (off1, bitmap) <- readNullBitmap bs off nrows
        (off2, v) <- readTextColumn bs off1 nrows
        let maybes =
                V.fromList
                    (zipWith (\valid x -> if valid then Just x else Nothing) bitmap (V.toList v)) ::
                    V.Vector (Maybe T.Text)
        return (off2, OptionalColumn maybes)
    | otherwise = Left ("unknown type tag " <> show tag)

{- | Zero-copy Int column read: reuses the ByteString buffer's ForeignPtr.
Safe as long as 'bs' stays live during the caller's use of the resulting vector.
Only correct on little-endian platforms (aarch64/x86_64).
-}
readIntColumn :: BS.ByteString -> Int -> Int -> ParseResult (VU.Vector Int)
readIntColumn bs off nrows
    | off + nrows * 8 > BS.length bs = Left "unexpected end of input"
    | otherwise =
        let (fp, bsOff, _) = BSI.toForeignPtr bs
            fp' = castForeignPtr (plusForeignPtr fp (bsOff + off)) :: ForeignPtr Int
            sv = VS.unsafeFromForeignPtr0 fp' nrows :: VS.Vector Int
         in Right (off + nrows * 8, VU.convert sv)

{- | Zero-copy Double column read: reuses the ByteString buffer's ForeignPtr.
Safe as long as 'bs' stays live during the caller's use of the resulting vector.
Only correct on little-endian platforms (aarch64/x86_64).
-}
readDoubleColumn ::
    BS.ByteString -> Int -> Int -> ParseResult (VU.Vector Double)
readDoubleColumn bs off nrows
    | off + nrows * 8 > BS.length bs = Left "unexpected end of input"
    | otherwise =
        let (fp, bsOff, _) = BSI.toForeignPtr bs
            fp' = castForeignPtr (plusForeignPtr fp (bsOff + off)) :: ForeignPtr Double
            sv = VS.unsafeFromForeignPtr0 fp' nrows :: VS.Vector Double
         in Right (off + nrows * 8, VU.convert sv)

readTextColumn :: BS.ByteString -> Int -> Int -> ParseResult (V.Vector T.Text)
readTextColumn bs off nrows = do
    offsets <- readWord32Array bs off (nrows + 1)
    let payloadStart = off + (nrows + 1) * 4
        totalPayload = fromIntegral (last offsets) :: Int
    when (payloadStart + totalPayload > BS.length bs) $
        Left "unexpected end of input"
    let sizes =
            zipWith
                (\a b -> fromIntegral b - fromIntegral a :: Int)
                offsets
                (drop 1 offsets)
        texts =
            zipWith
                ( \o sz ->
                    TE.decodeUtf8
                        (BS.take sz (BS.drop (payloadStart + fromIntegral o) bs))
                )
                offsets
                sizes
    return (payloadStart + totalPayload, V.fromList texts)

-- | Read @nrows@ null-bitmap bits (ceil(nrows\/8) bytes).
readNullBitmap :: BS.ByteString -> Int -> Int -> ParseResult [Bool]
readNullBitmap bs off nrows
    | off + numBytes > BS.length bs = Left "unexpected end of input"
    | otherwise =
        Right
            ( off + numBytes
            , take
                nrows
                [ testBit (BSU.unsafeIndex bs (off + row `div` 8)) (row `mod` 8)
                | row <- [0 ..]
                ]
            )
  where
    numBytes = (nrows + 7) `div` 8

readWord8 :: BS.ByteString -> Int -> ParseResult Word8
readWord8 bs off
    | off >= BS.length bs = Left "unexpected end of input"
    | otherwise = Right (off + 1, BSU.unsafeIndex bs off)

readWord16LE :: BS.ByteString -> Int -> ParseResult Word16
readWord16LE bs off
    | off + 2 > BS.length bs = Left "unexpected end of input"
    | otherwise =
        let b0 = fromIntegral (BSU.unsafeIndex bs off) :: Word16
            b1 = fromIntegral (BSU.unsafeIndex bs (off + 1)) :: Word16
         in Right (off + 2, b0 .|. (b1 `shiftL` 8))

readWord32LE :: BS.ByteString -> Int -> ParseResult Word32
readWord32LE bs off
    | off + 4 > BS.length bs = Left "unexpected end of input"
    | otherwise = Right (off + 4, Binary.littleEndianWord32 (BS.drop off bs))

readWord64LE :: BS.ByteString -> Int -> ParseResult Word64
readWord64LE bs off
    | off + 8 > BS.length bs = Left "unexpected end of input"
    | otherwise = Right (off + 8, Binary.littleEndianWord64 (BS.drop off bs))

-- | Read @n@ consecutive Word32LE values starting at offset @off@.
readWord32Array :: BS.ByteString -> Int -> Int -> Either String [Word32]
readWord32Array bs off n
    | off + n * 4 > BS.length bs = Left "unexpected end of input"
    | otherwise =
        Right
            [ let i = off + k * 4
               in Binary.littleEndianWord32 (BS.drop i bs)
            | k <- [0 .. n - 1]
            ]

-- | Read @n@ bytes from @bs@ at @off@.
readBytes :: BS.ByteString -> Int -> Int -> ParseResult BS.ByteString
readBytes bs off n
    | off + n > BS.length bs = Left "unexpected end of input"
    | otherwise = Right (off + n, BS.take n (BS.drop off bs))

-- | Apply @f@ @n@ times sequentially, threading the offset.
readN :: Int -> (Int -> ParseResult a) -> Int -> ParseResult [a]
readN 0 _ off = Right (off, [])
readN n f off = do
    (off', x) <- f off
    (off'', xs) <- readN (n - 1) f off'
    return (off'', x : xs)

-- ---------------------------------------------------------------------------
-- Bracket helper
-- ---------------------------------------------------------------------------

{- | Spill a DataFrame to a temporary file, run an action with the path,
then delete the file even if the action throws.
-}
withSpilled :: DataFrame -> (FilePath -> IO a) -> IO a
withSpilled df action = do
    tmpDir <- getTemporaryDirectory
    bracket
        ( do
            (path, h) <- openTempFile tmpDir "dataframe_spill.dfbn"
            hClose h
            spillToDisk path df
            return path
        )
        (\path -> void (try (removeFile path) :: IO (Either SomeException ())))
        action
