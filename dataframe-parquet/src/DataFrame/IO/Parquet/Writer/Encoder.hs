{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Writer.Encoder (
    Encoder (..),
    buildEncoder,
) where

import Control.Monad.ST (stToIO)
import Data.Bits (shiftL, (.|.))
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int32, Int64)
import Data.Primitive.ByteArray (
    withMutableByteArrayContents,
    writeByteArray,
 )
import qualified Data.Text as T
import qualified Data.Text.Array as TA
import Data.Text.Internal (Text (Text))
import Data.Time.Calendar (toModifiedJulianDay)
import Data.Time.Clock (UTCTime (UTCTime), diffTimeToPicoseconds)
import Data.Type.Equality (TestEquality (..), (:~:) (Refl))
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Utils.RandomAccess (
    MemoryBuffer (..),
    ensureCapacity,
    writeInteger64At,
    writeWord32At,
    writeWord64At,
 )
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    columnTypeString,
    hasElemType,
 )
import DataFrame.Internal.Column.Bitmap (
    Bitmap,
    bitmapTestBit,
 )
import DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    offAt,
    selAt,
 )
import Foreign (plusPtr)
import GHC.Float (castDoubleToWord64, castFloatToWord32)
import Pinch (enum, putField)
import Type.Reflection (typeRep)

data Encoder = Encoder
    { encType :: !ThriftType
    , convertedType :: !(Maybe ConvertedType)
    , logicalType :: !(Maybe LogicalType)
    , encodeValue :: !(MemoryBuffer -> Int -> Int -> IO (Int, Bool))
    , finishValues :: !(MemoryBuffer -> Int -> IO Int)
    }

buildEncoder :: Column -> IO Encoder
buildEncoder col
    | hasElemType @Int32 col =
        pure $
            scalarEncoder @Int32
                (INT32 enum)
                Nothing
                Nothing
                (\buffer pos v -> writeWord32At buffer pos (fromIntegral v) >> pure (pos + 4))
                col
    | hasElemType @Int64 col =
        pure $
            scalarEncoder @Int64
                (INT64 enum)
                Nothing
                Nothing
                (\buffer pos v -> writeWord64At buffer pos (fromIntegral v) >> pure (pos + 8))
                col
    -- Ints in GHC can be 32 bit or 64 bit integers depending on the
    -- underlying computers architecture. So we'll do 64bit integers
    -- to cover all our bases
    | hasElemType @Int col =
        pure $
            scalarEncoder @Int
                (INT64 enum)
                Nothing
                Nothing
                (\buffer pos v -> writeWord64At buffer pos (fromIntegral v) >> pure (pos + 8))
                col
    | hasElemType @Integer col =
        pure $
            scalarEncoder @Integer
                (INT64 enum)
                Nothing
                Nothing
                writeInteger64At
                col
    | hasElemType @Float col =
        pure $
            scalarEncoder @Float
                (FLOAT enum)
                Nothing
                Nothing
                ( \buffer pos v -> writeWord32At buffer pos (castFloatToWord32 v) >> pure (pos + 4)
                )
                col
    | hasElemType @Double col =
        pure $
            scalarEncoder @Double
                (DOUBLE enum)
                Nothing
                Nothing
                ( \buffer pos v -> writeWord64At buffer pos (castDoubleToWord64 v) >> pure (pos + 8)
                )
                col
    | hasElemType @Bool col = boolEncoder col
    | hasElemType @T.Text col = pure (textEncoder col)
    | hasElemType @UTCTime col = pure (timestampEncoder col)
    | otherwise =
        error ("writeParquet: unsupported column type " <> columnTypeString col)

scalarEncoder ::
    forall a.
    (Columnable a) =>
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> a -> IO Int) ->
    Column ->
    Encoder
scalarEncoder tt conv logical writePrim col =
    Encoder tt conv logical (columnWriter @a col writePrim) (\_ pos -> pure pos)
{-# INLINEABLE scalarEncoder #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Int32 -> IO Int) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Int64 -> IO Int) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Float -> IO Int) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Double -> IO Int) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Int -> IO Int) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> Integer -> IO Int) ->
    Column ->
    Encoder
    #-}

columnWriter ::
    forall a.
    (Columnable a) =>
    Column ->
    (MemoryBuffer -> Int -> a -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
columnWriter col writePrim = case col of
    BoxedColumn bitmap (values :: VB.Vector b) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> writeFrom bitmap (VB.unsafeIndex values)
            Nothing -> mismatch
    UnboxedColumn bitmap (values :: VU.Vector b) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> writeFrom bitmap (VU.unsafeIndex values)
            Nothing -> mismatch
    _ -> mismatch
  where
    writeFrom bitmap at buffer pos row
        | isPresent bitmap row = do
            pos' <- writePrim buffer pos (at row)
            pure (pos', True)
        | otherwise = pure (pos, False)
    mismatch =
        error
            ("writeParquet: incompatible column representation for " <> columnTypeString col)
{-# INLINEABLE columnWriter #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Int32 -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Int64 -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Float -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Double -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Bool -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> UTCTime -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Int -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}
{-# SPECIALIZE columnWriter ::
    Column ->
    (MemoryBuffer -> Int -> Integer -> IO Int) ->
    MemoryBuffer ->
    Int ->
    Int ->
    IO (Int, Bool)
    #-}

isPresent :: Maybe Bitmap -> Int -> Bool
isPresent Nothing _ = True
isPresent (Just bitmap) row = bitmapTestBit bitmap row
{-# INLINE isPresent #-}

boolEncoder :: Column -> IO Encoder
boolEncoder col = do
    bitsRef <- newIORef (0 :: Word8)
    countRef <- newIORef (0 :: Int)
    let addBit buffer pos value = do
            bits <- readIORef bitsRef
            count <- readIORef countRef
            let bits' = if value then bits .|. ((1 :: Word8) `shiftL` count) else bits
                count' = count + 1
            if count' == 8
                then do
                    arr <- readIORef buffer.arrayRef
                    writeByteArray arr pos bits'
                    writeIORef bitsRef 0
                    writeIORef countRef 0
                    pure (pos + 1)
                else do
                    writeIORef bitsRef bits'
                    writeIORef countRef count'
                    pure pos
        finish buffer pos = do
            count <- readIORef countRef
            pos' <-
                if count > 0
                    then do
                        bits <- readIORef bitsRef
                        arr <- readIORef buffer.arrayRef
                        writeByteArray arr pos bits
                        pure (pos + 1)
                    else pure pos
            writeIORef bitsRef 0
            writeIORef countRef 0
            pure pos'
    pure
        (Encoder (BOOLEAN enum) Nothing Nothing (columnWriter @Bool col addBit) finish)

textEncoder :: Column -> Encoder
textEncoder col =
    Encoder
        (BYTE_ARRAY enum)
        (Just (UTF8 enum))
        (Just (LT_STRING (putField StringType)))
        writePresent
        (\_ pos -> pure pos)
  where
    writePresent = case col of
        BoxedColumn bitmap (values :: VB.Vector a) ->
            case testEquality (typeRep @T.Text) (typeRep @a) of
                Just Refl -> writeBoxed bitmap values
                Nothing -> mismatch
        PackedText bitmap packed -> writePacked bitmap packed
        _ -> mismatch
    writeBoxed bitmap values buffer pos row
        | isPresent bitmap row = do
            let Text bytes offset count = VB.unsafeIndex values row
            pos' <- writeTextSlice buffer pos bytes offset count
            pure (pos', True)
        | otherwise = pure (pos, False)
    writePacked bitmap packed buffer pos row
        | isPresent bitmap row = do
            let baseRow = maybe row (`selAt` row) packed.ptSel
                start = offAt packed.ptOffsets baseRow
                end = offAt packed.ptOffsets (baseRow + 1)
            pos' <- writeTextSlice buffer pos packed.ptBytes start (end - start)
            pure (pos', True)
        | otherwise = pure (pos, False)
    writeTextSlice buffer pos bytes offset count = do
        writeIORef buffer.positionRef pos
        _ <- ensureCapacity buffer (pos + 4 + count)
        writeWord32At buffer pos (fromIntegral count)
        arr <- readIORef buffer.arrayRef
        withMutableByteArrayContents arr $ \ptr ->
            stToIO (TA.copyToPointer bytes offset (ptr `plusPtr` (pos + 4)) count)
        pure (pos + 4 + count)
    mismatch =
        error
            ("writeParquet: incompatible text representation for " <> columnTypeString col)

timestampEncoder :: Column -> Encoder
timestampEncoder col =
    Encoder
        (INT64 enum)
        (Just (TIMESTAMP_MICROS enum))
        (Just timestampLogical)
        (columnWriter @UTCTime col writeMicros)
        (\_ pos -> pure pos)
  where
    writeMicros buffer pos t = do
        writeWord64At buffer pos (fromIntegral (utcToMicros t))
        pure (pos + 8)

timestampLogical :: LogicalType
timestampLogical =
    LT_TIMESTAMP
        ( putField
            TimestampType
                { timestamp_isAdjustedToUTC = putField True
                , timestamp_unit = putField (MICROS (putField MicroSeconds))
                }
        )

utcToMicros :: UTCTime -> Int64
utcToMicros (UTCTime day dt) =
    fromIntegral
        ( (toModifiedJulianDay day - 40587) * 86400 * 1000000
            + diffTimeToPicoseconds dt `div` 1000000
        )
{-# INLINE utcToMicros #-}
