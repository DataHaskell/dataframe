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

import Control.Monad (when)
import Data.Bits (shiftL, (.|.))
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int32, Int64)
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
    MemoryBuffer,
    appendTextArraySlice,
    writeDoubleLE,
    writeFloatLE,
    writeInteger64,
    writeWord32LE,
    writeWord64LE,
    writeWord8,
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
import Pinch (enum, putField)
import Type.Reflection (typeRep)

data Encoder = Encoder
    { encType :: !ThriftType
    , convertedType :: !(Maybe ConvertedType)
    , logicalType :: !(Maybe LogicalType)
    , writeValue :: !(MemoryBuffer -> Int -> IO Bool)
    , finishValues :: !(MemoryBuffer -> IO ())
    }

buildEncoder :: Column -> IO Encoder
buildEncoder col
    | hasElemType @Int32 col =
        pure $
            scalarEncoder @Int32
                (INT32 enum)
                Nothing
                Nothing
                (\buffer -> writeWord32LE buffer . fromIntegral)
                col
    | hasElemType @Int64 col =
        pure $
            scalarEncoder @Int64
                (INT64 enum)
                Nothing
                Nothing
                (\buffer -> writeWord64LE buffer . fromIntegral)
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
                (\buffer -> writeWord64LE buffer . fromIntegral)
                col
    | hasElemType @Integer col =
        pure $
            scalarEncoder @Integer
                (INT64 enum)
                Nothing
                Nothing
                writeInteger64
                col
    | hasElemType @Float col =
        pure $ scalarEncoder @Float (FLOAT enum) Nothing Nothing writeFloatLE col
    | hasElemType @Double col =
        pure $ scalarEncoder @Double (DOUBLE enum) Nothing Nothing writeDoubleLE col
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
    (MemoryBuffer -> a -> IO ()) ->
    Column ->
    Encoder
scalarEncoder tt conv logical writeValue col =
    Encoder tt conv logical (columnWriter @a col writeValue) (const (pure ()))
{-# INLINEABLE scalarEncoder #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int32 -> IO ()) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int64 -> IO ()) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Float -> IO ()) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Double -> IO ()) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Int -> IO ()) ->
    Column ->
    Encoder
    #-}
{-# SPECIALIZE scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer -> Integer -> IO ()) ->
    Column ->
    Encoder
    #-}

columnWriter ::
    forall a.
    (Columnable a) =>
    Column ->
    (MemoryBuffer -> a -> IO ()) ->
    MemoryBuffer ->
    Int ->
    IO Bool
columnWriter col writeValue = case col of
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
    writeFrom bitmap at buffer row
        | isPresent bitmap row = writeValue buffer (at row) >> pure True
        | otherwise = pure False
    mismatch =
        error
            ("writeParquet: incompatible column representation for " <> columnTypeString col)
{-# INLINEABLE columnWriter #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Int32 -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Int64 -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Float -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Double -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Bool -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> UTCTime -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Int -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}
{-# SPECIALIZE columnWriter ::
    Column -> (MemoryBuffer -> Integer -> IO ()) -> MemoryBuffer -> Int -> IO Bool
    #-}

isPresent :: Maybe Bitmap -> Int -> Bool
isPresent Nothing _ = True
isPresent (Just bitmap) row = bitmapTestBit bitmap row
{-# INLINE isPresent #-}

boolEncoder :: Column -> IO Encoder
boolEncoder col = do
    bitsRef <- newIORef (0 :: Word8)
    countRef <- newIORef (0 :: Int)
    let addBit buffer value = do
            bits <- readIORef bitsRef
            count <- readIORef countRef
            let bits' = if value then bits .|. ((1 :: Word8) `shiftL` count) else bits
                count' = count + 1
            if count' == 8
                then writeWord8 buffer bits' >> writeIORef bitsRef 0 >> writeIORef countRef 0
                else writeIORef bitsRef bits' >> writeIORef countRef count'
        finish buffer = do
            count <- readIORef countRef
            when (count > 0) (readIORef bitsRef >>= writeWord8 buffer)
            writeIORef bitsRef 0
            writeIORef countRef 0
    pure
        (Encoder (BOOLEAN enum) Nothing Nothing (columnWriter @Bool col addBit) finish)

textEncoder :: Column -> Encoder
textEncoder col =
    Encoder
        (BYTE_ARRAY enum)
        (Just (UTF8 enum))
        (Just (LT_STRING (putField StringType)))
        writePresent
        (const (pure ()))
  where
    writePresent = case col of
        BoxedColumn bitmap (values :: VB.Vector a) ->
            case testEquality (typeRep @T.Text) (typeRep @a) of
                Just Refl -> writeBoxed bitmap values
                Nothing -> mismatch
        PackedText bitmap packed -> writePacked bitmap packed
        _ -> mismatch
    writeBoxed bitmap values buffer row
        | isPresent bitmap row =
            writeText buffer (VB.unsafeIndex values row) >> pure True
        | otherwise = pure False
    writePacked bitmap packed buffer row
        | isPresent bitmap row = do
            let baseRow = maybe row (`selAt` row) packed.ptSel
                start = offAt packed.ptOffsets baseRow
                end = offAt packed.ptOffsets (baseRow + 1)
            writeTextSlice buffer packed.ptBytes start (end - start)
            pure True
        | otherwise = pure False
    mismatch =
        error
            ("writeParquet: incompatible text representation for " <> columnTypeString col)

writeText :: MemoryBuffer -> T.Text -> IO ()
writeText buffer (Text bytes offset count) = writeTextSlice buffer bytes offset count
{-# INLINE writeText #-}

writeTextSlice :: MemoryBuffer -> TA.Array -> Int -> Int -> IO ()
writeTextSlice buffer bytes offset count = do
    writeWord32LE buffer (fromIntegral count)
    appendTextArraySlice buffer bytes offset count
{-# INLINE writeTextSlice #-}

timestampEncoder :: Column -> Encoder
timestampEncoder col =
    Encoder
        (INT64 enum)
        (Just (TIMESTAMP_MICROS enum))
        (Just timestampLogical)
        (columnWriter @UTCTime col writeMicros)
        (const (pure ()))
  where
    writeMicros buffer t = writeWord64LE buffer (fromIntegral (utcToMicros t))

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
