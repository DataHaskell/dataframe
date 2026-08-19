{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.IO.Parquet.Writer.Encoder (
    Encoder (..),
    buildEncoder,
) where

import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO (..))
import Data.Bits (shiftL, (.|.))
import Data.Int (Int32, Int64)
import Data.IORef (newIORef, readIORef, writeIORef)
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
import DataFrame.IO.Parquet.Writer.PageWriter (PageWriter)
import DataFrame.IO.Utils.RandomAccess (
    putDoubleLE,
    putFloatLE,
    putGenerated,
    putWord32LE,
    putWord64LE,
    putWord8,
 )
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    Columnable,
    bitmapTestBit,
    columnTypeString,
    hasElemType,
 )
import DataFrame.Internal.PackedText (
    PackedTextData (..),
    offAt,
    selAt,
 )
import Pinch (enum, putField)
import Type.Reflection (typeRep)

data Encoder = Encoder
    { encType :: !ThriftType
    , encConverted :: !(Maybe ConvertedType)
    , encLogical :: !(Maybe LogicalType)
    , encWriteValue :: !(Int -> PageWriter Bool) -- Boolean for wat def levels should be (see pushDef)
    , encFinishValues :: !(PageWriter ())
    }

buildEncoder :: Column -> IO Encoder
buildEncoder col
    | hasElemType @Int32 col =
        pure $ scalarEncoder @Int32 (INT32 enum) Nothing Nothing (putWord32LE . fromIntegral) col
    | hasElemType @Int64 col =
        pure $ scalarEncoder @Int64 (INT64 enum) Nothing Nothing (putWord64LE . fromIntegral) col
    | hasElemType @Float col =
        pure $ scalarEncoder @Float (FLOAT enum) Nothing Nothing putFloatLE col
    | hasElemType @Double col =
        pure $ scalarEncoder @Double (DOUBLE enum) Nothing Nothing putDoubleLE col
    | hasElemType @Bool col = boolEncoder col
    | hasElemType @T.Text col = pure (textEncoder col)
    | hasElemType @UTCTime col = pure (timestampEncoder col)
    | otherwise = error ("writeParquet: unsupported column type " <> columnTypeString col)

scalarEncoder ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (a -> PageWriter ()) ->
    Column ->
    Encoder
scalarEncoder tt conv logical writeValue col =
    Encoder tt conv logical (columnWriter @a col writeValue) (pure ())

columnWriter ::
    forall a.
    Columnable a =>
    Column ->
    (a -> PageWriter ()) ->
    Int ->
    PageWriter Bool
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
    writeFrom bitmap at row
        | isPresent bitmap row = writeValue (at row) >> pure True
        | otherwise = pure False
    mismatch = error ("writeParquet: incompatible column representation for " <> columnTypeString col)

isPresent :: Maybe Bitmap -> Int -> Bool
isPresent Nothing _ = True
isPresent (Just bitmap) row = bitmapTestBit bitmap row

boolEncoder :: Column -> IO Encoder
boolEncoder col = do
    bitsRef <- newIORef (0 :: Word8)
    countRef <- newIORef (0 :: Int)
    let addBit value = do
            bits <- liftIO (readIORef bitsRef)
            count <- liftIO (readIORef countRef)
            let bits' = if value then bits .|. ((1 :: Word8) `shiftL` count) else bits
                count' = count + 1
            if count' == 8
                then putWord8 bits' >> liftIO (writeIORef bitsRef 0 >> writeIORef countRef 0)
                else liftIO (writeIORef bitsRef bits' >> writeIORef countRef count')
        finish = do
            count <- liftIO (readIORef countRef)
            when (count > 0) (liftIO (readIORef bitsRef) >>= putWord8)
            liftIO (writeIORef bitsRef 0 >> writeIORef countRef 0)
    pure (Encoder (BOOLEAN enum) Nothing Nothing (columnWriter @Bool col addBit) finish)

textEncoder :: Column -> Encoder
textEncoder col =
    Encoder
        (BYTE_ARRAY enum)
        (Just (UTF8 enum))
        (Just (LT_STRING (putField StringType)))
        writePresent
        (pure ())
  where
    writePresent = case col of
        BoxedColumn bitmap (values :: VB.Vector a) ->
            case testEquality (typeRep @T.Text) (typeRep @a) of
                Just Refl -> writeBoxed bitmap values
                Nothing -> mismatch
        PackedText bitmap packed -> writePacked bitmap packed
        _ -> mismatch
    writeBoxed bitmap values row
        | isPresent bitmap row = writeText (VB.unsafeIndex values row) >> pure True
        | otherwise = pure False
    writePacked bitmap packed row
        | isPresent bitmap row = do
            let baseRow = maybe row (\selection -> selAt selection row) packed.ptSel
                start = offAt packed.ptOffsets baseRow
                end = offAt packed.ptOffsets (baseRow + 1)
            writeTextSlice packed.ptBytes start (end - start)
            pure True
        | otherwise = pure False
    mismatch = error ("writeParquet: incompatible text representation for " <> columnTypeString col)

writeText :: T.Text -> PageWriter ()
writeText (Text bytes offset count) = writeTextSlice bytes offset count

writeTextSlice :: TA.Array -> Int -> Int -> PageWriter ()
writeTextSlice bytes offset count = do
    putWord32LE (fromIntegral count)
    putGenerated count (TA.unsafeIndex bytes . (+ offset))

timestampEncoder :: Column -> Encoder
timestampEncoder col =
    Encoder
        (INT64 enum)
        (Just (TIMESTAMP_MICROS enum))
        (Just timestampLogical)
        (columnWriter @UTCTime col writeMicros)
        (pure ())
  where
    writeMicros t = putWord64LE (fromIntegral (utcToMicros t))

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
