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

import Control.Monad.ST (ST)
import Data.Bits (shiftL, (.|.))
import Data.Int (Int32, Int64)
import Data.Primitive.ByteArray (withMutableByteArrayContents, writeByteArray)
import Data.STRef (newSTRef, readSTRef, writeSTRef)
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

data Encoder s = Encoder
    { encType :: !ThriftType
    , convertedType :: !(Maybe ConvertedType)
    , logicalType :: !(Maybe LogicalType)
    , encodeValue :: !(MemoryBuffer s -> Int -> Int -> ST s (Int, Bool))
    , finishValues :: !(MemoryBuffer s -> Int -> ST s Int)
    }

buildEncoder :: Column -> ST s (Encoder s)
buildEncoder col
    | hasElemType @Int32 col =
        pure $
            scalarEncoder
                (INT32 enum)
                Nothing
                Nothing
                ( unboxedColumnWriter @Int32
                    col
                    (\buffer pos v -> write32 buffer pos (fromIntegral v))
                )
    | hasElemType @Int64 col =
        pure $
            scalarEncoder
                (INT64 enum)
                Nothing
                Nothing
                ( unboxedColumnWriter @Int64
                    col
                    (\buffer pos v -> write64 buffer pos (fromIntegral v))
                )
    -- Ints in GHC can be 32 bit or 64 bit integers depending on the
    -- underlying computers architecture. So we'll do 64bit integers
    -- to cover all our bases
    | hasElemType @Int col =
        pure $
            scalarEncoder
                (INT64 enum)
                Nothing
                Nothing
                ( unboxedColumnWriter @Int
                    col
                    (\buffer pos v -> write64 buffer pos (fromIntegral v))
                )
    | hasElemType @Integer col =
        pure $
            scalarEncoder
                (INT64 enum)
                Nothing
                Nothing
                (columnWriter @Integer col writeInteger64At)
    | hasElemType @Float col =
        pure $
            scalarEncoder
                (FLOAT enum)
                Nothing
                Nothing
                ( unboxedColumnWriter @Float
                    col
                    (\buffer pos v -> write32 buffer pos (castFloatToWord32 v))
                )
    | hasElemType @Double col =
        pure $
            scalarEncoder
                (DOUBLE enum)
                Nothing
                Nothing
                ( unboxedColumnWriter @Double
                    col
                    (\buffer pos v -> write64 buffer pos (castDoubleToWord64 v))
                )
    | hasElemType @Bool col = boolEncoder col
    | hasElemType @T.Text col = pure (textEncoder col)
    | hasElemType @UTCTime col = pure (timestampEncoder col)
    | otherwise =
        error ("writeParquet: unsupported column type " <> columnTypeString col)
  where
    write32 buffer pos value = do
        writeWord32At buffer pos value
        pure (pos + 4)
    write64 buffer pos value = do
        writeWord64At buffer pos value
        pure (pos + 8)

type ColumnWriter s = MemoryBuffer s -> Int -> Int -> ST s (Int, Bool)

scalarEncoder ::
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    ColumnWriter s ->
    Encoder s
scalarEncoder tt conv logical encode =
    Encoder tt conv logical encode (\_ pos -> pure pos)

-- | Encode a column whose element type has no 'VU.Unbox' instance.
columnWriter ::
    forall a s.
    (Columnable a) =>
    Column ->
    (MemoryBuffer s -> Int -> a -> ST s Int) ->
    ColumnWriter s
columnWriter = columnWriterWith Nothing
{-# INLINE columnWriter #-}

{- | Encode a column whose element type has a 'VU.Unbox' instance. Using
the caller's instance, rather than the one stored in the column, lets GHC
read the array directly when the element type is known.
-}
unboxedColumnWriter ::
    forall a s.
    (Columnable a, VU.Unbox a) =>
    Column ->
    (MemoryBuffer s -> Int -> a -> ST s Int) ->
    ColumnWriter s
unboxedColumnWriter = columnWriterWith (Just VU.unsafeIndex)
{-# INLINE unboxedColumnWriter #-}

columnWriterWith ::
    forall a s.
    (Columnable a) =>
    Maybe (VU.Vector a -> Int -> a) ->
    Column ->
    (MemoryBuffer s -> Int -> a -> ST s Int) ->
    ColumnWriter s
columnWriterWith unboxedIndex col writePrim = case col of
    BoxedColumn bitmap (values :: VB.Vector b) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> writeFrom bitmap (VB.unsafeIndex values)
            Nothing -> mismatch
    UnboxedColumn bitmap (values :: VU.Vector b) ->
        case testEquality (typeRep @a) (typeRep @b) of
            Just Refl ->
                writeFrom
                    bitmap
                    (maybe (VU.unsafeIndex values) ($ values) unboxedIndex)
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
{-# INLINE columnWriterWith #-}

isPresent :: Maybe Bitmap -> Int -> Bool
isPresent Nothing _ = True
isPresent (Just bitmap) row = bitmapTestBit bitmap row
{-# INLINE isPresent #-}

boolEncoder :: Column -> ST s (Encoder s)
boolEncoder col = do
    bitsRef <- newSTRef (0 :: Word8)
    countRef <- newSTRef (0 :: Int)
    let addBit buffer pos value = do
            bits <- readSTRef bitsRef
            count <- readSTRef countRef
            let bits' = if value then bits .|. ((1 :: Word8) `shiftL` count) else bits
                count' = count + 1
            if count' == 8
                then do
                    arr <- readSTRef buffer.arrayRef
                    writeByteArray arr pos bits'
                    writeSTRef bitsRef 0
                    writeSTRef countRef 0
                    pure (pos + 1)
                else do
                    writeSTRef bitsRef bits'
                    writeSTRef countRef count'
                    pure pos
        finish buffer pos = do
            count <- readSTRef countRef
            pos' <-
                if count > 0
                    then do
                        bits <- readSTRef bitsRef
                        arr <- readSTRef buffer.arrayRef
                        writeByteArray arr pos bits
                        pure (pos + 1)
                    else pure pos
            writeSTRef bitsRef 0
            writeSTRef countRef 0
            pure pos'
    pure
        ( Encoder
            (BOOLEAN enum)
            Nothing
            Nothing
            (unboxedColumnWriter @Bool col addBit)
            finish
        )

textEncoder :: Column -> Encoder s
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
        writeSTRef buffer.positionRef pos
        _ <- ensureCapacity buffer (pos + 4 + count)
        writeWord32At buffer pos (fromIntegral count)
        arr <- readSTRef buffer.arrayRef
        withMutableByteArrayContents arr $ \ptr ->
            TA.copyToPointer bytes offset (ptr `plusPtr` (pos + 4)) count
        pure (pos + 4 + count)
    mismatch =
        error
            ("writeParquet: incompatible text representation for " <> columnTypeString col)

timestampEncoder :: Column -> Encoder s
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
