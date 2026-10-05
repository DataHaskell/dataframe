{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# OPTIONS_GHC -funfolding-use-threshold=1000 #-}

module DataFrame.IO.Parquet.Writer.Encoder (
    Encoder (..),
    buildEncoder,
) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Primitive (PrimBase, PrimMonad, PrimState)
import Control.Monad.ST (stToIO)
import Data.Bits (shiftL, (.|.))
import Data.Int (Int32, Int64)
import Data.Primitive.ByteArray (withMutableByteArrayContents, writeByteArray)
import Data.Primitive.MutVar (newMutVar, readMutVar, writeMutVar)
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

data Encoder m = Encoder
    { encType :: !ThriftType
    , convertedType :: !(Maybe ConvertedType)
    , logicalType :: !(Maybe LogicalType)
    , encodeValue ::
        !(MemoryBuffer (PrimState m) -> Int -> Int -> m (Int, Bool))
    , finishValues :: !(MemoryBuffer (PrimState m) -> Int -> m Int)
    }

buildEncoder :: (PrimBase m, MonadIO m) => Column -> m (Encoder m)
buildEncoder col
    | hasElemType @Int32 col =
        pure $
            scalarEncoder @Int32
                (INT32 enum)
                Nothing
                Nothing
                (\buffer pos v -> write32 buffer pos (fromIntegral v))
                col
    | hasElemType @Int64 col =
        pure $
            scalarEncoder @Int64
                (INT64 enum)
                Nothing
                Nothing
                (\buffer pos v -> write64 buffer pos (fromIntegral v))
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
                (\buffer pos v -> write64 buffer pos (fromIntegral v))
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
                (\buffer pos v -> write32 buffer pos (castFloatToWord32 v))
                col
    | hasElemType @Double col =
        pure $
            scalarEncoder @Double
                (DOUBLE enum)
                Nothing
                Nothing
                (\buffer pos v -> write64 buffer pos (castDoubleToWord64 v))
                col
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

{-# SPECIALIZE buildEncoder :: Column -> IO (Encoder IO) #-}

scalarEncoder ::
    forall a m.
    (Columnable a, Monad m) =>
    ThriftType ->
    Maybe ConvertedType ->
    Maybe LogicalType ->
    (MemoryBuffer (PrimState m) -> Int -> a -> m Int) ->
    Column ->
    Encoder m
scalarEncoder tt conv logical writePrim col =
    Encoder tt conv logical (columnWriter @a col writePrim) (\_ pos -> pure pos)
columnWriter ::
    forall a m.
    (Columnable a, Monad m) =>
    Column ->
    (MemoryBuffer (PrimState m) -> Int -> a -> m Int) ->
    MemoryBuffer (PrimState m) ->
    Int ->
    Int ->
    m (Int, Bool)
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
{-# INLINE columnWriter #-}

isPresent :: Maybe Bitmap -> Int -> Bool
isPresent Nothing _ = True
isPresent (Just bitmap) row = bitmapTestBit bitmap row
{-# INLINE isPresent #-}

boolEncoder :: (PrimMonad m) => Column -> m (Encoder m)
boolEncoder col = do
    bitsRef <- newMutVar (0 :: Word8)
    countRef <- newMutVar (0 :: Int)
    let addBit buffer pos value = do
            bits <- readMutVar bitsRef
            count <- readMutVar countRef
            let bits' = if value then bits .|. ((1 :: Word8) `shiftL` count) else bits
                count' = count + 1
            if count' == 8
                then do
                    arr <- readMutVar buffer.arrayRef
                    writeByteArray arr pos bits'
                    writeMutVar bitsRef 0
                    writeMutVar countRef 0
                    pure (pos + 1)
                else do
                    writeMutVar bitsRef bits'
                    writeMutVar countRef count'
                    pure pos
        finish buffer pos = do
            count <- readMutVar countRef
            pos' <-
                if count > 0
                    then do
                        bits <- readMutVar bitsRef
                        arr <- readMutVar buffer.arrayRef
                        writeByteArray arr pos bits
                        pure (pos + 1)
                    else pure pos
            writeMutVar bitsRef 0
            writeMutVar countRef 0
            pure pos'
    pure
        (Encoder (BOOLEAN enum) Nothing Nothing (columnWriter @Bool col addBit) finish)

textEncoder ::
    (PrimBase m, MonadIO m) =>
    Column ->
    Encoder m
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
        writeMutVar buffer.positionRef pos
        _ <- ensureCapacity buffer (pos + 4 + count)
        writeWord32At buffer pos (fromIntegral count)
        arr <- readMutVar buffer.arrayRef
        withMutableByteArrayContents arr $ \ptr ->
            liftIO $
                stToIO
                    ( TA.copyToPointer
                        bytes
                        offset
                        (ptr `plusPtr` (pos + 4))
                        count
                    )
        pure (pos + 4 + count)
    mismatch =
        error
            ("writeParquet: incompatible text representation for " <> columnTypeString col)

timestampEncoder ::
    (PrimMonad m) =>
    Column ->
    Encoder m
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
