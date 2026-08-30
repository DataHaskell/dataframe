{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}

module DataFrame10GB (
    stressDataFrame,
    stressRows,
    stressColumns,
    stressResidentBytesLowerBound,
) where

import Control.Monad.ST (runST)
import Data.Int (Int32, Int64)
import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Time (UTCTime (UTCTime), addDays, fromGregorian, secondsToDiffTime)
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.Column.Bitmap (Bitmap)
import DataFrame.Internal.DataFrame (DataFrame, fromNamedColumns)
import DataFrame.Internal.Data.PackedText (mkPackedContiguous32)

stressRows :: Int
stressRows = 1_000_000

stressGroups :: Int
stressGroups = 16

stressColumns :: Int
stressColumns = stressGroups * 14

textBytesPerRow :: Int
textBytesPerRow = 320

stressResidentBytesLowerBound :: Integer
stressResidentBytesLowerBound =
    fromIntegral stressRows
        * fromIntegral stressGroups
        * fromIntegral (2 * textBytesPerRow + 2 * (4 + 8 + 4 + 8))

stressDataFrame :: DataFrame
stressDataFrame = fromNamedColumns (concatMap columnGroup [0 .. stressGroups - 1])

columnGroup :: Int -> [(T.Text, Column)]
columnGroup group =
    [ named "int32" (UnboxedColumn Nothing (int32Values group))
    , named "int64" (UnboxedColumn Nothing (int64Values group))
    , named "float" (UnboxedColumn Nothing (floatValues group))
    , named "double" (UnboxedColumn Nothing (doubleValues group))
    , named "bool" (UnboxedColumn Nothing (boolValues group))
    , named "timestamp" (BoxedColumn Nothing (timestampValues group))
    , named "text" (textColumn Nothing group)
    , named
        "nullable_int32"
        (UnboxedColumn (Just nullableBitmap) (int32Values (group + stressGroups)))
    , named
        "nullable_int64"
        (UnboxedColumn (Just nullableBitmap) (int64Values (group + stressGroups)))
    , named
        "nullable_float"
        (UnboxedColumn (Just nullableBitmap) (floatValues (group + stressGroups)))
    , named
        "nullable_double"
        (UnboxedColumn (Just nullableBitmap) (doubleValues (group + stressGroups)))
    , named
        "nullable_bool"
        (UnboxedColumn (Just nullableBitmap) (boolValues (group + stressGroups)))
    , named
        "nullable_timestamp"
        (BoxedColumn (Just nullableBitmap) (timestampValues (group + stressGroups)))
    , named "nullable_text" (textColumn (Just nullableBitmap) (group + stressGroups))
    ]
  where
    named suffix column = (T.pack ("group_" <> show group <> "_" <> suffix), column)

nullableBitmap :: Bitmap
nullableBitmap = VU.replicate (stressRows `div` 8) (0xFE :: Word8)

int32Values :: Int -> VU.Vector Int32
int32Values salt =
    VU.generate stressRows $ \row ->
        fromIntegral ((row + salt * 10_007) `mod` 2_000_001 - 1_000_000)

int64Values :: Int -> VU.Vector Int64
int64Values salt =
    VU.generate stressRows $ \row ->
        fromIntegral row * 1_000_003 - fromIntegral salt * 10_000_019

floatValues :: Int -> VU.Vector Float
floatValues salt =
    VU.generate stressRows $ \row ->
        fromIntegral ((row + salt * 101) `mod` 100_003) / 17

doubleValues :: Int -> VU.Vector Double
doubleValues salt =
    VU.generate stressRows $ \row ->
        fromIntegral row / 31.0 - fromIntegral salt * 1_000.25

boolValues :: Int -> VU.Vector Bool
boolValues salt = VU.generate stressRows (\row -> (row + salt) `mod` 3 == 0)

timestampValues :: Int -> VB.Vector UTCTime
timestampValues salt =
    VB.replicate
        stressRows
        ( UTCTime
            (addDays (fromIntegral salt) (fromGregorian 2020 1 1))
            (secondsToDiffTime (fromIntegral (salt * 1_337 `mod` 86_400)))
        )

textColumn :: Maybe Bitmap -> Int -> Column
textColumn bitmap salt = PackedText bitmap $ runST $ do
    target <- A.new (stressRows * textBytesPerRow)
    let template = textTemplate salt
        fill !row
            | row >= stressRows = pure ()
            | otherwise = do
                A.copyI textBytesPerRow target (row * textBytesPerRow) template 0
                fill (row + 1)
    fill 0
    bytes <- A.unsafeFreeze target
    let offsets =
            VU.generate
                (stressRows + 1)
                (\row -> fromIntegral (row * textBytesPerRow) :: Int32)
    pure (mkPackedContiguous32 bytes offsets)

textTemplate :: Int -> A.Array
textTemplate salt = A.run $ do
    bytes <- A.new textBytesPerRow
    let byte = fromIntegral (97 + salt `mod` 26)
        fill !index
            | index >= textBytesPerRow = pure ()
            | otherwise = A.unsafeWrite bytes index byte >> fill (index + 1)
    fill 0
    pure bytes
