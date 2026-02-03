{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Binary where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import Data.Char
import Data.IORef
import Data.Int
import Data.Word

littleEndianWord32 :: BS.ByteString -> Word32
littleEndianWord32 bytes
    | BS.length bytes >= 4 =
        foldr
            (.|.)
            0
            ( zipWith
                (\b i -> fromIntegral b `shiftL` i)
                (BS.unpack $ BS.take 4 bytes)
                [0, 8, 16, 24]
            )
    | otherwise =
        littleEndianWord32 (BS.take 4 $ bytes `BS.append` BS.pack [0, 0, 0, 0])

littleEndianWord64 :: BS.ByteString -> Word64
littleEndianWord64 bytes =
    foldr
        (.|.)
        0
        ( zipWith
            (\b i -> fromIntegral b `shiftL` i)
            (BS.unpack $ BS.take 8 bytes)
            [0, 8 ..]
        )

littleEndianInt32 :: BS.ByteString -> Int32
littleEndianInt32 = fromIntegral . littleEndianWord32

word64ToLittleEndian :: Word64 -> BS.ByteString
word64ToLittleEndian w =
    BS.map
        (\i -> fromIntegral (w `shiftR` fromIntegral i))
        (BS.pack [0, 8, 16, 24, 32, 40, 48, 56])

word32ToLittleEndian :: Word32 -> BS.ByteString
word32ToLittleEndian w =
    BS.map (\i -> fromIntegral (w `shiftR` fromIntegral i)) (BS.pack [0, 8, 16, 24])

readUVarInt :: BS.ByteString -> (Word64, BS.ByteString)
readUVarInt xs = loop xs 0 0 0
  where
    {-
    Each input byte contributes:
    - lower 7 payload bits
    - The high bit (0x80) is the continuation flag: 1 = more bytes follow, 0 = last byte
    Why the magic number 10: For a 64‑bit integer we need at most ceil(64 / 7) = 10 bytes
    -}
    loop :: BS.ByteString -> Word64 -> Int -> Int -> (Word64, BS.ByteString)
    loop bs result _ 10 = (result, bs)
    loop xs result shift i = case BS.uncons xs of
        Nothing -> error "readUVarInt: not enough input bytes"
        Just (b, bs) ->
            if b < 0x80
                then (result .|. (fromIntegral b `shiftL` shift), bs)
                else
                    let payloadBits = fromIntegral (b .&. 0x7f) :: Word64
                     in loop bs (result .|. (payloadBits `shiftL` shift)) (shift + 7) (i + 1)

readVarIntFromBytes :: (Integral a) => BS.ByteString -> (a, BS.ByteString)
readVarIntFromBytes bs = (fromIntegral n, rem)
  where
    (n, rem) = loop 0 0 bs
    loop shift result bs = case BS.uncons bs of
        Nothing -> (result, BS.empty)
        Just (x, xs) ->
            let res = result .|. (fromIntegral (x .&. 0x7f) :: Integer) `shiftL` shift
             in if x .&. 0x80 /= 0x80 then (res, xs) else loop (shift + 7) res xs

readIntFromBytes :: (Integral a) => BS.ByteString -> (a, BS.ByteString)
readIntFromBytes bs =
    let (n, rem) = readVarIntFromBytes bs
        u = fromIntegral n :: Word32
     in (fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readInt32FromBytes :: BS.ByteString -> (Int32, BS.ByteString)
readInt32FromBytes bs =
    let (n', rem) = readVarIntFromBytes @Int64 bs
        n = fromIntegral n' :: Int32
        u = fromIntegral n :: Word32
     in ((fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readAndAdvance :: IORef Int -> BS.ByteString -> IO Word8
readAndAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    let b = BS.index buffer pos
    modifyIORef bufferPos (+ 1)
    return b

readVarIntFromBuffer :: (Integral a) => BS.ByteString -> IORef Int -> IO a
readVarIntFromBuffer buf bufferPos = do
    start <- readIORef bufferPos
    let loop i shift result = do
            b <- readAndAdvance bufferPos buf
            let res = result .|. (fromIntegral (b .&. 0x7f) :: Integer) `shiftL` shift
            if b .&. 0x80 /= 0x80
                then return res
                else loop (i + 1) (shift + 7) res
    fromIntegral <$> loop start 0 0

readIntFromBuffer :: (Integral a) => BS.ByteString -> IORef Int -> IO a
readIntFromBuffer buf bufferPos = do
    n <- readVarIntFromBuffer buf bufferPos
    let u = fromIntegral n :: Word32
    return $ fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readInt32FromBuffer :: BS.ByteString -> IORef Int -> IO Int32
readInt32FromBuffer buf bufferPos = do
    n <- (fromIntegral <$> readVarIntFromBuffer @Int64 buf bufferPos) :: IO Int32
    let u = fromIntegral n :: Word32
    return $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readString :: BS.ByteString -> IORef Int -> IO String
readString buf pos = do
    nameSize <- readVarIntFromBuffer @Int buf pos
    map (chr . fromIntegral) <$> replicateM nameSize (readAndAdvance pos buf)

readByteStringFromBytes :: BS.ByteString -> (BS.ByteString, BS.ByteString)
readByteStringFromBytes xs =
    let
        (size, rem) = readVarIntFromBytes @Int xs
     in
        BS.splitAt size rem

readByteString :: BS.ByteString -> IORef Int -> IO BS.ByteString
readByteString buf pos = do
    size <- readVarIntFromBuffer @Int buf pos
    BS.pack <$> replicateM size (readAndAdvance pos buf)

readByteString' :: BS.ByteString -> Int64 -> IO BS.ByteString
readByteString' buf size = BS.pack <$> mapM (`readSingleByte` buf) [0 .. (size - 1)]

readSingleByte :: Int64 -> BS.ByteString -> IO Word8
readSingleByte pos buffer = return $ BS.index buffer (fromIntegral pos)

readNoAdvance :: IORef Int -> BS.ByteString -> IO Word8
readNoAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    return $ BS.index buffer pos
