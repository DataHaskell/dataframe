{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Binary where

import Control.Exception (bracketOnError)
import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Char
import Data.IORef
import Data.Int
import Data.Word
import qualified Foreign.Marshal.Alloc as Foreign
import qualified Foreign.Ptr as Foreign
import qualified Foreign.Storable as Foreign

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
    loop xs' result shiftAmt i = case BS.uncons xs' of
        Nothing -> error "readUVarInt: not enough input bytes"
        Just (b, bs') ->
            if b < 0x80
                then (result .|. (fromIntegral b `shiftL` shiftAmt), bs')
                else
                    let payloadBits = fromIntegral (b .&. 0x7f) :: Word64
                     in loop bs' (result .|. (payloadBits `shiftL` shiftAmt)) (shiftAmt + 7) (i + 1)

readVarIntFromBytes :: (Integral a) => BS.ByteString -> (a, BS.ByteString)
readVarIntFromBytes bs = (fromIntegral n, remainder)
  where
    (n, remainder) = loop 0 0 bs
    loop shiftAmt result bs' = case BS.uncons bs' of
        Nothing -> (result, BS.empty)
        Just (x, xs) ->
            let res = result .|. (fromIntegral (x .&. 0x7f) :: Integer) `shiftL` shiftAmt
             in if x .&. 0x80 /= 0x80 then (res, xs) else loop (shiftAmt + 7) res xs

readIntFromBytes :: (Integral a) => BS.ByteString -> (a, BS.ByteString)
readIntFromBytes bs =
    let (n, remainder) = readVarIntFromBytes bs
        u = fromIntegral n :: Word32
     in ( fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))
        , remainder
        )

readInt32FromBytes :: BS.ByteString -> (Int32, BS.ByteString)
readInt32FromBytes bs =
    let (n', remainder) = readVarIntFromBytes @Int64 bs
        n = fromIntegral n' :: Int32
        u = fromIntegral n :: Word32
     in ((fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), remainder)

readAndAdvance :: IORef Int -> BS.ByteString -> IO Word8
readAndAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    let b = BS.index buffer pos
    modifyIORef' bufferPos (+ 1)
    return b

readVarIntFromBuffer :: (Integral a) => BS.ByteString -> IORef Int -> IO a
readVarIntFromBuffer buf bufferPos = do
    start <- readIORef bufferPos
    let loop i shiftAmt result = do
            b <- readAndAdvance bufferPos buf
            let res = result .|. (fromIntegral (b .&. 0x7f) :: Integer) `shiftL` shiftAmt
            if b .&. 0x80 /= 0x80
                then return res
                else loop (i + 1) (shiftAmt + 7) res
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
    replicateM nameSize (chr . fromIntegral <$> readAndAdvance pos buf)

readByteStringFromBytes :: BS.ByteString -> (BS.ByteString, BS.ByteString)
readByteStringFromBytes xs =
    let
        (size, remainder) = readVarIntFromBytes @Int xs
     in
        BS.splitAt size remainder

readByteString :: BS.ByteString -> IORef Int -> IO BS.ByteString
readByteString buf pos = do
    size <- readVarIntFromBuffer @Int buf pos
    fillByteStringByWord8 size (\_ -> readAndAdvance pos buf)

readByteString' :: BS.ByteString -> Int64 -> IO BS.ByteString
readByteString' buf size =
    fillByteStringByWord8
        (fromIntegral size)
        ((`readSingleByte` buf) . fromIntegral)

{- | Allocate a fixed-size buffer, repeat the action on each index.
Fill it into the buffer to get a ByteString.
-}
fillByteStringByWord8 :: Int -> (Int -> IO Word8) -> IO BS.ByteString
fillByteStringByWord8 size getByte = do
    bracketOnError
        (Foreign.mallocBytes size :: IO (Foreign.Ptr Word8))
        Foreign.free
        -- \^ ensures p is freed if (IO Word8) throws.
        ( \p -> do
            fill 0 p
            BSU.unsafePackCStringFinalizer p size (Foreign.free p)
        )
  where
    fill i p
        | i >= size = pure ()
        | otherwise = getByte i >>= Foreign.pokeByteOff p i >> fill (i + 1) p
{-# INLINE fillByteStringByWord8 #-}

readSingleByte :: Int64 -> BS.ByteString -> IO Word8
readSingleByte pos buffer = return $ BS.index buffer (fromIntegral pos)

readNoAdvance :: IORef Int -> BS.ByteString -> IO Word8
readNoAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    return $ BS.index buffer pos
