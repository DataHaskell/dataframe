{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}

{- | Per-field microbenchmark for "DataFrame.Internal.Parsing.Fast"
against the reference parsers (WS-B exit criterion: parseDoubleField
>= 5x Data.Text.Read.double on doubles_100mb-shaped fields).

Style follows benchmarks/csv: monotonic-clock wall over a strict fold,
best of 5 runs, ns/field plus speedup printed per pairing.
-}
module Main (main) where

import qualified Data.ByteString.Char8 as C
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V

import Data.Bits (shiftR, xor)
import Data.Maybe (fromMaybe)
import Data.Time.Calendar (toModifiedJulianDay)
import Data.Word (Word64)
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Exts (Double (..), Int (..))
import Text.Printf (printf)

import qualified DataFrame.Internal.Parsing as Ref
import DataFrame.Internal.Parsing.Fast

fieldCount :: Int
fieldCount = 1000000

-- | SplitMix64 finalizer; deterministic pseudo-random stream.
mix64 :: Word64 -> Word64
mix64 z0 =
    let z1 = (z0 `xor` (z0 `shiftR` 33)) * 0xFF51AFD7ED558CCD
        z2 = (z1 `xor` (z1 `shiftR` 33)) * 0xC4CEB9FE1A85EC53
     in z2 `xor` (z2 `shiftR` 33)

{- | Round-trip-precision doubles, the doubles_100mb shape
(full 17-significant-digit mantissas from @show@).
-}
doubleFields :: V.Vector C.ByteString
doubleFields = V.generate fieldCount gen
  where
    gen i =
        let w = mix64 (fromIntegral i + 1)
            mag = fromIntegral (w `shiftR` 11) / 9007199254740992 :: Double
            scaled = (mag - 0.5) * 2000000
         in C.pack (show scaled)

intFields :: V.Vector C.ByteString
intFields = V.generate fieldCount gen
  where
    gen i =
        let w = mix64 (fromIntegral i + 0x9E3779B97F4A7C15)
            spans = [99, 9999, 999999, 99999999, 9223372036854775807] :: [Word64]
            bound = spans !! (i `mod` 5)
            v = toInteger (w `mod` bound) - toInteger (bound `div` 2)
         in C.pack (show v)

boolFields :: V.Vector C.ByteString
boolFields = V.generate fieldCount gen
  where
    gen i = map C.pack ["True", "False", "true", "false", "TRUE", "FALSE"] !! (i `mod` 6)

dateFields :: V.Vector C.ByteString
dateFields = V.generate fieldCount gen
  where
    gen i =
        let w = mix64 (fromIntegral i + 7)
            y = 1900 + fromIntegral (w `mod` 200) :: Int
            m = 1 + fromIntegral ((w `shiftR` 8) `mod` 12) :: Int
            d = 1 + fromIntegral ((w `shiftR` 16) `mod` 28) :: Int
            pad2 n = (if n < 10 then "0" else "") ++ show n
         in C.pack (show y ++ "-" ++ pad2 m ++ "-" ++ pad2 d)

-- | mixed_100mb-flavoured missing-check stream: mostly non-missing.
missingFields :: V.Vector C.ByteString
missingFields = V.generate fieldCount gen
  where
    gen i =
        map C.pack ["12345", "NA", "hello world", "", "3.14159", "Nothing", "x", "null"]
            !! (i `mod` 8)

forceBS :: V.Vector C.ByteString -> Int
forceBS = V.foldl' (\ !acc b -> acc + C.length b) 0

forceT :: V.Vector T.Text -> Int
forceT = V.foldl' (\ !acc t -> acc + T.length t) 0

-- | Best-of-5 wall nanoseconds for a strict fold over the fields.
bench :: V.Vector a -> (a -> Double) -> IO Double
bench fields step = minimum <$> mapM run [1 .. 5 :: Int]
  where
    -- Seed the accumulator with the run index so the fold cannot be
    -- floated out and shared between the five runs.
    run k = do
        t0 <- getMonotonicTimeNSec
        let !s = V.foldl' (\ !acc f -> acc + step f) (fromIntegral k) fields
        s `seq` return ()
        t1 <- getMonotonicTimeNSec
        return (fromIntegral (t1 - t0))

report :: String -> Double -> Double -> IO ()
report name refNs fastNs = do
    let n = fromIntegral fieldCount
    printf
        "%-34s ref %8.1f ns/field   fast %7.1f ns/field   speedup %5.2fx\n"
        name
        (refNs / n)
        (fastNs / n)
        (refNs / fastNs)

mb :: Maybe Bool -> Double
mb = maybe 0 (\b -> if b then 1 else 2)

mi :: Maybe Int -> Double
mi = fromIntegral . fromMaybe 0

md :: Maybe Double -> Double
md = fromMaybe 0

main :: IO ()
main = do
    printf "fields per stream: %d (best of 5 runs)\n\n" fieldCount
    let doubleTexts = V.map TE.decodeUtf8Lenient doubleFields
        intTexts = V.map TE.decodeUtf8Lenient intFields
    printf
        "force: %d\n\n"
        ( forceBS doubleFields
            + forceBS intFields
            + forceBS boolFields
            + forceBS missingFields
            + forceBS dateFields
            + forceT doubleTexts
            + forceT intTexts
        )

    refD <- bench doubleFields (md . Ref.readByteStringDouble)
    refDT <- bench doubleTexts (md . Ref.readDouble)
    fastD <- bench doubleFields (md . parseDoubleField)
    fastDU <-
        bench doubleFields $ \f ->
            case parseDoubleField# f 0 (C.length f) of
                (# 0#, _ #) -> 0
                (# _, d #) -> D# d
    report "double (vs readByteStringDouble)" refD fastD
    report "double (vs Text readDouble)" refDT fastD
    report "double unboxed core (vs BS ref)" refD fastDU

    refI <- bench intFields (mi . Ref.readByteStringInt)
    refIT <- bench intTexts (mi . Ref.readInt)
    fastI <- bench intFields (mi . parseIntField)
    fastIU <-
        bench intFields $ \f ->
            case parseIntField# f 0 (C.length f) of
                (# 0#, _ #) -> 0
                (# _, n #) -> fromIntegral (I# n)
    report "int (vs readByteStringInt)" refI fastI
    report "int (vs Text readInt)" refIT fastI
    report "int unboxed core (vs BS ref)" refI fastIU

    refB <- bench boolFields (mb . Ref.readByteStringBool)
    fastB <- bench boolFields (mb . parseBoolField)
    report "bool (vs readByteStringBool)" refB fastB

    refM <- bench missingFields (\f -> if Ref.isNullishBS f then 1 else 0)
    fastM <- bench missingFields (\f -> if isMissingField f then 1 else 0)
    report "missing (vs isNullishBS)" refM fastM

    let mday = maybe 0 (fromIntegral . toModifiedJulianDay)
    refDay <- bench dateFields (mday . Ref.readByteStringDate "%Y-%m-%d")
    fastDay <- bench dateFields (mday . parseDateField)
    report "date %Y-%m-%d (vs parseTimeM)" refDay fastDay
