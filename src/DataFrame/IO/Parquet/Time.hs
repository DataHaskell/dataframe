{-# LANGUAGE NumericUnderscores #-}

module DataFrame.IO.Parquet.Time where

import qualified Data.ByteString as BS
import Data.Time
import Data.Word

import DataFrame.IO.Parquet.Binary

int96ToUTCTime :: BS.ByteString -> UTCTime
int96ToUTCTime bytes
    | BS.length bytes /= 12 = error "INT96 must be exactly 12 bytes"
    | otherwise =
        let (nanosBytes, julianBytes) = BS.splitAt 8 bytes
            nanosSinceMidnight = littleEndianWord64 nanosBytes
            julianDay = littleEndianWord32 julianBytes
         in julianDayAndNanosToUTCTime (fromIntegral julianDay) nanosSinceMidnight

julianDayAndNanosToUTCTime :: Integer -> Word64 -> UTCTime
julianDayAndNanosToUTCTime julianDay nanosSinceMidnight =
    let day = julianDayToDay julianDay
        secondsSinceMidnight = fromIntegral nanosSinceMidnight / 1_000_000_000
        diffTime = secondsToDiffTime (floor secondsSinceMidnight)
     in UTCTime day diffTime

julianDayToDay :: Integer -> Day
julianDayToDay julianDay =
    let a = julianDay + 32_044
        b = (4 * a + 3) `div` 146_097
        c = a - (146_097 * b) `div` 4
        d = (4 * c + 3) `div` 1461
        e = c - (1461 * d) `div` 4
        m = (5 * e + 2) `div` 153
        day = e - (153 * m + 2) `div` 5 + 1
        month = m + 3 - 12 * (m `div` 10)
        year = 100 * b + d - 4800 + m `div` 10
     in fromGregorian year (fromIntegral month) (fromIntegral day)

-- I include this here even though it's unused because we'll likely use
-- it for the writer. Since int96 is deprecated this is only included for completeness anyway.
utcTimeToInt96 :: UTCTime -> BS.ByteString
utcTimeToInt96 (UTCTime day diffTime) =
    let julianDay = dayToJulianDay day
        nanosSinceMidnight = floor (realToFrac diffTime * 1_000_000_000)
        nanosBytes = word64ToLittleEndian nanosSinceMidnight
        julianBytes = word32ToLittleEndian (fromIntegral julianDay)
     in nanosBytes `BS.append` julianBytes

dayToJulianDay :: Day -> Integer
dayToJulianDay day =
    let (year, month, dayOfMonth) = toGregorian day
        a = fromIntegral $ (14 - fromIntegral month) `div` 12
        y = fromIntegral $ year + 4800 - a
        m = fromIntegral $ month + 12 * fromIntegral a - 3
     in fromIntegral dayOfMonth
            + (153 * m + 2) `div` 5
            + 365 * y
            + y `div` 4
            - y `div` 100
            + y `div` 400
            - 32_045
