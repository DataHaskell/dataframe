{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module OneBillionRowChallenge (run, runCalculation) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BSB
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified DataFrame as D
import qualified OneBillionRowChallenge.Fast as Fast

import Control.Concurrent (getNumCapabilities, setNumCapabilities)
import Control.Exception (bracket_)
import Data.List (sortOn)
import Data.Text (Text)
import Data.Time
import System.IO (
    Handle,
    IOMode (WriteMode),
    hFlush,
    hSetBinaryMode,
    stdout,
    withFile,
 )
import System.IO.Temp (withSystemTempDirectory)
import System.Random.Stateful (
    StatefulGen,
    newIOGenM,
    newStdGen,
    uniformRM,
 )
import Text.Printf (printf)

run :: IO ()
run = do
    withSystemTempDirectory "1brc" $ \tmpDir -> do
        let measurementsPath = tmpDir ++ "/measurements.csv"
        putStrLn $
            "Generating " ++ show numRows ++ " measurements at " ++ measurementsPath
        startGeneration <- getCurrentTime
        capabilities <- getNumCapabilities
        bracket_
            (setNumCapabilities 1)
            (setNumCapabilities capabilities)
            (generateMeasurements measurementsPath)
        endGeneration <- getCurrentTime
        putStrLn $
            "Generation Time: " ++ show (diffUTCTime endGeneration startGeneration)

        runCalculation measurementsPath

{- | Calculate an existing challenge file.  Keeping this separate from
generation makes the optimized phase directly benchmarkable.
-}
runCalculation :: FilePath -> IO ()
runCalculation measurementsPath = do
    startCalculation <- getCurrentTime
    stats <- Fast.aggregateMeasurements (map fst stations) measurementsPath
    print (statsDataFrame stats)
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime

-- The general CSV -> groupBy -> min/mean/max pipeline materializes both
-- billion-element columns before aggregating them.  The challenge's fixed CSV
-- dialect lets 'aggregateMeasurements' fuse those operations, after which we
-- construct only the 51-row result as a normal DataFrame.
statsDataFrame :: [Fast.MeasurementStats] -> D.DataFrame
statsDataFrame stats =
    D.fromNamedColumns
        [ ("names", D.fromList (map Fast.stationName orderedStats))
        , ("minimum", D.fromList (map Fast.minimumTemperature orderedStats))
        , ("mean", D.fromList (map Fast.meanTemperature orderedStats))
        , ("maximum", D.fromList (map Fast.maximumTemperature orderedStats))
        ]
  where
    orderedStats = sortOn Fast.stationName stats

stations :: [(Text, Double)]
stations =
    [ ("Abha", 18.0)
    , ("Abidjan", 26.0)
    , ("Abéché", 29.4)
    , ("Accra", 26.4)
    , ("Addis Ababa", 16.0)
    , ("Adelaide", 17.3)
    , ("Aden", 29.1)
    , ("Ahvaz", 25.4)
    , ("Albuquerque", 14.0)
    , ("Alexandra", 11.0)
    , ("Alexandria", 20.0)
    , ("Algiers", 18.2)
    , ("Alice Springs", 21.0)
    , ("Almaty", 10.0)
    , ("Amsterdam", 10.2)
    , ("Anadyr", -6.9)
    , ("Anchorage", 2.8)
    , ("Andorra la Vella", 9.8)
    , ("Ankara", 12.0)
    , ("Antananarivo", 17.9)
    , ("Antsiranana", 25.2)
    , ("Arkhangelsk", 1.3)
    , ("Ashgabat", 17.1)
    , ("Asmara", 15.6)
    , ("Assab", 30.5)
    , ("Astana", 3.5)
    , ("Athens", 19.2)
    , ("Atlanta", 17.0)
    , ("Auckland", 15.2)
    , ("Austin", 20.7)
    , ("Baghdad", 22.8)
    , ("Baguio", 19.5)
    , ("Baku", 15.1)
    , ("Baltimore", 13.1)
    , ("Bamako", 27.8)
    , ("Bangkok", 28.6)
    , ("Bangui", 26.0)
    , ("Banjul", 26.0)
    , ("Barcelona", 18.2)
    , ("Bata", 25.1)
    , ("Batumi", 14.0)
    , ("Beijing", 12.9)
    , ("Beirut", 20.9)
    , ("Belgrade", 12.5)
    , ("Belize City", 26.7)
    , ("Benghazi", 19.9)
    , ("Bergen", 7.7)
    , ("Berlin", 10.3)
    , ("Bilbao", 14.7)
    , ("Birao", 26.5)
    , ("Bishkek", 11.3)
    ]

numRows :: Int
numRows = 1_000_000_000

progressBatchSize :: Int
progressBatchSize = 500_000

-- Keep the live Builder graph small instead of retaining all 500,000 rows of
-- short-lived Builder closures until the next progress update.
builderChunkRows :: Int
builderChunkRows = 16_384

generateMeasurements :: FilePath -> IO ()
generateMeasurements path = do
    gen <- newIOGenM =<< newStdGen
    let stationVec = V.fromList [(TE.encodeUtf8 name, mean) | (name, mean) <- stations]
        numStations = V.length stationVec
    withFile path WriteMode $ \h -> do
        hSetBinaryMode h True
        BSB.hPutBuilder h (BSB.string7 "names,temperature\n")
        let go !written
                | written >= numRows = pure ()
                | otherwise = do
                    let thisBatch = min progressBatchSize (numRows - written)
                    writeBatch h gen stationVec numStations thisBatch
                    let written' = written + thisBatch
                    printf "Generated %d / %d rows...\r" written' numRows
                    hFlush stdout
                    go written'
        go 0
    putStrLn ""

writeBatch ::
    (StatefulGen g IO) =>
    Handle -> g -> V.Vector (BS.ByteString, Double) -> Int -> Int -> IO ()
writeBatch h gen stationVec numStations = go
  where
    go 0 = pure ()
    go remaining = do
        let chunkRows = min builderChunkRows remaining
        builder <- buildChunk chunkRows mempty
        BSB.hPutBuilder h builder
        go (remaining - chunkRows)

    buildChunk 0 !acc = pure acc
    buildChunk 1 !acc = do
        stationIx <- uniformRM (0, numStations - 1) gen
        let (name, mean) = stationVec V.! stationIx
        (noise, _) <- gaussianPair gen
        let temp = mean + 10 * noise
        pure (acc <> rowBuilder name temp)
    buildChunk k !acc = do
        stationIx1 <- uniformRM (0, numStations - 1) gen
        stationIx2 <- uniformRM (0, numStations - 1) gen
        let (name1, mean1) = stationVec V.! stationIx1
            (name2, mean2) = stationVec V.! stationIx2
        (noise1, noise2) <- gaussianPair gen
        let row1 = rowBuilder name1 (mean1 + 10 * noise1)
            row2 = rowBuilder name2 (mean2 + 10 * noise2)
        buildChunk (k - 2) (acc <> row1 <> row2)

rowBuilder :: BS.ByteString -> Double -> BSB.Builder
rowBuilder name temp =
    BSB.byteString name <> BSB.char7 ',' <> formatTenths temp <> BSB.char7 '\n'

-- | Format to exactly one decimal place, matching the challenge's data files.
formatTenths :: Double -> BSB.Builder
formatTenths x =
    (if tenths < 0 then BSB.char7 '-' else mempty)
        <> BSB.intDec whole
        <> BSB.char7 '.'
        <> BSB.intDec frac
  where
    tenths = round (x * 10) :: Int
    absTenths = abs tenths
    (whole, frac) = absTenths `quotRem` 10

-- | Two independent standard normal samples via the Box-Muller transform.
gaussianPair :: (StatefulGen g IO) => g -> IO (Double, Double)
gaussianPair gen = do
    u1 <- uniformRM (1e-12, 1.0) gen
    u2 <- uniformRM (0.0, 1.0) gen
    let radius = sqrt (-(2 * log u1))
        angle = 2 * pi * u2
    pure (radius * cos angle, radius * sin angle)
