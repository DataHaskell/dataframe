{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | End-to-end smoke test and benchmark for the Lazy streaming API.

     Usage:
       cabal run lazy-bench [-- [OPTIONS]]

     Options:
       --rows N       Number of rows to generate (default: 1_000_000_000)
       --file PATH    Output CSV path          (default: /tmp/lazy_1b.csv)
       --skip-gen     Skip generation if the file already exists

     The executable generates a CSV file (streaming, constant memory) then
     runs five Lazy queries over it, printing timing and result summaries.

     For heap/GC stats run with:
       cabal run lazy-bench -- +RTS -s -RTS
-}
module Main where

import Control.Monad (foldM, forM_, when)
import qualified Data.ByteString.Builder as Builder
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import qualified DataFrame as D
import DataFrame.Internal.Schema (Schema (..), schemaType)
import qualified DataFrame.Lazy as L
import DataFrame.Operators
import System.Directory (doesFileExist, getFileSize)
import System.Environment (getArgs)
import System.Exit (exitFailure)
import System.IO (
    BufferMode (..),
    IOMode (..),
    hFlush,
    hSetBuffering,
    stdout,
    withFile,
 )
import System.Random.Stateful

-- ---------------------------------------------------------------------------
-- Defaults
-- ---------------------------------------------------------------------------

defaultRows :: Int
defaultRows = 1_000_000_000

defaultFile :: FilePath
defaultFile = "/tmp/lazy_1b.csv"

-- Rows written per Builder flush to disk.
chunkSize :: Int
chunkSize = 500_000

-- ---------------------------------------------------------------------------
-- Argument parsing
-- ---------------------------------------------------------------------------

data Opts = Opts
    { optRows :: Int
    , optFile :: FilePath
    , optSkipGen :: Bool
    }

parseArgs :: [String] -> Either String Opts
parseArgs = go (Opts defaultRows defaultFile False)
  where
    go opts [] = Right opts
    go opts ("--rows" : n : rest) = case reads n of
        [(v, "")] -> go opts{optRows = v} rest
        _ -> Left $ "bad --rows value: " ++ n
    go opts ("--file" : p : rest) = go opts{optFile = p} rest
    go opts ("--skip-gen" : rest) = go opts{optSkipGen = True} rest
    go _ (flag : _) = Left $ "unknown flag: " ++ flag

-- ---------------------------------------------------------------------------
-- CSV generation
-- ---------------------------------------------------------------------------

{- | Write @n@ rows of schema

       id (Int), x (Double), y (Double), category (Text: A/B/C/D)

     to @path@ using a streaming Builder, keeping heap usage constant.
-}
generateCsv :: FilePath -> Int -> IO ()
generateCsv path n = do
    g <- newIOGenM =<< newStdGen
    t0 <- getCurrentTime
    withFile path WriteMode $ \h -> do
        hSetBuffering h (BlockBuffering (Just (4 * 1024 * 1024)))
        Builder.hPutBuilder h (Builder.byteString "id,x,y,category\n")
        let numChunks = n `div` chunkSize
            remainder = n `mod` chunkSize
        forM_ [0 .. numChunks - 1] $ \c -> do
            bldr <- buildChunk g (c * chunkSize) chunkSize
            Builder.hPutBuilder h bldr
            when (c `mod` 200 == 0) $ do
                let done = c * chunkSize
                    pct = (done * 100) `div` n
                putStr $ "\r  " ++ show pct ++ "% — " ++ commas done ++ " rows"
                hFlush stdout
        when (remainder > 0) $ do
            bldr <- buildChunk g (numChunks * chunkSize) remainder
            Builder.hPutBuilder h bldr
    t1 <- getCurrentTime
    putStrLn $ "\r  100% — " ++ commas n ++ " rows written in " ++ showDiff t0 t1

buildChunk :: IOGenM StdGen -> Int -> Int -> IO Builder.Builder
buildChunk g baseId count =
    foldM (\acc i -> (acc <>) <$> buildRow g baseId i) mempty [0 .. count - 1]

buildRow :: IOGenM StdGen -> Int -> Int -> IO Builder.Builder
buildRow g baseId i = do
    x <- uniformRM (0.0 :: Double, 1.0) g
    y <- uniformRM (0.0 :: Double, 1.0) g
    c <- uniformRM (0 :: Int, 3) g
    return $
        Builder.intDec (baseId + i)
            <> Builder.char7 ','
            <> buildDouble x
            <> Builder.char7 ','
            <> buildDouble y
            <> Builder.char7 ','
            <> catChar c
            <> Builder.char7 '\n'

buildDouble :: Double -> Builder.Builder
buildDouble x =
    let scaled = round (x * 1_000_000) :: Int
        whole = scaled `div` 1_000_000
        frac = scaled `mod` 1_000_000
     in Builder.intDec whole
            <> Builder.char7 '.'
            <> pad6 frac

pad6 :: Int -> Builder.Builder
pad6 n
    | n < 10 = Builder.byteString "00000" <> Builder.intDec n
    | n < 100 = Builder.byteString "0000" <> Builder.intDec n
    | n < 1000 = Builder.byteString "000" <> Builder.intDec n
    | n < 10_000 = Builder.byteString "00" <> Builder.intDec n
    | n < 100_000 = Builder.byteString "0" <> Builder.intDec n
    | otherwise = Builder.intDec n

catChar :: Int -> Builder.Builder
catChar 0 = Builder.char7 'A'
catChar 1 = Builder.char7 'B'
catChar 2 = Builder.char7 'C'
catChar _ = Builder.char7 'D'

-- ---------------------------------------------------------------------------
-- Lazy queries
-- ---------------------------------------------------------------------------

runQuery :: String -> IO D.DataFrame -> IO ()
runQuery label action = do
    putStrLn $ "\n── " ++ label
    t0 <- getCurrentTime
    df <- action
    t1 <- getCurrentTime
    let (rows, cols) = D.dimensions df
    putStrLn $ "   rows returned : " ++ commas rows
    putStrLn $ "   columns       : " ++ show cols
    putStrLn $ "   time          : " ++ showDiff t0 t1
    when (rows > 0 && rows <= 30) $ print df

-- ---------------------------------------------------------------------------
-- Main
-- ---------------------------------------------------------------------------

main :: IO ()
main = do
    hSetBuffering stdout LineBuffering
    args <- getArgs
    opts <- case parseArgs args of
        Left err -> putStrLn ("Error: " ++ err) >> exitFailure
        Right o -> return o

    let path = optFile opts
        n = optRows opts
        pathT = T.pack path

    -- -----------------------------------------------------------------------
    -- Phase 1: Generate
    -- -----------------------------------------------------------------------
    putStrLn "=== Lazy API 1B-row benchmark ==="
    putStrLn $ "    rows   : " ++ commas n
    putStrLn $ "    file   : " ++ path
    putStrLn "    tip    : run with '+RTS -s -RTS' for heap stats"
    putStrLn ""

    exists <- doesFileExist path
    if optSkipGen opts && exists
        then do
            sz <- getFileSize path
            putStrLn $ "Skipping generation — file exists (" ++ showBytes sz ++ ")"
        else do
            putStrLn "Phase 1: Generating CSV …"
            generateCsv path n
            sz <- getFileSize path
            putStrLn $ "  file size: " ++ showBytes sz

    -- -----------------------------------------------------------------------
    -- Phase 2: Lazy queries
    -- -----------------------------------------------------------------------
    putStrLn "\nPhase 2: Lazy queries"

    -- Schema for the generated CSV: id (Int), x (Double), y (Double), category (Text)
    let schema =
            Schema $
                M.fromList
                    [ ("id", schemaType Int)
                    , ("x", schemaType Double)
                    , ("y", schemaType Double)
                    , ("category", schemaType T.Text)
                    ]

    -- Q1: Preview — limit 20, no filter.
    -- Demonstrates that the executor reads only the first batch.
    runQuery "Q1 — preview first 20 rows (no filter)" $
        L.runDataFrame $
            L.take 20 $
                L.scanCsv schema pathT

    -- Q2: Filter + limit.
    -- x > 0.999 ≈ 0.1% of rows. With a 512K-row batch the executor finds
    -- ~512 matches in the first batch and stops — reads only one batch.
    runQuery "Q2 — filter (x > 0.999), limit 20" $
        L.runDataFrame $
            L.take 20 $
                L.filter (col Double "x" .> lit (0.999 :: Double)) $
                    L.scanCsv schema pathT

    -- Q3: Filter + derive + select + limit.
    -- Shows projection pushdown: only id/x/y/category are read, z is derived.
    -- Predicate pushdown moves the filter into the scan batch loop.
    runQuery "Q3 — filter (x > 0.999), derive z = x*y, select [id,z], limit 20" $
        L.runDataFrame $
            L.take 20 $
                L.select ["id", "z"] $
                    L.derive "z" (col Double "x" * col Double "y") $
                        L.filter (col Double "x" .> lit (0.999 :: Double)) $
                            L.scanCsv schema pathT

    -- Q4: Filter fusion demo.
    -- Two consecutive filters are fused into one AND predicate by the optimizer.
    -- Result: rows where x > 0.5 AND y > 0.5 (≈ 25% of total).
    -- We limit to keep result size manageable.
    runQuery "Q4 — filter fusion: (x > 0.5) . (y > 0.5), limit 20" $
        L.runDataFrame $
            L.take 20 $
                L.filter (col Double "y" .> lit (0.5 :: Double)) $
                    L.filter (col Double "x" .> lit (0.5 :: Double)) $
                        L.scanCsv schema pathT

    -- Q5: Full scan, heavy filter, count results.
    -- x > 0.999 across the whole file ≈ 0.1% × N rows.
    -- For 1B rows that is ~1M results — materialised into one DataFrame.
    -- This query exercises streaming across all batches.
    runQuery
        ( "Q5 — full scan, filter (x > 0.999), count (~"
            ++ approx (n `div` 1000)
            ++ " rows expected)"
        )
        $ L.runDataFrame
        $ L.select ["id", "x"]
        $ L.filter (col Double "x" .> lit (0.999 :: Double))
        $ L.scanCsv schema pathT

    putStrLn "\nDone."

-- ---------------------------------------------------------------------------
-- Formatting helpers
-- ---------------------------------------------------------------------------

showDiff :: UTCTime -> UTCTime -> String
showDiff t0 t1 = show (diffUTCTime t1 t0)

commas :: Int -> String
commas n
    | n < 1000 = show n
    | otherwise = commas (n `div` 1000) ++ "," ++ pad3 (n `mod` 1000)
  where
    pad3 x
        | x < 10 = "00" ++ show x
        | x < 100 = "0" ++ show x
        | otherwise = show x

approx :: Int -> String
approx n
    | n >= 1_000_000 = show (n `div` 1_000_000) ++ "M"
    | n >= 1_000 = show (n `div` 1_000) ++ "K"
    | otherwise = show n

showBytes :: Integer -> String
showBytes b
    | b >= 1_073_741_824 = fmt (fromIntegral b / 1_073_741_824) ++ " GiB"
    | b >= 1_048_576 = fmt (fromIntegral b / 1_048_576) ++ " MiB"
    | b >= 1_024 = fmt (fromIntegral b / 1_024) ++ " KiB"
    | otherwise = show b ++ " B"
  where
    fmt :: Double -> String
    fmt x =
        show (fromIntegral (round (x * 10) :: Int) `div` 10 :: Int)
            ++ "."
            ++ show (fromIntegral (round (x * 10) :: Int) `mod` 10 :: Int)
