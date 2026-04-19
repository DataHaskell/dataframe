{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Control.Monad (void)
import Criterion.Main
import DataFrame.Operations.Join
import DataFrame.Operators
import System.Process hiding (env)
import System.Random.Stateful

haskell :: IO ()
haskell = do
    output <- readProcess "cabal" ["run", "dataframe-benchmark-example", "-O2"] ""
    putStrLn output

polars :: IO ()
polars = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/polars/polars_benchmark.py"]
            ""
    putStrLn output

pandas :: IO ()
pandas = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/pandas/pandas_benchmark.py"]
            ""
    putStrLn output

explorer :: IO ()
explorer = do
    output <-
        readProcess "mix" ["run", "./benchmark/explorer/explorer_benchmark.exs"] ""
    putStrLn output

groupByHaskell :: IO ()
groupByHaskell = do
    df <- D.readCsv "./data/housing.csv"
    print $
        df
            |> D.groupBy ["ocean_proximity"]
            |> D.aggregate
                [ F.minimum (F.col @Double "median_house_value")
                    `as` "minimum_median_house_value"
                , F.maximum (F.col @Double "median_house_value")
                    `as` "maximum_median_house_value"
                ]

groupByPolars :: IO ()
groupByPolars = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/polars/group_by.py"]
            ""
    putStrLn output

groupByPandas :: IO ()
groupByPandas = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/pandas/group_by.py"]
            ""
    putStrLn output

groupByExplorer :: IO ()
groupByExplorer = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/mix"
            ["run", "./benchmark/explorer/group_by.exs"]
            ""
    putStrLn output

parseFile :: String -> IO ()
parseFile = void . D.readCsv

parseHousingCSV :: IO ()
parseHousingCSV = parseFile "./data/housing.csv"

parseStarWarsCSV :: IO ()
parseStarWarsCSV = parseFile "./data/starwars.csv"

parseChipotleTSV :: IO ()
parseChipotleTSV = void $ D.readTsv "./data/chipotle.tsv"

parseMeasurementsTXT :: IO ()
parseMeasurementsTXT = parseFile "./data/measurements.txt"

{- | Generate a pair of dataframes for a 1:1 join scenario.
  Left has keys [0..n-1], right has keys [0..n-1] shuffled.
  overlap controls what fraction of keys appear in both sides.
-}
mkOneToOne :: Int -> Double -> IO (D.DataFrame, D.DataFrame)
mkOneToOne n overlap = do
    g <- newIOGenM =<< newStdGen
    let rightSize = max 1 (round (fromIntegral n * overlap))
    -- Left: keys 0..n-1 with a payload column
    let leftKeys = [0 :: Int .. n - 1]
        leftVals = [0 :: Int .. n - 1]
        leftDf =
            D.fromNamedColumns
                [ ("key", D.fromList leftKeys)
                , ("A", D.fromList leftVals)
                ]
    -- Right: take first `rightSize` keys, add non-overlapping keys for the rest
    rightPayload <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. rightSize]
    let rightKeys = [0 :: Int .. rightSize - 1]
        rightDf =
            D.fromNamedColumns
                [ ("key", D.fromList rightKeys)
                , ("B", D.fromList rightPayload)
                ]
    return (leftDf, rightDf)

{- | Generate a pair of dataframes for a many-to-many join scenario.
  Keys are drawn from [0..cardinality-1], so rows share keys.
-}
mkManyToMany :: Int -> Int -> Int -> IO (D.DataFrame, D.DataFrame)
mkManyToMany leftRows rightRows cardinality = do
    g <- newIOGenM =<< newStdGen
    leftKeys <- mapM (\_ -> uniformRM (0 :: Int, cardinality - 1) g) [1 .. leftRows]
    rightKeys <-
        mapM (\_ -> uniformRM (0 :: Int, cardinality - 1) g) [1 .. rightRows]
    leftVals <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. leftRows]
    rightVals <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. rightRows]
    let leftDf =
            D.fromNamedColumns
                [ ("key", D.fromList leftKeys)
                , ("A", D.fromList leftVals)
                ]
        rightDf =
            D.fromNamedColumns
                [ ("key", D.fromList rightKeys)
                , ("B", D.fromList rightVals)
                ]
    return (leftDf, rightDf)

{- | Generate a pair of dataframes for a many-to-one join
  (fact table joining a dimension table).
  Left has n rows with keys drawn from [0..dimSize-1].
  Right has exactly dimSize rows with unique keys.
-}
mkManyToOne :: Int -> Int -> IO (D.DataFrame, D.DataFrame)
mkManyToOne factRows dimSize = do
    g <- newIOGenM =<< newStdGen
    factKeys <- mapM (\_ -> uniformRM (0 :: Int, dimSize - 1) g) [1 .. factRows]
    factVals <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. factRows]
    dimVals <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. dimSize]
    let factDf =
            D.fromNamedColumns
                [ ("key", D.fromList factKeys)
                , ("A", D.fromList factVals)
                ]
        dimDf =
            D.fromNamedColumns
                [ ("key", D.fromList [0 :: Int .. dimSize - 1])
                , ("B", D.fromList dimVals)
                ]
    return (factDf, dimDf)

mkMultiKey :: Int -> Int -> IO (D.DataFrame, D.DataFrame)
mkMultiKey leftRows rightRows = do
    g <- newIOGenM =<< newStdGen
    lk1 <- mapM (\_ -> uniformRM (0 :: Int, 99) g) [1 .. leftRows]
    lk2 <- mapM (\_ -> uniformRM (0 :: Int, 99) g) [1 .. leftRows]
    rk1 <- mapM (\_ -> uniformRM (0 :: Int, 99) g) [1 .. rightRows]
    rk2 <- mapM (\_ -> uniformRM (0 :: Int, 99) g) [1 .. rightRows]
    lv <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. leftRows]
    rv <- mapM (\_ -> uniformRM (0 :: Int, 1_000_000) g) [1 .. rightRows]
    let leftDf =
            D.fromNamedColumns
                [ ("key1", D.fromList lk1)
                , ("key2", D.fromList lk2)
                , ("A", D.fromList lv)
                ]
        rightDf =
            D.fromNamedColumns
                [ ("key1", D.fromList rk1)
                , ("key2", D.fromList rk2)
                , ("B", D.fromList rv)
                ]
    return (leftDf, rightDf)

main :: IO ()
main = do
    output <- readProcess "cabal" ["build", "-O2"] ""
    putStrLn output
    defaultMain
        [ bgroup
            "stats"
            [ bench "simpleStatsHaskell" $ nfIO haskell
            , bench "simpleStatsPandas" $ nfIO pandas
            , bench "simpleStatsPolars" $ nfIO polars
            , bench "groupByHaskell" $ nfIO groupByHaskell
            , bench "groupByPolars" $ nfIO groupByPolars
            , bench "groupByPandas" $ nfIO groupByPandas
            -- , bench "groupByExplorer" $ nfIO groupByExplorer
            ]
        , bgroup
            "housing.csv (1.4 MB)"
            [ bench "Attoparsec" $ nfIO $ parseFile "./data/housing.csv"
            ]
        , bgroup
            "effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv (9.1 MB)"
            [ bench "Attoparsec" $
                nfIO $
                    parseFile
                        "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
            ]
        , bgroup
            "join/inner/1:1"
            [ env (mkOneToOne 1_000 1.0) $ \ ~(l, r) ->
                bench "1K rows" $ nf (innerJoin ["key"] r) l
            , env (mkOneToOne 10_000 1.0) $ \ ~(l, r) ->
                bench "10K rows" $ nf (innerJoin ["key"] r) l
            , env (mkOneToOne 100_000 1.0) $ \ ~(l, r) ->
                bench "100K rows" $ nf (innerJoin ["key"] r) l
            ]
        , bgroup
            "join/inner/1:1-partial-overlap"
            [ env (mkOneToOne 100_000 0.5) $ \ ~(l, r) ->
                bench "100K rows, 50% overlap" $ nf (innerJoin ["key"] r) l
            , env (mkOneToOne 100_000 0.1) $ \ ~(l, r) ->
                bench "100K rows, 10% overlap" $ nf (innerJoin ["key"] r) l
            ]
        , bgroup
            "join/inner/many:1"
            [ env (mkManyToOne 10_000 100) $ \ ~(fact, dim) ->
                bench "10K fact x 100 dim" $ nf (innerJoin ["key"] dim) fact
            , env (mkManyToOne 100_000 1_000) $ \ ~(fact, dim) ->
                bench "100K fact x 1K dim" $ nf (innerJoin ["key"] dim) fact
            , env (mkManyToOne 100_000 100) $ \ ~(fact, dim) ->
                bench "100K fact x 100 dim" $ nf (innerJoin ["key"] dim) fact
            ]
        , bgroup
            "join/inner/many:many"
            [ env (mkManyToMany 1_000 1_000 100) $ \ ~(l, r) ->
                bench "1Kx1K, 100 keys" $ nf (innerJoin ["key"] r) l
            , env (mkManyToMany 10_000 10_000 1_000) $ \ ~(l, r) ->
                bench "10Kx10K, 1K keys" $ nf (innerJoin ["key"] r) l
            , env (mkManyToMany 10_000 10_000 100) $ \ ~(l, r) ->
                bench "10Kx10K, 100 keys" $ nf (innerJoin ["key"] r) l
            ]
        , bgroup
            "join/left"
            [ env (mkOneToOne 10_000 1.0) $ \ ~(l, r) ->
                bench "1:1, 10K rows" $ nf (leftJoin ["key"] r) l
            , env (mkOneToOne 100_000 1.0) $ \ ~(l, r) ->
                bench "1:1, 100K rows" $ nf (leftJoin ["key"] r) l
            , env (mkOneToOne 100_000 0.5) $ \ ~(l, r) ->
                bench "1:1, 100K rows, 50%" $ nf (leftJoin ["key"] r) l
            , env (mkManyToOne 100_000 1_000) $ \ ~(fact, dim) ->
                bench "many:1, 100K x 1K" $ nf (leftJoin ["key"] dim) fact
            , env (mkManyToMany 10_000 10_000 1_000) $ \ ~(l, r) ->
                bench "many:many, 10Kx10K" $ nf (leftJoin ["key"] r) l
            ]
        , bgroup
            "join/fullOuter"
            [ env (mkOneToOne 10_000 1.0) $ \ ~(l, r) ->
                bench "1:1, 10K rows" $ nf (fullOuterJoin ["key"] r) l
            , env (mkOneToOne 100_000 1.0) $ \ ~(l, r) ->
                bench "1:1, 100K rows" $ nf (fullOuterJoin ["key"] r) l
            , env (mkOneToOne 100_000 0.5) $ \ ~(l, r) ->
                bench "1:1, 100K rows, 50%" $ nf (fullOuterJoin ["key"] r) l
            , env (mkManyToOne 100_000 1_000) $ \ ~(fact, dim) ->
                bench "many:1, 100K x 1K" $ nf (fullOuterJoin ["key"] dim) fact
            ]
        , bgroup
            "join/multiKey"
            [ env (mkMultiKey 10_000 10_000) $ \ ~(l, r) ->
                bench "inner 10Kx10K, 2 keys" $ nf (innerJoin ["key1", "key2"] r) l
            , env (mkMultiKey 10_000 10_000) $ \ ~(l, r) ->
                bench "left 10Kx10K, 2 keys" $ nf (leftJoin ["key1", "key2"] r) l
            ]
        ]
