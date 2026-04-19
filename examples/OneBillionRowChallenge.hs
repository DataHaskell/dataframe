{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module OneBillionRowChallenge (run) where

import qualified Data.Map as M
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Lazy as L

import Data.Text (Text)
import Data.Time
import DataFrame.Internal.Schema (Schema (..), schemaType)
import DataFrame.Operators

run :: IO ()
run = do
    let city = F.col @Text "names"
    let measurement = F.col @Double "temperature"
    let schema =
            Schema $
                M.fromList
                    [ (F.name city, schemaType @Text)
                    , (F.name measurement, schemaType @Double)
                    ]

    startCalculation <- getCurrentTime
    df <-
        L.scanCsv schema "../../1brc/measurements.txt"
            |> L.groupBy
                [F.name city]
                [ F.minimum measurement `as` "minimum"
                , F.mean measurement `as` "mean"
                , F.maximum measurement `as` "maximum"
                ]
            |> L.runDataFrame
    print $ D.sortBy [D.Asc city] df
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
