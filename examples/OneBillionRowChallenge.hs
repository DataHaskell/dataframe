{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Data.Text (Text)
import Data.Time
import DataFrame ((|>))

$( F.declareColumnsFromCsvWithOpts
    (D.defaultReadOptions{D.columnSeparator = ';'})
    "../data/measurements.txt"
 )

main :: IO ()
main = do
    startRead <- getCurrentTime
    parsed <-
        D.readSeparated
            (D.defaultReadOptions{D.columnSeparator = ';'})
            "../data/measurements.txt"
    endRead <- getCurrentTime
    let readTime = diffUTCTime endRead startRead
    putStrLn $ "Read Time: " ++ show readTime

    startCalculation <- getCurrentTime
    print $
        parsed
            |> D.groupBy [F.name city]
            |> D.aggregate
                [ F.minimum measurement `F.as` "minimum"
                , F.mean measurement `F.as` "mean"
                , F.maximum measurement `F.as` "maximum"
                ]
            |> D.sortBy [D.Asc city]
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
