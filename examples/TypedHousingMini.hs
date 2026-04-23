{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module TypedHousingMini (run) where

import Data.Text (Text)
import qualified DataFrame as D
import qualified DataFrame.Typed as T

-- A small schema slice of `../data/housing.csv`.
--
-- NOTE: This is meant as a minimal TypedDataFrame integration example.
-- If the CSV inference/types drift, `T.freezeWithError` will report why.
type HousingMini =
  '[ T.Column "median_income" Double
   , T.Column "median_house_value" Double
   , T.Column "ocean_proximity" Text
   ]

run :: IO ()
run = do
  raw <- D.readCsv "../data/housing.csv"
  let small =
        D.select
          ["median_income", "median_house_value", "ocean_proximity"]
          raw
  case T.freezeWithError @HousingMini small of
    Left err -> do
      putStrLn "Failed to freeze into TypedDataFrame (schema mismatch):"
      putStrLn (show err)
    Right df -> do
      putStrLn "OK: loaded TypedDataFrame slice from housing.csv"
      print (T.nRows df)

      let expensive =
            T.filterWhere
              (T.col @"median_house_value" T..>. T.lit 500000)
              df
      putStrLn ("Rows with median_house_value > 500000: " ++ show (T.nRows expensive))
