{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module Chipotle (run) where

import Data.Maybe
import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import Text.Read (readMaybe)

import Data.Text (Text)
import DataFrame.Monad
import DataFrame.Operators

$( F.declareColumnsFromCsvWithOpts
    (D.defaultReadOptions{D.columnSeparator = '\t'})
    "../data/chipotle.tsv"
 )

run :: IO ()
run = do
    raw <- D.readTsv "../data/chipotle.tsv"
    print $ D.dimensions raw

    -- -- Sampling the dataframe
    print $ D.take 5 raw

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let df = execFrameM raw $ do
            _ <- deriveM "quantity" (F.ifThenElse (order_id .==. 1) (quantity + 2) quantity)
            -- Custom parsing: drop dollar sign and parse price as double
            itemPrice <-
                deriveM
                    "item_price"
                    (F.lift (readMaybe @Double . T.unpack . T.drop 1) item_price)
            totalPrice <-
                deriveM
                    "total_price"
                    (F.whenBothPresent (*) itemPrice (F.lift (Just . fromIntegral) quantity))
            pure ()

    -- sample the dataframe.
    print $ D.take 10 df

    print $
        df
            |> D.select [F.name item_name, F.name quantity]
            -- It's more efficient to filter before grouping.
            |> D.filterWhere (item_name .==. "Chicken Burrito")
            |> D.groupBy [F.name item_name]
            |> D.aggregate
                [ "sum" .= F.sum quantity
                , "max" .= F.maximum quantity
                , "mean" .= F.mean quantity
                ]
            |> D.sortBy [D.Desc (F.col "sum" `asTypeOf` quantity)]

    let firstOrder =
            df
                |> D.filterWhere
                    ( F.lift (maybe False (T.isInfixOf "Guacamole")) choice_description
                        .&&. (item_name .==. "Chicken Bowl")
                    )

    print $ D.take 10 firstOrder
