{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module CaliforniaHousing (run) where

import Control.Exception (throw)
import Control.Monad (when)
import Data.Either
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Hasktorch (toTensor)
import System.Random
import Torch

import Data.Text (Text)
import DataFrame.Operators

$( F.declareColumnsFromCsvWithOpts
    (D.defaultReadOptions{D.typeSpec = D.InferFromSample 300})
    "../data/housing.csv"
 )

run :: IO ()
run = do
    {- Feature ingestion and engineering -}
    df <- D.readCsv "../data/housing.csv"

    let meanTotalBedrooms = df |> D.meanMaybe total_bedrooms
    let cleaned =
            df
                |> D.impute total_bedrooms meanTotalBedrooms
                |> D.deriveMany
                    [ "ocean_proximity" .= F.lift oceanProximity ocean_proximity
                    , "rooms_per_household" .= total_rooms / households
                    ]
        (train, test) = D.randomSplit (mkStdGen 42) 0.8 cleaned

        -- Convert to hasktorch tensor
        trainFeatures =
            toTensor (train |> D.exclude [F.name median_house_value] |> normalizeFeatures)
        testFeatures = toTensor (test |> D.exclude [F.name median_house_value] |> normalizeFeatures)
        trainLabels = toTensor (D.select [F.name median_house_value] train)

    {- Train the model -}
    putStrLn "Training linear regression model..."
    init <-
        sample $
            LinearSpec{in_features = snd (D.dimensions train) - 1, out_features = 1}
    trained <- foldLoop init 100_000 $ \state i -> do
        let labels' = model state trainFeatures
            loss = mseLoss trainLabels labels'
        when (i `mod` 10_000 == 0) $ do
            putStrLn $ "Iteration: " ++ show i ++ " | Loss: " ++ show loss
        (state', _) <- runStep state GD loss 0.1
        pure state'

    {- Show predictions -}
    let predictionColumn = "predicted_house_value"
    let predictions =
            D.insert predictionColumn (asValue @[Float] (model trained testFeatures)) test
    print $ D.select [F.name median_house_value, predictionColumn] predictions

normalizeFeatures :: D.DataFrame -> D.DataFrame
normalizeFeatures df =
    df
        |> D.fold
            ( \name d ->
                let
                    -- Convenience reference to the column.
                    col = F.col @Double name
                 in
                    D.derive name ((col - F.minimum col) / (F.maximum col - F.minimum col)) d
            )
            (D.columnNames (df |> D.selectBy [D.byProperty (D.hasElemType @Double)]))

model :: Linear -> Tensor -> Tensor
model state input = squeezeAll $ linear state input

oceanProximity :: T.Text -> Double
oceanProximity op = case op of
    "ISLAND" -> 0
    "NEAR OCEAN" -> 1
    "NEAR BAY" -> 2
    "<1H OCEAN" -> 3
    "INLAND" -> 4
    _ -> error ("Unknown ocean proximity value: " ++ T.unpack op)
