{-# LANGUAGE DataKinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

import Data.Char
import qualified Data.Text as T
import qualified DataFrame as D
import DataFrame.DecisionTree
import qualified DataFrame.Functions as F
import DataFrame.Operators
import qualified DataFrame.Typed as DT
import System.Random

$( DT.deriveSchemaFromCsvFileWith
    D.defaultReadOptions{D.safeRead = D.MaybeRead}
    "TrainSchema"
    "./data/titanic/train.csv"
 )
$( DT.deriveSchemaFromCsvFileWith
    D.defaultReadOptions{D.safeRead = D.MaybeRead}
    "TestSchema"
    "./data/titanic/test.csv"
 )

-- Survived is Maybe Int (safeRead = MaybeRead); prediction is Int (model output).
type RawPredSchema =
    '[DT.Column "Survived" (Maybe Int), DT.Column "prediction" Int]

prediction :: D.Expr Int
prediction = F.col @Int "prediction"

main :: IO ()
main = do
    rawTrain <- D.readCsv "./data/titanic/train.csv"
    rawTest <- D.readCsv "./data/titanic/test.csv"

    train <-
        maybe (fail "train.csv schema mismatch") pure (DT.freeze @TrainSchema rawTrain)
    test <-
        maybe (fail "test.csv schema mismatch") pure (DT.freeze @TestSchema rawTest)

    let (trainDf, validDf) =
            D.randomSplit (mkStdGen 4232) 0.7 (DT.thaw (clean train))
        testDf = DT.thaw (clean test)

        model =
            fitDecisionTree
                ( defaultTreeConfig
                    { maxTreeDepth = 5
                    , minSamplesSplit = 5
                    , minLeafSize = 3
                    , taoIterations = 100
                    , synthConfig =
                        defaultSynthConfig
                            { complexityPenalty = 0.1
                            , maxExprDepth = 3
                            , disallowedCombinations =
                                [ ("Age", "Fare")
                                , ("passenger_class", "number_of_siblings_and_spouses")
                                , ("passenger_class", "number_of_parents_and_children")
                                ]
                            }
                    }
                )
                (F.fromMaybe 0 (F.col @(Maybe Int) "Survived"))
                (trainDf |> D.exclude ["PassengerId"])

    print model

    putStrLn "Training accuracy: "
    print $ computeAccuracy (trainDf |> D.derive (F.name prediction) model)

    putStrLn "Validation accuracy: "
    print $ computeAccuracy (validDf |> D.derive (F.name prediction) model)

    D.writeCsv
        "./predictions.csv"
        ( testDf
            |> D.derive "Survived" model
            |> D.select ["PassengerId", "Survived"]
        )

clean ::
    ( DT.AssertPresent "Ticket" cols
    , DT.SafeLookup "Ticket" cols ~ Maybe T.Text
    , DT.AssertPresent "Name" cols
    , DT.SafeLookup "Name" cols ~ Maybe T.Text
    , DT.AssertPresent "Cabin" cols
    , DT.SafeLookup "Cabin" cols ~ Maybe T.Text
    ) =>
    DT.TypedDataFrame cols ->
    DT.TypedDataFrame
        ( DT.RenameManyInSchema
            '[ '("Name", "title")
             , '("Cabin", "cabin_prefix")
             , '("Pclass", "passenger_class")
             , '("SibSp", "number_of_siblings_and_spouses")
             , '("Parch", "number_of_parents_and_children")
             ]
            cols
        )
clean tdf =
    tdf
        |> DT.replaceColumn @"Ticket" (DT.nullLift (T.filter isAlpha) (DT.col @"Ticket"))
        |> DT.replaceColumn @"Name" (DT.nullLift extractTitle (DT.col @"Name"))
        |> DT.replaceColumn @"Cabin" (DT.nullLift (T.take 1) (DT.col @"Cabin"))
        |> DT.renameMany
            @'[ '("Name", "title")
              , '("Cabin", "cabin_prefix")
              , '("Pclass", "passenger_class")
              , '("SibSp", "number_of_siblings_and_spouses")
              , '("Parch", "number_of_parents_and_children")
              ]

-- | Extract title (e.g. "Mr", "Mrs") from a full Titanic passenger name.
extractTitle :: T.Text -> T.Text
extractTitle fullName =
    case filter (T.isSuffixOf ".") (T.words fullName) of
        (w : _) -> T.dropEnd 1 w
        [] -> ""

{- | Compute binary classification accuracy from a DataFrame containing
  "Survived" and "prediction" columns.
-}
computeAccuracy :: D.DataFrame -> Double
computeAccuracy df =
    let tdf =
            DT.impute @"Survived" 0 $
                DT.unsafeFreeze @RawPredSchema $
                    df |> D.select ["Survived", "prediction"]
        survived = DT.col @"Survived"
        predCol = DT.col @"prediction"
        count expr = fromIntegral (DT.nRows (DT.filterWhere expr tdf))
        tp = count ((survived DT..==. DT.lit 1) DT..&&. (predCol DT..==. DT.lit 1))
        tn = count ((survived DT..==. DT.lit 0) DT..&&. (predCol DT..==. DT.lit 0))
        fp = count ((survived DT..==. DT.lit 0) DT..&&. (predCol DT..==. DT.lit 1))
        fn = count ((survived DT..==. DT.lit 1) DT..&&. (predCol DT..==. DT.lit 0))
     in (tp + tn) / (tp + tn + fp + fn)
