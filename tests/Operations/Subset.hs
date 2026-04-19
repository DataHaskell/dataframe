{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Subset where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as Col
import DataFrame.Internal.DataFrame
import DataFrame.Operations.Merge ()
import System.Random
import Test.HUnit

prop_dropZero :: DataFrame -> Bool
prop_dropZero df = D.drop 0 df == df

prop_takeZero :: DataFrame -> Bool
prop_takeZero df = fst (dataframeDimensions (D.take 0 df)) == 0

prop_takeAll :: DataFrame -> Bool
prop_takeAll df =
    let n = fst (dataframeDimensions df)
     in D.take n df == df

prop_dropAll :: DataFrame -> Bool
prop_dropAll df =
    let n = fst (dataframeDimensions df)
     in fst (dataframeDimensions (D.drop n df)) == 0

prop_takeLastZero :: DataFrame -> Bool
prop_takeLastZero df = fst (dataframeDimensions (D.takeLast 0 df)) == 0

prop_dropLastZero :: DataFrame -> Bool
prop_dropLastZero df = D.dropLast 0 df == df

prop_takeLastAll :: DataFrame -> Bool
prop_takeLastAll df =
    let n = fst (dataframeDimensions df)
     in D.takeLast n df == df

prop_dropLastAll :: DataFrame -> Bool
prop_dropLastAll df =
    let n = fst (dataframeDimensions df)
     in fst (dataframeDimensions (D.dropLast n df)) == 0

prop_rangeEmpty :: DataFrame -> Bool
prop_rangeEmpty df =
    fst (dataframeDimensions (D.range (5, 5) df)) == 0

prop_rangeFull :: DataFrame -> Bool
prop_rangeFull df =
    let rows = fst (dataframeDimensions df)
     in D.range (0, rows) df == df

prop_selectAll :: DataFrame -> Bool
prop_selectAll df = D.select (D.columnNames df) df == df

prop_selectEmpty :: DataFrame -> Bool
prop_selectEmpty df =
    let result = D.select [] df
     in dataframeDimensions result == (0, 0)

prop_excludeEmpty :: DataFrame -> Bool
prop_excludeEmpty df = D.exclude [] df == df

prop_excludeAll :: DataFrame -> Bool
prop_excludeAll df =
    let result = D.exclude (D.columnNames df) df
     in snd (dataframeDimensions result) == 0

prop_cubePreservesSmall :: DataFrame -> Bool
prop_cubePreservesSmall df =
    let (rows, cols) = dataframeDimensions df
     in D.cube (rows + 100, cols + 100) df == df

prop_sampleEmptyApprox :: DataFrame -> Bool
prop_sampleEmptyApprox df =
    let gen = mkStdGen 42
        sampled = D.sample gen 0.0 df
     in fst (dataframeDimensions sampled) == 0

prop_stratifiedSplit_deterministic :: DataFrame -> Bool
prop_stratifiedSplit_deterministic _ =
    let df =
            D.fromNamedColumns
                [ ("label", Col.fromList (replicate 50 ("A" :: T.Text) ++ replicate 50 "B"))
                , ("val", Col.fromList ([1 .. 100] :: [Int]))
                ]
        (tr, va) = D.stratifiedSplit (mkStdGen 314) 0.7 (D.col @T.Text "label") df
     in fst (dataframeDimensions tr) + fst (dataframeDimensions va) == 100

strataDf :: DataFrame
strataDf =
    D.fromNamedColumns
        [ ("label", Col.fromList (replicate 5 ("A" :: T.Text) ++ replicate 5 "B"))
        , ("val", Col.fromList ([1 .. 10] :: [Int]))
        ]

unit_stratifiedSample_full :: Test
unit_stratifiedSample_full =
    TestCase $
        let sampled = D.stratifiedSample (mkStdGen 42) 1.0 (D.col @T.Text "label") strataDf
         in assertEqual
                "p=1.0 preserves row count"
                (fst $ dataframeDimensions strataDf)
                (fst $ dataframeDimensions sampled)

unit_stratifiedSplit_rowCount :: Test
unit_stratifiedSplit_rowCount =
    TestCase $
        let (tr, va) = D.stratifiedSplit (mkStdGen 99) 0.8 (D.col @T.Text "label") strataDf
         in assertEqual
                "train+validation == total"
                (fst $ dataframeDimensions strataDf)
                (fst (dataframeDimensions tr) + fst (dataframeDimensions va))

unit_stratifiedSplit_singleRowStratum :: Test
unit_stratifiedSplit_singleRowStratum =
    TestCase $
        let tinyDf =
                D.fromNamedColumns
                    [ ("label", Col.fromList (["A", "A", "A", "A", "A", "B"] :: [T.Text]))
                    , ("val", Col.fromList ([1 .. 6] :: [Int]))
                    ]
            (tr, va) = D.stratifiedSplit (mkStdGen 7) 0.8 (D.col @T.Text "label") tinyDf
         in assertEqual
                "single-row stratum: no rows lost"
                (fst $ dataframeDimensions tinyDf)
                (fst (dataframeDimensions tr) + fst (dataframeDimensions va))

-- | Count occurrences of a label in a column, expressed as a fraction of total rows.
labelProportion :: T.Text -> T.Text -> DataFrame -> Double
labelProportion col label df =
    let total = fst (dataframeDimensions df)
        vals = case getColumn col df of
            Just c -> Col.toList @T.Text c
            Nothing -> []
        n = length (filter (== label) vals)
     in fromIntegral n / fromIntegral total

unit_stratifiedSplit_proportions :: Test
unit_stratifiedSplit_proportions =
    TestCase $
        let aCount = 100
            bCount = 50
            df =
                D.fromNamedColumns
                    [
                        ( "label"
                        , Col.fromList (replicate aCount ("A" :: T.Text) ++ replicate bCount "B")
                        )
                    , ("val", Col.fromList ([1 .. aCount + bCount] :: [Int]))
                    ]
            (tr, va) = D.stratifiedSplit (mkStdGen 42) 0.8 (D.col @T.Text "label") df
            origProp = labelProportion "label" "A" df
            trProp = labelProportion "label" "A" tr
            vaProp = labelProportion "label" "A" va
            tol = 0.05 :: Double
         in do
                assertBool
                    ( "train A-proportion "
                        ++ show trProp
                        ++ " differs from original "
                        ++ show origProp
                        ++ " by more than "
                        ++ show tol
                    )
                    (abs (trProp - origProp) < tol)
                assertBool
                    ( "validation A-proportion "
                        ++ show vaProp
                        ++ " differs from original "
                        ++ show origProp
                        ++ " by more than "
                        ++ show tol
                    )
                    (abs (vaProp - origProp) < tol)

hunitTests :: [Test]
hunitTests =
    [ TestLabel "unit_stratifiedSample_full" unit_stratifiedSample_full
    , TestLabel "unit_stratifiedSplit_rowCount" unit_stratifiedSplit_rowCount
    , TestLabel
        "unit_stratifiedSplit_singleRowStratum"
        unit_stratifiedSplit_singleRowStratum
    , TestLabel "unit_stratifiedSplit_proportions" unit_stratifiedSplit_proportions
    ]

tests :: [DataFrame -> Bool]
tests =
    [ prop_dropZero
    , prop_takeZero
    , prop_takeAll
    , prop_dropAll
    , prop_takeLastZero
    , prop_dropLastZero
    , prop_takeLastAll
    , prop_dropLastAll
    , prop_rangeEmpty
    , prop_rangeFull
    , prop_selectAll
    , prop_selectEmpty
    , prop_excludeEmpty
    , prop_excludeAll
    , prop_cubePreservesSmall
    , prop_sampleEmptyApprox
    , prop_stratifiedSplit_deterministic
    ]
