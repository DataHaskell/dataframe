{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}


module Main (main) where

import Data.Function ((&))
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing, getTemporaryDirectory)
import qualified System.Exit as Exit
import System.FilePath ((</>))
import System.IO (hPutStrLn, stderr)
import Test.HUnit (
    Test (..),
    assertEqual,
    errors,
    failures,
    runTestTT,
 )

import DataFrame.Fusion.Typed (Column)
import qualified DataFrame.Fusion.Typed as Fusion
import qualified DataFrame.Internal.Column as IC
import qualified DataFrame.Internal.DataFrame as DF
import qualified DataFrame.Operations.Core as Core
import DataFrame.Typed.Freeze (thaw)

-- | A minimal smoke schema: id (int), score (double), name (text).
type CsvCols =
    '[ Column "id" Int
     , Column "score" Double
     , Column "name" T.Text
     ]

testCsv :: T.Text
testCsv =
    T.unlines
        [ "id,score,name"
        , "1,1.5,alice"
        , "2,2.5,bob"
        , "3,3.5,carol"
        , "4,4.5,dan"
        ]

-- | Companion CSV for the join test: department per id.
deptsCsv :: T.Text
deptsCsv =
    T.unlines
        [ "id,dept"
        , "1,eng"
        , "2,eng"
        , "3,sales"
        ]

type DeptCols =
    '[ Column "id" Int
     , Column "dept" T.Text
     ]

testFixtureDir :: IO FilePath
testFixtureDir = do
    base <- getTemporaryDirectory
    let dir = base </> "dataframe-fusion-tests"
    createDirectoryIfMissing True dir
    return dir

writeTestCsv :: IO FilePath
writeTestCsv = do
    dir <- testFixtureDir
    let path = dir </> "smoke.csv"
    TIO.writeFile path testCsv
    return path

writeDeptsCsv :: IO FilePath
writeDeptsCsv = do
    dir <- testFixtureDir
    let path = dir </> "depts.csv"
    TIO.writeFile path deptsCsv
    return path

main :: IO ()
main = do
    path <- writeTestCsv
    deptPath <- writeDeptsCsv
    ctx <- Fusion.newContext

    let scan = Fusion.scanCsv @CsvCols ctx (T.pack path)
        scanDepts = Fusion.scanCsv @DeptCols ctx (T.pack deptPath)

    let tests =
            TestList
                [ TestLabel "scanCsv + run round-trips column count" $ TestCase $ do
                    fdf <- scan
                    tdf <- Fusion.run fdf
                    let df = thaw tdf
                    assertEqual "row count" 4 (Core.nRows df)
                    assertEqual "column count" 3 (Core.nColumns df)
                , TestLabel "take limits row count" $ TestCase $ do
                    fdf <- scan
                    fdf' <- Fusion.take 2 fdf
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "row count after take 2" 2 (Core.nRows df)
                , TestLabel "select projects to fewer columns" $ TestCase $ do
                    fdf <- scan
                    fdf' <- Fusion.select @'["id", "name"] fdf
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "columns after select" 2 (Core.nColumns df)
                , TestLabel "filter drops non-matching rows" $ TestCase $ do
                    fdf <- scan
                    let pred_ = Fusion.col @"id" Fusion..>. Fusion.lit (2 :: Int)
                    fdf' <- Fusion.filter pred_ fdf
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "row count after filter id > 2" 2 (Core.nRows df)
                , TestLabel "derive adds a computed column" $ TestCase $ do
                    fdf <- scan
                    let doubled = Fusion.col @"score" Fusion..*. Fusion.lit (2.0 :: Double)
                    fdf' <- Fusion.derive @"doubled" doubled fdf
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "row count after derive" 4 (Core.nRows df)
                    assertEqual "column count after derive" 4 (Core.nColumns df)
                , TestLabel "sortBy reorders rows" $ TestCase $ do
                    fdf <- scan
                    fdf' <- Fusion.sortBy [("score", Fusion.Descending)] fdf
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "row count after sort" 4 (Core.nRows df)
                , TestLabel "groupBy + aggregate produces one row per key" $ TestCase $ do
                    fdf <- scan
                    let
                        -- group by name, sum scores (each name unique here, so 4 groups)
                        aggs =
                            Fusion.agg @"total" (Fusion.sum (Fusion.col @"score")) Fusion.aggNil
                    fdf' <- Fusion.aggregate aggs (Fusion.groupBy @'["name"] fdf)
                    df <- thaw <$> Fusion.run fdf'
                    assertEqual "row count after groupBy" 4 (Core.nRows df)
                    assertEqual "column count after groupBy" 2 (Core.nColumns df)
                , TestLabel "innerJoin matches rows on id" $ TestCase $ do
                    fdf <- scan
                    fdf2 <- scanDepts
                    joined <- Fusion.innerJoin "id" "id" fdf fdf2
                    df <- thaw <$> Fusion.run joined
                    -- 3 ids in depts.csv match against 4 ids in smoke.csv -> 3 rows
                    assertEqual "row count after inner join" 3 (Core.nRows df)
                ]

    counts <- runTestTT tests
    if errors counts + failures counts == 0
        then return ()
        else do
            hPutStrLn stderr "FAILED"
            Exit.exitWith (Exit.ExitFailure 1)
