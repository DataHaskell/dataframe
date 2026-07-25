{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | 'readColumns' semantics for the SIMD reader, pinned against the
pure reader. Every case asserts both readers agree: the two disagreed
over 'ProvideNames' for a long time, and a projection only one of them
honours is worse than no projection at all.
-}
module Operations.Projection (tests) where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Control.Exception (SomeException, evaluate, try)
import Data.List (isInfixOf)
import qualified DataFrame.IO.CSV as CSV
import qualified DataFrame.IO.CSV.Fast as Fast
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnIndices,
    dataframeDimensions,
    forceDataFrame,
 )
import System.Directory (removeFile)
import Test.HUnit

withCsv :: String -> T.Text -> (FilePath -> IO a) -> IO a
withCsv label contents k = do
    let path = "./tests/data/unstable_csv/projection_" <> label <> ".csv"
    TIO.writeFile path contents
    r <- k path
    removeFile path
    pure r

{- | Read @contents@ through both readers with @opts@, check they agree,
then hand the frame to the assertion.
-}
bothReaders ::
    String -> CSV.ReadOptions -> T.Text -> (DataFrame -> Assertion) -> Test
bothReaders label opts contents check = TestLabel label $
    TestCase $
        withCsv label contents $ \path -> do
            pureDf <- forceDataFrame <$> CSV.readSeparated opts path
            fastDf <- forceDataFrame <$> Fast.fastReadCsvWithOpts opts path
            assertEqual (label <> ": readers agree") pureDf fastDf
            check pureDf

-- | Both readers must reject a name the file does not have.
bothReadersFail :: String -> CSV.ReadOptions -> T.Text -> String -> Test
bothReadersFail label opts contents needle = TestLabel label $
    TestCase $
        withCsv label contents $ \path -> do
            let expectFail which act = do
                    r <- try (act >>= evaluate . forceDataFrame)
                    case r of
                        Left (e :: SomeException) ->
                            assertBool
                                (label <> ": " <> which <> " should mention " <> show needle)
                                (needle `isInfixOf` show e)
                        Right _ ->
                            assertFailure (label <> ": " <> which <> " should have failed")
            expectFail "pure reader" (CSV.readSeparated opts path)
            expectFail "fast reader" (Fast.fastReadCsvWithOpts opts path)

sample :: T.Text
sample = "id,name,surname,dob\n1,Ada,Lovelace,1815\n2,Alan,Turing,1912\n"

select :: [T.Text] -> CSV.ReadOptions
select cs = CSV.defaultReadOptions{CSV.readColumns = Just cs}

tests :: [Test]
tests =
    [ bothReaders "prefix_subset" (select ["id", "name"]) sample $
        assertEqual "kept" (2, 2) . dataframeDimensions
    , bothReaders "non_prefix_subset" (select ["id", "dob"]) sample $ \df -> do
        assertEqual "kept" (2, 2) (dataframeDimensions df)
        assertEqual "names" [("dob", 1), ("id", 0)] (M.toList (columnIndices df))
    , bothReaders "requested_order_wins" (select ["dob", "id"]) sample $
        assertEqual "order" [("dob", 0), ("id", 1)] . M.toList . columnIndices
    , bothReaders "single_column" (select ["surname"]) sample $
        assertEqual "kept" (2, 1) . dataframeDimensions
    , bothReaders "no_selection_keeps_all" CSV.defaultReadOptions sample $
        assertEqual "kept" (2, 4) . dataframeDimensions
    , bothReadersFail
        "missing_column"
        (select ["id", "nope"])
        sample
        "Column not found"
    ]
