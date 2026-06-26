{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Tests for the typed CSV reader: a write\/read round-trip matches the untyped
reader, a wrong schema surfaces as 'Left' via 'readCsvWithError', and the
throwing 'readCsv' raises a 'DataFrameException'. The Parquet reader uses the
identical freeze-on-read path.
-}
module Typed.IOReaders (tests) where

import Control.Exception (evaluate, try)
import Data.Either (isLeft)
import qualified Data.Text as T
import System.IO (hClose)
import System.IO.Temp (withSystemTempFile)

import qualified DataFrame as D
import DataFrame.Errors (DataFrameException)
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.IO.CSV as TCSV

import Test.HUnit

type S =
    '[ DT.Column "x" Int
     , DT.Column "y" Double
     , DT.Column "g" T.Text
     ]

sampleDF :: D.DataFrame
sampleDF =
    D.fromNamedColumns
        [ ("x", DI.fromList [1, 2, 3 :: Int])
        , ("y", DI.fromList [1.5, 2.5, 3.5 :: Double])
        , ("g", DI.fromList ["a", "b", "c" :: T.Text])
        ]

roundTrip :: Test
roundTrip = TestCase $ withSystemTempFile "typed_parity.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp sampleDF
    untyped <- D.readCsv fp
    typed <- DT.thaw <$> TCSV.readCsv @S fp
    assertEqual "typed readCsv round-trips like untyped readCsv" untyped typed

wrongSchemaEither :: Test
wrongSchemaEither = TestCase $ withSystemTempFile "typed_err.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp sampleDF
    res <- TCSV.readCsvWithError @'[DT.Column "nope" Int] fp
    assertBool "wrong schema => Left" (isLeft res)

wrongSchemaThrows :: Test
wrongSchemaThrows = TestCase $ withSystemTempFile "typed_throw.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp sampleDF
    r <-
        try (TCSV.readCsv @'[DT.Column "nope" Int] fp >>= evaluate . DT.nRows) ::
            IO (Either DataFrameException Int)
    assertBool "wrong schema => throws DataFrameException" (isLeft r)

tests :: [Test]
tests = [roundTrip, wrongSchemaEither, wrongSchemaThrows]
