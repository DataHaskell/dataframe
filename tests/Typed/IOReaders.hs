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
import DataFrame.Internal.DataFrame (getColumn)
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.IO.CSV as TCSV

import Test.HUnit

type S =
    '[ '("x", Int)
     , '("y", Double)
     , '("g", T.Text)
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
    res <- TCSV.readCsvWithError @'[ '("nope", Int)] fp
    assertBool "wrong schema => Left" (isLeft res)

wrongSchemaThrows :: Test
wrongSchemaThrows = TestCase $ withSystemTempFile "typed_throw.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp sampleDF
    r <-
        try (TCSV.readCsv @'[ '("nope", Int)] fp >>= evaluate . DT.nRows) ::
            IO (Either DataFrameException Int)
    assertBool "wrong schema => throws DataFrameException" (isLeft r)

{- | The schema is the read specification: only its columns are fetched, out
of a file that holds more.
-}
typedReadProjects :: Test
typedReadProjects = TestCase $ withSystemTempFile "typed_proj.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp sampleDF
    narrow <- DT.thaw <$> TCSV.readCsv @'[ '("g", T.Text)] fp
    assertEqual "only the schema's column is read" ["g"] (D.columnNames narrow)
    assertEqual "rows intact" (3, 1) (D.dimensions narrow)

{- | The schema's types win over inference: a column of digits declared 'Text'
comes back as 'Text', which a plain read would have called 'Int'.
-}
typedReadSharesTypes :: Test
typedReadSharesTypes = TestCase $ withSystemTempFile "typed_types.csv" $ \fp h -> do
    hClose h
    D.writeCsv fp digits
    inferred <- D.readCsv fp
    assertEqual
        "untyped read infers Int"
        (Just (DI.fromList [1, 2, 3 :: Int]))
        (getColumn "n" inferred)
    typed <- DT.thaw <$> TCSV.readCsv @'[ '("n", T.Text)] fp
    assertEqual
        "schema types the column as Text"
        (Just (DI.fromList ["1", "2", "3" :: T.Text]))
        (getColumn "n" typed)
  where
    digits = D.fromNamedColumns [("n", DI.fromList [1, 2, 3 :: Int])]

tests :: [Test]
tests =
    [ roundTrip
    , wrongSchemaEither
    , wrongSchemaThrows
    , typedReadProjects
    , typedReadSharesTypes
    ]
