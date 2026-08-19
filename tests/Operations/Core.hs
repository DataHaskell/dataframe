{-# LANGUAGE OverloadedStrings #-}

module Operations.Core where

import qualified Data.Text as T

import Assertions (assertExpectException)
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Row (Any (..))

import Test.HUnit

testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("A", DI.fromList ([1 .. 3] :: [Int]))
        , ("B", DI.fromList ['a' .. 'c'])
        ]

createsDataFrameFromRows :: Test
createsDataFrameFromRows =
    TestCase
        ( assertEqual
            "dataframe created from rows"
            testData
            ( D.fromRows
                ["A", "B"]
                [ [D.toAny (1 :: Int), D.toAny ('a' :: Char)]
                , [D.toAny (2 :: Int), D.toAny ('b' :: Char)]
                , [D.toAny (3 :: Int), D.toAny ('c' :: Char)]
                ]
            )
        )

fromRowsThrowsOnTypeMismatch :: Test
fromRowsThrowsOnTypeMismatch =
    TestCase
        ( assertExpectException
            "[Error Case]"
            "fromRows"
            ( print $
                D.fromRows
                    ["A"]
                    [ [D.toAny (1 :: Int)]
                    , [D.toAny ('x' :: Char)]
                    , [D.toAny (3 :: Int)]
                    ]
            )
        )

fromRowsThrowsOnShortRow :: Test
fromRowsThrowsOnShortRow =
    TestCase
        ( assertExpectException
            "[Error Case]"
            "fromRows"
            ( print $
                D.fromRows
                    ["A", "B"]
                    [ [D.toAny (1 :: Int), D.toAny (10 :: Int)]
                    , [D.toAny (2 :: Int)]
                    ]
            )
        )

-- | A null keeps its row: the column stays full length and values stay put.
fromRowsKeepsNullsInPlace :: Test
fromRowsKeepsNullsInPlace =
    TestCase
        ( assertEqual
            "null cell preserves row alignment"
            ( D.fromNamedColumns
                [("A", DI.fromList ([Just 1, Nothing, Just 3] :: [Maybe Int]))]
            )
            (D.fromRows ["A"] [[D.toAny (1 :: Int)], [Null], [D.toAny (3 :: Int)]])
        )

{- | An all-null column has as many rows as it was given. Collapsing it to an
empty column silently truncates the frame.
-}
fromRowsAllNullColumnKeepsRows :: Test
fromRowsAllNullColumnKeepsRows =
    TestCase
        ( assertEqual
            "all-null column keeps its rows"
            3
            (D.nRows (D.fromRows ["A"] [[Null], [Null], [Null]]))
        )

{- | A frame with a null survives the round trip at full length. Guards the
alignment invariant through 'toRowList' as well as 'fromRows'.
-}
fromRowsRoundTripsWithNulls :: Test
fromRowsRoundTripsWithNulls =
    TestCase
        ( let df =
                D.fromNamedColumns
                    [ ("A", DI.fromList ([Just 1, Nothing, Just 3] :: [Maybe Int]))
                    , ("B", DI.fromList (["x", "y", "z"] :: [T.Text]))
                    ]
           in assertEqual
                "round trip through rows preserves the frame"
                df
                (D.fromRows (D.columnNames df) (map (map snd) (D.toRowList df)))
        )

tests :: [Test]
tests =
    [ TestLabel "createsDataFrameFromRows" createsDataFrameFromRows
    , TestLabel "fromRowsThrowsOnTypeMismatch" fromRowsThrowsOnTypeMismatch
    , TestLabel "fromRowsThrowsOnShortRow" fromRowsThrowsOnShortRow
    , TestLabel "fromRowsKeepsNullsInPlace" fromRowsKeepsNullsInPlace
    , TestLabel "fromRowsAllNullColumnKeepsRows" fromRowsAllNullColumnKeepsRows
    , TestLabel "fromRowsRoundTripsWithNulls" fromRowsRoundTripsWithNulls
    ]
