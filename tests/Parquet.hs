{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Parquet where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Data.Int
import Data.Text (Text)
import Data.Time
import GHC.IO (unsafePerformIO)
import Test.HUnit

allTypes :: D.DataFrame
allTypes =
    D.fromNamedColumns
        [ ("id", D.fromList [4 :: Int32, 5, 6, 7, 2, 3, 0, 1])
        , ("bool_col", D.fromList [True, False, True, False, True, False, True, False])
        , ("tinyint_col", D.fromList [0 :: Int32, 1, 0, 1, 0, 1, 0, 1])
        , ("smallint_col", D.fromList [0 :: Int32, 1, 0, 1, 0, 1, 0, 1])
        , ("int_col", D.fromList [0 :: Int32, 1, 0, 1, 0, 1, 0, 1])
        , ("bigint_col", D.fromList [0 :: Int64, 10, 0, 10, 0, 10, 0, 10])
        , ("float_col", D.fromList [0 :: Float, 1.1, 0, 1.1, 0, 1.1, 0, 1.1])
        , ("double_col", D.fromList [0 :: Double, 10.1, 0, 10.1, 0, 10.1, 0, 10.1])
        ,
            ( "date_string_col"
            , D.fromList
                [ "03/01/09" :: Text
                , "03/01/09"
                , "04/01/09"
                , "04/01/09"
                , "02/01/09"
                , "02/01/09"
                , "01/01/09"
                , "01/01/09"
                ]
            )
        , ("string_col", D.fromList (take 8 (cycle ["0" :: Text, "1"])))
        ,
            ( "timestamp_col"
            , D.fromList
                [ UTCTime (fromGregorian 2009 3 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 3 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 4 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 4 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 2 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 2 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 1 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 1 1) (secondsToDiffTime 60)
                ]
            )
        ]

allTypesPlain :: Test
allTypesPlain =
    TestCase
        ( assertEqual
            "allTypesPlain"
            allTypes
            (unsafePerformIO (D.readParquet "./tests/data/alltypes_plain.parquet"))
        )

allTypesTinyPagesDimensions :: Test
allTypesTinyPagesDimensions =
    TestCase
        ( assertEqual
            "allTypesTinyPages last few"
            (7300, 13)
            ( unsafePerformIO
                (fmap D.dimensions (D.readParquet "./tests/data/alltypes_tiny_pages.parquet"))
            )
        )

tinyPagesLast10 :: D.DataFrame
tinyPagesLast10 =
    D.fromNamedColumns
        [ ("id", D.fromList @Int32 (reverse [6174 .. 6183]))
        , ("bool_col", D.fromList @Bool (Prelude.take 10 (cycle [False, True])))
        , ("tinyint_col", D.fromList @Int32 [3, 2, 1, 0, 9, 8, 7, 6, 5, 4])
        , ("smallint_col", D.fromList @Int32 [3, 2, 1, 0, 9, 8, 7, 6, 5, 4])
        , ("int_col", D.fromList @Int32 [3, 2, 1, 0, 9, 8, 7, 6, 5, 4])
        , ("bigint_col", D.fromList @Int64 [30, 20, 10, 0, 90, 80, 70, 60, 50, 40])
        ,
            ( "float_col"
            , D.fromList @Float [3.3, 2.2, 1.1, 0, 9.9, 8.8, 7.7, 6.6, 5.5, 4.4]
            )
        ,
            ( "date_string_col"
            , D.fromList @Text
                [ "09/11/10"
                , "09/11/10"
                , "09/11/10"
                , "09/11/10"
                , "09/10/10"
                , "09/10/10"
                , "09/10/10"
                , "09/10/10"
                , "09/10/10"
                , "09/10/10"
                ]
            )
        ,
            ( "string_col"
            , D.fromList @Text ["3", "2", "1", "0", "9", "8", "7", "6", "5", "4"]
            )
        ,
            ( "timestamp_col"
            , D.fromList @UTCTime
                [ UTCTime (fromGregorian 2010 9 10) (secondsToDiffTime 85384)
                , UTCTime (fromGregorian 2010 9 10) (secondsToDiffTime 85324)
                , UTCTime (fromGregorian 2010 9 10) (secondsToDiffTime 85264)
                , UTCTime (fromGregorian 2010 9 10) (secondsToDiffTime 85204)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 85144)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 85084)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 85024)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 84964)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 84904)
                , UTCTime (fromGregorian 2010 9 9) (secondsToDiffTime 84844)
                ]
            )
        , ("year", D.fromList @Int32 (replicate 10 2010))
        , ("month", D.fromList @Int32 (replicate 10 9))
        ]

allTypesTinyPagesLastFew :: Test
allTypesTinyPagesLastFew =
    TestCase
        ( assertEqual
            "allTypesTinyPages dimensions"
            tinyPagesLast10
            ( unsafePerformIO
                -- Excluding doubles because they are weird to compare.
                ( fmap
                    (D.takeLast 10 . D.exclude ["double_col"])
                    (D.readParquet "./tests/data/alltypes_tiny_pages.parquet")
                )
            )
        )

allTypesPlainSnappy :: Test
allTypesPlainSnappy =
    TestCase
        ( assertEqual
            "allTypesPlainSnappy"
            (D.filter (F.col @Int32 "id") (`elem` [6, 7]) allTypes)
            (unsafePerformIO (D.readParquet "./tests/data/alltypes_plain.snappy.parquet"))
        )

allTypesDictionary :: Test
allTypesDictionary =
    TestCase
        ( assertEqual
            "allTypesPlainSnappy"
            (D.filter (F.col @Int32 "id") (`elem` [0, 1]) allTypes)
            (unsafePerformIO (D.readParquet "./tests/data/alltypes_dictionary.parquet"))
        )

mtCarsDataset :: D.DataFrame
mtCarsDataset =
    D.fromNamedColumns
        [
            ( "model"
            , D.fromList
                [ "Mazda RX4" :: Text
                , "Mazda RX4 Wag"
                , "Datsun 710"
                , "Hornet 4 Drive"
                , "Hornet Sportabout"
                , "Valiant"
                , "Duster 360"
                , "Merc 240D"
                , "Merc 230"
                , "Merc 280"
                , "Merc 280C"
                , "Merc 450SE"
                , "Merc 450SL"
                , "Merc 450SLC"
                , "Cadillac Fleetwood"
                , "Lincoln Continental"
                , "Chrysler Imperial"
                , "Fiat 128"
                , "Honda Civic"
                , "Toyota Corolla"
                , "Toyota Corona"
                , "Dodge Challenger"
                , "AMC Javelin"
                , "Camaro Z28"
                , "Pontiac Firebird"
                , "Fiat X1-9"
                , "Porsche 914-2"
                , "Lotus Europa"
                , "Ford Pantera L"
                , "Ferrari Dino"
                , "Maserati Bora"
                , "Volvo 142E"
                ]
            )
        ,
            ( "mpg"
            , D.fromList
                [ 21.0 :: Double
                , 21.0
                , 22.8
                , 21.4
                , 18.7
                , 18.1
                , 14.3
                , 24.4
                , 22.8
                , 19.2
                , 17.8
                , 16.4
                , 17.3
                , 15.2
                , 10.4
                , 10.4
                , 14.7
                , 32.4
                , 30.4
                , 33.9
                , 21.5
                , 15.5
                , 15.2
                , 13.3
                , 19.2
                , 27.3
                , 26.0
                , 30.4
                , 15.8
                , 19.7
                , 15.0
                , 21.4
                ]
            )
        ,
            ( "cyl"
            , D.fromList
                [ 6 :: Int32
                , 6
                , 4
                , 6
                , 8
                , 6
                , 8
                , 4
                , 4
                , 6
                , 6
                , 8
                , 8
                , 8
                , 8
                , 8
                , 8
                , 4
                , 4
                , 4
                , 4
                , 8
                , 8
                , 8
                , 8
                , 4
                , 4
                , 4
                , 8
                , 6
                , 8
                , 4
                ]
            )
        ,
            ( "disp"
            , D.fromList
                [ 160.0 :: Double
                , 160.0
                , 108.0
                , 258.0
                , 360.0
                , 225.0
                , 360.0
                , 146.7
                , 140.8
                , 167.6
                , 167.6
                , 275.8
                , 275.8
                , 275.8
                , 472.0
                , 460.0
                , 440.0
                , 78.7
                , 75.7
                , 71.1
                , 120.1
                , 318.0
                , 304.0
                , 350.0
                , 400.0
                , 79.0
                , 120.3
                , 95.1
                , 351.0
                , 145.0
                , 301.0
                , 121.0
                ]
            )
        ,
            ( "hp"
            , D.fromList
                [ 110 :: Int32
                , 110
                , 93
                , 110
                , 175
                , 105
                , 245
                , 62
                , 95
                , 123
                , 123
                , 180
                , 180
                , 180
                , 205
                , 215
                , 230
                , 66
                , 52
                , 65
                , 97
                , 150
                , 150
                , 245
                , 175
                , 66
                , 91
                , 113
                , 264
                , 175
                , 335
                , 109
                ]
            )
        ,
            ( "drat"
            , D.fromList
                [ 3.9 :: Double
                , 3.9
                , 3.85
                , 3.08
                , 3.15
                , 2.76
                , 3.21
                , 3.69
                , 3.92
                , 3.92
                , 3.92
                , 3.07
                , 3.07
                , 3.07
                , 2.93
                , 3.0
                , 3.23
                , 4.08
                , 4.93
                , 4.22
                , 3.7
                , 2.76
                , 3.15
                , 3.73
                , 3.08
                , 4.08
                , 4.43
                , 3.77
                , 4.22
                , 3.62
                , 3.54
                , 4.11
                ]
            )
        ,
            ( "wt"
            , D.fromList
                [ 2.62 :: Double
                , 2.875
                , 2.32
                , 3.215
                , 3.44
                , 3.46
                , 3.57
                , 3.19
                , 3.15
                , 3.44
                , 3.44
                , 4.07
                , 3.73
                , 3.78
                , 5.25
                , 5.424
                , 5.345
                , 2.2
                , 1.615
                , 1.835
                , 2.465
                , 3.52
                , 3.435
                , 3.84
                , 3.845
                , 1.935
                , 2.14
                , 1.513
                , 3.17
                , 2.77
                , 3.57
                , 2.78
                ]
            )
        ,
            ( "qsec"
            , D.fromList
                [ 16.46 :: Double
                , 17.02
                , 18.61
                , 19.44
                , 17.02
                , 20.22
                , 15.84
                , 20.0
                , 22.9
                , 18.3
                , 18.9
                , 17.4
                , 17.6
                , 18.0
                , 17.98
                , 17.82
                , 17.42
                , 19.47
                , 18.52
                , 19.9
                , 20.01
                , 16.87
                , 17.3
                , 15.41
                , 17.05
                , 18.9
                , 16.7
                , 16.9
                , 14.5
                , 15.5
                , 14.6
                , 18.6
                ]
            )
        ,
            ( "vs"
            , D.fromList
                [ 0 :: Int32
                , 0
                , 1
                , 1
                , 0
                , 1
                , 0
                , 1
                , 1
                , 1
                , 1
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 1
                , 1
                , 1
                , 1
                , 0
                , 0
                , 0
                , 0
                , 1
                , 0
                , 1
                , 0
                , 0
                , 0
                , 1
                ]
            )
        ,
            ( "am"
            , D.fromList
                [ 1 :: Int32
                , 1
                , 1
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 0
                , 1
                , 1
                , 1
                , 0
                , 0
                , 0
                , 0
                , 0
                , 1
                , 1
                , 1
                , 1
                , 1
                , 1
                , 1
                ]
            )
        ,
            ( "gear"
            , D.fromList
                [ 4 :: Int32
                , 4
                , 4
                , 3
                , 3
                , 3
                , 3
                , 4
                , 4
                , 4
                , 4
                , 3
                , 3
                , 3
                , 3
                , 3
                , 3
                , 4
                , 4
                , 4
                , 3
                , 3
                , 3
                , 3
                , 3
                , 4
                , 5
                , 5
                , 5
                , 5
                , 5
                , 4
                ]
            )
        ,
            ( "carb"
            , D.fromList
                [ 4 :: Int32
                , 4
                , 1
                , 1
                , 2
                , 1
                , 4
                , 2
                , 2
                , 4
                , 4
                , 3
                , 3
                , 3
                , 4
                , 4
                , 4
                , 1
                , 2
                , 1
                , 1
                , 2
                , 2
                , 4
                , 2
                , 1
                , 2
                , 2
                , 4
                , 6
                , 8
                , 2
                ]
            )
        ]

mtCars :: Test
mtCars =
    TestCase
        ( assertEqual
            "mt_cars"
            mtCarsDataset
            (unsafePerformIO (D.readParquet "./tests/data/mtcars.parquet"))
        )

tests :: [Test]
tests =
    [ allTypesPlain
    , allTypesPlainSnappy
    , allTypesDictionary
    , mtCars
    , allTypesTinyPagesLastFew
    , allTypesTinyPagesDimensions
    ]
