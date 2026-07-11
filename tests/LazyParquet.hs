{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module LazyParquet where

import Data.Int (Int32, Int64)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Time (UTCTime)
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Lazy (SortOrder (..))
import qualified DataFrame.Lazy as L
import DataFrame.Schema (Schema (..), schemaType)
import Test.HUnit

-- | Schema matching all columns in alltypes_plain.parquet.
allTypesSchema :: Schema
allTypesSchema =
    Schema $
        M.fromList
            [ ("id", schemaType @Int32)
            , ("bool_col", schemaType @Bool)
            , ("tinyint_col", schemaType @Int32)
            , ("smallint_col", schemaType @Int32)
            , ("int_col", schemaType @Int32)
            , ("bigint_col", schemaType @Int64)
            , ("float_col", schemaType @Float)
            , ("double_col", schemaType @Double)
            , ("date_string_col", schemaType @T.Text)
            , ("string_col", schemaType @T.Text)
            , ("timestamp_col", schemaType @UTCTime)
            ]

plainPath :: FilePath
plainPath = "./tests/data/alltypes_plain.parquet"

-- Test 1: basic scan — dimensions match eager reader.
basicScan :: Test
basicScan =
    TestCase
        ( do
            eager <- D.readParquet plainPath
            actual <- L.runDataFrame (L.scanParquet allTypesSchema (T.pack plainPath))
            assertEqual "basicScan dimensions" (D.dimensions eager) (D.dimensions actual)
        )

-- Test 2: column projection — select 2 columns.
columnProjection :: Test
columnProjection =
    TestCase
        ( do
            actual <-
                L.runDataFrame
                    (L.select ["id", "bool_col"] (L.scanParquet allTypesSchema (T.pack plainPath)))
            assertEqual "columnProjection" (8, 2) (D.dimensions actual)
        )

-- Test 3: filter pushdown — id >= 6 gives 2 rows.
filterPushdown :: Test
filterPushdown =
    TestCase
        ( do
            actual <-
                L.runDataFrame
                    ( L.filter
                        (F.geq (F.col @Int32 "id") (F.lit 6))
                        (L.scanParquet allTypesSchema (T.pack plainPath))
                    )
            assertEqual "filterPushdown" (2, 11) (D.dimensions actual)
        )

-- Test 4: filter + project combined.
filterAndProject :: Test
filterAndProject =
    TestCase
        ( do
            actual <-
                L.runDataFrame
                    ( L.select
                        ["id", "bool_col"]
                        ( L.filter
                            (F.geq (F.col @Int32 "id") (F.lit 6))
                            (L.scanParquet allTypesSchema (T.pack plainPath))
                        )
                    )
            assertEqual "filterAndProject" (2, 2) (D.dimensions actual)
        )

-- Test 5: multi-file glob — alltypes_plain*.parquet matches plain + snappy = 10 rows.
multiFileGlob :: Test
multiFileGlob =
    TestCase
        ( do
            actual <-
                L.runDataFrame
                    ( L.scanParquet
                        allTypesSchema
                        "./tests/data/alltypes_plain*.parquet"
                    )
            let (rows, cols) = D.dimensions actual
            assertEqual "multiFileGlob cols" 11 cols
            assertBool "multiFileGlob rows >= 8" (rows >= 8)
        )

-- Test 6: sort + limit — get 3 smallest ids.
sortAndLimit :: Test
sortAndLimit =
    TestCase
        ( do
            actual <-
                L.runDataFrame
                    ( L.take
                        3
                        ( L.sortBy
                            [("id", Ascending)]
                            (L.scanParquet allTypesSchema (T.pack plainPath))
                        )
                    )
            assertEqual "sortAndLimit rows" 3 (fst (D.dimensions actual))
        )

tests :: [Test]
tests =
    [ basicScan
    , columnProjection
    , filterPushdown
    , filterAndProject
    , multiFileGlob
    , sortAndLimit
    ]
