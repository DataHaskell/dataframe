{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Operations.VectorKernel where

import Control.Exception (throw)
import qualified Data.Text as T

import qualified DataFrame as D
import DataFrame.Errors (DataFrameException (..))
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (GroupedDataFrame, insertColumn)
import DataFrame.Internal.Expression (
    Expr (..),
    SomeExpr (..),
 )
import DataFrame.Internal.Interpreter (
    AggregationResult (..),
    interpretAggregation,
 )
import qualified DataFrame.Operations.Aggregation as DA

import Assertions ()
import Test.HUnit

-- | Aggregate forcing every expression through the interpreter (no kernel).
interpretOnly :: [(T.Text, SomeExpr)] -> GroupedDataFrame -> D.DataFrame
interpretOnly aggs gdf =
    let base = DA.aggregate [] gdf
        f :: (T.Text, SomeExpr) -> D.DataFrame -> D.DataFrame
        f (name, SomeExpr (expr :: Expr a)) d =
            let value :: DI.Column
                value = case interpretAggregation @a gdf expr of
                    Left e -> throw e
                    Right (UnAggregated _) -> throw (UnaggregatedException (T.pack (show expr)))
                    Right (Aggregated (DI.TColumn col)) -> col
             in insertColumn name value d
     in foldl (flip f) base aggs

grid :: Int -> D.DataFrame
grid n =
    D.fromColumns
        [ ("ki", DI.fromList [i `mod` 17 | i <- [0 .. n - 1]])
        , ("kt", DI.fromList [T.pack ('g' : show (i `mod` 11)) | i <- [0 .. n - 1]])
        , -- A high-cardinality key so the group-range split sees many groups.
          ("kh", DI.fromList [(i * 2654435761) `mod` 50021 :: Int | i <- [0 .. n - 1]])
        , ("vi", DI.fromList [(i * 31 + 7) `mod` 1000 - 500 :: Int | i <- [0 .. n - 1]])
        , ("vj", DI.fromList [(i * 13) `mod` 97 :: Int | i <- [0 .. n - 1]])
        ,
            ( "vd"
            , DI.fromList [fromIntegral ((i * 7) `mod` 211) / 3 :: Double | i <- [0 .. n - 1]]
            )
        ]

vi, vj :: Expr Int
vi = F.int "vi"
vj = F.int "vj"

vd :: Expr Double
vd = F.double "vd"

aggSets :: [[(T.Text, SomeExpr)]]
aggSets =
    [ ["s" F..= F.sum vi]
    , ["s" F..= F.sum vd, "c" F..= F.count vi]
    , ["mn" F..= F.minimum vi, "mx" F..= F.maximum vi]
    , ["mnd" F..= F.minimum vd, "mxd" F..= F.maximum vd]
    , ["m" F..= F.mean vi, "md" F..= F.mean vd]
    , ["sd" F..= F.stddev vd, "var" F..= F.variance vd]
    , ["med" F..= F.median vd]
    , ["diff" F..= (F.maximum vi - F.minimum vj)]
    ,
        [ "s" F..= F.sum vi
        , "m" F..= F.mean vd
        , "sd" F..= F.stddev vd
        , "med" F..= F.median vd
        , "c" F..= F.count vj
        , "diff" F..= (F.maximum vi - F.minimum vj)
        ]
    ]

keySets :: [[T.Text]]
keySets = [["ki"], ["kt"], ["ki", "kt"], ["kh"]]

parityCase :: Int -> [T.Text] -> [(T.Text, SomeExpr)] -> Test
parityCase n keys aggs =
    TestCase $
        let df = grid n
            gdf = D.groupBy keys df
            fast = D.aggregate aggs gdf
            ref = interpretOnly aggs gdf
            label = "n=" ++ show n ++ " keys=" ++ show keys ++ " #aggs=" ++ show (length aggs)
         in assertEqual ("kernel==interpreter " ++ label) ref fast

tests :: [Test]
tests =
    [ TestLabel "vectorKernelParity" (parityCase n keys aggs)
    | -- 200001 and 500000 straddle and clear the parallel threshold, so the
    -- parallel scatter kernel is the one validated against the interpreter.
    n <- [1, 7, 5000, 200001, 500000]
    , keys <- keySets
    , aggs <- aggSets
    ]
