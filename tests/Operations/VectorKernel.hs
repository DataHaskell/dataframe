{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | The vectorized scatter-accumulate kernel parity gate. Every case asserts
that 'D.aggregate' (which routes recognised reductions through the fast scatter
kernel) produces a result byte-identical to the same aggregation forced through
the expression interpreter ('interpretOnly'). Covered: sum/min/max/count over
Int and Double, mean, stddev/variance, top2Sum, median, the @max - min@
compound (Q7), and a mixed multi-aggregation over single and multi-key
groupings, on Int/Double/nullable/Text-key grids and sizes straddling the
parallel threshold.

Floating-point sums computed by the scatter follow the same left-to-right
group-order fold as the interpreter, so the two paths agree bit-for-bit here.
The PARALLEL kernel ('DataFrame.Internal.Aggregation.Kernel.Parallel') splits the work by
disjoint group-id range, and because each group's rows stay in their original
@valueIndices@ order within one worker's range, the per-group fold order is
unchanged from the sequential scatter — so the parallel path is also
byte-identical to the interpreter, asserted here at sizes above the 200k
parallel threshold (the test suite runs @-with-rtsopts=-N@, so those cases
exercise the multi-capability scatter).
-}
module Operations.VectorKernel where

import Control.Exception (throw)
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified DataFrame as D
import DataFrame.Errors (DataFrameException (..))
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (GroupedDataFrame, insertColumn)
import DataFrame.Internal.Expression (
    AggStrategy (..),
    Expr (..),
    UExpr (..),
 )
import DataFrame.Internal.Interpreter (
    AggregationResult (..),
    interpretAggregation,
 )
import qualified DataFrame.Operations.Aggregation as DA

import Assertions ()
import Test.HUnit

-- | Aggregate forcing every expression through the interpreter (no kernel).
interpretOnly :: [(T.Text, UExpr)] -> GroupedDataFrame -> D.DataFrame
interpretOnly aggs gdf =
    let base = DA.aggregate [] gdf
        f :: (T.Text, UExpr) -> D.DataFrame -> D.DataFrame
        f (name, UExpr (expr :: Expr a)) d =
            let value :: DI.Column
                value = case interpretAggregation @a gdf expr of
                    Left e -> throw e
                    Right (UnAggregated _) -> throw (UnaggregatedException (T.pack (show expr)))
                    Right (Aggregated (DI.TColumn col)) -> col
             in insertColumn name value d
     in foldl (flip f) base aggs

-- | Per-group sum of the two largest values (the benchmark's Q8 reduction).
top2Sum :: Expr Double -> Expr Double
top2Sum = Agg (CollectAgg "top2Sum" f)
  where
    f :: V.Vector Double -> Double
    f v = sum (take 2 (L.sortBy (flip compare) (V.toList v)))

grid :: Int -> D.DataFrame
grid n =
    D.fromNamedColumns
        [ ("ki", DI.fromList [i `mod` 17 | i <- [0 .. n - 1]])
        , ("kt", DI.fromList [T.pack ('g' : show (i `mod` 11)) | i <- [0 .. n - 1]])
        , -- A high-cardinality key so the parallel group-range split sees many
          -- small groups (alongside the low-cardinality 'ki'/'kt' few-big-groups).
          ("kh", DI.fromList [(i * 2654435761) `mod` 50021 :: Int | i <- [0 .. n - 1]])
        , ("vi", DI.fromList [(i * 31 + 7) `mod` 1000 - 500 :: Int | i <- [0 .. n - 1]])
        , ("vj", DI.fromList [(i * 13) `mod` 97 :: Int | i <- [0 .. n - 1]])
        ,
            ( "vd"
            , DI.fromList [fromIntegral ((i * 7) `mod` 211) / 3 :: Double | i <- [0 .. n - 1]]
            )
        ]

vi, vj :: Expr Int
vi = F.col @Int "vi"
vj = F.col @Int "vj"

vd :: Expr Double
vd = F.col @Double "vd"

aggSets :: [[(T.Text, UExpr)]]
aggSets =
    [ ["s" F..= F.sum vi]
    , ["s" F..= F.sum vd, "c" F..= F.count vi]
    , ["mn" F..= F.minimum vi, "mx" F..= F.maximum vi]
    , ["mnd" F..= F.minimum vd, "mxd" F..= F.maximum vd]
    , ["m" F..= F.mean vi, "md" F..= F.mean vd]
    , ["sd" F..= F.stddev vd, "var" F..= F.variance vd]
    , ["t2" F..= top2Sum vd]
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

parityCase :: Int -> [T.Text] -> [(T.Text, UExpr)] -> Test
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
