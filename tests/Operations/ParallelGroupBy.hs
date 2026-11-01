{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Parallel grouping must be bit-for-bit identical to the sequential one.
module Operations.ParallelGroupBy where

import qualified Data.ByteString as BS
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Text.Encoding as TE
import Data.Text.Internal (Text (Text))
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import DataFrame.Expression.Operators ((./=.))
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)
import qualified DataFrame.Internal.DataFrame as DD
import DataFrame.Internal.Grouping (groupByPar, groupBySeq)

import Assertions ()
import Test.HUnit

-- | A grouping is canonical-equal when all three index structures agree.
sameGrouping :: DD.GroupedDataFrame -> DD.GroupedDataFrame -> Bool
sameGrouping a b =
    DD.valueIndices a == DD.valueIndices b
        && DD.offsets a == DD.offsets b
        && DD.rowToGroup a == DD.rowToGroup b

mkInt :: T.Text -> [Int] -> (T.Text, DI.Column)
mkInt name xs = (name, DI.fromList xs)

mkText :: T.Text -> [T.Text] -> (T.Text, DI.Column)
mkText name xs = (name, DI.fromList xs)

mkDouble :: T.Text -> [Double] -> (T.Text, DI.Column)
mkDouble name xs = (name, DI.fromList xs)

mkMaybeInt :: T.Text -> [Maybe Int] -> (T.Text, DI.Column)
mkMaybeInt name xs = (name, DI.fromList xs)

-- | Build a frame of @n@ rows with several key columns and a value column.
caseFrame :: Int -> DD.DataFrame
caseFrame n =
    D.fromColumns
        [ mkInt "ki" [i `mod` 997 | i <- [0 .. n - 1]]
        , mkText "kt" [T.pack ("g" ++ show (i `mod` 503)) | i <- [0 .. n - 1]]
        , mkDouble "kd" [fromIntegral (i `mod` 311) / 7 | i <- [0 .. n - 1]]
        , mkMaybeInt
            "kn"
            [if i `mod` 13 == 0 then Nothing else Just (i `mod` 251) | i <- [0 .. n - 1]]
        , mkInt "v" [0 .. n - 1]
        , mkDouble "vd" [fromIntegral i * 1.5 | i <- [0 .. n - 1]]
        ]

-- | Collision-heavy small grid (forces hash-collision key re-verification).
collisionFrame :: DD.DataFrame
collisionFrame =
    D.fromColumns
        [ mkInt "k1" (map (`mod` 5) [0 .. 999])
        , mkInt "k2" (map (\i -> (i * 7) `mod` 3) [0 .. 999])
        , mkInt "v" [0 .. 999]
        ]

keySets :: [[T.Text]]
keySets =
    [ ["ki"]
    , ["kt"]
    , ["kd"]
    , ["kn"]
    , ["ki", "kt"]
    , ["kt", "kn"]
    , ["ki", "kt", "kd", "kn"]
    ]

-- | Sizes straddling the 200k parallel threshold.
sizes :: [Int]
sizes = [1, 50, 4999, 200001, 350000]

parityFor :: Int -> [T.Text] -> Test
parityFor n keys =
    TestCase $
        let df = caseFrame n
            s = groupBySeq keys df
            p = groupByPar keys df
            label = "n=" ++ show n ++ " keys=" ++ show keys
         in assertBool ("parallel==sequential grouping for " ++ label) (sameGrouping s p)

-- | Aggregating through both paths must give identical numbers.
aggParityFor :: Int -> Test
aggParityFor n =
    TestCase $
        let df = caseFrame n
            v = F.int "v"
            vd = F.double "vd"
            aggs =
                [ "vsum" F..= F.sum v
                , "vmean" F..= F.mean vd
                , "vmin" F..= F.minimum v
                , "vmax" F..= F.maximum v
                , "vcount" F..= F.count v
                , "vsd" F..= F.stddev vd
                ]
            seqDf = D.aggregate aggs (groupBySeq ["ki", "kt"] df)
            parDf = D.aggregate aggs (groupByPar ["ki", "kt"] df)
         in assertEqual ("aggregate parity n=" ++ show n) seqDf parDf

collisionParity :: Test
collisionParity =
    TestCase $
        assertBool
            "collision-heavy parallel==sequential"
            ( sameGrouping
                (groupBySeq ["k1", "k2"] collisionFrame)
                (groupByPar ["k1", "k2"] collisionFrame)
            )

-- | Frame for the moment-statistics aggregations checked against an oracle.
momentFrame :: Int -> DD.DataFrame
momentFrame n =
    D.fromColumns
        [ mkInt "id2" [i `mod` 17 | i <- [0 .. n - 1]]
        , mkInt "id4" [(i * 3) `mod` 11 | i <- [0 .. n - 1]]
        , mkInt "v1" [(i * 7) `mod` 100 | i <- [0 .. n - 1]]
        , mkInt "v2" [(i * 13 + 1) `mod` 100 | i <- [0 .. n - 1]]
        ]

-- | Derive the product columns exactly as the benchmark does, then aggregate.
momentResult ::
    ([T.Text] -> DD.DataFrame -> DD.GroupedDataFrame) -> Int -> DD.DataFrame
momentResult grp n =
    let dv1 = F.toDouble (F.int "v1")
        dv2 = F.toDouble (F.int "v2")
        df =
            D.derive "v2v2" (dv2 * dv2) $
                D.derive "v1v1" (dv1 * dv1) $
                    D.derive "v1v2" (dv1 * dv2) (momentFrame n)
        aggs =
            [ "n" F..= F.count (F.int "v1")
            , "sx" F..= F.sum dv1
            , "sy" F..= F.sum dv2
            , "sxy" F..= F.sum (F.double "v1v2")
            , "sxx" F..= F.sum (F.double "v1v1")
            , "syy" F..= F.sum (F.double "v2v2")
            ]
     in D.aggregate aggs (grp ["id2", "id4"] df)

-- | The fused path's numbers must equal the parallel-vs-sequential reference.
momentParity :: Int -> Test
momentParity n =
    TestCase $
        assertEqual
            ("moment fused parity n=" ++ show n)
            (momentResult groupBySeq n)
            (momentResult groupByPar n)

-- | Oracle recomputing the six moment sums per group with plain Map folds.
momentOracle :: Int -> Test
momentOracle n =
    TestCase $ do
        let res = momentResult groupBySeq n
            ks = [(i `mod` 17, (i * 3) `mod` 11) | i <- [0 .. n - 1]]
            accMap = foldr step M.empty (reverse ks)
            step k = M.insertWith (\_ g -> g + 1) k (1 :: Int)
            refN = sum (M.elems accMap)
            outN = case DD.getColumn "n" res of
                Just c -> VU.sum (VU.fromList (DI.toList @Int c))
                Nothing -> -1
        assertEqual ("oracle group-row count n=" ++ show n) refN outN

-- | Direct-indexed grouping must give the same per-key aggregates as hashing.
directGroupAggMatches :: Int -> Test
directGroupAggMatches n =
    TestCase $
        let df = caseFrame n
            v = F.int "v"
            aggs =
                [ "vsum" F..= F.sum v
                , "vcount" F..= F.count v
                , "vmin" F..= F.minimum v
                , "vmax" F..= F.maximum v
                ]
            -- 'D.groupBy' takes the direct path; 'groupBySeq' is the hash path.
            directRes = D.aggregate aggs (D.groupBy ["ki"] df)
            hashRes = D.aggregate aggs (groupBySeq ["ki"] df)
         in assertEqual
                ("direct-group agg matches hash by key, n=" ++ show n)
                (keyedAgg hashRes)
                (keyedAgg directRes)

-- | Read an aggregated frame into a Map keyed by group, ignoring group order.
keyedAgg :: DD.DataFrame -> M.Map Int (Int, Int, Int, Int)
keyedAgg res =
    M.fromList
        (zip (col "ki") (zip4 (col "vsum") (col "vcount") (col "vmin") (col "vmax")))
  where
    col name = case DD.getColumn name res of
        Just c -> DI.toList @Int c
        Nothing -> error ("missing column " ++ T.unpack name)
    zip4 (a : as) (b : bs) (c : cs) (d : ds) = (a, b, c, d) : zip4 as bs cs ds
    zip4 _ _ _ _ = []

-- | A packed UTF-8 column, the layout the Parquet reader produces.
packedCol :: [T.Text] -> DI.Column
packedCol ts =
    let bytes = BS.concat (map TE.encodeUtf8 ts)
        offs = scanl (+) 0 (map (BS.length . TE.encodeUtf8) ts)
        Text arr _ _ = TE.decodeUtf8 bytes
     in DI.PackedText Nothing (mkPackedContiguous (arr :: A.Array) (VU.fromList offs))

-- | Skewed packed-text keys with a few hot keys, grouped in parallel.
skewedTextParity :: Bool -> Test
skewedTextParity filtered = TestCase $ do
    let n = 300000
        key i
            | i `mod` 3 == 0 = ""
            | i `mod` 5 == 0 = "hot"
            | i `mod` 7 == 0 = "warm"
            | otherwise = T.pack ("k" ++ show ((i * 7919) `mod` 40000))
        df0 =
            D.fromColumns [("k", packedCol (map key [0 .. n - 1])), mkInt "v" [0 .. n - 1]]
        df = if filtered then D.filter (F.text "k" ./=. "") df0 else df0
    assertBool
        "skewed packed text: par == seq"
        (sameGrouping (groupByPar ["k"] df) (groupBySeq ["k"] df))

tests :: [Test]
tests =
    [ TestLabel ("skewedTextParity " ++ show f) (skewedTextParity f)
    | f <- [False, True]
    ]
        ++ [ TestLabel ("parity " ++ show n ++ " " ++ show keys) (parityFor n keys)
           | n <- sizes
           , keys <- keySets
           ]
        ++ [TestLabel ("aggParity " ++ show n) (aggParityFor n) | n <- [50, 200001]]
        ++ [TestLabel "collisionParity" collisionParity]
        ++ [TestLabel ("momentParity " ++ show n) (momentParity n) | n <- [3, 50, 200001]]
        ++ [ TestLabel ("directGroupAgg " ++ show n) (directGroupAggMatches n)
           | n <- [1, 50, 4999, 200001]
           ]
        ++ [TestLabel ("momentOracle " ++ show n) (momentOracle n) | n <- [50, 200001]]
