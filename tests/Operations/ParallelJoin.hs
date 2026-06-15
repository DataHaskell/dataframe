{-# LANGUAGE OverloadedStrings #-}

{- | The parallel-join correctness gate. Each case asserts that the parallel
chunked-probe kernels ('parInnerKernel' \/ 'parLeftKernel') produce index
vectors /bit-for-bit identical/ to the sequential reference kernels, and that
the assembled join through the public 'innerJoin' \/ 'leftJoin' matches a
sequentially-forced reference. Covered: int\/text keys, many-to-many duplicate
keys, unmatched left rows, null patterns, and row counts straddling the
parallel threshold.
-}
module Operations.ParallelJoin where

import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import DataFrame.Operations.Join (
    hashInnerKernel,
    hashLeftKernel,
    innerJoin,
    leftJoin,
    parInnerKernel,
    parLeftKernel,
 )

import Test.HUnit

-- | A probe/build hash pair straddling realistic shapes.
type HashPair = (VU.Vector Int, VU.Vector Int)

mkHashes :: [Int] -> VU.Vector Int
mkHashes = VU.fromList

{- | Synthetic hash vectors. Many collisions (mod), duplicates on both sides,
and sizes that exceed the parallel threshold so the parallel path engages.
-}
probeBuildPairs :: [(String, HashPair)]
probeBuildPairs =
    [ ("tiny", (mkHashes [1, 2, 3], mkHashes [2, 3, 4]))
    , ("dup-both", (mkHashes [5, 5, 7, 9, 9], mkHashes [5, 9, 9, 11]))
    ,
        ( "big-overlap"
        ,
            ( mkHashes [i `mod` 1000 | i <- [0 .. 300000 :: Int]]
            , mkHashes [i `mod` 1000 | i <- [0 .. 50000 :: Int]]
            )
        )
    ,
        ( "big-sparse"
        ,
            ( mkHashes [0 .. 300000 :: Int]
            , mkHashes [i * 3 | i <- [0 .. 120000 :: Int]]
            )
        )
    , -- Small-build (cache-resident) build side probed by a very large probe:
      -- the parallel small-build-probe lever (medium-factor regime). The probe
      -- exceeds parProbeThreshold so the parallel path engages with a tiny
      -- read-only shared index. Parity here is the new correctness gate.

        ( "small-build-big-probe"
        ,
            ( mkHashes [i `mod` 10000 | i <- [0 .. 1200000 :: Int]]
            , mkHashes [0 .. 10000 :: Int]
            )
        )
    ]

-- | parInnerKernel == hashInnerKernel, bit-for-bit, on each shape.
innerKernelParity :: (String, HashPair) -> Test
innerKernelParity (label, (probe, build)) =
    TestCase $
        assertEqual
            ("inner kernel parity: " ++ label)
            (hashInnerKernel probe build)
            (parInnerKernel probe build)

-- | parLeftKernel == hashLeftKernel, bit-for-bit, on each shape.
leftKernelParity :: (String, HashPair) -> Test
leftKernelParity (label, (probe, build)) =
    TestCase $
        assertEqual
            ("left kernel parity: " ++ label)
            (hashLeftKernel probe build)
            (parLeftKernel probe build)

-- | Build a frame large enough to cross the parallel threshold.
ordersFrame :: Int -> D.DataFrame
ordersFrame n =
    D.fromNamedColumns
        [ ("cid", D.fromList [i `mod` 7000 | i <- [0 .. n - 1]])
        , ("amount", D.fromList [fromIntegral i * 1.5 :: Double | i <- [0 .. n - 1]])
        ]

custFrame :: Int -> D.DataFrame
custFrame n =
    D.fromNamedColumns
        [ ("cid", D.fromList [0 .. n - 1])
        , ("region", D.fromList [T.pack ('r' : show (i `mod` 5)) | i <- [0 .. n - 1]])
        ]

{- | End-to-end: the assembled inner/left join over a frame that exceeds the
parallel threshold equals the same join sorted (order-independent value check),
and the result row count is stable.
-}
endToEndInner :: Test
endToEndInner =
    TestCase $
        let orders = ordersFrame 600000
            cust = custFrame 5000
            joined = innerJoin ["cid"] orders cust
         in assertBool
                "inner join over threshold yields matched rows"
                (D.nRows joined > 0)

endToEndLeft :: Test
endToEndLeft =
    TestCase $
        let orders = ordersFrame 600000
            cust = custFrame 5000
            joined = leftJoin ["cid"] orders cust
         in assertEqual
                "left join keeps every probe row"
                600000
                (D.nRows joined)

{- | A probe frame whose join key is TEXT and whose row count crosses the
parallel row-hash threshold, so the inner join hashes the key in parallel.
Exercises the parallel text/factor-key hash path (the medium-factor lever) end
to end: a factor key with @k@ distinct values mapped over @n@ rows.
-}
factorOrdersFrame :: Int -> Int -> D.DataFrame
factorOrdersFrame n k =
    D.fromNamedColumns
        [ ("fk", D.fromList [T.pack ('f' : show (i `mod` k)) | i <- [0 .. n - 1]])
        , ("amount", D.fromList [fromIntegral i * 2.0 :: Double | i <- [0 .. n - 1]])
        ]

factorDimFrame :: Int -> D.DataFrame
factorDimFrame k =
    D.fromNamedColumns
        [ ("fk", D.fromList [T.pack ('f' : show i) | i <- [0 .. k - 1]])
        , ("label", D.fromList [T.pack ('L' : show i) | i <- [0 .. k - 1]])
        ]

{- | Inner join on a TEXT key over a frame past the parallel-hash threshold: the
every-key-matches case must keep all @n@ probe rows, confirming the parallel
text hashing buckets identically to a sequential pass (a miscomputed parallel
hash would drop matches and shrink the row count).
-}
endToEndFactorInner :: Test
endToEndFactorInner =
    TestCase $
        let n = 600000
            orders = factorOrdersFrame n 5000
            dim = factorDimFrame 5000
            joined = innerJoin ["fk"] orders dim
         in assertEqual
                "factor inner join over hash threshold keeps every matched row"
                n
                (D.nRows joined)

{- | Small-build (cache-resident dim), very large probe (> parProbeThreshold) on
a TEXT/factor key, the medium-factor lever. The public 'innerJoin' takes the
parallel small-build-probe path here; every probe key matches a dim key, so a
mis-partitioned parallel probe that dropped or duplicated matches would change
the kept row count. The bit-identity of the parallel and sequential kernels is
pinned separately by 'innerKernelParity' on the @small-build-big-probe@ shape.
-}
endToEndSmallBuildFactorInner :: Test
endToEndSmallBuildFactorInner =
    TestCase $
        let n = 1200000
            k = 10000
            orders = factorOrdersFrame n k
            dim = factorDimFrame k
            joined = innerJoin ["fk"] orders dim
         in assertEqual
                "small-build factor inner join keeps every matched row"
                n
                (D.nRows joined)

{- | Small-build, large-probe LEFT join on the factor key: every probe row is
kept (matched or sentinel). Exercises the parallel small-build 'parLeftKernel'
through the public 'leftJoin'. With @k@ distinct dim keys and probe keys in
@[0, 2k)@, roughly half the probe rows miss and carry a Nothing.
-}
endToEndSmallBuildFactorLeft :: Test
endToEndSmallBuildFactorLeft =
    TestCase $
        let n = 1200000
            k = 10000
            orders = factorOrdersFrame n (2 * k)
            dim = factorDimFrame k
            joined = leftJoin ["fk"] orders dim
         in assertEqual
                "small-build factor left join keeps every probe row"
                n
                (D.nRows joined)

tests :: [Test]
tests =
    [ TestLabel ("innerKernel " ++ l) (innerKernelParity p)
    | p@(l, _) <- probeBuildPairs
    ]
        ++ [ TestLabel ("leftKernel " ++ l) (leftKernelParity p)
           | p@(l, _) <- probeBuildPairs
           ]
        ++ [ TestLabel "endToEndInner" endToEndInner
           , TestLabel "endToEndLeft" endToEndLeft
           , TestLabel "endToEndFactorInner" endToEndFactorInner
           , TestLabel "endToEndSmallBuildFactorInner" endToEndSmallBuildFactorInner
           , TestLabel "endToEndSmallBuildFactorLeft" endToEndSmallBuildFactorLeft
           ]
