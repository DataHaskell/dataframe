{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Operations.CountDistinct where

import qualified Data.ByteString as BS
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Text.Encoding as TE
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI

import Data.Bits (shiftR, xor)
import Data.Function ((&))
import Data.Int (Int32, Int64)
import Data.Text.Internal (Text (Text))
import DataFrame.Expression.Operators (as)
import DataFrame.Internal.Aggregation.Kernel.Distinct (
    countDistinctColumn,
    countDistinctGroups,
 )
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)
import Test.HUnit

-- | Deterministic pseudo-random stream (splitmix-style).
stream :: Int -> Int -> [Int]
stream seed n = take n (drop 1 (map mix (iterate (+ (-7046029254386353131)) seed)))
  where
    mix z0 =
        let z1 = (z0 `xor` (z0 `shiftR` 30)) * (-4658895280553007687)
            z2 = (z1 `xor` (z1 `shiftR` 27)) * (-7723592293110705685)
         in z2 `xor` (z2 `shiftR` 31)

-- | Values in @[0, range)@.
draws :: Int -> Int -> Int -> [Int]
draws seed range n = map (`mod` range) (stream seed n)

refGlobal :: (Ord a) => [a] -> Int
refGlobal = S.size . S.fromList

refGrouped :: (Ord a) => [Int] -> [a] -> Int -> [Int]
refGrouped gs xs nGroups =
    let m = M.fromListWith S.union [(g, S.singleton x) | (g, x) <- zip gs xs]
     in [maybe 0 S.size (M.lookup g m) | g <- [0 .. nGroups - 1]]

-- | Packed UTF-8 column of the given texts.
packed :: [T.Text] -> DI.Column
packed ts =
    let bytes = BS.concat (map TE.encodeUtf8 ts)
        offs = scanl (+) 0 (map (BS.length . TE.encodeUtf8) ts)
        Text arr _ _ = TE.decodeUtf8 bytes
     in DI.PackedText Nothing (mkPackedContiguous (arr :: A.Array) (VU.fromList offs))

word :: Int -> T.Text
word k = "w" <> T.pack (show k)

globalCase :: String -> DI.Column -> Int -> Test
globalCase label col expected =
    TestCase (assertEqual label (Just expected) (countDistinctColumn col))

groupedCase :: String -> [Int] -> Int -> DI.Column -> [Int] -> Test
groupedCase label gs nGroups col expected =
    TestCase
        ( assertEqual
            label
            (Just (VU.fromList expected))
            (countDistinctGroups (VU.fromList gs) nGroups col)
        )

-- Sizes straddle the parallel threshold and force table growth.
sizes :: [Int]
sizes = [0, 1, 17, 5000, 300000]

intGlobal :: [Test]
intGlobal =
    [ globalCase
        ("Int n=" ++ show n ++ " r=" ++ show r)
        (DI.fromList xs)
        (refGlobal xs)
    | n <- sizes
    , r <- [3, n `div` 3 + 1, maxBound]
    , let xs = draws n r n
    ]

-- 'minBound' is the kernel's empty-slot marker, so it must still count once.
intSentinel :: Test
intSentinel =
    let xs = take 300000 (cycle [minBound, 0, -1, maxBound, minBound, 7 :: Int])
     in globalCase "Int with minBound" (DI.fromList xs) (refGlobal xs)

narrowInts :: [Test]
narrowInts =
    [ let xs = map fromIntegral (draws 1 100000 250000) :: [Int64]
       in globalCase "Int64" (DI.fromList xs) (refGlobal xs)
    , let xs = map fromIntegral (draws 2 100000 250000) :: [Int32]
       in globalCase "Int32" (DI.fromList xs) (refGlobal xs)
    ]

doubles :: Test
doubles =
    let xs =
            [-0.0, 0.0, 1.5, 1.5, -2.25, 1 / 0, -(1 / 0)]
                ++ map fromIntegral (draws 3 50 1000) ::
                [Double]
     in globalCase "Double, -0.0 joins 0.0" (DI.fromList xs) (refGlobal xs)

texts :: [Test]
texts =
    [ globalCase ("Text boxed n=" ++ show n) (DI.fromList ts) (refGlobal ts)
    | n <- sizes
    , let ts = map word (draws 4 (n `div` 4 + 1) n)
    ]
        ++ [ globalCase ("Text packed n=" ++ show n) (packed ts) (refGlobal ts)
           | n <- sizes
           , let ts = map word (draws 5 (n `div` 4 + 1) n) ++ ["", "", "é", "é"]
           ]

grouped :: [Test]
grouped =
    [ groupedCase
        ("Int grouped n=" ++ show n ++ " g=" ++ show ng)
        gs
        ng
        (DI.fromList xs)
        (refGrouped gs xs ng)
    | n <- sizes
    , ng <- [1, 7, 1000]
    , let gs = draws (n + 11) ng n
    , let xs = draws (n + 13) (n `div` 2 + 1) n
    ]
        ++ [ groupedCase "Text grouped" gs 50 (packed ts) (refGrouped gs ts 50)
           | let n = 300000
           , let gs = draws 21 50 n
           , let ts = map word (draws 22 20000 n)
           ]
        -- Enough groups to take the shared atomic-count path.
        ++ [ groupedCase
                "Int grouped, wide domain"
                gs
                ng
                (DI.fromList xs)
                (refGrouped gs xs ng)
           | let n = 1500000
           , let ng = 600000
           , let gs = draws 31 ng n
           , let xs = draws 32 4 n
           ]
        -- A group id absent from rowToGroup reports zero.
        ++ [groupedCase "empty group" [0, 0, 2] 3 (DI.fromList [5, 5, 6 :: Int]) [1, 0, 1]]

-- The public API: aggregate after groupBy dispatches to the kernel.
viaAggregate :: Test
viaAggregate =
    let n = 250000
        gs = draws 41 37 n
        xs = draws 42 5000 n
        df = D.fromColumns [("g", DI.fromList gs), ("x", DI.fromList xs)]
        expected =
            D.fromColumns
                [ ("g", DI.fromList [0 .. 36 :: Int])
                , ("u", DI.fromList (refGrouped gs xs 37))
                ]
     in TestCase
            ( assertEqual
                "aggregate countDistinct"
                expected
                ( df
                    & D.groupBy ["g"]
                    & D.aggregate [F.countDistinct (F.int "x") `as` "u"]
                    & D.sortBy [D.Asc (F.int "g")]
                )
            )

-- An Int32 key takes the direct grouping path and must match the reference.
viaAggregateInt32Key :: Test
viaAggregateInt32Key =
    let n = 250000
        gs = draws 43 37 n
        xs = draws 44 5000 n
        df =
            D.fromColumns
                [("g", DI.fromList (map fromIntegral gs :: [Int32])), ("x", DI.fromList xs)]
        expected =
            D.fromColumns
                [ ("g", DI.fromList [0 .. 36 :: Int32])
                , ("u", DI.fromList (refGrouped gs xs 37))
                ]
     in TestCase
            ( assertEqual
                "aggregate countDistinct, Int32 key"
                expected
                ( df
                    & D.groupBy ["g"]
                    & D.aggregate [F.countDistinct (F.int "x") `as` "u"]
                    & D.sortBy [D.Asc (F.int32 "g")]
                )
            )

tests :: [Test]
tests =
    map
        (TestLabel "countDistinct")
        ( intGlobal
            ++ [intSentinel, doubles, viaAggregate, viaAggregateInt32Key]
            ++ narrowInts
            ++ texts
            ++ grouped
        )
