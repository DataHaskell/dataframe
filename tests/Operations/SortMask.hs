{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The radix sort and parallel mask paths agree with simple references.
module Operations.SortMask where

import Data.Bits (shiftR, xor)
import Data.Function ((&))
import Data.Int (Int32)
import qualified Data.List as L
import Data.Ord (Down (..), comparing)
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (maskIndices)
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DD
import Test.HUnit

-- | Deterministic pseudo-random values in @[0, range)@.
draws :: Int -> Int -> Int -> [Int]
draws seed range n = [mix (seed * 1000003 + i) `mod` range | i <- [1 .. n]]
  where
    mix z0 =
        let z1 = (z0 `xor` (z0 `shiftR` 30)) * (-4658895280553007687)
            z2 = (z1 `xor` (z1 `shiftR` 27)) * (-7723592293110705685)
         in abs (z2 `xor` (z2 `shiftR` 31))

-- | Row order of a stable sort, the order 'D.sortBy' must reproduce.
stableOrder :: (Ord k) => (a -> k) -> [a] -> [Int]
stableOrder key xs = map fst (L.sortBy (comparing (key . snd)) (zip [0 ..] xs))

sortedV :: D.DataFrame -> [Int]
sortedV df = maybe [] (DI.toList @Int) (DD.getColumn "v" df)

-- Many ties and negative keys, so stability and sign handling both show.
intSort :: Bool -> Test
intSort desc = TestCase $ do
    let n = 300000
        ks = map (subtract 500) (draws 7 1000 n)
        df = D.fromColumns [("k", DI.fromList ks), ("v", DI.fromList [0 .. n - 1 :: Int])]
        ord = if desc then D.Desc (F.int "k") else D.Asc (F.int "k")
        expected = if desc then stableOrder Down ks else stableOrder id ks
    assertEqual
        ("Int sort desc=" ++ show desc)
        expected
        (sortedV (df & D.sortBy [ord]))

narrowSort :: Test
narrowSort = TestCase $ do
    let ks = map (fromIntegral . subtract 50) (draws 8 100 5000) :: [Int32]
        df = D.fromColumns [("k", DI.fromList ks), ("v", DI.fromList [0 .. 4999 :: Int])]
    assertEqual
        "Int32 sort"
        (stableOrder id ks)
        (sortedV (df & D.sortBy [D.Asc (F.int32 "k")]))

-- -0.0 and 0.0 compare equal, so they keep row order.
doubleSort :: Test
doubleSort = TestCase $ do
    let ks =
            [0.0, -0.0, 1.5, -2.25, 1 / 0, -(1 / 0), 0.0, 1.5, -1.0e-300, 1.0e-300] ::
                [Double]
        df =
            D.fromColumns [("k", DI.fromList ks), ("v", DI.fromList [0 .. length ks - 1])]
    assertEqual
        "Double sort"
        (stableOrder id ks)
        (sortedV (df & D.sortBy [D.Asc (F.double "k")]))
    assertEqual
        "Double sort desc"
        (stableOrder Down ks)
        (sortedV (df & D.sortBy [D.Desc (F.double "k")]))

-- A NaN column keeps the comparison sort; this only checks it still sorts.
nanSort :: Test
nanSort = TestCase $ do
    let ks = [2, 0 / 0, 1, 3] :: [Double]
        df = D.fromColumns [("k", DI.fromList ks), ("v", DI.fromList [0 .. 3 :: Int])]
    assertEqual
        "NaN sort length"
        4
        (length (sortedV (df & D.sortBy [D.Asc (F.double "k")])))

maskCases :: [Test]
maskCases =
    [ TestCase $
        let bs = map ((< t) . (`mod` 10)) (draws n 10 n)
         in assertEqual
                ("maskIndices n=" ++ show n ++ " t=" ++ show t)
                (VU.fromList [i | (i, True) <- zip [0 ..] bs])
                (maskIndices (VU.fromList bs))
    | n <- [0, 1, 1000, 300001]
    , t <- [0, 3, 10]
    ]

tests :: [Test]
tests =
    map
        (TestLabel "sortMask")
        ([intSort False, intSort True, narrowSort, doubleSort, nanSort] ++ maskCases)
