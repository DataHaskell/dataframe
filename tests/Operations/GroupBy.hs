{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.GroupBy where

import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as D

import Assertions
import Test.HUnit

values :: [(T.Text, DI.Column)]
values =
    [ ("test1", DI.fromList (concatMap (replicate 10) [1 :: Int, 2, 3, 4]))
    , ("test2", DI.fromList (take 40 $ cycle [1 :: Int, 2]))
    , ("test3", DI.fromList [(1 :: Int) .. 40])
    , ("test4", DI.fromList (reverse [(1 :: Int) .. 40]))
    ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

groupBySingleRowWAI :: Test
groupBySingleRowWAI =
    TestCase
        ( assertEqual
            "Groups by single column"
            -- We don't yet compare offsets and indices
            (D.Grouped testData ["test1"] VU.empty VU.empty VU.empty)
            (D.groupBy ["test1"] testData)
        )

groupByMultipleRowsWAI :: Test
groupByMultipleRowsWAI =
    TestCase
        ( assertEqual
            "Groups by single column"
            -- We don't yet compare offsets and indices
            (D.Grouped testData ["test1", "test2"] VU.empty VU.empty VU.empty)
            (D.groupBy ["test1", "test2"] testData)
        )

groupByColumnDoesNotExist :: Test
groupByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnsNotFound ["test0"] "groupBy" (D.columnNames testData))
            (print $ D.groupBy ["test0"] testData)
        )

{- | The grouping invariant every backend (sequential now, parallel in Phase 2)
must satisfy: 'valueIndices' / 'offsets' / 'rowToGroup' agree, groups partition
all rows exactly once, each group is exactly the rows sharing a key tuple, and
no two groups share a key. Checked on a frame with several keys colliding under
the hash (forces the open-addressing key re-verification path).
-}
groupingInvariantHolds :: Test
groupingInvariantHolds =
    TestCase
        (assertBool "grouping partitions rows by exact key tuple" (checkGrouping df))
  where
    -- Repeated, interleaved keys across two columns; the dense int grid is the
    -- classic hash-collision stressor.
    df =
        D.fromNamedColumns
            [ ("k1", DI.fromList (map (`mod` 5) [0 .. 199 :: Int]))
            , ("k2", DI.fromList (map (\i -> (i * 7) `mod` 3) [0 .. 199 :: Int]))
            , ("v", DI.fromList [0 .. 199 :: Int])
            ]

{- | Recompute the reference grouping by the key tuple and assert the library's
'valueIndices'/'offsets'/'rowToGroup' describe the same partition.
-}
checkGrouping :: D.DataFrame -> Bool
checkGrouping df =
    let g = D.groupBy ["k1", "k2"] df
        vis = VU.toList (D.valueIndices g)
        os = VU.toList (D.offsets g)
        rtg = VU.toList (D.rowToGroup g)
        n = fst (D.dimensions df)
        k1 = colInts "k1"
        k2 = colInts "k2"
        colInts name = case D.getColumn name df of
            Just c -> DI.toList @Int c
            Nothing -> error "missing column"
        key i = (k1 !! i, k2 !! i)
        -- The group slices read out of vis/os.
        slices = [take (e - s) (drop s vis) | (s, e) <- zip os (tail os)]
        -- valueIndices must be a permutation of all rows.
        permutationOk = L.sort vis == [0 .. n - 1]
        -- Every slice is non-empty and internally key-constant.
        constKeys = all (\sl -> not (L.null sl) && allSame (map key sl)) slices
        -- Distinct groups have distinct keys.
        groupKeys = map (key . head) slices
        distinctKeys = L.length (L.nub groupKeys) == L.length groupKeys
        -- The number of groups equals the number of distinct key tuples.
        nGroupsOk = L.length slices == M.size (M.fromList [(key i, ()) | i <- [0 .. n - 1]])
        -- rowToGroup agrees with the slice each row lands in.
        rtgOk =
            and
                [ (rtg !! r) == gIdx
                | (gIdx, sl) <- zip [0 ..] slices
                , r <- sl
                ]
     in permutationOk && constKeys && distinctKeys && nGroupsOk && rtgOk

allSame :: (Eq a) => [a] -> Bool
allSame [] = True
allSame (x : xs) = all (== x) xs

tests :: [Test]
tests =
    [ TestLabel "groupBySingleRowWAI" groupBySingleRowWAI
    , TestLabel "groupByMultipleRowsWAI" groupByMultipleRowsWAI
    , TestLabel "groupByColumnDoesNotExist" groupByColumnDoesNotExist
    , TestLabel "groupingInvariantHolds" groupingInvariantHolds
    ]
