{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeApplications #-}

{- | Complexity regression for 'DI.concatManyColumns', the batch collector
behind the lazy executor's @collectStream@. It exists to concatenate every
chunk in one pass rather than folding @acc <> batch@, which would recopy the
accumulator on every step.

Allocation is deterministic where wall-clock is not, so its growth rate is a
stable stand-in for the complexity.
-}
module Internal.ColumnConcat (tests) where

import qualified Data.Vector as VB
import qualified DataFrame.Internal.Column as DI

import GHC.Conc.Sync (getAllocationCounter)
import Test.HUnit

-- | Nullable, so the concat has a bitmap to build as well as data to copy.
chunk :: DI.Column
chunk = DI.fromList (map Just [1 .. 250 :: Int])

-- | Bytes allocated forcing a concat of @k@ chunks, bitmap included.
allocationFor :: Int -> IO Integer
allocationFor k = do
    before <- getAllocationCounter
    let !n = DI.numElements (DI.concatManyColumns (replicate k chunk))
    after <- n `seq` getAllocationCounter
    pure (fromIntegral (before - after))

{- | The values and the null positions have to survive the concat, not just its
allocation profile. Chunks are deliberately uneven and land off byte boundaries
so a bitmap written at the wrong offset shows up.
-}
unevenChunks :: [DI.Column]
unevenChunks =
    [ nullableChunk 3 [1]
    , nullableChunk 8 [0, 7]
    , nullableChunk 5 []
    , nullableChunk 1 [0]
    , nullableChunk 11 [2, 10]
    ]

-- | @nullableChunk n nulls@ has values 0..n-1 with @nulls@ marked missing.
nullableChunk :: Int -> [Int] -> DI.Column
nullableChunk n nulls =
    DI.fromVector
        (VB.generate n (\i -> if i `elem` nulls then Nothing else Just i))

concatManyColumnsMatchesChunks :: Test
concatManyColumnsMatchesChunks =
    TestCase
        ( assertEqual
            "concat preserves every value and null position"
            (concat [DI.toList @(Maybe Int) c | c <- unevenChunks])
            (DI.toList @(Maybe Int) (DI.concatManyColumns unevenChunks))
        )

{- | Quadrupling the chunk count quadruples the rows, so a single-pass concat
allocates about 4x as much. Recopying the accumulator per chunk allocates about
16x; the bound sits between the two.
-}
concatManyColumnsAllocatesLinearly :: Test
concatManyColumnsAllocatesLinearly = TestCase $ do
    small <- allocationFor 40
    large <- allocationFor 160
    assertBool
        ( "allocation grew "
            ++ show (large `div` max 1 small)
            ++ "x for 4x the rows"
        )
        (large <= 8 * small)

tests :: [Test]
tests =
    [ TestLabel
        "concatManyColumns preserves values and nulls"
        concatManyColumnsMatchesChunks
    , TestLabel
        "concatManyColumns allocates linearly"
        concatManyColumnsAllocatesLinearly
    ]
