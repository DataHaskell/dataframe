{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Low-cardinality direct-indexed grouping fast path: when the key is a single
clean unboxed @Int@ column of small value range, the value itself indexes a dense
accumulator (no hashing/probing). Emits groups in ascending value order.
-}
module DataFrame.Internal.Grouping.Direct (
    directGroupThreshold,
    tryDirectGroupColumn,
    DirectGrouping (..),
) where

import Control.Concurrent (forkFinally, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Column (..))

{- | Largest key value RANGE (max - min + 1) the direct grouping path accepts. A
@2^20@-slot histogram is 8MB; the low-cardinality questions sit far below it
(id4 range 100, id6 range 1e5). Wider ranges fall back to the hash group-by.
-}
directGroupThreshold :: Int
directGroupThreshold = 1048576

{- | The grouping layout the hash path also produces: @rowToGroup@, the
group-sorted @valueIndices@, the @offsets@ prefix array, and the group count.
-}
data DirectGrouping = DirectGrouping
    { dgRowToGroup :: !(VU.Vector Int)
    , dgValueIndices :: !(VU.Vector Int)
    , dgOffsets :: !(VU.Vector Int)
    , dgNGroups :: !Int
    }

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

parThreshold :: Int
parThreshold = 200000

{- | Take the direct path if the (single) key column is a clean non-null unboxed
@Int@ column with a small value range. Returns 'Nothing' to fall back to the
hash group-by on anything else (boxed/text keys, nullable, wide ranges, empty).
-}
tryDirectGroupColumn :: Column -> Maybe DirectGrouping
tryDirectGroupColumn (UnboxedColumn Nothing (v :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int)
    , not (VU.null v) =
        let (!mn, !mx) = rangeOf v
            !range = mx - mn + 1
         in if range >= 1 && range <= directGroupThreshold
                then Just (directGroup v mn range)
                else Nothing
tryDirectGroupColumn _ = Nothing

-- | Parallel min/max reduce (order-independent).
rangeOf :: VU.Vector Int -> (Int, Int)
rangeOf v
    | not (shouldPar n) = rangeChunk v 0 n
    | otherwise = unsafePerformIO $ do
        let !caps = capabilities
            !per = (n + caps - 1) `div` caps
            spawn w = do
                var <- newEmptyMVar
                let !lo = min n (w * per)
                    !hi = min n (lo + per)
                _ <- forkFinally (pure $! rangeChunk v lo hi) (putMVar var)
                pure var
        vars <- mapM spawn [0 .. caps - 1]
        rs <- mapM takeMVar vars
        rs' <- mapM (either (throwIO @SomeException) pure) rs
        pure (combineRanges (filter (\(a, _) -> a /= maxBound) rs'))
  where
    !n = VU.length v
{-# NOINLINE rangeOf #-}

rangeChunk :: VU.Vector Int -> Int -> Int -> (Int, Int)
rangeChunk v lo hi = go lo maxBound minBound
  where
    go !i !mn !mx
        | i >= hi = (mn, mx)
        | otherwise =
            let !x = VU.unsafeIndex v i
             in go (i + 1) (min mn x) (max mx x)

combineRanges :: [(Int, Int)] -> (Int, Int)
combineRanges [] = (0, 0)
combineRanges ((a0, b0) : rest) = foldr (\(a, b) (ma, mb) -> (min ma a, max mb b)) (a0, b0) rest

shouldPar :: Int -> Bool
shouldPar n = n >= parThreshold && capabilities > 1

{- | Build the grouping by counting sort on @value - min@: a (parallel) per-value
histogram, compaction of non-empty values into ascending dense ids, a scan into
offsets, then a stable placement pass building @valueIndices@ and @rowToGroup@.
-}
directGroup :: VU.Vector Int -> Int -> Int -> DirectGrouping
directGroup v mn range = unsafePerformIO $ do
    let !n = VU.length v
    hist <- buildHistogram v mn range n
    valToGroup <- VUM.replicate range (-1 :: Int)
    grpCount <- VUM.new range
    nGroups <- compact hist range valToGroup grpCount
    offsM <- VUM.new (nGroups + 1)
    cursor <- VUM.new nGroups
    scanOffsets grpCount nGroups offsM cursor
    rtg <- VUM.new n
    vis <- VUM.new n
    place v mn n valToGroup cursor rtg vis
    frozenRtg <- VU.unsafeFreeze rtg
    frozenVis <- VU.unsafeFreeze vis
    frozenOffs <- VU.unsafeFreeze offsM
    pure (DirectGrouping frozenRtg frozenVis frozenOffs nGroups)
{-# NOINLINE directGroup #-}

{- | Parallel per-value histogram: each worker fills a private @range@-slot
count over its row chunk, then the partials are summed (exact integers, so the
merge order is irrelevant). Sequential single pass below 'parThreshold'.
-}
buildHistogram :: VU.Vector Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
buildHistogram v mn range n
    | not (shouldPar n) = histChunk v mn range 0 n
    | otherwise = do
        let !caps = capabilities
            !per = (n + caps - 1) `div` caps
            spawn w = do
                var <- newEmptyMVar
                let !lo = min n (w * per)
                    !hi = min n (lo + per)
                _ <- forkFinally (histChunk v mn range lo hi) (putMVar var)
                pure var
        vars <- mapM spawn [0 .. caps - 1]
        rs <- mapM takeMVar vars
        parts <- mapM (either (throwIO @SomeException) pure) rs
        case parts of
            [] -> VUM.replicate range 0
            (p0 : rest) -> do
                mapM_ (addInto p0 range) rest
                pure p0

histChunk :: VU.Vector Int -> Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
histChunk v mn range lo hi = do
    acc <- VUM.replicate range (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex v i - mn
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + 1)
                go (i + 1)
    go lo
    pure acc

addInto :: VUM.IOVector Int -> Int -> VUM.IOVector Int -> IO ()
addInto dst range src = go 0
  where
    go !k
        | k >= range = pure ()
        | otherwise = do
            a <- VUM.unsafeRead dst k
            b <- VUM.unsafeRead src k
            VUM.unsafeWrite dst k (a + b)
            go (k + 1)

{- | Walk the histogram in ascending value order, assigning a dense group id to
each non-empty value and copying its count into @grpCount@ at that id. Returns
the group count.
-}
compact ::
    VUM.IOVector Int -> Int -> VUM.IOVector Int -> VUM.IOVector Int -> IO Int
compact hist range valToGroup grpCount = go 0 0
  where
    go !val !next
        | val >= range = pure next
        | otherwise = do
            c <- VUM.unsafeRead hist val
            if c == 0
                then go (val + 1) next
                else do
                    VUM.unsafeWrite valToGroup val next
                    VUM.unsafeWrite grpCount next c
                    go (val + 1) (next + 1)

{- | Exclusive prefix scan of group counts into @offsM@ (length nGroups+1) and
seed the per-group write @cursor@ at each group's start offset.
-}
scanOffsets ::
    VUM.IOVector Int -> Int -> VUM.IOVector Int -> VUM.IOVector Int -> IO ()
scanOffsets grpCount nGroups offsM cursor = go 0 0
  where
    go !g !acc
        | g >= nGroups = VUM.unsafeWrite offsM nGroups acc
        | otherwise = do
            VUM.unsafeWrite offsM g acc
            VUM.unsafeWrite cursor g acc
            c <- VUM.unsafeRead grpCount g
            go (g + 1) (acc + c)

{- | Stable placement pass: for each row in original order, look up its group id
through the value map, write @rowToGroup@, and append the row to its group's run
in @valueIndices@ via the advancing cursor (rows keep original order per group).
-}
place ::
    VU.Vector Int ->
    Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
place v mn n valToGroup cursor rtg vis = go 0
  where
    go !i
        | i >= n = pure ()
        | otherwise = do
            let !val = VU.unsafeIndex v i - mn
            g <- VUM.unsafeRead valToGroup val
            VUM.unsafeWrite rtg i g
            pos <- VUM.unsafeRead cursor g
            VUM.unsafeWrite vis pos i
            VUM.unsafeWrite cursor g (pos + 1)
            go (i + 1)
