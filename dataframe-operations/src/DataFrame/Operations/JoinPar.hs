{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Parallel chunked-probe join kernels. The build side is indexed once into a
shared, read-only 'CompactIndex' (open-addressing, from
"DataFrame.Operations.Join"); the probe side is split into @caps@ /contiguous/
row ranges and probed in parallel by 'forkFinally' workers (no sparks). Each worker
makes two passes over its range — a count pass to size its slice, then a fill
pass — and writes into the single shared output buffers at a precomputed
prefix-sum offset. Because ranges are contiguous and laid out in range order,
the produced @(probeIxs, buildIxs)@ vectors are /bit-for-bit identical/ to the
sequential 'hashInnerKernel' \/ 'hashLeftKernel': probe rows appear in original
order and, within a probe row, build matches in @ciSortedIndices@ order.

This is the parallel==sequential correctness gate (see
@tests/Operations/ParallelJoin.hs@). A sequential fallback is used when there is
a single capability or the probe side is below 'parJoinThreshold'; the caller
('innerJoin' \/ 'leftJoin') decides via 'shouldParallelizeJoin'.
-}
module DataFrame.Operations.JoinPar (
    parInnerProbe,
    parLeftProbe,
    shouldParallelizeJoin,
    shouldParallelizeSmallBuildProbe,
    parJoinThreshold,
    parBuildThreshold,
    parProbeThreshold,
) where

import Control.Concurrent (forkFinally, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)

{- | Below this many probe rows the fork/coordination overhead is not worth it;
the caller uses its sequential 'ST' kernel instead.
-}
parJoinThreshold :: Int
parJoinThreshold = 200000

{- | Below this many build rows the shared 'CompactIndex' is small and hot, so
the sequential hash probe is already memory-bound-fast and the fork overhead
loses (measured: a 1e4-row Text-key build probed by 1e7 rows is /slower/ in
parallel). Parallelism only pays once the build index is large enough to spill
cache — exactly the regime where sort-merge used to be chosen.
-}
parBuildThreshold :: Int
parBuildThreshold = 500000

{- | Whether a join should take the parallel probe path: more than one
capability, a probe side of at least 'parJoinThreshold' rows, and a build side
of at least 'parBuildThreshold' rows (a small/hot index is faster probed
sequentially).
-}
shouldParallelizeJoin :: Int -> Int -> Bool
shouldParallelizeJoin probeRows buildRows =
    probeRows >= parJoinThreshold
        && buildRows >= parBuildThreshold
        && capabilities > 1
{-# NOINLINE shouldParallelizeJoin #-}

{- | Above this many probe rows the probe-side row hashing and table lookups
dominate the join, so partitioning the probe across cores wins even when the
build side is small and cache-resident (the regime 'shouldParallelizeJoin'
deliberately leaves sequential). Sized at 1e6: below it the per-question gain is
swamped by 'forkFinally'/coordination overhead, so small/medium-inner joins stay
sequential (measured). This is the small-build large-probe lever closing the
medium-factor 1e7 join (1e7 probe x ~1e4 build).
-}
parProbeThreshold :: Int
parProbeThreshold = 1000000

{- | Whether a /small-build/ join (build below 'parBuildThreshold', so radix
partitioning / sort-merge is not used) should take the parallel probe path: a
very large probe side (at least 'parProbeThreshold') and more than one
capability. The shared build index is read-only across threads, so probing it in
parallel needs no synchronization. Independent of build size on purpose: the
build is already tiny; the cost is the 1e7-row probe hashing, which parallelizes
cleanly.
-}
shouldParallelizeSmallBuildProbe :: Int -> Bool
shouldParallelizeSmallBuildProbe probeRows =
    probeRows >= parProbeThreshold
        && capabilities > 1
{-# NOINLINE shouldParallelizeSmallBuildProbe #-}

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

{- | A read-only view of the build-side index needed by the probe: the lookup
returns @(start, len)@ of the matching run in @sortedIndices@, or @(-1, 0)@ on a
miss. Passed in by the caller so this module need not depend on the
'CompactIndex' record directly.
-}
data ProbeIndex = ProbeIndex
    { piSorted :: !(VU.Vector Int)
    , piLookup :: !(Int -> (Int, Int))
    }

{- | Parallel inner-join probe. @parInnerProbe sortedIdxs lookup probeHashes@
returns @(probeIxs, buildIxs)@ identical to a sequential probe of the same
index. The build index must already be constructed from the build side.
-}
parInnerProbe ::
    VU.Vector Int ->
    (Int -> (Int, Int)) ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parInnerProbe sortedIdxs lookupFn =
    runProbe False (ProbeIndex sortedIdxs lookupFn)

{- | Parallel left-join probe. Like 'parInnerProbe' but every probe row emits at
least one output row; unmatched rows carry a @-1@ sentinel in the build column.
-}
parLeftProbe ::
    VU.Vector Int ->
    (Int -> (Int, Int)) ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parLeftProbe sortedIdxs lookupFn =
    runProbe True (ProbeIndex sortedIdxs lookupFn)

{- | Shared two-pass parallel probe. @keepUnmatched@ selects left- vs
inner-join semantics. Splits @[0, probeN)@ into @caps@ contiguous ranges, counts
each range's output, prefix-sums to global offsets, then fills the single output
buffers in parallel.
-}
runProbe ::
    Bool ->
    ProbeIndex ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
runProbe keepUnmatched pidx probeHashes = do
    caps <- getNumCapabilities
    let !probeN = VU.length probeHashes
        !nChunks = max 1 (min caps probeN)
        !sorted = piSorted pidx
        !lookupFn = piLookup pidx
        chunkBounds k = (lo, hi)
          where
            !lo = (probeN * k) `div` nChunks
            !hi = (probeN * (k + 1)) `div` nChunks
        -- Count pass: output rows produced by probe range [lo, hi).
        countRange !lo !hi =
            let go !i !acc
                    | i >= hi = acc
                    | otherwise =
                        let (!start, !len) = lookupFn (VU.unsafeIndex probeHashes i)
                         in if start < 0
                                then go (i + 1) (if keepUnmatched then acc + 1 else acc)
                                else go (i + 1) (acc + len)
             in go lo 0
    chunkCounts <- VUM.new (nChunks + 1)
    forkRanges nChunks $ \k ->
        let (lo, hi) = chunkBounds k
         in VUM.unsafeWrite chunkCounts k (countRange lo hi)
    -- Exclusive prefix sum -> per-chunk global start offsets; total at [nChunks].
    let scan !k !acc
            | k > nChunks = pure acc
            | otherwise = do
                c <- if k < nChunks then VUM.unsafeRead chunkCounts k else pure 0
                VUM.unsafeWrite chunkCounts k acc
                scan (k + 1) (acc + c)
    !total <- scan 0 0
    pv <- VUM.unsafeNew (max 1 total)
    bv <- VUM.unsafeNew (max 1 total)
    -- Fill pass: each chunk writes from its prefix-sum offset.
    offs <- VU.unsafeFreeze chunkCounts
    forkRanges nChunks $ \k -> do
        let (lo, hi) = chunkBounds k
            !base = VU.unsafeIndex offs k
            fill !i !p
                | i >= hi = pure ()
                | otherwise = do
                    let (!start, !len) = lookupFn (VU.unsafeIndex probeHashes i)
                    if start < 0
                        then
                            if keepUnmatched
                                then do
                                    VUM.unsafeWrite pv p i
                                    VUM.unsafeWrite bv p (-1)
                                    fill (i + 1) (p + 1)
                                else fill (i + 1) p
                        else do
                            let writeMatch !j !q
                                    | j >= len = pure ()
                                    | otherwise = do
                                        VUM.unsafeWrite pv q i
                                        VUM.unsafeWrite bv q (VU.unsafeIndex sorted (start + j))
                                        writeMatch (j + 1) (q + 1)
                            writeMatch 0 p
                            fill (i + 1) (p + len)
        fill lo base
    pf <- VU.unsafeFreeze (VUM.slice 0 total pv)
    bf <- VU.unsafeFreeze (VUM.slice 0 total bv)
    pure (pf, bf)

{- | Run @body k@ for @k@ in @[0, nChunks)@, one chunk per task, on @nChunks@
forked threads; rethrow the first failure. Chunk @k@ is owned by exactly one
thread, so concurrent writes to disjoint output regions are race-free.
-}
forkRanges :: Int -> (Int -> IO ()) -> IO ()
forkRanges nChunks body = do
    vars <- mapM spawn [0 .. nChunks - 1]
    results <- mapM takeMVar vars
    mapM_ (either (throwIO :: SomeException -> IO ()) pure) results
  where
    spawn k = do
        var <- newEmptyMVar
        _ <- forkFinally (body k) (putMVar var)
        pure var
