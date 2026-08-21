{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Parallel chunked-probe join kernels. The build side is indexed once into a
shared, read-only 'CompactIndex' (open-addressing, from
"DataFrame.Operations.Join"); the probe side is split into @caps@ /contiguous/
row ranges and probed in parallel by 'forkIO' workers (no sparks). Each worker
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
    ProbeTable (..),
    parInnerProbe,
    parLeftProbe,
    shouldParallelizeJoin,
    shouldParallelizeSmallBuildProbe,
    parJoinThreshold,
    parBuildThreshold,
    parProbeThreshold,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Data.Bits (popCount, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
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
swamped by 'forkIO'/coordination overhead, so small/medium-inner joins stay
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

{- | A read-only view of the build-side index needed by the probe: the raw
open-addressing table vectors of the @CompactIndex@ (which lives in
"DataFrame.Operations.Join"; passing the fields avoids an import cycle).
Passing concrete vectors instead of a lookup closure keeps the per-row probe
loop free of unknown calls and boxed-tuple allocation — the lookup is inlined
into the count and fill loops.
-}
data ProbeTable = ProbeTable
    { ptSorted :: !(VU.Vector Int)
    , ptKeys :: !(VU.Vector Int)
    , ptRuns :: !(VU.Vector Int)
    -- ^ @(start, len)@ packed 32\/32 per slot; @-1@ = empty (see @ciRuns@).
    , ptMask :: {-# UNPACK #-} !Int
    }

{- | Parallel inner-join probe. @parInnerProbe table probeHashes@ returns
@(probeIxs, buildIxs)@ identical to a sequential probe of the same index. The
build index must already be constructed from the build side.
-}
parInnerProbe ::
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parInnerProbe = runProbe False

{- | Parallel left-join probe. Like 'parInnerProbe' but every probe row emits at
least one output row; unmatched rows carry a @-1@ sentinel in the build column.
-}
parLeftProbe ::
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parLeftProbe = runProbe True

{- | Shared two-pass parallel probe. @keepUnmatched@ selects left- vs
inner-join semantics. Splits @[0, probeN)@ into @caps@ contiguous ranges, counts
each range's output, prefix-sums to global offsets, then fills the single output
buffers in parallel.
-}
runProbe ::
    Bool ->
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
runProbe keepUnmatched pt probeHashes = do
    caps <- getNumCapabilities
    let !probeN = VU.length probeHashes
        !nChunks = max 1 (min caps probeN)
        !sorted = ptSorted pt
        !keys = ptKeys pt
        !runs = ptRuns pt
        !mask = ptMask pt
        !shift = 64 - popCount mask
        -- Packed (start,len) run for hash @h@, or -1 on a miss. Home slot is
        -- the top log2(cap) bits of a Fibonacci multiply (the row hash's low
        -- bits are poorly diffused); must match the build-side ciSlot exactly.
        findRun !h = go (fromIntegral ((fromIntegral h * (0x9E3779B97F4A7C15 :: Word64)) `unsafeShiftR` shift))
          where
            go !slot =
                let !w = runs `VU.unsafeIndex` slot
                 in if w < 0
                        then -1
                        else
                            if keys `VU.unsafeIndex` slot == h
                                then w
                                else go ((slot + 1) .&. mask)
        chunkBounds k = (lo, hi)
          where
            !lo = (probeN * k) `div` nChunks
            !hi = (probeN * (k + 1)) `div` nChunks
        -- Count pass: output rows produced by probe range [lo, hi).
        countRange !lo !hi =
            let go !i !acc
                    | i >= hi = acc
                    | otherwise =
                        let !w = findRun (VU.unsafeIndex probeHashes i)
                         in if w < 0
                                then go (i + 1) (if keepUnmatched then acc + 1 else acc)
                                else go (i + 1) (acc + (w .&. 0xFFFFFFFF))
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
                    let !w = findRun (VU.unsafeIndex probeHashes i)
                    if w < 0
                        then
                            if keepUnmatched
                                then do
                                    VUM.unsafeWrite pv p i
                                    VUM.unsafeWrite bv p (-1)
                                    fill (i + 1) (p + 1)
                                else fill (i + 1) p
                        else do
                            let !start = w `unsafeShiftR` 32
                                !len = w .&. 0xFFFFFFFF
                                writeMatch !j !q
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
        _ <- forkIO (try (body k) >>= putMVar var)
        pure var
