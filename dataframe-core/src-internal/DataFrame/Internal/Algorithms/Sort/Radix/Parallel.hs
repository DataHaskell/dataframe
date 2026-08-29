{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Parallel stable sort of row indices by ascending unsigned order of a per-row
'Int' hash, used by the join build side. A counting sort buckets rows into
key-ordered partitions that workers LSD-radix-sort in parallel, with no merge step.
-}
module DataFrame.Internal.Algorithms.Sort.Radix.Parallel (
    parSortByHash,
    parSortThreshold,
) where

import Control.Concurrent (getNumCapabilities)
import Control.Monad (forM_, when)
import Data.Bits (countLeadingZeros, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.Algorithms.Rank.Radix (sortKey)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    forkJoin_,
    pooledIndices,
 )
import System.IO.Unsafe (unsafePerformIO)

{- | Below this many rows the partition/fork overhead is not worth it; the
caller's sequential LSD radix path is used instead.
-}
parSortThreshold :: Int
parSortThreshold = 500000

{- | Top-bits partition index of a hash: the high @64 - shift@ bits of its
unsigned 'sortKey'. Ascending partition order equals ascending key order.
-}
partIx :: Int -> Int -> Int
partIx shift h = fromIntegral ((fromIntegral (sortKey h) :: Word64) `unsafeShiftR` shift)
{-# INLINE partIx #-}

-- | Number of partitions: a power of two, at least @4 * caps@, floored at 256.
numPartitionsFor :: Int -> Int
numPartitionsFor caps = go 1
  where
    target = max 256 (4 * caps)
    go p
        | p >= target = p
        | otherwise = go (p * 2)

-- | @floor (log2 x)@ for a power-of-two @x@.
intLog2 :: Int -> Int
intLog2 x = 63 - countLeadingZeros x
{-# INLINE intLog2 #-}

{- | Parallel stable sort of @[0, n)@ by ascending unsigned hash order. See the
module header for the ordering contract.
-}
parSortByHash :: Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
parSortByHash n hashes
    | n <= 1 =
        (hashes, VU.enumFromN 0 n)
    | n < parSortThreshold || capabilities <= 1 =
        seqSortByHash n hashes
    | otherwise = unsafePerformIO (parSortByHashIO n hashes)
{-# NOINLINE parSortByHash #-}

-------------------------------------------------------------------------------
-- Sequential LSD radix sort (also the per-partition worker kernel)
-------------------------------------------------------------------------------

{- | Stable LSD radix sort of @[0, n)@ by ascending 'sortKey' of their hash, 8
bits per pass over the full 64-bit key. Returns @(sortedHashes, sortedIndices)@.
-}
seqSortByHash :: Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
seqSortByHash n hashes = unsafePerformIO $ do
    keysA <- VUM.new n
    orderA <- VUM.new n
    let seed !i
            | i >= n = pure ()
            | otherwise = do
                VUM.unsafeWrite keysA i (sortKey (VU.unsafeIndex hashes i))
                VUM.unsafeWrite orderA i i
                seed (i + 1)
    seed 0
    keysB <- VUM.new n
    orderB <- VUM.new n
    radixPasses n keysA orderA keysB orderB
    order <- VU.unsafeFreeze orderA
    pure (VU.unsafeBackpermute hashes order, order)

{- | Run all eight stable 8-bit LSD passes, ping-ponging between the two
key/order buffer pairs so the sorted order lands back in @(keysA, orderA)@.
@keysA[i]@ must already hold @sortKey (hash of orderA[i])@ on entry.
-}
radixPasses ::
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
radixPasses n keysA orderA keysB orderB =
    radixPassesN 8 n keysA orderA keysB orderB

{- | Run the first @np@ stable 8-bit LSD passes (bits @0 .. 8*np-1@),
ping-ponging between the buffer pairs. For odd @np@ the sorted order lands in
@(keysB, orderB)@, for even in @(keysA, orderA)@. Callers whose rows share
their top bytes (per-partition sorts partitioned on the top byte) can pass
@np = 7@: the eighth pass would be a stable identity copy.
-}
radixPassesN ::
    Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
radixPassesN np n keysA orderA keysB orderB = do
    counts <- VUM.new 256
    let pass ::
            Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            IO ()
        pass !shiftBits !srcK !srcO !dstK !dstO = do
            VUM.set counts 0
            let count !i
                    | i >= n = pure ()
                    | otherwise = do
                        k <- VUM.unsafeRead srcK i
                        let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                        VUM.unsafeRead counts b >>= VUM.unsafeWrite counts b . (+ 1)
                        count (i + 1)
            count 0
            let scan !b !acc
                    | b >= 256 = pure ()
                    | otherwise = do
                        c <- VUM.unsafeRead counts b
                        VUM.unsafeWrite counts b acc
                        scan (b + 1) (acc + c)
            scan 0 0
            let place !i
                    | i >= n = pure ()
                    | otherwise = do
                        k <- VUM.unsafeRead srcK i
                        o <- VUM.unsafeRead srcO i
                        let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                        pos <- VUM.unsafeRead counts b
                        VUM.unsafeWrite counts b (pos + 1)
                        VUM.unsafeWrite dstK pos k
                        VUM.unsafeWrite dstO pos o
                        place (i + 1)
            place 0
        run !k
            | k >= np = pure ()
            | even k = pass (8 * k) keysA orderA keysB orderB >> run (k + 1)
            | otherwise = pass (8 * k) keysB orderB keysA orderA >> run (k + 1)
    run 0

-------------------------------------------------------------------------------
-- Parallel path: counting-sort partition, then per-partition sort in parallel
-------------------------------------------------------------------------------

parSortByHashIO :: Int -> VU.Vector Int -> IO (VU.Vector Int, VU.Vector Int)
parSortByHashIO n hashes = do
    caps <- getNumCapabilities
    let !p = numPartitionsFor caps
        !shift = 64 - intLog2 p
    (partStart, partRows, partHashes) <- partitionRows n hashes p shift
    outOrder <- VUM.new n
    outKeys <- VUM.new n
    sortPartitions caps p partStart partRows partHashes outOrder outKeys
    order <- VU.unsafeFreeze outOrder
    sortedHashes <- VU.unsafeFreeze outKeys
    pure (sortedHashes, order)

{- | Bucket every row index into its top-bits partition by a counting sort.
Returns the exclusive prefix sum @partStart@ (length @p+1@, @partStart[p] == n@),
the row indices laid out partition-by-partition in ascending key order, and
each sorted position's hash in the same layout (so downstream passes read
hashes sequentially instead of a random @hashes[row]@ gather per row).

Runs chunked across capabilities: per-chunk partition histograms are prefix
summed (in chunk order) into disjoint per-chunk write cursors, so the scatter
threads never contend and each partition keeps its rows in ascending original
row order — bit-for-bit the sequential counting sort's layout.
-}
partitionRows ::
    Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    IO (VU.Vector Int, VU.Vector Int, VU.Vector Int)
partitionRows n hashes p shift = do
    caps <- getNumCapabilities
    let chunks = rowChunks caps n
    cursors <- forkJoin [histChunk hashes p shift lo hi | (lo, hi) <- chunks]
    -- Exclusive prefix over partitions (outer) and chunks (inner): partStart
    -- from the totals, and each chunk's histogram rewritten into its cursor.
    partStartM <- VUM.new (p + 1)
    let seed !pp !acc
            | pp >= p = VUM.unsafeWrite partStartM p acc
            | otherwise = do
                VUM.unsafeWrite partStartM pp acc
                let inner [] !a = pure a
                    inner (cur : rest) !a = do
                        t <- VUM.unsafeRead cur pp
                        VUM.unsafeWrite cur pp a
                        inner rest (a + t)
                acc' <- inner cursors acc
                seed (pp + 1) acc'
    seed 0 0
    rowsM <- VUM.new (max 1 n)
    rowHashM <- VUM.new (max 1 n)
    forkJoin_
        [ scatterChunk hashes shift cur rowsM rowHashM lo hi
        | ((lo, hi), cur) <- zip chunks cursors
        ]
    partStart <- VU.unsafeFreeze partStartM
    partRows <- VU.unsafeFreeze rowsM
    partHashes <- VU.unsafeFreeze rowHashM
    pure (partStart, partRows, partHashes)

-- | Contiguous near-equal row chunks, one per capability; empties dropped.
rowChunks :: Int -> Int -> [(Int, Int)]
rowChunks caps n =
    [ (lo, hi)
    | w <- [0 .. caps - 1]
    , let lo = min n (w * per)
    , let hi = min n (lo + per)
    , lo < hi
    ]
  where
    !per = (n + max 1 caps - 1) `div` max 1 caps

-- | Per-partition counts of one row chunk.
histChunk :: VU.Vector Int -> Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
histChunk hashes p shift lo hi = do
    acc <- VUM.replicate p (0 :: Int)
    let go !i
            | i >= hi = pure acc
            | otherwise = do
                let !pp = partIx shift (VU.unsafeIndex hashes i)
                c <- VUM.unsafeRead acc pp
                VUM.unsafeWrite acc pp (c + 1)
                go (i + 1)
    go lo

{- | Scatter one row chunk into the partitioned layout using the chunk's
pre-summed cursor (disjoint write regions per chunk, no contention).
-}
scatterChunk ::
    VU.Vector Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterChunk hashes shift cursor rowsM rowHashM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !h = VU.unsafeIndex hashes i
                !pp = partIx shift h
            pos <- VUM.unsafeRead cursor pp
            VUM.unsafeWrite rowsM pos i
            VUM.unsafeWrite rowHashM pos h
            VUM.unsafeWrite cursor pp (pos + 1)
            go (i + 1)

{- | Stable-sort each partition by full key, writing sorted original indices
into @outOrder@ and their hashes into @outKeys@ at the partition's slot range.
Forks @caps@ workers that pull partition indices off a shared atomic counter.
Within a partition the counting sort already left rows in ascending original
order, so the LSD radix sort's stability reproduces the global @(key, row)@
order. Partitions below two elements are already sorted (counting sort kept
original order) and are copied directly.

@partHashes@ is the partition-layout hash vector from 'partitionRows', so
seeding reads hashes sequentially; only 7 LSD passes run (the top byte is the
partition byte, constant within a partition), and the sorted hash is recovered
from the sort key ('sortKey' is self-inverse) instead of a random gather.
-}
sortPartitions ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
sortPartitions caps p partStart partRows partHashes outOrder outKeys =
    pooledIndices caps p sortOne
  where
    sortOne !pp = do
        let !s = VU.unsafeIndex partStart pp
            !e = VU.unsafeIndex partStart (pp + 1)
            !sz = e - s
        when (sz > 0) $
            if sz == 1
                then do
                    VUM.unsafeWrite outOrder s (VU.unsafeIndex partRows s)
                    VUM.unsafeWrite outKeys s (VU.unsafeIndex partHashes s)
                else do
                    keysA <- VUM.new sz
                    orderA <- VUM.new sz
                    let seed !i
                            | i >= sz = pure ()
                            | otherwise = do
                                VUM.unsafeWrite keysA i (sortKey (VU.unsafeIndex partHashes (s + i)))
                                VUM.unsafeWrite orderA i (VU.unsafeIndex partRows (s + i))
                                seed (i + 1)
                    seed 0
                    keysB <- VUM.new sz
                    orderB <- VUM.new sz
                    radixPassesN 7 sz keysA orderA keysB orderB
                    let emit !i
                            | i >= sz = pure ()
                            | otherwise = do
                                o <- VUM.unsafeRead orderB i
                                k <- VUM.unsafeRead keysB i
                                VUM.unsafeWrite outOrder (s + i) o
                                -- sortKey is an involution: recover the hash.
                                VUM.unsafeWrite outKeys (s + i) (sortKey k)
                                emit (i + 1)
                    emit 0
