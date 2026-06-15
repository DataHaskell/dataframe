{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Parallel stable sort of row indices by the ascending unsigned order of a
per-row 'Int' hash. Shared by the join build-side 'CompactIndex' construction
(@DataFrame.Operations.Join@), which previously paid a single-threaded
comparison sort over the whole build side — the dominant serial cost of a large
inner join (the @1e7 x 1e7@ big-inner case).

@parSortByHash n hashes@ returns @(sortedHashes, sortedIndices)@ where
@sortedIndices@ lists @[0, n)@ in ascending 'sortKey' order of their hash, ties
broken by ascending original index (stable), and @sortedHashes[k] ==
hashes[sortedIndices[k]]@. Bit-for-bit identical to the old stable merge sort's
output ordering, so equal-hash rows stay contiguous (the run scan in
'buildCompactIndex' depends on this) and within a run keep original-row order.

Strategy (mirrors "DataFrame.Internal.GroupingPar"): a counting sort buckets
rows by the top @log2 p@ bits of their unsigned key into @p@ partitions laid out
in ascending key order; @caps@ 'forkIO' workers then LSD-radix-sort each
partition by the full 56 remaining low bits. Because partitions are already in
global key order and each per-partition sort is stable, concatenating them
reproduces the global stable order with no merge step. A sequential LSD radix
sort is used below 'parSortThreshold' or on a single capability.
-}
module DataFrame.Internal.ParRadixSort (
    parSortByHash,
    parSortThreshold,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_, when)
import Data.Bits (countLeadingZeros, unsafeShiftR, (.&.))
import Data.IORef (atomicModifyIORef', newIORef)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.RadixRank (sortKey)
import System.IO.Unsafe (unsafePerformIO)

{- | Below this many rows the partition/fork overhead is not worth it; the
caller's sequential LSD radix path is used instead.
-}
parSortThreshold :: Int
parSortThreshold = 500000

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

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
radixPasses n keysA orderA keysB orderB = do
    counts <- VUM.new 256
    let pass !shiftBits !srcK !srcO !dstK !dstO = do
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
    pass 0 keysA orderA keysB orderB
    pass 8 keysB orderB keysA orderA
    pass 16 keysA orderA keysB orderB
    pass 24 keysB orderB keysA orderA
    pass 32 keysA orderA keysB orderB
    pass 40 keysB orderB keysA orderA
    pass 48 keysA orderA keysB orderB
    pass 56 keysB orderB keysA orderA

-------------------------------------------------------------------------------
-- Parallel path: counting-sort partition, then per-partition sort in parallel
-------------------------------------------------------------------------------

parSortByHashIO :: Int -> VU.Vector Int -> IO (VU.Vector Int, VU.Vector Int)
parSortByHashIO n hashes = do
    caps <- getNumCapabilities
    let !p = numPartitionsFor caps
        !shift = 64 - intLog2 p
    -- Phase 1: counting sort of row indices into ascending-key partitions.
    (partStart, partRows) <- partitionRows n hashes p shift
    -- Phase 2: stable-sort each partition by full key, in parallel. Each worker
    -- owns disjoint [partStart[pp], partStart[pp+1]) output ranges, so the
    -- single shared output buffers are written race-free.
    outOrder <- VUM.new n
    outKeys <- VUM.new n
    sortPartitions caps p partStart partRows hashes outOrder outKeys
    order <- VU.unsafeFreeze outOrder
    pure (VU.unsafeBackpermute hashes order, order)

{- | Bucket every row index into its top-bits partition by a counting sort.
Returns the exclusive prefix sum @partStart@ (length @p+1@, @partStart[p] == n@)
and the row indices laid out partition-by-partition in ascending key order.
-}
partitionRows ::
    Int -> VU.Vector Int -> Int -> Int -> IO (VU.Vector Int, VU.Vector Int)
partitionRows n hashes p shift = do
    counts <- VUM.replicate (p + 1) (0 :: Int)
    let countLoop !i
            | i >= n = pure ()
            | otherwise = do
                let !pp = partIx shift (VU.unsafeIndex hashes i)
                c <- VUM.unsafeRead counts pp
                VUM.unsafeWrite counts pp (c + 1)
                countLoop (i + 1)
    countLoop 0
    partStartM <- VUM.new (p + 1)
    let scan !k !acc
            | k > p = pure ()
            | otherwise = do
                VUM.unsafeWrite partStartM k acc
                c <- if k < p then VUM.unsafeRead counts k else pure 0
                scan (k + 1) (acc + c)
    scan 0 0
    cursor <- VUM.new p
    forM_ [0 .. p - 1] $ \k -> VUM.unsafeRead partStartM k >>= VUM.unsafeWrite cursor k
    rowsM <- VUM.new (max 1 n)
    let place !i
            | i >= n = pure ()
            | otherwise = do
                let !pp = partIx shift (VU.unsafeIndex hashes i)
                pos <- VUM.unsafeRead cursor pp
                VUM.unsafeWrite rowsM pos i
                VUM.unsafeWrite cursor pp (pos + 1)
                place (i + 1)
    place 0
    partStart <- VU.unsafeFreeze partStartM
    partRows <- VU.unsafeFreeze rowsM
    pure (partStart, partRows)

{- | Stable-sort each partition by full key, writing sorted original indices
into @outOrder@ and their hashes into @outKeys@ at the partition's slot range.
Forks @caps@ workers that pull partition indices off a shared atomic counter.
Within a partition the counting sort already left rows in ascending original
order, so the LSD radix sort's stability reproduces the global @(key, row)@
order. Partitions below two elements are already sorted (counting sort kept
original order) and are copied directly.
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
sortPartitions caps p partStart partRows hashes outOrder outKeys = do
    next <- newIORef 0
    let sortOne !pp = do
            let !s = VU.unsafeIndex partStart pp
                !e = VU.unsafeIndex partStart (pp + 1)
                !sz = e - s
            when (sz > 0) $
                if sz == 1
                    then do
                        let !r = VU.unsafeIndex partRows s
                        VUM.unsafeWrite outOrder s r
                        VUM.unsafeWrite outKeys s (VU.unsafeIndex hashes r)
                    else do
                        keysA <- VUM.new sz
                        orderA <- VUM.new sz
                        let seed !i
                                | i >= sz = pure ()
                                | otherwise = do
                                    let !r = VU.unsafeIndex partRows (s + i)
                                    VUM.unsafeWrite keysA i (sortKey (VU.unsafeIndex hashes r))
                                    VUM.unsafeWrite orderA i r
                                    seed (i + 1)
                        seed 0
                        keysB <- VUM.new sz
                        orderB <- VUM.new sz
                        radixPasses sz keysA orderA keysB orderB
                        let emit !i
                                | i >= sz = pure ()
                                | otherwise = do
                                    o <- VUM.unsafeRead orderA i
                                    VUM.unsafeWrite outOrder (s + i) o
                                    VUM.unsafeWrite outKeys (s + i) (VU.unsafeIndex hashes o)
                                    emit (i + 1)
                        emit 0
        worker = do
            i <- atomicModifyIORef' next (\j -> (j + 1, j))
            when (i < p) $ sortOne i >> worker
    forkJoin_ (replicate caps worker)

-- | Run each action on its own thread; rethrow the first failure (in order).
forkJoin_ :: [IO ()] -> IO ()
forkJoin_ actions = do
    vars <- mapM spawn actions
    results <- mapM takeMVar vars
    mapM_ (either (throwIO :: SomeException -> IO ()) pure) results
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkIO (try act >>= putMVar var)
        pure var
