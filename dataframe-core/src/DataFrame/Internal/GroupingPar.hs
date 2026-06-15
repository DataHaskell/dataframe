{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}

{- |
Parallel partitioned group-by assignment. Row indices are partitioned by the
high bits of their key-hash (a counting sort: per-range histogram, prefix-sum,
scatter into one index buffer laid out partition-by-partition). One task per
capability then groups its partitions with its OWN open-addressing hash table
('DataFrame.Internal.HashTable') — keys are disjoint across partitions, so the
per-partition group sets concatenate with NO merge.

The output @(rowToGroup, valueIndices, offsets)@ is /bit-for-bit identical/ to
the sequential 'DataFrame.Internal.Grouping.groupBy': groups are emitted in
ascending @(repHash, repRow)@ order (signed-Int order on the hash, tie-broken by
the representative row). Because the partition key is the top bits of a
sign-preserving unsigned remap of the same hash, partition order already agrees
with that global order; within a partition we sort the local groups by the same
key. This is the parallel==sequential correctness gate.

The driver forks plain 'forkIO' workers (no sparks) over a shared atomic-counter
work queue, so partition skew is balanced. A sequential fallback is used when
there is a single capability or the row count is below 'parThreshold' (decided in
'shouldParallelize', which 'groupBy' consults before calling here).
-}
module DataFrame.Internal.GroupingPar (
    parallelAssignGroups,
    shouldParallelize,
    parThreshold,
    numPartitionsFor,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (forM_, when)
import Data.Bits (countLeadingZeros, unsafeShiftR)
import Data.IORef (atomicModifyIORef', newIORef)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.HashTable (
    htInsert,
    newHashTable,
 )
import DataFrame.Internal.RadixRank (rankByHash)
import System.IO.Unsafe (unsafePerformIO)

{- | Below this many rows the partition/fork overhead is not worth it; 'groupBy'
uses its sequential 'ST' path instead.
-}
parThreshold :: Int
parThreshold = 200000

{- | Whether 'groupBy' should take the parallel path: more than one capability
and at least 'parThreshold' rows.
-}
shouldParallelize :: Int -> Bool
shouldParallelize n = n >= parThreshold && capabilities > 1
{-# NOINLINE shouldParallelize #-}

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

{- | Sign-preserving unsigned remap: ascending 'Word64' order of @key h@ equals
ascending signed-'Int' order of @h@, so partitioning and sorting on it reproduce
the sequential @compare \`on\` repHash@ ordering exactly.
-}
key :: Int -> Word64
key h = fromIntegral h + 0x8000000000000000
{-# INLINE key #-}

-- | Partition index of a hash: the top @log2 p@ bits of its unsigned key.
partIx :: Int -> Int -> Int
partIx shift h = fromIntegral (key h `unsafeShiftR` shift)
{-# INLINE partIx #-}

{- | Number of partitions: a power of two, at least @4 * caps@ (P >> cores for
skew tolerance), floored at 256.
-}
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

{- | Parallel group assignment. @parallelAssignGroups n hashes eqRow@ returns
@(rowToGroup, valueIndices, offsets)@ in canonical group order. @eqRow a b@ must
report whether rows @a@ and @b@ share all key columns (null-aware).
-}
parallelAssignGroups ::
    Int ->
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    IO (VU.Vector Int, VU.Vector Int, VU.Vector Int)
parallelAssignGroups n hashes eqRow = do
    caps <- getNumCapabilities
    let !p = numPartitionsFor caps
        !shift = 64 - intLog2 p
    -- Phase 1: counting sort of row indices by partition.
    (partStart, sortedRows) <- partitionRows n hashes p shift
    -- Phase 2: per-partition grouping + per-partition canonical ranking (both
    -- inside the parallel worker). localGid[pos] = local group id of the row at
    -- sorted position 'pos'; canonBoxes[part] = its rank vector.
    localGid <- VUM.new (max 1 n)
    canonBoxes <- VM.replicate p (VU.empty :: VU.Vector Int)
    nLocalGroups <- VUM.replicate p (0 :: Int)
    runPartitions
        caps
        p
        partStart
        sortedRows
        hashes
        eqRow
        localGid
        canonBoxes
        nLocalGroups
    -- Phase 3: global base ids (serial prefix sum; the ranking is already done).
    (globalBase, canonOf, nGroups) <- canonicalize p canonBoxes nLocalGroups
    assemble n p partStart sortedRows localGid globalBase canonOf nGroups

-------------------------------------------------------------------------------
-- Phase 1: counting sort by partition
-------------------------------------------------------------------------------

{- | Bucket every row index into its partition by a counting sort. Returns the
exclusive prefix-sum @partStart@ (length @p+1@, @partStart[p] == n@) and the row
indices laid out partition-by-partition in @sortedRows@.
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
    sortedM <- VUM.new (max 1 n)
    let place !i
            | i >= n = pure ()
            | otherwise = do
                let !pp = partIx shift (VU.unsafeIndex hashes i)
                pos <- VUM.unsafeRead cursor pp
                VUM.unsafeWrite sortedM pos i
                VUM.unsafeWrite cursor pp (pos + 1)
                place (i + 1)
    place 0
    partStart <- VU.unsafeFreeze partStartM
    sortedRows <- VU.unsafeFreeze sortedM
    pure (partStart, sortedRows)

-------------------------------------------------------------------------------
-- Phase 2: per-partition grouping (parallel)
-------------------------------------------------------------------------------

{- | Group each partition with its own hash table, then rank its local groups
into canonical order — all inside the parallel worker. Forks @caps@ workers that
pull partition indices off a shared counter. For partition @pp@ spanning
@[partStart[pp], partStart[pp+1])@ of @sortedRows@ a worker assigns dense local
group ids (first-appearance order) into @localGid@ at the same sorted positions,
recording each new group's representative hash and row into unboxed
first-appearance vectors. It then computes @canonBoxes[pp]@ — @canon[localGid] =
within-partition canonical rank — via a stable radix sort on the unsigned
representative hash (ties keep first-appearance order, which is ascending repRow,
so the @(key hash, repRow)@ order of the old comparison sort is reproduced with
no boxed tuples). @nLocalGroups[pp]@ holds the group count.
-}
runPartitions ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    VUM.IOVector Int ->
    VM.IOVector (VU.Vector Int) ->
    VUM.IOVector Int ->
    IO ()
runPartitions caps p partStart sortedRows hashes eqRow localGid canonBoxes nLocalGroups = do
    next <- newIORef 0
    let groupPartition !pp = do
            let !s = VU.unsafeIndex partStart pp
                !e = VU.unsafeIndex partStart (pp + 1)
                !sz = e - s
            when (sz > 0) $ do
                ht <- newHashTable sz
                -- repHash indexed by local gid (first-appearance order). The
                -- representative row is implicitly ascending in gid order (sorted
                -- positions are in original-row order, a stable counting sort),
                -- so the stable hash-rank below needs no explicit repRow.
                repHashM <- VUM.new sz
                let loop !pos !nextGid
                        | pos >= e = pure nextGid
                        | otherwise = do
                            let !row = VU.unsafeIndex sortedRows pos
                                !h = VU.unsafeIndex hashes row
                            (gid, isNew) <- htInsert ht eqRow nextGid row h
                            VUM.unsafeWrite localGid pos gid
                            if isNew
                                then do
                                    VUM.unsafeWrite repHashM nextGid h
                                    loop (pos + 1) (nextGid + 1)
                                else loop (pos + 1) nextGid
                ng <- loop s 0
                VUM.unsafeWrite nLocalGroups pp ng
                -- Rank this partition's groups into canonical order (shared with
                -- the sequential path). repRow is ascending in gid order (stable
                -- counting sort keeps sorted positions in original-row order), so
                -- the stable hash-rank's tie-break reproduces (hash, repRow).
                canon <- rankByHash (VUM.unsafeRead repHashM) ng
                VM.unsafeWrite canonBoxes pp canon
        worker = do
            i <- atomicModifyIORef' next (\j -> (j + 1, j))
            when (i < p) $ groupPartition i >> worker
    forkJoin_ (replicate caps worker)

-------------------------------------------------------------------------------
-- Phase 3: global base ids + assembly
-------------------------------------------------------------------------------

{- | Exclusive prefix sum of the per-partition group counts into @globalBase@
(length @p+1@, @globalBase[pp]@ is the first global id of partition @pp@,
@globalBase[p]@ the total). The per-partition canonical ranks were already
computed in 'runPartitions'; partitions are in ascending key order so prepending
@globalBase[pp]@ to each rank yields the sequential @canonicalRemap@ order.
-}
canonicalize ::
    Int ->
    VM.IOVector (VU.Vector Int) ->
    VUM.IOVector Int ->
    IO (VU.Vector Int, V.Vector (VU.Vector Int), Int)
canonicalize p canonBoxes nLocalGroups = do
    globalBaseM <- VUM.new (p + 1)
    let go !pp !base
            | pp >= p = VUM.unsafeWrite globalBaseM p base >> pure base
            | otherwise = do
                VUM.unsafeWrite globalBaseM pp base
                ng <- VUM.unsafeRead nLocalGroups pp
                go (pp + 1) (base + ng)
    total <- go 0 0
    globalBase <- VU.unsafeFreeze globalBaseM
    canonOf <- V.unsafeFreeze canonBoxes
    pure (globalBase, canonOf, total)

{- | Build the final @(rowToGroup, valueIndices, offsets)@. For each sorted
position we know its partition, its local group id and the canonical maps, so the
global group id is @globalBase[pp] + canonOf[pp][localGid]@. @valueIndices@ is the
rows ordered by global group; @offsets@ the per-group boundaries; @rowToGroup@ the
inverse mapping per original row.
-}
assemble ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VU.Vector Int ->
    V.Vector (VU.Vector Int) ->
    Int ->
    IO (VU.Vector Int, VU.Vector Int, VU.Vector Int)
assemble n p partStart sortedRows localGid globalBase canonOf nGroups = do
    rtgM <- VUM.new (max 1 n)
    -- Global group id of each sorted position, plus per-group counts.
    counts <- VUM.replicate (nGroups + 1) (0 :: Int)
    gidAt <- VUM.new (max 1 n)
    let scanPos !pp
            | pp >= p = pure ()
            | otherwise = do
                let !s = VU.unsafeIndex partStart pp
                    !e = VU.unsafeIndex partStart (pp + 1)
                    !base = VU.unsafeIndex globalBase pp
                    !canon = V.unsafeIndex canonOf pp
                let inner !pos
                        | pos >= e = pure ()
                        | otherwise = do
                            lg <- VUM.unsafeRead localGid pos
                            let !g = base + VU.unsafeIndex canon lg
                                !row = VU.unsafeIndex sortedRows pos
                            VUM.unsafeWrite gidAt pos g
                            VUM.unsafeWrite rtgM row g
                            c <- VUM.unsafeRead counts g
                            VUM.unsafeWrite counts g (c + 1)
                            inner (pos + 1)
                inner s
                scanPos (pp + 1)
    scanPos 0
    -- offsets = exclusive prefix sum of counts.
    offsM <- VUM.new (nGroups + 1)
    let scan !k !acc
            | k > nGroups = pure ()
            | otherwise = do
                VUM.unsafeWrite offsM k acc
                c <- if k < nGroups then VUM.unsafeRead counts k else pure 0
                scan (k + 1) (acc + c)
    scan 0 0
    -- valueIndices: place each sorted position's row at its group's cursor.
    -- Iterating sorted positions in order keeps rows in original order within a
    -- group (the partition counting sort and grouping both preserve it).
    cursor <- VUM.new (max 1 nGroups)
    forM_ [0 .. nGroups - 1] $ \k -> VUM.unsafeRead offsM k >>= VUM.unsafeWrite cursor k
    visM <- VUM.new (max 1 n)
    let placeVis !pos
            | pos >= n = pure ()
            | otherwise = do
                g <- VUM.unsafeRead gidAt pos
                let !row = VU.unsafeIndex sortedRows pos
                c <- VUM.unsafeRead cursor g
                VUM.unsafeWrite visM c row
                VUM.unsafeWrite cursor g (c + 1)
                placeVis (pos + 1)
    placeVis 0
    rtg <- VU.unsafeFreeze rtgM
    offs <- VU.unsafeFreeze offsM
    vis <- VU.unsafeFreeze visM
    pure (rtg, vis, offs)

-------------------------------------------------------------------------------
-- Thread fan-out (plain forkIO + MVar join, no sparks)
-------------------------------------------------------------------------------

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
