{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}

{- | Parallel partitioned group-by: rows are counting-sorted into partitions by the
top hash bits, then one task per capability groups its partitions independently.
Output is bit-for-bit identical to the sequential 'DataFrame.Internal.Grouping.groupBy'.
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
import Control.Monad (when)
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
    (partStart, sortedRows) <- partitionRows n hashes p shift
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
    (globalBase, canonOf, nGroups) <- canonicalize p canonBoxes nLocalGroups
    assemble n p partStart sortedRows localGid globalBase canonOf nGroups

-------------------------------------------------------------------------------
-- Phase 1: counting sort by partition
-------------------------------------------------------------------------------

{- | Bucket every row index into its partition by a counting sort. Returns the
exclusive prefix-sum @partStart@ (length @p+1@, @partStart[p] == n@) and the row
indices laid out partition-by-partition in @sortedRows@.

Runs chunked across capabilities: per-chunk partition histograms are prefix
summed (in chunk order) into disjoint per-chunk write cursors, so the scatter
threads never contend and each partition keeps its rows in ascending original
row order — bit-for-bit the sequential counting sort's layout.
-}
partitionRows ::
    Int -> VU.Vector Int -> Int -> Int -> IO (VU.Vector Int, VU.Vector Int)
partitionRows n hashes p shift = do
    caps <- getNumCapabilities
    let chunks = rowChunks caps n
    cursors <- forkJoinResults [histChunk hashes p shift lo hi | (lo, hi) <- chunks]
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
    sortedM <- VUM.new (max 1 n)
    forkJoin_
        [ scatterChunk hashes shift cur sortedM lo hi
        | ((lo, hi), cur) <- zip chunks cursors
        ]
    partStart <- VU.unsafeFreeze partStartM
    sortedRows <- VU.unsafeFreeze sortedM
    pure (partStart, sortedRows)

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

-- | Scatter one row chunk into @sortedM@ through the chunk's private cursor.
scatterChunk ::
    VU.Vector Int -> Int -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
scatterChunk hashes shift cursor sortedM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !pp = partIx shift (VU.unsafeIndex hashes i)
            pos <- VUM.unsafeRead cursor pp
            VUM.unsafeWrite sortedM pos i
            VUM.unsafeWrite cursor pp (pos + 1)
            go (i + 1)

-------------------------------------------------------------------------------
-- Phase 2: per-partition grouping (parallel)
-------------------------------------------------------------------------------

{- | Group each partition with its own hash table, then rank its local groups into
canonical order — all inside the parallel worker. Forks @caps@ workers pulling
partition indices off a shared counter; disjoint keys mean no cross-partition merge.
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
(@globalBase[pp]@ = first global id of partition @pp@). Ranks were computed in
'runPartitions'; prepending the base to each yields the sequential order.
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

{- | Build the final @(rowToGroup, valueIndices, offsets)@: the global group id of a
sorted position is @globalBase[pp] + canonOf[pp][localGid]@. @valueIndices@ orders
rows by group, @offsets@ the boundaries, @rowToGroup@ the inverse per original row.

Each partition owns a disjoint @sortedRows@ range and a disjoint global group-id
range, and its rows are exactly its groups' rows — so its first group's offset is
its own @partStart@ and every pass (group ids, offsets, placement) runs per
partition on parallel workers with no shared writes. @sortedRows@ keeps ascending
original row order inside a partition, so per-group row order matches the
sequential pass exactly.
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
    caps <- getNumCapabilities
    rtgM <- VUM.new (max 1 n)
    gidAt <- VUM.new (max 1 n)
    counts <- VUM.new (max 1 nGroups)
    offsM <- VUM.new (nGroups + 1)
    visM <- VUM.new (max 1 n)
    next <- newIORef 0
    let doPartition !pp = do
            let !s = VU.unsafeIndex partStart pp
                !e = VU.unsafeIndex partStart (pp + 1)
                !base = VU.unsafeIndex globalBase pp
                !gEnd = VU.unsafeIndex globalBase (pp + 1)
                !canon = V.unsafeIndex canonOf pp
            let zero !g
                    | g >= gEnd = pure ()
                    | otherwise = VUM.unsafeWrite counts g 0 >> zero (g + 1)
            zero base
            -- Pass 1: global group ids, rowToGroup, per-group counts.
            let pass1 !pos
                    | pos >= e = pure ()
                    | otherwise = do
                        lg <- VUM.unsafeRead localGid pos
                        let !g = base + VU.unsafeIndex canon lg
                            !row = VU.unsafeIndex sortedRows pos
                        VUM.unsafeWrite gidAt pos g
                        VUM.unsafeWrite rtgM row g
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite counts g (c + 1)
                        pass1 (pos + 1)
            pass1 s
            -- Offsets for our group range (they start at our partStart);
            -- counts becomes the per-group write cursor.
            let offsLoop !g !acc
                    | g >= gEnd = pure ()
                    | otherwise = do
                        VUM.unsafeWrite offsM g acc
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite counts g acc
                        offsLoop (g + 1) (acc + c)
            offsLoop base s
            -- Pass 2: stable placement into valueIndices.
            let pass2 !pos
                    | pos >= e = pure ()
                    | otherwise = do
                        g <- VUM.unsafeRead gidAt pos
                        let !row = VU.unsafeIndex sortedRows pos
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite visM c row
                        VUM.unsafeWrite counts g (c + 1)
                        pass2 (pos + 1)
            pass2 s
        worker = do
            i <- atomicModifyIORef' next (\j -> (j + 1, j))
            when (i < p) $ doPartition i >> worker
    forkJoin_ (replicate caps worker)
    VUM.unsafeWrite offsM nGroups n
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

{- | Run each action on its own thread and collect the results in order;
rethrow the first failure. A single action runs on the calling thread.
-}
forkJoinResults :: [IO a] -> IO [a]
forkJoinResults [act] = fmap (: []) act
forkJoinResults actions = do
    vars <- mapM spawn actions
    rs <- mapM takeMVar vars
    mapM (either (throwIO :: SomeException -> IO a) pure) rs
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkIO (try act >>= putMVar var)
        pure var
