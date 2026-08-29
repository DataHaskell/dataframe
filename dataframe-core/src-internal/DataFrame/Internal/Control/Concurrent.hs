{-# LANGUAGE BangPatterns #-}

-- | Shared concurrency primitives for the dataframe packages.
module DataFrame.Internal.Control.Concurrent (
    -- * Capabilities
    capabilities,
    capabilitiesIO,
    shouldParallelize,
    parThreshold,

    -- * Chunk planning (pure)
    splitChunkRange,
    chunksFor,
    boundsChunks,

    -- * Thread fan-out
    forkJoin,
    forkJoin_,

    -- * Chunked fan-out (per-chunk callbacks only)
    parallelChunks,
    parallelChunks_,
    parallelBounds_,

    -- * Work-stealing pools
    pooledIndices,
    pooledRun,
) where

import Control.Concurrent (forkFinally, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO)
import Control.Monad (when)
import Data.IORef (atomicModifyIORef', newIORef)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import System.IO.Unsafe (unsafePerformIO)

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

capabilitiesIO :: IO Int
capabilitiesIO = getNumCapabilities
{-# INLINE capabilitiesIO #-}

shouldParallelize :: Int -> Int -> Bool
shouldParallelize threshold n = n >= threshold && capabilities > 1
{-# INLINE shouldParallelize #-}

{- | Row count below which a fan-out does not pay for itself, for the fixed-cost
per-row loops (grouping, the aggregation kernels). Pair it with
'shouldParallelize'. Kernels with a materially different per-row cost —
the join probe, the radix sort — set their own thresholds.
-}
parThreshold :: Int
parThreshold = 200000

splitChunkRange :: Int -> Int -> [(Int, Int)]
splitChunkRange k n
    | n <= 0 = []
    | otherwise =
        [ (lo, lo + len)
        | w <- [0 .. k' - 1]
        , let lo = w * q + min w r
        , let len = q + fromEnum (w < r)
        , len > 0
        ]
  where
    -- A non-positive width is a caller bug; clamp to one chunk rather than
    -- return [] and silently skip the rows.
    !k' = max 1 k
    (!q, !r) = n `quotRem` k'

chunksFor :: Int -> Int -> [(Int, Int)]
chunksFor !threshold !n
    | not (shouldParallelize threshold n) = [(0, n)]
    | otherwise = splitChunkRange capabilities n
{-# INLINE chunksFor #-}

{- | Adjacent pairs of a precomputed bounds vector of length @caps + 1@. Empty
ranges are NOT dropped: the bounds are the caller's and the slot count is
often meaningful. Endpoints are forced here, on the spawning thread, so a
worker never starts by evaluating an index thunk that retains @bs@.
-}
boundsChunks :: Int -> VU.Vector Int -> [(Int, Int)]
boundsChunks caps bs =
    [ (lo, hi)
    | w <- [0 .. caps - 1]
    , let !lo = VU.unsafeIndex bs w
    , let !hi = VU.unsafeIndex bs (w + 1)
    ]
{-# INLINE boundsChunks #-}

rethrow :: Either SomeException a -> IO a
rethrow = either throwIO pure
{-# INLINE rethrow #-}

forkJoin :: [IO a] -> IO [a]
forkJoin [] = pure []
forkJoin [act] = fmap (: []) act
forkJoin actions = do
    vars <- mapM spawn actions
    results <- mapM takeMVar vars
    mapM rethrow results
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkFinally act (putMVar var)
        pure var
{-# INLINEABLE forkJoin #-}

forkJoin_ :: [IO ()] -> IO ()
forkJoin_ [] = pure ()
forkJoin_ [act] = act
forkJoin_ actions = do
    vars <- mapM spawn actions
    results <- mapM takeMVar vars
    mapM_ rethrow results
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkFinally act (putMVar var)
        pure var
{-# INLINEABLE forkJoin_ #-}

parallelChunks :: Int -> Int -> (Int -> Int -> IO a) -> IO [a]
parallelChunks threshold n body =
    forkJoin [body lo hi | (!lo, !hi) <- chunksFor threshold n]
{-# NOINLINE parallelChunks #-} -- INLINE worsens performance here.

-- | 'parallelChunks' for chunk bodies run only for their effects.
parallelChunks_ :: Int -> Int -> (Int -> Int -> IO ()) -> IO ()
parallelChunks_ threshold n body =
    forkJoin_ [body lo hi | (!lo, !hi) <- chunksFor threshold n]
{-# INLINE parallelChunks_ #-}

parallelBounds_ :: Int -> VU.Vector Int -> (Int -> Int -> IO ()) -> IO ()
parallelBounds_ caps bs body =
    forkJoin_ [body lo hi | (!lo, !hi) <- boundsChunks caps bs]
{-# NOINLINE parallelBounds_ #-}

pooledIndices :: Int -> Int -> (Int -> IO ()) -> IO ()
pooledIndices width count body
    | count <= 0 = pure ()
    | width <= 1 = mapM_ body [0 .. count - 1]
    | otherwise = do
        next <- newIORef 0
        let worker = do
                i <- atomicModifyIORef' next (\j -> (j + 1, j))
                when (i < count) (body i >> worker)
        forkJoin_ (replicate (min width count) worker)
{-# NOINLINE pooledIndices #-}

pooledRun :: Int -> [IO a] -> IO [a]
pooledRun width actions
    | width >= n = forkJoin actions
    | otherwise = do
        next <- newIORef 0
        out <- VM.unsafeNew n
        acts <- VM.unsafeNew n
        sequence_ [VM.unsafeWrite acts i a | (i, a) <- zip [0 ..] actions]
        let worker = do
                i <- atomicModifyIORef' next (\j -> (j + 1, j))
                when (i < n) $ do
                    act <- VM.unsafeRead acts i
                    VM.unsafeWrite acts i consumed
                    r <- act
                    VM.write out i r
                    worker
        forkJoin_ (replicate width worker)
        V.toList <$> V.freeze out
  where
    n = length actions
    consumed = throwIO (ErrorCall "pooledRun: slot already consumed")
{-# INLINEABLE pooledRun #-}
