{- | Thread fan-out primitives shared by the parallel scan, the chunk
parse and the parallel merge: plain 'forkFinally' workers joined through
'MVar's (no sparks), with a counter-based pool for finer-grained chunks.
-}
module DataFrame.IO.CSV.Fast.Workers (
    forkJoin,
    pooledRun,
) where

import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM

import Control.Concurrent (forkFinally)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (ErrorCall (..), SomeException, throwIO, try)
import Control.Monad (when)
import Data.IORef (atomicModifyIORef', newIORef)

-- | Run each action in its own thread; rethrow the first failure in order.
forkJoin :: [IO a] -> IO [a]
forkJoin actions = do
    vars <- mapM spawn actions
    results <- mapM takeMVar vars
    either (throwIO :: SomeException -> IO [a]) pure (sequence results)
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkFinally act (putMVar var)
        pure var

{- | Run the actions on a pool of @width@ threads (work-stealing via a
shared counter), so finer-grained chunks balance load without running
every chunk's builders concurrently. Results keep their input order.

Each action slot is cleared before the action runs, so data captured by a
completed closure is unreachable as soon as it finishes — callers rely on
this to release per-column chunk payloads during the parallel merge.
-}
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
        _ <- forkJoin (replicate width worker)
        V.toList <$> V.freeze out
  where
    n = length actions
    consumed = throwIO (ErrorCall "pooledRun: slot already consumed")
