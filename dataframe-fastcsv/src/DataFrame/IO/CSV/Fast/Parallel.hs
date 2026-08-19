{-# LANGUAGE TupleSections #-}

{- | Chunk-parallel extraction (Round-2 WS-E2): the data-row range is split
into contiguous, newline-aligned chunks (alignment is by construction —
chunks are row ranges over the quote-resolved delimiter index), each
worker runs every column's 'ColumnPlan' over its chunk with right-sized
builders, and the merger splices the results ('mergeColumns' /
'mergeTextChunksPar'). Inference classifies its sample once, globally,
before fan-out; when chunks resolve different types the merger re-parses
only the narrower-typed chunks.

Parallelism requires the executable to be linked with @-threaded@ and run
with @+RTS -N@; otherwise ('getNumCapabilities' == 1) the sequential path
runs. Inputs under 4 MB are always read sequentially.
-}
module DataFrame.IO.CSV.Fast.Parallel (
    buildAllColumns,
    autoChunkCount,
) where

import qualified Data.Text as T
import qualified Data.Vector.Storable as VS

import Control.Concurrent (getNumCapabilities)
import Control.Exception (evaluate, throwIO)
import Data.Either (partitionEithers)
import qualified Data.List as L
import Debug.Trace (traceMarkerIO)
import System.Mem (performMajorGC)

import DataFrame.IO.CSV.Fast.Columns
import DataFrame.IO.CSV.Fast.Passes (NullSpec, PassCol (..))
import DataFrame.IO.CSV.Fast.TextMerge (mergeTextChunksPar)
import DataFrame.IO.CSV.Fast.Workers (pooledRun)
import DataFrame.Internal.Column (Column, forceColumn)
import DataFrame.Internal.ColumnBuilder (mergeColumns)
import DataFrame.Internal.DictEncode (dictCompactColumn)
import DataFrame.Operations.Typing (SafeReadMode)

-- | Below this input size the fan-out overhead outweighs the parallelism.
parallelThresholdBytes :: Int
parallelThresholdBytes = 4 * 1024 * 1024

{- | Chunk count for an automatic read: four chunks per capability (the
worker pool bounds concurrency at the capability count; the extra chunks
absorb straggler imbalance), or the sequential path (1) on a single
capability or a small input.
-}
autoChunkCount :: Int -> IO Int
autoChunkCount contentLen = do
    caps <- getNumCapabilities
    pure $
        if caps <= 1 || contentLen < parallelThresholdBytes
            then 1
            else caps * 4

-- | What one worker produced for one column over its chunk.
data ChunkCol
    = CCChain !ChunkOutcome
    | -- | @Left@ carries the global data-row index of the first failure.
      CCSchema !(Either Int PassCol)
    | -- | Legacy (cold-path) columns are built sequentially by the merger.
      CCSkip

-- | Splice homogeneous chunk passes (text chunks merge at the byte level).
mergePassCols :: Int -> [PassCol] -> IO Column
mergePassCols width ps@(PassText{} : _) =
    mergeTextChunksPar width [tc | PassText tc <- ps]
mergePassCols _ ps = pure $! mergeColumns [c | PassFull c <- ps]

{- | Build every column, fanning the row range out over @nChunks@ chunks
on a capability-wide worker pool (sequentially when @nChunks <= 1@). Same
result as the sequential path by construction; see the chunk-determinism
tests and property.
-}
buildAllColumns ::
    Int -> ColumnEnv -> [(T.Text, Int)] -> IO [(Column, Bool)]
buildAllColumns nChunks env wanted
    | chunks <= 1 =
        mapM (uncurry (buildColumn env)) wanted
    | otherwise = do
        width <- getNumCapabilities
        plans <-
            mapM
                (\(name, fieldIx) -> evaluate (planColumn env name fieldIx))
                wanted
        let fields = map snd wanted
            ranges = chunkRanges chunks (ceNumRow env)
        traceMarkerIO "fastcsv:fanout"
        perChunk <- pooledRun width [runChunk env (zip fields plans) r | r <- ranges]
        traceMarkerIO "fastcsv:join-done"
        let byCol = L.transpose perChunk
        mapM_ (mapM_ (\cc -> cc `seq` pure ())) byCol
        -- Reset the major-GC trigger here, where the copyable live set is
        -- small (chunk payloads are large objects). Otherwise the heap
        -- doubling crosses its threshold mid-merge and a ~0.5s gen-1
        -- collection lands at the worst point of the read.
        performMajorGC
        -- Merge in parallel too (chunk-level inside each column, pooled
        -- across columns), forcing each spliced column here so no memcpy
        -- is deferred to the final forceDataFrame walk. 'evaluate' resolves
        -- the plan dispatch now, so a schema task is a closure over its own
        -- column's data only — never 'env'.
        tasks <-
            mapM
                (\(wp, cps) -> evaluate (mergeTask width env ranges wp cps))
                (zip (zip wanted plans) byCol)
        merged <- pooledRun width tasks
        traceMarkerIO "fastcsv:merge-done"
        pure merged
  where
    chunks = min nChunks (ceNumRow env)

mergeTask ::
    Int ->
    ColumnEnv ->
    [(Int, Int)] ->
    ((T.Text, Int), ColumnPlan) ->
    [ChunkCol] ->
    IO (Column, Bool)
mergeTask width env ranges ((name, fieldIx), plan) colParts =
    case plan of
        PlanSchema mode _ t -> mergeSchema width name mode t parts
        _ -> forced (mergeColumn width env name plan fieldIx parts)
  where
    parts = zip ranges colParts

-- | Splice a schema column's chunks; no 'ColumnEnv' capture by design.
mergeSchema ::
    Int ->
    T.Text ->
    SafeReadMode ->
    TargetType ->
    [((Int, Int), ChunkCol)] ->
    IO (Column, Bool)
mergeSchema width name mode t parts =
    case partitionEithers [r | (_, CCSchema r) <- parts] of
        ([], cs) -> forced $ do
            c <- mergePassCols width cs
            pure (finishMode mode (dictCompactColumn c), False)
        (failed, _) -> throwIO (schemaError name t (minimum failed))

-- | Force the merged payload inside the task, not on the result walk.
forced :: IO (Column, Bool) -> IO (Column, Bool)
forced act = do
    r@(col, _) <- act
    forceColumn col `seq` pure r

-- | Even split of @n@ rows into @k@ @(offset, length)@ ranges.
chunkRanges :: Int -> Int -> [(Int, Int)]
chunkRanges k n =
    [ (q * i + min i r, q + (if i < r then 1 else 0))
    | i <- [0 .. k - 1]
    ]
  where
    (q, r) = n `divMod` k

-- | Restrict an env to a row sub-range (builder hints scaled to match).
chunkEnv :: ColumnEnv -> (Int, Int) -> ColumnEnv
chunkEnv env (off, len) =
    env
        { ceRows = VS.slice off len (ceRows env)
        , ceNumRow = len
        , ceTextHint =
            max 64 ((ceTextHint env * len) `div` max 1 (ceNumRow env))
        }

-- | One worker: run every column's plan over one chunk of rows.
runChunk :: ColumnEnv -> [(Int, ColumnPlan)] -> (Int, Int) -> IO [ChunkCol]
runChunk env plans range@(off, _) = mapM (uncurry build) plans
  where
    cenv = chunkEnv env range
    build col plan = case plan of
        PlanLegacy{} -> pure CCSkip
        PlanSchema mode nspec t ->
            CCSchema <$> runSchemaChunk cenv nspec mode t col off
        PlanChain _ nspec steps checkAllNull ->
            CCChain <$> runChainChunk cenv nspec steps checkAllNull col

{- | Splice one column's chunk results back together, resolving chunk-level
type promotion. Mirrors 'buildColumn' for the single-chunk case.
-}
mergeColumn ::
    Int ->
    ColumnEnv ->
    T.Text ->
    ColumnPlan ->
    Int ->
    [((Int, Int), ChunkCol)] ->
    IO (Column, Bool)
mergeColumn width env name plan col parts = case plan of
    PlanLegacy mode pwt -> (,pwt) <$> legacyColumn env mode col
    PlanSchema mode _ t -> mergeSchema width name mode t parts
    PlanChain mode nspec steps _ -> do
        c <- mergeChain width env nspec steps col [(r, oc) | (r, CCChain oc) <- parts]
        pure (finishMode mode (dictCompactColumn c), False)

{- | Merge chain-plan chunks. The candidate type starts at the widest chunk
resolution; any chunk that resolved narrower is re-parsed at the candidate
step, and a re-parse failure escalates the candidate further down the
chain (exactly the rows the sequential pass would have failed on). The
final 'StepText' never fails, so this terminates.
-}
mergeChain ::
    Int ->
    ColumnEnv ->
    NullSpec ->
    [Step] ->
    Int ->
    [((Int, Int), ChunkOutcome)] ->
    IO Column
mergeChain width env nspec steps col parts
    | null resolved = pure (allNullColumn (ceNumRow env))
    | otherwise = settle (maximum resolved)
  where
    resolved = [i | (_, Resolved i _) <- parts]
    settle cand = do
        results <- mapM (reparse cand) parts
        case sequence results of
            Just cs -> mergePassCols width cs
            Nothing -> settle (cand + 1)
    reparse cand (range, oc) = case oc of
        Resolved i c | i == cand -> pure (Just c)
        _ -> do
            r <- runStep (chunkEnv env range) nspec col False (steps !! cand)
            pure (either (const Nothing) Just r)
