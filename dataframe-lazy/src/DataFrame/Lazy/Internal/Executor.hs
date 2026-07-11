{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

{- | Pull-based (iterator) execution engine: each operator returns a 'Stream'
yielding the next 'DataFrame' batch or 'Nothing' at end. Bounded local sources
materialise into eager whole-frame ops; unbounded sources stream in constant memory.
-}
module DataFrame.Lazy.Internal.Executor (
    CsvReader,
    execute,
    foldBatches,
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue (newTBQueueIO, readTBQueue, writeTBQueue)
import Control.Exception (evaluate)
import Control.Monad (filterM, forM, forM_, unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import Data.IORef
import Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.Map as M
import qualified Data.Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Typeable (Typeable)
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import DataFrame.IO.CSV (CsvReader)
import qualified DataFrame.IO.Parquet as Parquet
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Lazy.IO.Binary as Bin
import DataFrame.Lazy.Internal.LogicalPlan (DataSource (..), SortOrder (..))
import DataFrame.Lazy.Internal.PhysicalPlan
import qualified DataFrame.Operations.Aggregation as Agg
import qualified DataFrame.Operations.Core as Core
import qualified DataFrame.Operations.Join as Join
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Permutation as Perm
import qualified DataFrame.Operations.Subset as Sub
import qualified DataFrame.Operations.Transformations as Trans
import DataFrame.Schema (elements)
import System.Directory (doesDirectoryExist, removeFile)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import System.IO (IOMode (ReadMode), hIsEOF, withFile)
import System.IO.Temp (emptySystemTempFile)
import Type.Reflection (typeRep)

-- ---------------------------------------------------------------------------
-- Stream abstraction
-- ---------------------------------------------------------------------------

{- | A pull-based stream: each call to the action yields the next batch or
'Nothing' when the stream is exhausted.  State is captured by the closure.
-}
newtype Stream = Stream {pullBatch :: IO (Maybe D.DataFrame)}

{- | Wrap an already-computed 'DataFrame' as a single-batch stream: the first
pull yields it, every subsequent pull yields 'Nothing'.  Used by blocking
operators that materialise their whole result up front.
-}
materialized :: D.DataFrame -> IO Stream
materialized df = do
    ref <- newIORef (Just df)
    return . Stream $ do
        mb <- readIORef ref
        writeIORef ref Nothing
        return mb

{- | Drain all batches from a stream and concatenate them into one DataFrame.
Columns are concatenated in a single multi-way pass (O(rows)) rather than a
left-fold of @acc <> batch@ that would recopy the accumulator on every step.
-}
collectStream :: Stream -> IO D.DataFrame
collectStream stream = go []
  where
    go acc = do
        mb <- pullBatch stream
        case mb of
            Nothing -> return (concatBatches (reverse acc))
            Just df -> go (df : acc)

-- | Concatenate a list of same-schema batches column-by-column in one pass.
concatBatches :: [D.DataFrame] -> D.DataFrame
concatBatches [] = D.empty
concatBatches [df] = df
concatBatches batches@(first : _) =
    D.fromNamedColumns
        [ (name, C.concatManyColumns [D.unsafeGetColumn name b | b <- batches])
        | name <- D.columnNames first
        ]

-- ---------------------------------------------------------------------------
-- Boundedness analysis
-- ---------------------------------------------------------------------------

{- | True when every scan leaf is a finite local source, so the whole result can
be materialised and routed through the eager whole-frame ops. False for
online/streaming sources, which keep the constant-memory streaming paths.
-}
isBounded :: PhysicalPlan -> Bool
isBounded (PhysicalScan (CsvSource{}) _) = True
isBounded (PhysicalScan (CsvSourceStreaming{}) _) = False
isBounded (PhysicalScan (ParquetSource _) _) = True
isBounded (PhysicalProject _ c) = isBounded c
isBounded (PhysicalFilter _ c) = isBounded c
isBounded (PhysicalDerive _ _ c) = isBounded c
isBounded (PhysicalLimit _ c) = isBounded c
isBounded (PhysicalSort _ c) = isBounded c
isBounded (PhysicalHashAggregate _ _ c) = isBounded c
isBounded (PhysicalSpill c _) = isBounded c
isBounded (PhysicalSourceDF _ _) = True
isBounded (PhysicalHashJoin _ _ _ l r) = isBounded l && isBounded r
isBounded (PhysicalSortMergeJoin _ _ _ l r) = isBounded l && isBounded r

-- ---------------------------------------------------------------------------
-- Top-level entry point
-- ---------------------------------------------------------------------------

{- | Execute a physical plan, returning the complete result as a single
'DataFrame'.
-}
execute :: PhysicalPlan -> IO D.DataFrame
execute plan = buildStream plan >>= collectStream

{- | Fold a function over every batch produced by a physical plan.
The fold is strict in the accumulator; each batch is discarded after folding.
-}
foldBatches ::
    (b -> D.DataFrame -> IO b) -> b -> PhysicalPlan -> IO b
foldBatches f seed plan = do
    stream <- buildStream plan
    let loop !acc = do
            mb <- pullBatch stream
            case mb of
                Nothing -> return acc
                Just batch -> do
                    !acc' <- f acc batch
                    loop acc'
    loop seed

-- ---------------------------------------------------------------------------
-- Per-operator stream builders
-- ---------------------------------------------------------------------------

buildStream :: PhysicalPlan -> IO Stream
buildStream (PhysicalScan (CsvSource path sep reader) cfg) =
    executeCsvScan path sep reader cfg
buildStream (PhysicalScan (CsvSourceStreaming path _sep reader) cfg) =
    executeCsvScanStreaming path reader cfg
buildStream (PhysicalScan (ParquetSource path) cfg) =
    executeParquetScan path cfg
buildStream (PhysicalSpill child path) = do
    df <- execute child
    Bin.spillToDisk path df
    df' <- Bin.readSpilled path
    materialized df'
buildStream (PhysicalFilter p child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.filterWhere p) mb
        )
buildStream (PhysicalProject cols child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.select cols) mb
        )
buildStream (PhysicalDerive name uexpr child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Trans.deriveMany [(name, uexpr)]) mb
        )
buildStream (PhysicalLimit n child) = do
    childStream <- buildStream child
    countRef <- newIORef (0 :: Int)
    return . Stream $
        ( do
            remaining <- readIORef countRef
            if remaining >= n
                then return Nothing
                else do
                    mb <- pullBatch childStream
                    case mb of
                        Nothing -> return Nothing
                        Just df -> do
                            let toTake = min (Core.nRows df) (n - remaining)
                            modifyIORef' countRef (+ toTake)
                            return $ Just (Sub.take toTake df)
        )
buildStream (PhysicalSort cols child) = do
    df <- execute child
    let sortOrds = fmap (toPermSortOrder df) cols
    materialized (Perm.sortBy sortOrds df)
buildStream (PhysicalHashAggregate keys aggs child)
    | isBounded child = do
        df <- execute child
        let result = Agg.aggregate aggs (Agg.groupBy keys df)
        materialized result
buildStream (PhysicalHashAggregate keys aggs child) = do
    childStream <- buildStream child
    if all (isStreamableAgg . snd) aggs
        then do
            let (partialAggs, mergeAggs, finalizer) = buildAggPlan aggs
            nCaps <- getNumCapabilities
            let workers = max 1 nCaps
            partials <-
                mapConcurrently
                    (\_ -> workerLoop childStream keys partialAggs mergeAggs)
                    [1 .. workers]
            mFinal <-
                let nonEmpty = Data.Maybe.catMaybes partials
                 in case nonEmpty of
                        [] -> return Nothing
                        [single] -> return (Just (finalizer single))
                        (a : rest) -> do
                            !merged <- mergePartials keys mergeAggs a rest
                            return (Just (finalizer merged))
            ref <- newIORef mFinal
            return . Stream $ do
                mb <- readIORef ref
                writeIORef ref Nothing
                return mb
        else do
            df <- collectStream childStream
            materialized (Agg.aggregate aggs (Agg.groupBy keys df))
buildStream (PhysicalSourceDF bs df) = do
    let total = Core.nRows df
    posRef <- newIORef (0 :: Int)
    return . Stream $ do
        i <- readIORef posRef
        if i >= total
            then return Nothing
            else do
                let n = min bs (total - i)
                    batch = Sub.range (i, i + n) df
                writeIORef posRef (i + n)
                return (Just batch)
buildStream (PhysicalHashJoin jt leftKey rightKey leftPlan rightPlan)
    | isBounded leftPlan = do
        leftDf <- execute leftPlan
        rightDf <- execute rightPlan
        materialized (performJoin jt leftKey rightKey leftDf rightDf)
buildStream (PhysicalHashJoin jt leftKey rightKey leftPlan rightPlan) =
    case jt of
        Join.INNER -> streamingHashJoin assembleInnerBatch
        Join.LEFT -> streamingHashJoin assembleLeftBatch
        _ -> do
            leftDf <- execute leftPlan
            rightDf <- execute rightPlan
            materialized (performJoin jt leftKey rightKey leftDf rightDf)
  where
    streamingHashJoin assembleFn = do
        rightDf <- execute rightPlan
        let rightDf' =
                if leftKey == rightKey
                    then rightDf
                    else Core.rename rightKey leftKey rightDf
            joinKey = leftKey
            csSet = S.fromList [joinKey]
            rightHashes = Join.buildHashColumn [joinKey] rightDf'
            ci = Join.buildCompactIndex rightHashes
        leftStream <- buildStream leftPlan
        return . Stream $ do
            mBatch <- pullBatch leftStream
            case mBatch of
                Nothing -> return Nothing
                Just probeBatch -> do
                    let probeHashes = Join.buildHashColumn [joinKey] probeBatch
                        (probeIxs, buildIxs) = Join.hashProbeKernel ci probeHashes
                    return . Just $ assembleFn csSet probeBatch rightDf' probeIxs buildIxs

    assembleLeftBatch csSet probeBatch rightDf' probeIxs buildIxs =
        let batchN = Core.nRows probeBatch
            matched =
                VU.accumulate
                    (\_ b -> b)
                    (VU.replicate batchN False)
                    (VU.map (,True) probeIxs)
            unmatchedIxs = VU.findIndices not matched
            allProbeIxs = probeIxs VU.++ unmatchedIxs
            allBuildIxs = buildIxs VU.++ VU.replicate (VU.length unmatchedIxs) (-1)
         in Join.assembleLeft csSet probeBatch rightDf' allProbeIxs allBuildIxs

    assembleInnerBatch = Join.assembleInner
buildStream (PhysicalSortMergeJoin jt leftKey rightKey leftPlan rightPlan) = do
    leftDf <- execute leftPlan
    rightDf <- execute rightPlan
    materialized (performJoin jt leftKey rightKey leftDf rightDf)

-- ---------------------------------------------------------------------------
-- Streaming aggregation helpers
-- ---------------------------------------------------------------------------

{- | One worker's loop: pull batches off the shared child stream until
exhausted, building up a per-worker accumulator.
-}
workerLoop ::
    Stream ->
    [T.Text] ->
    [E.NamedExpr] ->
    [E.NamedExpr] ->
    IO (Maybe D.DataFrame)
workerLoop childStream keys partialAggs mergeAggs = loop Nothing
  where
    loop !acc = do
        mb <- pullBatch childStream
        case mb of
            Nothing -> return acc
            Just batch -> do
                !partial <-
                    evaluate . D.forceDataFrame $
                        Agg.aggregate partialAggs (Agg.groupBy keys batch)
                !next <- case acc of
                    Nothing -> return (Just partial)
                    Just a -> do
                        !merged <-
                            evaluate . D.forceDataFrame $
                                Agg.aggregate mergeAggs (Agg.groupBy keys (a <> partial))
                        return (Just merged)
                loop next

-- | Merge a head accumulator with the rest of the workers' partials.
mergePartials ::
    [T.Text] ->
    [E.NamedExpr] ->
    D.DataFrame ->
    [D.DataFrame] ->
    IO D.DataFrame
mergePartials keys mergeAggs = go
  where
    go !acc [] = return acc
    go !acc (p : ps) = do
        !merged <-
            evaluate . D.forceDataFrame $
                Agg.aggregate mergeAggs (Agg.groupBy keys (acc <> p))
        go merged ps

isStreamableAgg :: E.UExpr -> Bool
isStreamableAgg (E.UExpr (E.Agg (E.CollectAgg _ _) _)) = False
isStreamableAgg (E.UExpr (E.Agg (E.FoldAgg _ Nothing (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> True
        Nothing -> False
isStreamableAgg (E.UExpr (E.Agg (E.FoldAgg _ (Just _) (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> True
        Nothing ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> True
                Nothing -> False
isStreamableAgg (E.UExpr (E.Agg (E.MergeAgg{}) _)) = True
isStreamableAgg _ = False

{- | Build the (partial, merge, finalizer) plan for a list of streamable
aggregates: @partialAggs@ run per batch, @mergeAggs@ combine two partial
results, and @finalizer@ post-processes (for 'MergeAgg' acc≠output types).
-}
buildAggPlan ::
    [(T.Text, E.UExpr)] ->
    ( [(T.Text, E.UExpr)]
    , [(T.Text, E.UExpr)]
    , D.DataFrame -> D.DataFrame
    )
buildAggPlan aggs = foldl combine ([], [], id) (map processAgg aggs)
  where
    combine (p1, m1, f1) (p2, m2, f2) = (p1 ++ p2, m1 ++ m2, f1 . f2)

    processAgg ::
        (T.Text, E.UExpr) ->
        ([(T.Text, E.UExpr)], [(T.Text, E.UExpr)], D.DataFrame -> D.DataFrame)
    processAgg (name, ue) = case ue of
        E.UExpr (E.Agg (E.FoldAgg n Nothing (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl ->
                    ( [(name, ue)]
                    , [(name, E.UExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                    , id
                    )
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Int) of
                        Just Refl ->
                            ( [(name, ue)]
                            ,
                                [
                                    ( name
                                    , E.UExpr
                                        (E.Agg (E.FoldAgg "sum" Nothing ((+) :: Int -> Int -> Int)) (E.Col @Int name))
                                    )
                                ]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        E.UExpr (E.Agg (E.FoldAgg n (Just _) (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    ( [(name, ue)]
                    ,
                        [
                            ( name
                            , E.UExpr
                                (E.Agg (E.FoldAgg "sum" Nothing ((+) :: Int -> Int -> Int)) (E.Col @Int name))
                            )
                        ]
                    , id
                    )
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl ->
                            ( [(name, ue)]
                            , [(name, E.UExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        E.UExpr
            ( E.Agg
                    ( E.MergeAgg
                            n
                            seed
                            (step :: acc -> b -> acc)
                            (merge :: acc -> acc -> acc)
                            (fin :: acc -> a)
                        )
                    (inner :: E.Expr b)
                ) ->
                let partialExpr =
                        E.UExpr
                            ( E.Agg
                                (E.MergeAgg n seed step merge (id :: acc -> acc))
                                inner
                            )
                    mergeExpr =
                        E.UExpr
                            ( E.Agg
                                (E.FoldAgg ("merge_" <> n) Nothing merge)
                                (E.Col @acc name)
                            )
                    finalize df =
                        let accCol = D.unsafeGetColumn name df
                            finalCol =
                                either
                                    (error "buildAggPlan: MergeAgg finalize failed")
                                    id
                                    (C.mapColumn @acc @a fin accCol)
                         in D.insertColumn name finalCol df
                 in ( [(name, partialExpr)]
                    , [(name, mergeExpr)]
                    , finalize
                    )
        _ -> ([(name, ue)], [(name, ue)], id)

-- ---------------------------------------------------------------------------
-- Parquet scan implementation
-- ---------------------------------------------------------------------------

{- | Scan a Parquet file, directory, or glob.  Each file becomes one batch.
Column projection and predicate pushdown are forwarded to 'readParquetWithOpts'
via 'ParquetReadOptions'.
-}
executeParquetScan :: FilePath -> ScanConfig -> IO Stream
executeParquetScan path cfg = do
    isDir <- doesDirectoryExist path
    let pat = if isDir then path </> "*" else path
    matches <- glob pat
    files <- filterM (fmap not . doesDirectoryExist) matches
    when (null files) $
        error ("executeParquetScan: no parquet files found for " ++ path)
    let opts =
            Parquet.defaultParquetReadOptions
                { Parquet.selectedColumns = Just (M.keys (elements (scanSchema cfg)))
                , Parquet.predicate = scanPushdownPredicate cfg
                }
    ref <- newIORef files
    return . Stream $ do
        fs <- readIORef ref
        case fs of
            [] -> return Nothing
            (f : rest) -> do
                writeIORef ref rest
                Just <$> Parquet.readParquetWithOpts opts f

-- ---------------------------------------------------------------------------
-- CSV scan implementation
-- ---------------------------------------------------------------------------

{- | SIMD-parallel CSV scan: the file is split at newline boundaries into one
slice per capability, parsed concurrently, sliced into batches, and fed through
a bounded queue. Pushdown predicates are applied per batch by the consumer.
-}
executeCsvScan :: FilePath -> Char -> CsvReader -> ScanConfig -> IO Stream
executeCsvScan path _sep reader cfg = do
    nCaps <- getNumCapabilities
    chunkPaths <- splitCsvAtNewlines (max 1 nCaps) path

    let schema = scanSchema cfg
        batchSz = scanBatchSize cfg
    chunkDfs <- mapConcurrently (reader schema) chunkPaths
    mapM_ removeFile chunkPaths

    queue <- newTBQueueIO (fromIntegral (max 4 (2 * nCaps)))
    _ <- forkIO $ do
        forM_ chunkDfs $ \df ->
            forM_ (sliceIntoBatches batchSz df) $ \b ->
                atomically (writeTBQueue queue (Just b))
        atomically (writeTBQueue queue Nothing)
    return . Stream $
        ( do
            mb <- atomically (readTBQueue queue)
            case mb of
                Nothing -> atomically (writeTBQueue queue Nothing) >> return Nothing
                Just df ->
                    let df' = case scanPushdownPredicate cfg of
                            Nothing -> df
                            Just p -> Sub.filterWhere p df
                     in return (Just df')
        )

executeCsvScanStreaming :: FilePath -> CsvReader -> ScanConfig -> IO Stream
executeCsvScanStreaming path reader cfg = do
    let schema = scanSchema cfg
        batchSz = scanBatchSize cfg
        windowBytes = 64 * 1024 * 1024 :: Int
    queue <- newTBQueueIO 8
    _ <- forkIO $
        withFile path ReadMode $ \h -> do
            header <- C8.hGetLine h
            let feed bytes =
                    unless (BS.null bytes) $ do
                        p <- emptySystemTempFile "lazy_csv_win_.csv"
                        BS.writeFile p (header <> BS.singleton nl <> bytes)
                        df <- reader schema p
                        removeFile p
                        forM_ (sliceIntoBatches batchSz df) $ \b ->
                            atomically (writeTBQueue queue (Just b))
                loop leftover = do
                    eof <- hIsEOF h
                    if eof
                        then feed leftover >> atomically (writeTBQueue queue Nothing)
                        else do
                            chunk <- BS.hGetSome h windowBytes
                            let buf = leftover <> chunk
                            case BS.elemIndexEnd nl buf of
                                Nothing -> loop buf
                                Just i -> feed (BS.take i buf) >> loop (BS.drop (i + 1) buf)
            loop BS.empty
    return . Stream $ do
        mb <- atomically (readTBQueue queue)
        case mb of
            Nothing -> atomically (writeTBQueue queue Nothing) >> return Nothing
            Just df ->
                let df' = case scanPushdownPredicate cfg of
                        Nothing -> df
                        Just p -> Sub.filterWhere p df
                 in return (Just df')
  where
    nl :: Word8
    nl = 0x0A

-- | Slice a 'DataFrame' into row-bounded batches of at most @n@ rows.
sliceIntoBatches :: Int -> D.DataFrame -> [D.DataFrame]
sliceIntoBatches n df =
    let total = Core.nRows df
        starts = [0, n .. total - 1]
     in [Sub.range (s, min (s + n) total) df | s <- starts]

{- | Split a CSV file at newline boundaries into @n@ temp files, each with the
original header plus a body slice. Returns the paths (caller removes them); the
per-file reader mmap's each, giving OS-paged reads instead of one monolithic read.
-}
splitCsvAtNewlines :: Int -> FilePath -> IO [FilePath]
splitCsvAtNewlines n path = do
    bs <- BS.readFile path
    let (header, rest) = BS.break (== nl) bs
        body = BS.drop 1 rest
        bodyLen = BS.length body
        rawOffsets = [(bodyLen * i) `div` n | i <- [0 .. n]]
        snapped = 0 : map (snap body) (init (drop 1 rawOffsets)) ++ [bodyLen]
        ranges = zip snapped (drop 1 snapped)
        slices =
            [ BS.take (hi - lo) (BS.drop lo body)
            | (lo, hi) <- ranges
            , hi > lo
            ]
    forM slices $ \chunk -> do
        p <- emptySystemTempFile "lazy_csv_chunk_.csv"
        BS.writeFile p (header <> BS.singleton nl <> chunk)
        return p
  where
    nl :: Word8
    nl = 0x0A
    snap body off =
        case BS.elemIndex nl (BS.drop off body) of
            Just i -> off + i + 1
            Nothing -> BS.length body

-- ---------------------------------------------------------------------------
-- Join helper
-- ---------------------------------------------------------------------------

{- | Route a join to 'Operations.Join', renaming the right key when the names
differ. 'Join.join' keeps its first argument, so LEFT/RIGHT must pass 'leftDf'
first to retain the intended side; symmetric INNER/FULL_OUTER pass @rightDf@ first.
-}
performJoin ::
    Join.JoinType -> T.Text -> T.Text -> D.DataFrame -> D.DataFrame -> D.DataFrame
performJoin jt leftKey rightKey leftDf rightDf =
    case jt of
        Join.LEFT -> Join.join jt [leftKey] leftDf rightRenamed
        Join.RIGHT -> Join.join jt [leftKey] leftDf rightRenamed
        _ -> Join.join jt [leftKey] rightRenamed leftDf
  where
    rightRenamed
        | leftKey == rightKey = rightDf
        | otherwise = Core.rename rightKey leftKey rightDf

-- ---------------------------------------------------------------------------
-- Sort order conversion
-- ---------------------------------------------------------------------------

{- | Convert a plan-level @(column, direction)@ into a Permutation 'SortOrder',
emitting @E.Col@ at the materialised column's element type so 'Perm.sortBy'
dispatches its comparator correctly; unknown/non-'Ord' types fall back to 'T.Text'.
-}
toPermSortOrder :: D.DataFrame -> (T.Text, SortOrder) -> Perm.SortOrder
toPermSortOrder df (col, dir) =
    case M.lookup col (D.columnIndices df) of
        Nothing -> mk @T.Text
        Just idx -> dispatch (D.columns df VB.! idx)
  where
    mk :: forall a. (C.Columnable a, Ord a) => Perm.SortOrder
    mk = case dir of
        Ascending -> Perm.Asc (E.Col @a col)
        Descending -> Perm.Desc (E.Col @a col)

    dispatch :: C.Column -> Perm.SortOrder
    dispatch column = case column of
        C.PackedText{} -> mk @T.Text
        C.BoxedColumn _ (_ :: VB.Vector b) -> pick @b
        C.UnboxedColumn _ (_ :: VU.Vector b) -> pick @b

    pick :: forall b. (Typeable b) => Perm.SortOrder
    pick =
        tryT @b @Int $
            tryT @b @Double $
                tryT @b @Float $
                    tryT @b @Integer $
                        tryT @b @Int8 $
                            tryT @b @Int16 $
                                tryT @b @Int32 $
                                    tryT @b @Int64 $
                                        tryT @b @Word8 $
                                            tryT @b @Word16 $
                                                tryT @b @Word32 $
                                                    tryT @b @Word64 $
                                                        tryT @b @Bool $
                                                            tryT @b @Char $
                                                                tryT @b @T.Text (mk @T.Text)

    tryT ::
        forall b a.
        (Typeable b, C.Columnable a, Ord a) =>
        Perm.SortOrder ->
        Perm.SortOrder
    tryT fallback = case testEquality (typeRep @b) (typeRep @a) of
        Just Refl -> mk @a
        Nothing -> fallback
