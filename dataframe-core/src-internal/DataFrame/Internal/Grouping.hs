{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Grouping (
    groupBy,
    groupBySeq,
    groupByPar,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throw, throwIO, try)
import Control.Monad
import Control.Monad.ST (ST, runST)
import Data.Bits (unsafeShiftR, (.&.))
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    bitmapTestBit,
    materializeMerged,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.DictEncode (dictEncodeColumnUpTo)
import DataFrame.Internal.GroupingDirect (
    ascendingCodeGroups,
    directGroupThreshold,
    rangeOf,
 )
import qualified DataFrame.Internal.GroupingDirect as GD
import DataFrame.Internal.GroupingPar (parallelAssignGroups, shouldParallelize)
import DataFrame.Internal.Hash
import DataFrame.Internal.HashTable (htInsert, newHashTable)
import DataFrame.Internal.PackedText (
    PackedTextData (..),
    offCount,
    packedLength,
    packedSlice,
    selAt,
    sliceEqBytes,
 )
import DataFrame.Internal.RadixRank (rankByHash)
import DataFrame.Internal.RowHash (computeRowHashesWithIO)
import DataFrame.Internal.Types
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

{- | O(k * n) group the dataframe by the given key columns, bucketing rows with an
open-addressing hash table that re-verifies keys on each hash hit. Groups are
numbered in first-appearance order; 'valueIndices'/'offsets' follow by counting sort.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | nRows df == 0 =
        Grouped
            df
            names
            VU.empty
            (VU.fromList [0])
            VU.empty
    | Just dg <- tryDirectGroup names df = dg
    | shouldParallelize n = groupByPar names df
    | otherwise = groupBySeq names df
  where
    !n = nRows df

{- | Low-cardinality direct-indexed grouping fast path: fires for key lists
where every key is a single clean small-range @Int@ column or a canonical
dict-encoded text column, and the product of the key domains stays within
'directGroupThreshold' (the keys fuse into one mixed-radix code; a single key
degenerates to its own code). Returns 'Nothing' on any other key shape, falling
back to the hash path.

Narrow domains build @offsets@/@groupRepRows@ eagerly (histogram-sized work
only) and leave BOTH per-row outputs lazy: @valueIndices@ ('visFromCodes')
only materializes for consumers that gather (median/top-k, set ops,
interpreter slices), and @rowToGroup@ ('rtgFromCodes') only for the streaming
scatter aggregations — each aggregate pays for exactly one O(n) output pass,
not both. Wide domains run the two-level radix engine instead
('DataFrame.Internal.GroupingDirect.directLayoutLazy'): @rowToGroup@ eager,
@valueIndices@ deferred (see 'fusedDirectGroup').
-}
tryDirectGroup :: [T.Text] -> DataFrame -> Maybe GroupedDataFrame
tryDirectGroup [] _ = Nothing
tryDirectGroup names df = do
    cols <- traverse (\nm -> M.lookup nm (columnIndices df) >>= (columns df V.!?)) names
    case traverse fusedKey cols of
        Just keys -> fusedDirectGroup names df keys
        Nothing -> case (names, cols) of
            ([name], [col]) -> tryDictGroup (nRows df) df [name] col
            _ -> Nothing

{- | One key column of a fused multi-key direct grouping: a per-row component
code in @[0, fkDomain)@ (negative marks an invalid/corrupt code, which aborts
the direct path).
-}
data FusedKey = FusedKey
    { fkCode :: Int -> Int
    , fkDomain :: !Int
    }

{- | Classify a key column for the fused multi-key direct path: a clean non-null
unboxed @Int@ of small range, or a non-null canonical dict-encoded text column
of small dictionary. Anything else falls back to the hash group-by.
-}
fusedKey :: Column -> Maybe FusedKey
fusedKey (UnboxedColumn Nothing (v :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int)
    , not (VU.null v) =
        let (!mn, !mx) = rangeOf v
            !range = mx - mn + 1
         in if range >= 1 && range <= directGroupThreshold
                then Just (FusedKey (\i -> VU.unsafeIndex v i - mn) range)
                else Nothing
fusedKey (PackedText Nothing p)
    | Just sel <- ptSel p
    , ptCanonicalSel p =
        let offs = ptOffsets p
            !card = offCount offs - 1
         in if card >= 1 && card <= directGroupThreshold
                then
                    Just
                        ( FusedKey
                            (\i -> let c = selAt sel i in if c >= card then -1 else c)
                            card
                        )
                else Nothing
fusedKey _ = Nothing

{- | Fuse the per-key codes into one mixed-radix code per row
(@((k1*d2)+k2)*d3+...@) and feed the direct counting-sort machinery. Group
order: ascending fused code (lexicographic in key order) — ascending value
order for @Int@ keys (mirroring the order the single-@Int@-key direct path
always had) and ascending dictionary code for dict-encoded text keys (the
dictionary's first-appearance order, a fixed property of the column). The
ascending order keeps @codeToGroup@ an identity map whenever the domain is
fully occupied, so the deferred @rowToGroup@ pass skips its per-row random
remap lookup; ranking dict groups by string hash instead (the historical
order) profiled ~0.6s slower per 1e8 rows at 1e6 groups.

On the narrow-domain engine, @valueIndices@ and @rowToGroup@ are passed to the
constructor as unevaluated applications of 'visFromCodes' / 'rtgFromCodes'
(constructor arguments are not forced even under @-XStrict@, and the fields
are lazy at their definition site), so each per-row output pass is deferred
until a consumer demands it. The wide-domain engine defers only
@valueIndices@ (see the branch comment below).
-}
fusedDirectGroup :: [T.Text] -> DataFrame -> [FusedKey] -> Maybe GroupedDataFrame
fusedDirectGroup names df keys = do
    domain <- fusedDomain (map fkDomain keys)
    let n = nRows df
        codeAt' = fusedCodeAt keys
    if GD.useTwoLevel n domain
        then do
            {- Wide domains (> ~1024 codes at parallel scale): the two-level
            radix engine — no pass random-writes a multi-megabyte table per
            worker, unlike the per-chunk direct histograms below (measured ~2x
            on the eager layout at 1e6 codes / 1e8 rows). It builds
            @rowToGroup@ eagerly (the streaming aggregations force it first
            thing anyway); only @valueIndices@ stays deferred, reconstructed
            from @rowToGroup@ by the same engine on demand. -}
            (rtg, offs, reps, nGroups) <-
                GD.directLayoutLazy codeAt' n domain ascendingCodeGroups
            Just
                ( GroupedInternal
                    df
                    names
                    (GD.visFromRowToGroup n nGroups offs rtg)
                    offs
                    rtg
                    reps
                )
        else do
            (offs, reps, counts, ctg, hists, nGroups) <-
                directLayoutLazy codeAt' n domain ascendingCodeGroups
            Just
                ( GroupedInternal
                    df
                    names
                    (visFromCodes codeAt' counts ctg offs hists n domain)
                    offs
                    (rtgFromCodes codeAt' ctg n)
                    reps
                )

{- | Product of the per-key domains, 'Nothing' once it (or any factor) passes
'directGroupThreshold'. Factors are capped before multiplying, so the running
product never exceeds @threshold^2@ and cannot overflow.
-}
fusedDomain :: [Int] -> Maybe Int
fusedDomain = go 1
  where
    go !acc [] = Just acc
    go !acc (d : ds)
        | d < 1 || d > directGroupThreshold = Nothing
        | acc * d > directGroupThreshold = Nothing
        | otherwise = go (acc * d) ds

{- | Per-row fused mixed-radix code; @-1@ when any component code is invalid
(only possible for corrupt dict codes), making 'groupCodesMaybe' bail to the
hash path. Valid components compose to a code in @[0, product of domains)@.
-}
fusedCodeAt :: [FusedKey] -> (Int -> Int)
fusedCodeAt [] = const (-1)
fusedCodeAt (k0 : ks0) = go (fkCode k0) ks0
  where
    go f [] = f
    go f (k : ks) =
        let !d = fkDomain k
            g = fkCode k
         in go
                ( \i ->
                    let a = f i
                     in if a < 0
                            then -1
                            else let b = g i in if b < 0 then -1 else a * d + b
                )
                ks

{- | Dictionary-encode a single text key to dense int codes, then derive
@valueIndices@/@offsets@ by counting sort. Profiled slower than the fused hash
group-by on every db-benchmark question, so it always falls back ('dictGroupEnabled').
-}
tryDictGroup ::
    Int -> DataFrame -> [T.Text] -> Column -> Maybe GroupedDataFrame
tryDictGroup n df names col
    | dictGroupEnabled && not (shouldParallelize n) = do
        (codes, card) <- dictEncodeColumnUpTo dictSingleThreshold col
        let (vis, os) = indicesFromGroups codes card
        Just (Grouped df names vis os codes)
    | otherwise = Nothing

{- | Master switch for the single-key dict-encode grouping path. 'False' because
it profiled slower than the hash group-by on every db-benchmark group-by question
(see 'tryDictGroup'); the path is kept compiled and tested but not taken.
-}
dictGroupEnabled :: Bool
dictGroupEnabled = False

{- | Cardinality ceiling for the single-key dict-encode probe: it bails to 'Nothing'
once the distinct count passes this. Only consulted when 'dictGroupEnabled' is 'True'.
-}
dictSingleThreshold :: Int
dictSingleThreshold = 4096

{- | The sequential grouping path: a single open-addressing table over all rows,
canonically remapped. Always available regardless of capabilities; the parallel
path is verified equal to it by a property test.
-}
groupBySeq :: [T.Text] -> DataFrame -> GroupedDataFrame
groupBySeq names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        (rtg0, repHash, repRow) = assignGroups df indicesToGroup n
        !nGroups = VU.length repHash
        !remap = canonicalRemap repHash repRow
        !rtg = VU.map (VU.unsafeIndex remap) rtg0
        (vis, os) = indicesFromGroups rtg nGroups
     in Grouped df names vis os rtg

{- | The parallel partitioned grouping path (see 'DataFrame.Internal.GroupingPar'):
forks one task per capability, producing output bit-for-bit identical to
'groupBySeq'. Pure via 'unsafePerformIO' (deterministic thread fan-out only).
-}
groupByPar :: [T.Text] -> DataFrame -> GroupedDataFrame
groupByPar names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        -- Merged key columns are exotic; hash their eager form.
        selectedCols = map (materializeMerged . (columns df V.!)) indicesToGroup
        !eqRow = eqKeyRow df indicesToGroup
        (rtg, vis, os) = unsafePerformIO $ do
            -- Parallel row-hash kernel, bit-identical to 'computeHashes' at the
            -- same dict-code setting (grouping always hashes canonical dict
            -- columns by code; see 'hashPacked').
            hashes <- computeRowHashesWithIO True n selectedCols
            parallelAssignGroups n hashes eqRow
     in Grouped df names vis os rtg
{-# NOINLINE groupByPar #-}

-- | Column indices of the requested key columns, in column order.
keyColIndices :: [T.Text] -> DataFrame -> [Int]
keyColIndices names df =
    M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)

{- | Assign every row to a dense group id in first-appearance order. Returns
@(rowToGroup, repHash, repRow)@ — the hash and representative row of each group.
'eqKeyRow' re-verifies the real key on each hash hit so colliding keys stay apart.
-}
assignGroups ::
    DataFrame -> [Int] -> Int -> (VU.Vector Int, VU.Vector Int, VU.Vector Int)
assignGroups df indicesToGroup n = runST $ do
    hashes <- computeHashes df indicesToGroup n
    let !eqRow = eqKeyRow df indicesToGroup
    ht <- newHashTable n
    rtg <- VUM.new n
    repHashM <- VUM.new n
    repRowM <- VUM.new n
    let go !i !next
            | i >= n = pure next
            | otherwise = do
                let !h = VU.unsafeIndex hashes i
                (gid, isNew) <- htInsert ht eqRow next i h
                VUM.unsafeWrite rtg i gid
                when isNew $ do
                    VUM.unsafeWrite repHashM next h
                    VUM.unsafeWrite repRowM next i
                go (i + 1) (if isNew then next + 1 else next)
    !nGroups <- go 0 0
    frozen <- VU.unsafeFreeze rtg
    repHash <- VU.unsafeFreeze (VUM.slice 0 nGroups repHashM)
    repRow <- VU.unsafeFreeze (VUM.slice 0 nGroups repRowM)
    pure (frozen, repHash, repRow)

{- | Map each first-appearance group id to its canonical id: groups ordered by
ascending representative hash (tie-broken by representative row), making group
order a deterministic function of the key set so set ops commute. O(g), no sort.
-}
canonicalRemap :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
canonicalRemap repHash _repRow =
    runST (rankByHash (pure . VU.unsafeIndex repHash) (VU.length repHash))

{- | Compute the FNV row-hash of the key columns into a fresh unboxed vector,
mixing 'nullSalt' for null slots so a missing value never collides with a
present one of the same bits.
-}
computeHashes :: DataFrame -> [Int] -> Int -> ST s (VU.Vector Int)
computeHashes df indicesToGroup n = do
    mh <- VUM.replicate n fnvOffset
    -- Merged key columns are exotic; hash their eager form.
    let selectedCols = map (materializeMerged . (columns df V.!)) indicesToGroup
    forM_ selectedCols $ \case
        UnboxedColumn ubm (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> hashUnboxed mh ubm mixInt v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl -> hashUnboxed mh ubm mixDouble v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    hashUnboxed mh ubm (\h d -> mixInt h (fromIntegral @a @Int d)) v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            hashUnboxed mh ubm (\h d -> mixDouble h (realToFrac d :: Double)) v
                                        SFalse ->
                                            hashUnboxed mh ubm mixShow v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i t -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixText h t
                            VUM.unsafeWrite mh i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixShow h d
                            VUM.unsafeWrite mh i h'
                        )
                        v
        PackedText bm p -> hashPacked mh bm p
        MergedColumn _ _ ->
            error "computeHashes: MergedColumn is normalized before hashing"
    VU.unsafeFreeze mh

{- | Build the row-key equality predicate over the selected key columns.
@eqKeyRow df idxs a b@ is 'True' iff rows @a@ and @b@ agree on all key columns
(validity first, a null equals only a null). Used to reject hash collisions.
-}
eqKeyRow :: DataFrame -> [Int] -> Int -> Int -> Bool
eqKeyRow df indicesToGroup =
    let !preds = map (colEqRow . (columns df V.!)) indicesToGroup
        go [] _ _ = True
        go (p : ps) a b = p a b && go ps a b
     in go preds

{- | Per-column row equality respecting nulls. Two rows are equal at a column
when both are null, or both are valid and their values compare equal.
-}
colEqRow :: Column -> (Int -> Int -> Bool)
colEqRow c@(MergedColumn _ _) = colEqRow (materializeMerged c)
colEqRow (UnboxedColumn bm v) =
    let eqV a b = VU.unsafeIndex v a == VU.unsafeIndex v b
     in withNulls bm eqV
colEqRow (BoxedColumn bm v) =
    let eqV a b = V.unsafeIndex v a == V.unsafeIndex v b
     in withNulls bm eqV
colEqRow (PackedText bm p) =
    -- A canonical dictionary selection assigns equal strings the same code,
    -- so two rows are byte-equal iff their codes agree.
    let eqV = case ptSel p of
            Just sel
                | ptCanonicalSel p ->
                    \a b -> selAt sel a == selAt sel b
            _ -> \a b ->
                let (arrA, oA, lA) = packedSlice p a
                    (arrB, oB, lB) = packedSlice p b
                 in sliceEqBytes arrA oA lA arrB oB lB
     in withNulls bm eqV
{-# INLINE colEqRow #-}

{- | Wrap a value-equality with null handling: equal iff both valid and the
values agree, or both null.
-}
withNulls :: Maybe Bitmap -> (Int -> Int -> Bool) -> (Int -> Int -> Bool)
withNulls Nothing eqV = eqV
withNulls (Just bm) eqV = \a b ->
    case (bitmapTestBit bm a, bitmapTestBit bm b) of
        (True, True) -> eqV a b
        (False, False) -> True
        _ -> False
{-# INLINE withNulls #-}

{- | Derive @(valueIndices, offsets)@ from @rowToGroup@ via a stable counting
sort on the group id: a per-group count, a prefix-sum into group offsets, then a
single placement pass keeps rows in original order within each group.
-}
indicesFromGroups :: VU.Vector Int -> Int -> (VU.Vector Int, VU.Vector Int)
indicesFromGroups rtg nGroups = runST $ do
    let !n = VU.length rtg
    counts <- VUM.replicate (nGroups + 1) 0
    let countLoop !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                c <- VUM.unsafeRead counts g
                VUM.unsafeWrite counts g (c + 1)
                countLoop (i + 1)
    countLoop 0
    offsM <- VUM.new (nGroups + 1)
    let scan !k !acc
            | k > nGroups = pure ()
            | otherwise = do
                VUM.unsafeWrite offsM k acc
                c <- VUM.unsafeRead counts k
                scan (k + 1) (acc + c)
    scan 0 0
    let seed !k
            | k > nGroups = pure ()
            | otherwise = do
                s <- VUM.unsafeRead offsM k
                VUM.unsafeWrite counts k s
                seed (k + 1)
    seed 0
    vis <- VUM.new n
    let place !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                pos <- VUM.unsafeRead counts g
                VUM.unsafeWrite vis pos i
                VUM.unsafeWrite counts g (pos + 1)
                place (i + 1)
    place 0
    offs <- VU.unsafeFreeze offsM
    frozenVis <- VU.unsafeFreeze vis
    pure (frozenVis, offs)

-------------------------------------------------------------------------------
-- Deferred-placement direct grouping
-------------------------------------------------------------------------------

groupingCaps :: Int
groupingCaps = unsafePerformIO getNumCapabilities
{-# NOINLINE groupingCaps #-}

{- | Contiguous per-worker row ranges: one chunk per capability above the
parallel threshold, a single chunk otherwise.
-}
directRowChunks :: Int -> [(Int, Int)]
directRowChunks n
    | not (shouldParallelize n) = [(0, n)]
    | otherwise = splitChunkRange groupingCaps n

-- | @k@ near-equal contiguous subranges of @[0, n)@, empties dropped.
splitChunkRange :: Int -> Int -> [(Int, Int)]
splitChunkRange k n =
    [ (lo, hi)
    | w <- [0 .. k - 1]
    , let lo = min n (w * per)
    , let hi = min n (lo + per)
    , lo < hi
    ]
  where
    per = (n + k - 1) `div` k

-- | Like 'directRowChunks' but over a code/group domain (merge/seed passes).
directCodeSlices :: Int -> [(Int, Int)]
directCodeSlices card
    | card < 4096 || groupingCaps <= 1 = [(0, card)]
    | otherwise = splitChunkRange groupingCaps card

{- | Run each action on its own thread and collect the results in order;
rethrow the first failure. A single action runs on the calling thread.
-}
parForkJoin :: [IO a] -> IO [a]
parForkJoin [act] = fmap (: []) act
parForkJoin actions = do
    vars <- mapM spawn actions
    rs <- mapM takeMVar vars
    mapM (either (throwIO @SomeException) pure) rs
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkIO (try act >>= putMVar var)
        pure var

{- | The eager phases of the direct counting-sort grouping — WITHOUT either
per-row output pass: per-chunk validated code histograms (parallel), per-code
totals and first occurrences (parallel over code slices), the caller-chosen
code->group mapping, the offsets prefix scan and per-group representative
rows. Returns
@(offsets, groupRepRows, counts, codeToGroup, chunkHists, nGroups)@ — the last
three feed the deferred @valueIndices@ placement ('visFromCodes') and
@rowToGroup@ ('rtgFromCodes') thunks, so a consumer pays only for the per-row
output it actually demands. 'Nothing' when any row's code falls outside
@[0, card)@ (fall back to hashing).

Pure w.r.t. its immutable inputs: the fork fan-out is a fixed function of the
row count and capability count, and every merge runs in fixed chunk order, so
the result is deterministic and the 'unsafePerformIO' is safe.
-}
directLayoutLazy ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    Maybe
        ( VU.Vector Int
        , VU.Vector Int
        , VU.Vector Int
        , VU.Vector Int
        , [VU.Vector Int]
        , Int
        )
directLayoutLazy codeAt n card mkGroups
    | n <= 0 || card <= 0 = Nothing
    | n < packedRowLimit = unsafePerformIO $ do
        -- Packed variant: count and first-occurrence row share one word per
        -- code, keeping phase 1 at a single accumulator array per chunk
        -- (measured ~0.15s/1e8 rows cheaper than a second firstOcc array).
        let chunks = directRowChunks n
        parts <- parForkJoin [histFirstChunkPacked codeAt card lo hi | (lo, hi) <- chunks]
        if not (all snd parts)
            then pure Nothing
            else finishLayout card mkGroups (map fst parts) $ \histsM totalsM firstAllM lo hi ->
                sumFirstSlicePacked histsM totalsM firstAllM lo hi
    | otherwise = unsafePerformIO $ do
        -- Fallback for gigantic frames where a row index does not fit the
        -- packed word: separate count and firstOcc arrays, same results.
        let chunks = directRowChunks n
        parts <- parForkJoin [histFirstChunk codeAt card lo hi | (lo, hi) <- chunks]
        if not (all (\(_, _, ok) -> ok) parts)
            then pure Nothing
            else
                finishLayout
                    card
                    mkGroups
                    (map (\(h, _, _) -> h) parts)
                    ( \histsM totalsM firstAllM lo hi ->
                        sumFirstSlice histsM (map (\(_, f, _) -> f) parts) totalsM firstAllM lo hi
                    )
{-# NOINLINE directLayoutLazy #-}

{- | Shared tail of 'directLayoutLazy': run the totals/first-occurrence merge
(which also normalizes each chunk histogram to plain counts, see
'sumFirstSlicePacked'), derive the group mapping, offsets and representative
rows, and freeze the retained chunk histograms.
-}
finishLayout ::
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    [VUM.IOVector Int] ->
    ([VUM.IOVector Int] -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()) ->
    IO
        ( Maybe
            ( VU.Vector Int
            , VU.Vector Int
            , VU.Vector Int
            , VU.Vector Int
            , [VU.Vector Int]
            , Int
            )
        )
finishLayout card mkGroups histsM mergeSlice = do
    totalsM <- VUM.new card
    firstAllM <- VUM.new card
    _ <-
        parForkJoin
            [ mergeSlice histsM totalsM firstAllM lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    counts <- VU.unsafeFreeze totalsM
    firstAll <- VU.unsafeFreeze firstAllM
    let (codeToGroup, nGroups) = mkGroups counts
    offs <- scanGroupOffsets counts codeToGroup nGroups
    repsM <- VUM.new nGroups
    _ <-
        parForkJoin
            [ scatterRepsSlice counts codeToGroup firstAll repsM lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    reps <- VU.unsafeFreeze repsM
    hists <- mapM VU.unsafeFreeze histsM
    pure (Just (offs, reps, counts, codeToGroup, hists, nGroups))

{- | Rows must satisfy @row + 1 < 2^31@ for the packed count/first-row encoding
(count in the high bits, first row + 1 in the low 31). Above it (a >2e9-row
frame, >17GB per Int column) the unpacked variant runs instead.
-}
packedRowLimit :: Int
packedRowLimit = 0x7FFFFFFF

-- | One unit of count in the packed encoding; also the low-bits mask + 1.
packedCountOne :: Int
packedCountOne = 0x80000000

{- | Whether @codeToGroup@ maps every code to itself (fully occupied ascending
domain — e.g. a dense Int key covering its whole range). The rowToGroup pass
then skips the random remap lookup entirely.
-}
isIdentityMap :: VU.Vector Int -> Bool
isIdentityMap m = go 0
  where
    !k = VU.length m
    go !i
        | i >= k = True
        | VU.unsafeIndex m i /= i = False
        | otherwise = go (i + 1)

{- | Histogram one row chunk with the packed encoding: slot @c@ holds
@count(c) * 2^31 + (firstRow(c) + 1)@ (zero = never seen). One accumulator
array per chunk. Reports 'False' as soon as any code escapes @[0, card)@.
-}
histFirstChunkPacked ::
    (Int -> Int) -> Int -> Int -> Int -> IO (VUM.IOVector Int, Bool)
histFirstChunkPacked codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        VUM.unsafeWrite
                            acc
                            c
                            (if x == 0 then packedCountOne + (i + 1) else x + packedCountOne)
                        go (i + 1)
    ok <- go lo
    pure (acc, ok)

{- | Per-code totals and overall first occurrences from the PACKED chunk
histograms, rewriting each histogram slot to its plain count in place (the
placement thunk then sees ordinary counts). Chunks are ordered by row range, so
the first chunk with a nonzero slot holds the code's globally first row.
-}
sumFirstSlicePacked ::
    [VUM.IOVector Int] ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
sumFirstSlicePacked hists totals firstAll lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] !acc !firstRow = pure (acc, firstRow)
                sumP (h : hs) !acc !firstRow = do
                    x <- VUM.unsafeRead h c
                    let !cnt = x `unsafeShiftR` 31
                    VUM.unsafeWrite h c cnt
                    if firstRow < 0 && x /= 0
                        then sumP hs (acc + cnt) ((x .&. (packedCountOne - 1)) - 1)
                        else sumP hs (acc + cnt) firstRow
            (s, fo) <- sumP hists 0 (-1)
            VUM.unsafeWrite totals c s
            VUM.unsafeWrite firstAll c fo
            go (c + 1)

{- | Histogram one row chunk into a private @card@-slot count plus the chunk's
first occurrence of each code, reporting 'False' as soon as any code escapes
@[0, card)@ (the counts are then abandoned). Fallback for frames beyond
'packedRowLimit'.
-}
histFirstChunk ::
    (Int -> Int) ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int, Bool)
histFirstChunk codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    firstOcc <- VUM.replicate card (-1 :: Int)
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        VUM.unsafeWrite acc c (x + 1)
                        when (x == 0) (VUM.unsafeWrite firstOcc c i)
                        go (i + 1)
    ok <- go lo
    pure (acc, firstOcc, ok)

{- | Per-code totals over one code slice, plus the overall first occurrence of
each code: the chunks are ordered by row range, so the first chunk with a
nonzero count for a code holds its globally first row.
-}
sumFirstSlice ::
    [VUM.IOVector Int] ->
    [VUM.IOVector Int] ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
sumFirstSlice hists firsts totals firstAll lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] [] !acc !firstRow = pure (acc, firstRow)
                sumP (h : hs) (f : fs) !acc !firstRow = do
                    x <- VUM.unsafeRead h c
                    if firstRow < 0 && x > 0
                        then do
                            fo <- VUM.unsafeRead f c
                            sumP hs fs (acc + x) fo
                        else sumP hs fs (acc + x) firstRow
                sumP _ _ _ _ = error "sumFirstSlice: mismatched partials"
            (s, fo) <- sumP hists firsts 0 (-1)
            VUM.unsafeWrite totals c s
            VUM.unsafeWrite firstAll c fo
            go (c + 1)

{- | Exclusive prefix scan of per-group counts (gathered through @codeToGroup@)
into the offsets array of length @nGroups + 1@.
-}
scanGroupOffsets :: VU.Vector Int -> VU.Vector Int -> Int -> IO (VU.Vector Int)
scanGroupOffsets counts codeToGroup nGroups = do
    let !card = VU.length counts
    grpCount <- VUM.new nGroups
    let gather !c
            | c >= card = pure ()
            | otherwise = do
                let !cnt = VU.unsafeIndex counts c
                if cnt == 0
                    then gather (c + 1)
                    else do
                        VUM.unsafeWrite grpCount (VU.unsafeIndex codeToGroup c) cnt
                        gather (c + 1)
    gather 0
    offsM <- VUM.new (nGroups + 1)
    let scan !g !acc
            | g >= nGroups = VUM.unsafeWrite offsM nGroups acc
            | otherwise = do
                VUM.unsafeWrite offsM g acc
                c <- VUM.unsafeRead grpCount g
                scan (g + 1) (acc + c)
    scan 0 0
    VU.unsafeFreeze offsM

{- | @reps[codeToGroup c] = firstAll c@ for every occupied code: each group is
exactly one occupied code, so this is a disjoint parallel write and equals
@vis[offs[g]]@ (the group's first row in original order).
-}
scatterRepsSlice ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterRepsSlice counts codeToGroup firstAll repsM lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | VU.unsafeIndex counts c == 0 = go (c + 1)
        | otherwise = do
            VUM.unsafeWrite
                repsM
                (VU.unsafeIndex codeToGroup c)
                (VU.unsafeIndex firstAll c)
            go (c + 1)

{- | Deferred @rowToGroup@: one parallel per-row pass mapping each row's code
through @codeToGroup@ (skipping the lookup entirely when the map is the
identity, i.e. a fully occupied ascending domain). Only the streaming
aggregation paths force this; a purely gather-driven consumer (median, top-k)
never pays for it.

Pure w.r.t. its immutable inputs and deterministic (fixed chunking), so the
'unsafePerformIO' behind a lazy field is safe: whenever and however many times
the thunk is forced it yields the same vector.
-}
rtgFromCodes :: (Int -> Int) -> VU.Vector Int -> Int -> VU.Vector Int
rtgFromCodes codeAt codeToGroup n = unsafePerformIO $ do
    rtgM <- VUM.new n
    let identity = isIdentityMap codeToGroup
    _ <-
        parForkJoin
            [ ( if identity
                    then rtgChunkIdentity codeAt rtgM lo hi
                    else rtgChunk codeAt codeToGroup rtgM lo hi
              )
            | (lo, hi) <- directRowChunks n
            ]
    VU.unsafeFreeze rtgM
{-# NOINLINE rtgFromCodes #-}

-- | @rtg[i] = codeToGroup (codeAt i)@ over one row chunk (disjoint writes).
rtgChunk ::
    (Int -> Int) -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunk codeAt codeToGroup rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite rtgM i (VU.unsafeIndex codeToGroup (codeAt i))
            go (i + 1)

-- | 'rtgChunk' without the remap lookup (codeToGroup is the identity).
rtgChunkIdentity ::
    (Int -> Int) -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunkIdentity codeAt rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite rtgM i (codeAt i)
            go (i + 1)

{- | Deferred stable placement: build the @valueIndices@ permutation from the
per-row codes and the RETAINED phase-1 chunk histograms — the same
seed-cursors-then-place structure (and cost) the eager path used, minus the
@rowToGroup@ writes. Each chunk's code-indexed cursor starts at the group
offset plus everything earlier chunks (in row order) place there, so rows keep
original order within each group: the result is the unique group-major,
original-row-order permutation, bit-identical to the eager placement at any
chunk count.

Pure w.r.t. its immutable inputs and deterministic (fixed chunking, fixed merge
order), so the 'unsafePerformIO' behind a lazy field is safe: whenever and
however many times the thunk is forced it yields the same vector. The thunk
retains the chunk histograms (capabilities x card words) until forced or the
grouping is dropped.
-}
visFromCodes ::
    (Int -> Int) ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    [VU.Vector Int] ->
    Int ->
    Int ->
    VU.Vector Int
visFromCodes codeAt counts codeToGroup offs hists n card = unsafePerformIO $ do
    let chunks = directRowChunks n
    -- Private mutable copies of the retained histograms, rewritten in place
    -- into the per-chunk write cursors.
    cursors <- mapM VU.thaw hists
    _ <-
        parForkJoin
            [ seedCursorSlice counts codeToGroup offs cursors lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    vis <- VUM.new n
    _ <-
        parForkJoin
            [ placeVisChunk codeAt cursor vis lo hi
            | ((lo, hi), cursor) <- zip chunks cursors
            ]
    VU.unsafeFreeze vis
{-# NOINLINE visFromCodes #-}

{- | Rewrite each chunk's histogram copy in place into its disjoint write
cursor: chunk w's run for code c starts at the offset of c's group plus what
earlier chunks (in row order) place there.
-}
seedCursorSlice ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    [VUM.IOVector Int] ->
    Int ->
    Int ->
    IO ()
seedCursorSlice counts codeToGroup offs cursors lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | VU.unsafeIndex counts c == 0 = go (c + 1)
        | otherwise = do
            let !g = VU.unsafeIndex codeToGroup c
                loop [] !_ = pure ()
                loop (cur : rest) !acc = do
                    t <- VUM.unsafeRead cur c
                    VUM.unsafeWrite cur c acc
                    loop rest (acc + t)
            loop cursors (VU.unsafeIndex offs g)
            go (c + 1)

{- | Stable placement over one row chunk via the chunk's advancing
code-indexed cursors.
-}
placeVisChunk ::
    (Int -> Int) -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
placeVisChunk codeAt cursor vis lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !c = codeAt i
            pos <- VUM.unsafeRead cursor c
            VUM.unsafeWrite vis pos i
            VUM.unsafeWrite cursor c (pos + 1)
            go (i + 1)

{- | Fold a value-mix over an unboxed column into the running hash vector,
respecting the null bitmap: a null slot mixes a fixed 'nullSalt' sentinel.
-}
hashUnboxed ::
    (VU.Unbox a) =>
    VUM.MVector s Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    VU.Vector a ->
    ST s ()
hashUnboxed mh ubm mix v = case ubm of
    Nothing ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite mh i (mix h x)
            )
            v
    Just bm ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite
                    mh
                    i
                    (if bitmapTestBit bm i then mix h x else mixInt h nullSalt)
            )
            v
{-# INLINE hashUnboxed #-}

{- | Hash a packed-text column over its raw UTF-8 byte slices (no per-row
'Data.Text.Text'), mixing 'nullSalt' for null rows. Shares 'mixBytes' with
'mixText' so packed and boxed Text columns hash identically. A canonical
dict-encoded column (equal strings share a code) instead mixes its 'Int' code
with one 'mixInt' per row; 'DataFrame.Internal.RowHash.packedRange' applies the
same rule under the grouping setting so 'groupBySeq' and 'groupByPar' bucket
identically.
-}
hashPacked ::
    VUM.MVector s Int -> Maybe Bitmap -> PackedTextData -> ST s ()
hashPacked mh bm p = case ptSel p of
    Just sel | ptCanonicalSel p -> goCodes sel 0
    _ -> go 0
  where
    !n = packedLength p
    go !i
        | i >= n = pure ()
        | otherwise = do
            !h <- VUM.unsafeRead mh i
            let h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> let (arr, o, l) = packedSlice p i in mixBytes h arr o l
            VUM.unsafeWrite mh i h'
            go (i + 1)
    goCodes !sel !i
        | i >= n = pure ()
        | otherwise = do
            !h <- VUM.unsafeRead mh i
            let h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> mixInt h (selAt sel i)
            VUM.unsafeWrite mh i h'
            goCodes sel (i + 1)
{-# INLINE hashPacked #-}

-- Inline accessors to avoid depending on Operations.Core

columnNames :: DataFrame -> [T.Text]
columnNames = M.keys . columnIndices

nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

{- | Build the rowToGroup lookup vector from valueIndices and offsets.
rowToGroup[i] = k means row i belongs to group k.
-}
buildRowToGroup :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
buildRowToGroup n vis os = runST $ do
    rtg <- VUM.new n
    let nGroups = VU.length os - 1
    forM_ [0 .. nGroups - 1] $ \k ->
        let s = VU.unsafeIndex os k
            e = VU.unsafeIndex os (k + 1)
         in forM_ [s .. e - 1] $ \i ->
                VUM.unsafeWrite rtg (VU.unsafeIndex vis i) k
    VU.unsafeFreeze rtg
{-# NOINLINE buildRowToGroup #-}

changingPoints :: VU.Vector (Int, Int) -> VU.Vector Int
changingPoints vs =
    VU.reverse
        (VU.fromList (VU.length vs : fst (VU.ifoldl' findChangePoints initialState vs)))
  where
    initialState = ([0], snd (VU.head vs))
    findChangePoints (!offs, !currentVal) index (_, !newVal)
        | currentVal == newVal = (offs, currentVal)
        | otherwise = (index : offs, newVal)
