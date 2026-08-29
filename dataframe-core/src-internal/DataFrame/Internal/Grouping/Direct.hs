{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Low-cardinality direct-indexed grouping fast path: when every row's key
reduces to a dense @Int@ code in a small domain, the code itself indexes a dense
accumulator (no hashing/probing). All O(n) passes run chunked across
capabilities — per-chunk histograms feed prefix-summed disjoint write cursors —
so the stable within-group row order of the sequential counting sort is
reproduced exactly.

Two engines cover the code-domain spectrum with bit-identical results:

* narrow domains (@card <= 'twoLevelCardThreshold'@) index per-worker
  histogram\/cursor tables directly — they stay cache-resident;

* wide domains use a two-level radix split (top code bits pick one of ~@2^10@
  buckets, cursors cache-resident) so no pass ever random-writes a
  multi-megabyte table per worker.

'directLayoutLazy' is the aggregation entry point: it builds @rowToGroup@,
@offsets@ and the per-group representative rows eagerly but skips the O(n)
stable placement entirely; 'visFromRowToGroup' reconstructs @valueIndices@
on demand (its value is the unique stable counting-sort permutation, so WHEN it
runs is unobservable).
-}
module DataFrame.Internal.Grouping.Direct (
    directGroupThreshold,
    tryDirectGroupColumn,
    groupCodesMaybe,
    directLayoutLazy,
    visFromRowToGroup,
    ascendingCodeGroups,
    rangeOf,
    useTwoLevel,
    DirectGrouping (..),
) where

import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import Control.Monad (when)
import Control.Monad.ST (runST)
import Data.Bits (countLeadingZeros, unsafeShiftL, unsafeShiftR, (.&.))
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    chunksFor,
    forkJoin,
    parThreshold,
    parallelChunks,
    pooledIndices,
    shouldParallelize,
    splitChunkRange,
 )

{- | Largest key code DOMAIN (single-key value range, or the product of per-key
domains for a fused multi-key code) the direct grouping path accepts. A @2^20@-slot
histogram is 8MB; the low-cardinality questions sit far below it (id4 range 100,
id6 range 1e5). Wider domains fall back to the hash group-by.
-}
directGroupThreshold :: Int
directGroupThreshold = 1048576

{- | The grouping layout the hash path also produces: @rowToGroup@, the
group-sorted @valueIndices@, the @offsets@ prefix array, and the group count.
-}
data DirectGrouping = DirectGrouping
    { dgRowToGroup :: !(VU.Vector Int)
    , dgValueIndices :: !(VU.Vector Int)
    , dgOffsets :: !(VU.Vector Int)
    , dgNGroups :: !Int
    }

{- | Take the direct path if the (single) key column is a clean non-null unboxed
@Int@ column with a small value range. Returns 'Nothing' to fall back to the
hash group-by on anything else (boxed/text keys, nullable, wide ranges, empty).
-}
tryDirectGroupColumn :: Column -> Maybe DirectGrouping
tryDirectGroupColumn (UnboxedColumn Nothing (v :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int)
    , not (VU.null v) =
        let (!mn, !mx) = rangeOf v
            !range = mx - mn + 1
         in if range >= 1 && range <= directGroupThreshold
                then
                    groupCodesMaybe
                        (\i -> VU.unsafeIndex v i - mn)
                        (VU.length v)
                        range
                        ascendingCodeGroups
                else Nothing
tryDirectGroupColumn _ = Nothing

-- | Parallel min/max reduce (order-independent).
rangeOf :: VU.Vector Int -> (Int, Int)
rangeOf v
    | not (shouldParallelize parThreshold n) = rangeChunk v 0 n
    | otherwise = unsafePerformIO $ do
        rs <- parallelChunks parThreshold n (\lo hi -> pure $! rangeChunk v lo hi)
        pure (combineRanges (filter (\(a, _) -> a /= maxBound) rs))
  where
    !n = VU.length v
{-# NOINLINE rangeOf #-}

rangeChunk :: VU.Vector Int -> Int -> Int -> (Int, Int)
rangeChunk v lo hi = go lo maxBound minBound
  where
    go !i !mn !mx
        | i >= hi = (mn, mx)
        | otherwise =
            let !x = VU.unsafeIndex v i
             in go (i + 1) (min mn x) (max mx x)

combineRanges :: [(Int, Int)] -> (Int, Int)
combineRanges [] = (0, 0)
combineRanges ((a0, b0) : rest) = foldr (\(a, b) (ma, mb) -> (min ma a, max mb b)) (a0, b0) rest

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

{- | Contiguous per-worker row ranges: one chunk per capability above the
parallel threshold, a single chunk otherwise (the sequential fallback runs the
same code on the calling thread).
-}
rowChunks :: Int -> [(Int, Int)]
rowChunks = chunksFor parThreshold

{- | Like 'rowChunks' but over the code domain (parallel merge/seed passes),
which pays for a fan-out at a much lower width than the row passes do.
-}
codeSlices :: Int -> [(Int, Int)]
codeSlices = chunksFor 4096

{- | Run each action on its own thread and collect the results in order;
rethrow the first failure. A single action runs on the calling thread.
-}

-------------------------------------------------------------------------------
-- Full grouping (eager valueIndices): compatibility entry point
-------------------------------------------------------------------------------

{- | Build the grouping by counting sort on a per-row code in @[0, card)@.
Returns 'Nothing' when any row's code falls outside @[0, card)@ (fall back to
hashing).

@mkGroups counts@ must return a dense group id for every code with a nonzero
count (other slots are never read; occupied codes must get distinct ids) and
the group count; it decides group order.

Equivalent to 'directLayoutLazy' plus a forced 'visFromRowToGroup'; kept for
callers that want the whole layout eagerly.
-}
groupCodesMaybe ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    Maybe DirectGrouping
groupCodesMaybe codeAt n card mkGroups = do
    (rtg, offs, _reps, nGroups) <- directLayoutLazy codeAt n card mkGroups
    let !vis = visFromRowToGroup n nGroups offs rtg
    Just (DirectGrouping rtg vis offs nGroups)

-------------------------------------------------------------------------------
-- Eager layout without placement: rowToGroup + offsets + group rep rows
-------------------------------------------------------------------------------

{- | The layout every aggregation needs, WITHOUT the O(n) stable placement:
@(rowToGroup, offsets, groupRepRows, nGroups)@, all four computed eagerly.
@groupRepRows[g]@ is the first original row of group @g@ (what
@valueIndices[offsets[g]]@ evaluates to). Pair with 'visFromRowToGroup' for a
deferred @valueIndices@. Returns 'Nothing' when any row's code falls outside
@[0, card)@.

@mkGroups@ contract as in 'groupCodesMaybe'.
-}
directLayoutLazy ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    Maybe (VU.Vector Int, VU.Vector Int, VU.Vector Int, Int)
directLayoutLazy codeAt n card mkGroups
    | n <= 0 || card <= 0 = Nothing
    | useTwoLevel n card = unsafePerformIO (layoutWide codeAt n card mkGroups)
    | otherwise = unsafePerformIO (layoutNarrow codeAt n card mkGroups)
{-# NOINLINE directLayoutLazy #-}

{- | Narrow domains (and the sequential small-@n@ fallback): per-chunk direct
histograms with first-occurrence tracking, merged over code slices.
-}
layoutNarrow ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    IO (Maybe (VU.Vector Int, VU.Vector Int, VU.Vector Int, Int))
layoutNarrow codeAt n card mkGroups = do
    let chunks = rowChunks n
    parts <- forkJoin [histFirstChunk codeAt card lo hi | (lo, hi) <- chunks]
    if not (all (\(_, _, ok) -> ok) parts)
        then pure Nothing
        else do
            let partials = [(cs, fs) | (cs, fs, _) <- parts]
            totalsM <- VUM.unsafeNew card
            firstRowM <- VUM.unsafeNew card
            _ <-
                forkJoin
                    [mergeSlice partials totalsM firstRowM lo hi | (lo, hi) <- codeSlices card]
            counts <- VU.unsafeFreeze totalsM
            firstRow <- VU.unsafeFreeze firstRowM
            finishLayout codeAt n card mkGroups counts firstRow

{- | Histogram one row chunk into a private @card@-slot count, recording the
chunk's first row of each code, and reporting invalid codes (third component
'False'; the arrays are then abandoned).
-}
histFirstChunk ::
    (Int -> Int) ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int, Bool)
histFirstChunk codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    firstV <- VUM.unsafeNew card
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        when (x == 0) (VUM.unsafeWrite firstV c i)
                        VUM.unsafeWrite acc c (x + 1)
                        go (i + 1)
    ok <- go lo
    pure (acc, firstV, ok)

{- | @totals[c] = Σ_w counts_w[c]@ and @firstRow[c]@ = the first chunk's first
occurrence (chunks are in row order, so that IS the global first row of @c@),
over one code slice. Every @totals@ slot is written; @firstRow[c]@ only where
the count is nonzero (never read otherwise).
-}
mergeSlice ::
    [(VUM.IOVector Int, VUM.IOVector Int)] ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
mergeSlice partials totals firstRow lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] !acc = pure acc
                sumP ((cs, fs) : ps) !acc = do
                    x <- VUM.unsafeRead cs c
                    when (acc == 0 && x > 0) $
                        VUM.unsafeRead fs c >>= VUM.unsafeWrite firstRow c
                    sumP ps (acc + x)
            s <- sumP partials 0
            VUM.unsafeWrite totals c s
            go (c + 1)

{- | Wide domains: two-level radix. Rows are bucketed by the top code bits
(cache-resident cursors) as packed @(code, row)@ words; each bucket's slice —
in original row order — yields its exact per-code counts and first rows from an
L1-resident table. No placement pass runs here.
-}
layoutWide ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    IO (Maybe (VU.Vector Int, VU.Vector Int, VU.Vector Int, Int))
layoutWide codeAt n card mkGroups = do
    mPacked <- packBucketed True codeAt n card
    case mPacked of
        Nothing -> pure Nothing
        Just (shift, bucketStart, packed) -> do
            countsM <- VUM.unsafeNew card
            firstRowM <- VUM.unsafeNew card
            overBuckets
                bucketStart
                n
                (countFirstBucket shift card bucketStart packed countsM firstRowM)
                (countFirstBucketPar shift card bucketStart packed countsM firstRowM)
            counts <- VU.unsafeFreeze countsM
            firstRow <- VU.unsafeFreeze firstRowM
            finishLayout codeAt n card mkGroups counts firstRow

{- | Shared tail of 'directLayoutLazy': group mapping, offsets, representative
rows gathered through the code-to-group table, and the parallel @rowToGroup@
pass (one sequential read of the codes, one table lookup each).
-}
finishLayout ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    VU.Vector Int ->
    VU.Vector Int ->
    IO (Maybe (VU.Vector Int, VU.Vector Int, VU.Vector Int, Int))
finishLayout codeAt n card mkGroups counts firstRow = do
    let (!codeToGroup, !nGroups) = mkGroups counts
    offs <- scanOffsets counts codeToGroup nGroups
    repsM <- VUM.unsafeNew nGroups
    let repLoop !c
            | c >= card = pure ()
            | VU.unsafeIndex counts c == 0 = repLoop (c + 1)
            | otherwise = do
                VUM.unsafeWrite
                    repsM
                    (VU.unsafeIndex codeToGroup c)
                    (VU.unsafeIndex firstRow c)
                repLoop (c + 1)
    repLoop 0
    reps <- VU.unsafeFreeze repsM
    rtgM <- VUM.unsafeNew n
    -- A fully occupied ascending domain maps every code to itself; skipping
    -- the per-row random table lookup then leaves one sequential read+write.
    let identity = isIdentityMap codeToGroup
    _ <-
        forkJoin
            [ ( if identity
                    then rtgChunkIdentity codeAt rtgM lo hi
                    else rtgChunk codeAt codeToGroup rtgM lo hi
              )
            | (lo, hi) <- rowChunks n
            ]
    rtg <- VU.unsafeFreeze rtgM
    pure (Just (rtg, offs, reps, nGroups))

-- | Whether @codeToGroup@ maps every code to itself (fully occupied domain).
isIdentityMap :: VU.Vector Int -> Bool
isIdentityMap m = go 0
  where
    !k = VU.length m
    go !i
        | i >= k = True
        | VU.unsafeIndex m i /= i = False
        | otherwise = go (i + 1)

{- | Exclusive prefix scan of per-group counts (gathered through @codeToGroup@)
into the offsets array of length @nGroups + 1@.
-}
scanOffsets :: VU.Vector Int -> VU.Vector Int -> Int -> IO (VU.Vector Int)
scanOffsets counts codeToGroup nGroups = do
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

-- | @rowToGroup@ for one row chunk: remap each row's code through the table.
rtgChunk ::
    (Int -> Int) -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunk codeAt codeToGroup rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !c = codeAt i
            VUM.unsafeWrite rtgM i (VU.unsafeIndex codeToGroup c)
            go (i + 1)

-- | 'rtgChunk' without the remap lookup (codeToGroup is the identity).
rtgChunkIdentity :: (Int -> Int) -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunkIdentity codeAt rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite rtgM i (codeAt i)
            go (i + 1)

-------------------------------------------------------------------------------
-- Deferred stable placement: valueIndices from rowToGroup
-------------------------------------------------------------------------------

{- | The unique stable counting-sort permutation of @rowToGroup@: rows sorted by
group id, original order within each group — exactly what the eager engines'
placement pass produces. Pure (integer bookkeeping only, deterministic), so it
can sit under a lazy 'DataFrame.Internal.DataFrame.valueIndices' field.

Preconditions (all guaranteed by the grouping paths): @rtg@ has length @n@ with
every value in @[0, nGroups)@, and @offs@ is the group-count prefix array of
length @nGroups + 1@ with @offs[nGroups] == n@.
-}
visFromRowToGroup ::
    Int -> Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
visFromRowToGroup n nGroups offs rtg
    | n <= 0 = VU.empty
    | useTwoLevel n nGroups = unsafePerformIO (visWide n nGroups offs rtg)
    | otherwise = unsafePerformIO (visNarrow n nGroups offs rtg)
{-# NOINLINE visFromRowToGroup #-}

{- | Narrow group domains: per-chunk group histograms prefix-summed (chunk
order) into disjoint cursors seeded from @offs@, then parallel stable placement.
-}
visNarrow :: Int -> Int -> VU.Vector Int -> VU.Vector Int -> IO (VU.Vector Int)
visNarrow n nGroups offs rtg = do
    let chunks = rowChunks n
    hists <-
        forkJoin
            [ do
                acc <- VUM.replicate nGroups (0 :: Int)
                let go !i
                        | i >= hi = pure acc
                        | otherwise = do
                            let !g = VU.unsafeIndex rtg i
                            x <- VUM.unsafeRead acc g
                            VUM.unsafeWrite acc g (x + 1)
                            go (i + 1)
                go lo
            | (lo, hi) <- chunks
            ]
    -- Rewrite each chunk histogram into its write cursor:
    -- cursor_w[g] = offs[g] + Σ_{w'<w} hist_w'[g].
    _ <-
        forkJoin
            [ let seed !g
                    | g >= hi = pure ()
                    | otherwise = do
                        let inner [] !_ = pure ()
                            inner (h : hs) !a = do
                                t <- VUM.unsafeRead h g
                                VUM.unsafeWrite h g a
                                inner hs (a + t)
                        inner hists (VU.unsafeIndex offs g)
                        seed (g + 1)
               in seed lo
            | (lo, hi) <- codeSlices nGroups
            ]
    visM <- VUM.unsafeNew n
    _ <-
        forkJoin
            [ let place !i
                    | i >= hi = pure ()
                    | otherwise = do
                        let !g = VU.unsafeIndex rtg i
                        p <- VUM.unsafeRead cursor g
                        VUM.unsafeWrite visM p i
                        VUM.unsafeWrite cursor g (p + 1)
                        place (i + 1)
               in place lo
            | ((lo, hi), cursor) <- zip chunks hists
            ]
    VU.unsafeFreeze visM

{- | Wide group domains: two-level radix. Group ids ascend with buckets, so the
bucket-sorted layout written at @offs@-seeded cursors IS @valueIndices@ — each
bucket writes one contiguous region.
-}
visWide :: Int -> Int -> VU.Vector Int -> VU.Vector Int -> IO (VU.Vector Int)
visWide n nGroups offs rtg = do
    mPacked <- packBucketed False (VU.unsafeIndex rtg) n nGroups
    case mPacked of
        Nothing -> visNarrow n nGroups offs rtg -- unreachable: no validation
        Just (shift, bucketStart, packed) -> do
            visM <- VUM.unsafeNew n
            overBuckets
                bucketStart
                n
                (placeBucketOffs shift nGroups bucketStart offs packed visM)
                (placeBucketOffsPar shift nGroups bucketStart offs packed visM)
            VU.unsafeFreeze visM

{- | Stable placement of one bucket's packed slice at cursors seeded straight
from the group offsets (the bucket's groups own a contiguous @vis@ region).
-}
placeBucketOffs ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    PackedBuckets ->
    VUM.IOVector Int ->
    Int ->
    IO ()
placeBucketOffs shift card bucketStart offs (PackedBuckets packed) visM b = do
    let !s = VU.unsafeIndex bucketStart b
        !e = VU.unsafeIndex bucketStart (b + 1)
        !base = b `unsafeShiftL` shift
        !range = min (1 `unsafeShiftL` shift) (card - base)
    cursor <- VUM.unsafeNew range
    let initC !j
            | j >= range = pure ()
            | otherwise = do
                VUM.unsafeWrite cursor j (VU.unsafeIndex offs (base + j))
                initC (j + 1)
    initC 0
    let place !pos
            | pos >= e = pure ()
            | otherwise = do
                pc <- VUM.unsafeRead packed pos
                let !j = (pc `unsafeShiftR` packShift) - base
                p <- VUM.unsafeRead cursor j
                VUM.unsafeWrite visM p (pc .&. packRowMask)
                VUM.unsafeWrite cursor j (p + 1)
                place (pos + 1)
    place s

{- | 'placeBucketOffs' for an oversized bucket: per-sub-chunk histograms
prefix-summed (sub-chunks in row order) onto the offset-seeded cursors keep the
placement identical to the serial walk.
-}
placeBucketOffsPar ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    PackedBuckets ->
    VUM.IOVector Int ->
    Int ->
    IO ()
placeBucketOffsPar shift card bucketStart offs pb visM b = do
    let !s = VU.unsafeIndex bucketStart b
        !e = VU.unsafeIndex bucketStart (b + 1)
        !base = b `unsafeShiftL` shift
        !range = min (1 `unsafeShiftL` shift) (card - base)
        packed = packedVec pb
        subChunks = [(s + lo, s + hi) | (lo, hi) <- splitChunkRange capabilities (e - s)]
    hists <-
        forkJoin
            [subHist pb base range lo hi | (lo, hi) <- subChunks]
    let seed !j
            | j >= range = pure ()
            | otherwise = do
                let inner [] !_ = pure ()
                    inner (h : hs) !a = do
                        t <- VUM.unsafeRead h j
                        VUM.unsafeWrite h j a
                        inner hs (a + t)
                inner hists (VU.unsafeIndex offs (base + j))
                seed (j + 1)
    seed 0
    _ <-
        forkJoin
            [ let place !pos
                    | pos >= hi = pure ()
                    | otherwise = do
                        pc <- VUM.unsafeRead packed pos
                        let !j = (pc `unsafeShiftR` packShift) - base
                        p <- VUM.unsafeRead cursor j
                        VUM.unsafeWrite visM p (pc .&. packRowMask)
                        VUM.unsafeWrite cursor j (p + 1)
                        place (pos + 1)
               in place lo
            | ((lo, hi), cursor) <- zip subChunks hists
            ]
    pure ()

-------------------------------------------------------------------------------
-- Two-level radix plumbing
-------------------------------------------------------------------------------

{- | Above this code-domain size the parallel passes switch to the two-level
radix engine; below it, per-worker direct tables stay cache-resident and are
faster (no bucket store).
-}
twoLevelCardThreshold :: Int
twoLevelCardThreshold = 1024

{- | The two-level engine packs @(code, row)@ into one machine word: row in the
low 'packShift' bits, code above them. Codes are capped at 'directGroupThreshold'
(@2^20@) by every caller, so the packed value stays well within 63 bits; the
guards in 'useTwoLevel' keep the narrow engine for anything larger.
-}
packShift :: Int
packShift = 40

packRowMask :: Int
packRowMask = (1 `unsafeShiftL` packShift) - 1

-- | Bucket-count target of the two-level engine (@2^10@ buckets).
bucketBits :: Int
bucketBits = 10

-- | Use the two-level engine? (Parallel-scale @n@, wide but packable domain.)
useTwoLevel :: Int -> Int -> Bool
useTwoLevel n card =
    shouldPar n
        && card > twoLevelCardThreshold
        && card <= (1 `unsafeShiftL` 22)
        && n <= packRowMask

-- | @ceilLog2 x@: smallest @s@ with @2^s >= x@ (for @x >= 1@).
ceilLog2 :: Int -> Int
ceilLog2 x
    | x <= 1 = 0
    | otherwise = 64 - countLeadingZeros (x - 1)

{- | The bucket store: rows partitioned by the top code bits, each bucket
holding packed @(code, row)@ words in original row order.
-}
newtype PackedBuckets = PackedBuckets (VUM.IOVector Int)

packedVec :: PackedBuckets -> VUM.IOVector Int
packedVec (PackedBuckets v) = v

{- | Partition rows into ~@2^'bucketBits'@ buckets by the top bits of their
code, as packed @(code, row)@ words: per-chunk bucket histograms (validating
every code when asked), prefix-summed in (bucket, chunk) order into disjoint
cursors, then a parallel scatter. Chunks are processed in row order, so every
bucket keeps its rows in ascending original row order. Returns 'Nothing' iff
validation was requested and some code fell outside @[0, card)@.
-}
packBucketed ::
    Bool ->
    (Int -> Int) ->
    Int ->
    Int ->
    IO (Maybe (Int, VU.Vector Int, PackedBuckets))
packBucketed validate codeAt n card = do
    let !shift = max 0 (ceilLog2 card - bucketBits)
        !nBuckets = ((card - 1) `unsafeShiftR` shift) + 1
        chunks = rowChunks n
    parts <-
        forkJoin
            [bucketHist validate codeAt card shift nBuckets lo hi | (lo, hi) <- chunks]
    if not (all snd parts)
        then pure Nothing
        else do
            let cursors = map fst parts
            bucketStartM <- VUM.unsafeNew (nBuckets + 1)
            let seed !b !acc
                    | b >= nBuckets = VUM.unsafeWrite bucketStartM nBuckets acc
                    | otherwise = do
                        VUM.unsafeWrite bucketStartM b acc
                        let inner [] !a = pure a
                            inner (cur : rest) !a = do
                                t <- VUM.unsafeRead cur b
                                VUM.unsafeWrite cur b a
                                inner rest (a + t)
                        acc' <- inner cursors acc
                        seed (b + 1) acc'
            seed 0 0
            bucketStart <- VU.unsafeFreeze bucketStartM
            packed <- VUM.unsafeNew n
            _ <-
                forkJoin
                    [ scatterPacked codeAt shift cur packed lo hi
                    | ((lo, hi), cur) <- zip chunks cursors
                    ]
            pure (Just (shift, bucketStart, PackedBuckets packed))

{- | Histogram one row chunk by bucket (top code bits) into a private
@nBuckets@-slot count; with @validate@, report 'False' as soon as any code
escapes @[0, card)@ (the counts are then abandoned).
-}
bucketHist ::
    Bool ->
    (Int -> Int) ->
    Int ->
    Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, Bool)
bucketHist validate codeAt card shift nBuckets lo hi = do
    acc <- VUM.replicate nBuckets (0 :: Int)
    let bump !c !i = do
            let !b = c `unsafeShiftR` shift
            x <- VUM.unsafeRead acc b
            VUM.unsafeWrite acc b (x + 1)
            go (i + 1)
        go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if validate && (c < 0 || c >= card)
                    then pure False
                    else bump c i
    ok <- go lo
    pure (acc, ok)

-- | Scatter one row chunk's packed @(code, row)@ words through its bucket cursor.
scatterPacked ::
    (Int -> Int) ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterPacked codeAt shift cursor packed lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !c = codeAt i
                !b = c `unsafeShiftR` shift
            pos <- VUM.unsafeRead cursor b
            VUM.unsafeWrite packed pos ((c `unsafeShiftL` packShift) + i)
            VUM.unsafeWrite cursor b (pos + 1)
            go (i + 1)

{- | Drive one action per bucket: buckets far above the fair per-worker share
run first through @big@ (internally parallel, one at a time), the rest are
pulled off a shared counter by one worker per capability. Every bucket —
including empty ones — is visited exactly once, so per-bucket passes may rely
on covering their whole output slice.
-}
overBuckets :: VU.Vector Int -> Int -> (Int -> IO ()) -> (Int -> IO ()) -> IO ()
overBuckets bucketStart n small big = do
    let !nBuckets = VU.length bucketStart - 1
        !bigCut = max parThreshold (2 * (n `div` max 1 capabilities))
        size b = VU.unsafeIndex bucketStart (b + 1) - VU.unsafeIndex bucketStart b
    mapM_ big [b | b <- [0 .. nBuckets - 1], size b >= bigCut]
    pooledIndices capabilities nBuckets $ \b ->
        when (size b < bigCut) (small b)

{- | One bucket's exact per-code counts and first rows from its (row-ordered)
packed slice, via an L1-resident table spanning only the bucket's code range.
Writes the bucket's whole slice of @counts@ (zeros included); @firstRow@ only
where the count is nonzero.
-}
countFirstBucket ::
    Int ->
    Int ->
    VU.Vector Int ->
    PackedBuckets ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    IO ()
countFirstBucket shift card bucketStart (PackedBuckets packed) countsM firstRowM b = do
    let !s = VU.unsafeIndex bucketStart b
        !e = VU.unsafeIndex bucketStart (b + 1)
        !base = b `unsafeShiftL` shift
        !range = min (1 `unsafeShiftL` shift) (card - base)
    local <- VUM.replicate range (0 :: Int)
    let hist !pos
            | pos >= e = pure ()
            | otherwise = do
                pc <- VUM.unsafeRead packed pos
                let !j = (pc `unsafeShiftR` packShift) - base
                x <- VUM.unsafeRead local j
                when (x == 0) $
                    VUM.unsafeWrite firstRowM (base + j) (pc .&. packRowMask)
                VUM.unsafeWrite local j (x + 1)
                hist (pos + 1)
    hist s
    let flush !j
            | j >= range = pure ()
            | otherwise = do
                t <- VUM.unsafeRead local j
                VUM.unsafeWrite countsM (base + j) t
                flush (j + 1)
    flush 0

-- | 'countFirstBucket' for an oversized bucket, chunked across capabilities.
countFirstBucketPar ::
    Int ->
    Int ->
    VU.Vector Int ->
    PackedBuckets ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    IO ()
countFirstBucketPar shift card bucketStart pb countsM firstRowM b = do
    let !s = VU.unsafeIndex bucketStart b
        !e = VU.unsafeIndex bucketStart (b + 1)
        !base = b `unsafeShiftL` shift
        !range = min (1 `unsafeShiftL` shift) (card - base)
        packed = packedVec pb
        subChunks = [(s + lo, s + hi) | (lo, hi) <- splitChunkRange capabilities (e - s)]
    parts <-
        forkJoin
            [ do
                local <- VUM.replicate range (0 :: Int)
                firstL <- VUM.unsafeNew range
                let hist !pos
                        | pos >= hi = pure (local, firstL)
                        | otherwise = do
                            pc <- VUM.unsafeRead packed pos
                            let !j = (pc `unsafeShiftR` packShift) - base
                            x <- VUM.unsafeRead local j
                            when (x == 0) $
                                VUM.unsafeWrite firstL j (pc .&. packRowMask)
                            VUM.unsafeWrite local j (x + 1)
                            hist (pos + 1)
                hist lo
            | (lo, hi) <- subChunks
            ]
    -- Merge in sub-chunk (= row) order: totals and global first occurrence.
    let merge !j
            | j >= range = pure ()
            | otherwise = do
                let inner [] !acc = pure acc
                    inner ((cs, fs) : ps) !acc = do
                        x <- VUM.unsafeRead cs j
                        when (acc == 0 && x > 0) $
                            VUM.unsafeRead fs j >>= VUM.unsafeWrite firstRowM (base + j)
                        inner ps (acc + x)
                t <- inner parts 0
                VUM.unsafeWrite countsM (base + j) t
                merge (j + 1)
    merge 0

-- | Per-sub-chunk histogram of one bucket's packed slice (codes only).
subHist :: PackedBuckets -> Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
subHist (PackedBuckets packed) base range lo hi = do
    acc <- VUM.replicate range (0 :: Int)
    let go !pos
            | pos >= hi = pure acc
            | otherwise = do
                pc <- VUM.unsafeRead packed pos
                let !j = (pc `unsafeShiftR` packShift) - base
                x <- VUM.unsafeRead acc j
                VUM.unsafeWrite acc j (x + 1)
                go (pos + 1)
    go lo

{- | The ascending-code group order: walk the counts in code order, assigning a
dense group id to each non-empty code (empty codes get no id and no output
group). The single-Int-key path keeps its groups in ascending value order.
-}
ascendingCodeGroups :: VU.Vector Int -> (VU.Vector Int, Int)
ascendingCodeGroups counts = runST $ do
    let !card = VU.length counts
    m <- VUM.new card
    let go !c !next
            | c >= card = pure next
            | VU.unsafeIndex counts c > 0 = do
                VUM.unsafeWrite m c next
                go (c + 1) (next + 1)
            | otherwise = go (c + 1) next
    nGroups <- go 0 0
    frozen <- VU.unsafeFreeze m
    pure (frozen, nGroups)
