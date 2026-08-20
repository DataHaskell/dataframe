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
-}
module DataFrame.Internal.GroupingDirect (
    directGroupThreshold,
    tryDirectGroupColumn,
    groupCodesMaybe,
    ascendingCodeGroups,
    rangeOf,
    DirectGrouping (..),
) where

import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad.ST (runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Column (..))

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

capabilities :: Int
capabilities = unsafePerformIO getNumCapabilities
{-# NOINLINE capabilities #-}

parThreshold :: Int
parThreshold = 200000

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
    | not (shouldPar n) = rangeChunk v 0 n
    | otherwise = unsafePerformIO $ do
        rs <- forkJoinResults [pure $! rangeChunk v lo hi | (lo, hi) <- rowChunks n]
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

shouldPar :: Int -> Bool
shouldPar n = n >= parThreshold && capabilities > 1

{- | Contiguous per-worker row (or code) ranges: one chunk per capability above
the parallel threshold, a single chunk otherwise (the sequential fallback runs
the same code on the calling thread).
-}
rowChunks :: Int -> [(Int, Int)]
rowChunks n
    | not (shouldPar n) = [(0, n)]
    | otherwise = splitRange capabilities n

-- | @k@ near-equal contiguous subranges of @[0, n)@, empties dropped.
splitRange :: Int -> Int -> [(Int, Int)]
splitRange k n =
    [ (lo, hi)
    | w <- [0 .. k - 1]
    , let lo = min n (w * per)
    , let hi = min n (lo + per)
    , lo < hi
    ]
  where
    !per = (n + k - 1) `div` k

-- | Like 'rowChunks' but over the code domain (parallel merge/seed passes).
codeSlices :: Int -> [(Int, Int)]
codeSlices card
    | card < 4096 || capabilities <= 1 = [(0, card)]
    | otherwise = splitRange capabilities card

{- | Run each action on its own thread and collect the results in order;
rethrow the first failure. A single action runs on the calling thread.
-}
forkJoinResults :: [IO a] -> IO [a]
forkJoinResults [act] = fmap (: []) act
forkJoinResults actions = do
    vars <- mapM spawn actions
    rs <- mapM takeMVar vars
    mapM (either (throwIO @SomeException) pure) rs
  where
    spawn act = do
        var <- newEmptyMVar
        _ <- forkIO (try act >>= putMVar var)
        pure var

{- | Build the grouping by counting sort on a per-row code in @[0, card)@:
per-chunk (parallel) histograms with bounds validation, a caller-chosen
code-to-dense-group mapping over the summed counts, a scan into offsets, then a
stable placement pass building @valueIndices@ and @rowToGroup@ in parallel —
each chunk owns a disjoint prefix-summed cursor per code, so rows keep original
order within each group exactly as the sequential pass produced. Returns
'Nothing' when any row's code falls outside @[0, card)@ (fall back to hashing).

@mkGroups counts@ must return a dense group id for every code with a nonzero
count (other slots are never read) and the group count; it decides group order.
-}
groupCodesMaybe ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    Maybe DirectGrouping
groupCodesMaybe codeAt n card mkGroups
    | n <= 0 || card <= 0 = Nothing
    | otherwise = unsafePerformIO $ do
        let chunks = rowChunks n
        -- Phase 1: per-chunk code histograms, validating every code.
        parts <- forkJoinResults [histValidChunk codeAt card lo hi | (lo, hi) <- chunks]
        if not (all snd parts)
            then pure Nothing
            else do
                let partials = map fst parts
                -- Phase 2: per-code totals, parallel over code slices.
                totalsM <- VUM.new card
                _ <-
                    forkJoinResults
                        [sumSlice partials totalsM lo hi | (lo, hi) <- codeSlices card]
                counts <- VU.unsafeFreeze totalsM
                let (!codeToGroup, !nGroups) = mkGroups counts
                -- Phase 3: group offsets (O(card + nGroups), sequential).
                offs <- scanOffsets counts codeToGroup nGroups
                -- Phase 4: turn each partial histogram into its chunk's cursor:
                -- cursor_w[c] = offs[group c] + Σ_{w'<w} partial_w'[c].
                _ <-
                    forkJoinResults
                        [ seedSlice counts codeToGroup offs partials lo hi
                        | (lo, hi) <- codeSlices card
                        ]
                -- Phase 5: stable placement, parallel over the same row chunks.
                rtg <- VUM.new n
                vis <- VUM.new n
                _ <-
                    forkJoinResults
                        [ placeChunk codeAt codeToGroup cursor rtg vis lo hi
                        | ((lo, hi), cursor) <- zip chunks partials
                        ]
                frozenRtg <- VU.unsafeFreeze rtg
                frozenVis <- VU.unsafeFreeze vis
                pure (Just (DirectGrouping frozenRtg frozenVis offs nGroups))
{-# NOINLINE groupCodesMaybe #-}

{- | Histogram one row chunk into a private @card@-slot count, reporting 'False'
as soon as any code escapes @[0, card)@ (the counts are then abandoned).
-}
histValidChunk ::
    (Int -> Int) -> Int -> Int -> Int -> IO (VUM.IOVector Int, Bool)
histValidChunk codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        VUM.unsafeWrite acc c (x + 1)
                        go (i + 1)
    ok <- go lo
    pure (acc, ok)

-- | @totals[c] = Σ_w partial_w[c]@ over one code slice (exact integer sums).
sumSlice :: [VUM.IOVector Int] -> VUM.IOVector Int -> Int -> Int -> IO ()
sumSlice partials totals lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] !acc = pure acc
                sumP (p : ps) !acc = do
                    x <- VUM.unsafeRead p c
                    sumP ps (acc + x)
            s <- sumP partials 0
            VUM.unsafeWrite totals c s
            go (c + 1)

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

{- | Rewrite each chunk's partial histogram in place into its disjoint write
cursor: chunk @w@ starts at the group offset plus everything earlier chunks
will place there, keeping the counting sort stable across chunks.
-}
seedSlice ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    [VUM.IOVector Int] ->
    Int ->
    Int ->
    IO ()
seedSlice counts codeToGroup offs partials lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | VU.unsafeIndex counts c == 0 = go (c + 1)
        | otherwise = do
            let !g = VU.unsafeIndex codeToGroup c
                loop [] !_ = pure ()
                loop (p : ps) !acc = do
                    t <- VUM.unsafeRead p c
                    VUM.unsafeWrite p c acc
                    loop ps (acc + t)
            loop partials (VU.unsafeIndex offs g)
            go (c + 1)

{- | Stable placement over one row chunk: for each row in original order, write
@rowToGroup@ and append the row to its group's run in @valueIndices@ via the
chunk's advancing cursor.
-}
placeChunk ::
    (Int -> Int) ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
placeChunk codeAt codeToGroup cursor rtg vis lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !c = codeAt i
                !g = VU.unsafeIndex codeToGroup c
            VUM.unsafeWrite rtg i g
            pos <- VUM.unsafeRead cursor c
            VUM.unsafeWrite vis pos i
            VUM.unsafeWrite cursor c (pos + 1)
            go (i + 1)

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
