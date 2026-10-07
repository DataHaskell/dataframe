{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TupleSections #-}

{- | Regression trees grown from per-bin histograms, for gradient boosting.
Each feature is bucketed once per fit; a node's split search then costs
O(bins) per feature instead of a sweep over its sorted rows. Splits, null
handling and leaf values follow 'DataFrame.DecisionTree.Regression': with no
more distinct values than bins, the trees partition the rows identically.
-}
module DataFrame.DecisionTree.Histogram (
    Binned,
    BinnedFeatures,
    binFeatures,
    TreeLimits (..),
    fitBinnedTree,
    fitBinnedTreeOn,
) where

import Control.Monad.ST (runST)
import Control.Parallel (par, pseq)
import Data.Maybe (fromMaybe, maybeToList)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word16)

import DataFrame.DecisionTree.Cart (
    CartFeature (..),
    NullSide (..),
    sortIndicesByValue,
    splitMidpoint,
 )
import DataFrame.DecisionTree.Types (Tree (..))
import DataFrame.Internal.Expression (Expr)

-- | A feature's bins: rows in bin @b@ satisfy @value <= upper ! b@.
data Binned = Binned
    { bnFeature :: !CartFeature
    , bnBins :: !(VU.Vector Word16)
    -- ^ Bin per row; null rows hold 'bnNBins'.
    , bnNBins :: !Int
    , bnUpper :: !(VU.Vector Double)
    }

{- | Every feature's bins, plus the same bins stored row by row: row @i@'s bin
for feature @j@ is at @i * features + j@, so one row's bins share a cache line.
-}
data BinnedFeatures = BinnedFeatures
    { bfFeatures :: !(V.Vector Binned)
    , bfRows :: !(VU.Vector Word16)
    , bfOffsets :: !(VU.Vector Int)
    -- ^ Where each feature's histogram starts in a node's histogram buffer.
    , bfSlots :: !Int
    }

-- | Bin every feature into at most @maxBins@ bins (capped at 65535).
binFeatures :: Int -> V.Vector CartFeature -> BinnedFeatures
binFeatures maxBins features = BinnedFeatures binned rows offsets (VU.last sizes)
  where
    binned = V.map (binFeature (max 2 (min 65535 maxBins))) features
    nf = V.length binned
    n = if nf == 0 then 0 else VU.length (bnBins (V.head binned))
    sizes =
        VU.prescanl'
            (+)
            0
            (VU.fromList [4 * (bnNBins b + 1) | b <- V.toList binned] `VU.snoc` 0)
    offsets = VU.take nf sizes
    rows = VU.create $ do
        m <- VUM.unsafeNew (n * nf)
        V.iforM_ binned $ \j b ->
            VU.iforM_ (bnBins b) $ \i bin -> VUM.unsafeWrite m (i * nf + j) bin
        pure m

binFeature :: Int -> CartFeature -> Binned
binFeature maxBins f = Binned f bins nb upper
  where
    vals = cfValues f
    isNull i = maybe False (VU.! i) (cfNulls f)
    sorted =
        VU.map (vals VU.!) (VU.filter (not . isNull) (sortIndicesByValue vals))
    runs = runLengths sorted
    distinct = map fst runs
    edges
        | length runs <= maxBins =
            VU.fromList (zipWith splitMidpoint distinct (drop 1 distinct))
        | otherwise = VU.fromList (cutEdges maxBins (VU.length sorted) runs)
    nb = VU.length edges + 1
    -- The last bin's threshold is the largest value, so "every non-null row
    -- left" stays a valid null-vs-non-null split, as in the exact tree.
    upper = VU.snoc edges (if VU.null sorted then 0 else VU.last sorted)
    bins = VU.generate (VU.length vals) binOf
    binOf i
        | isNull i = fromIntegral nb
        | otherwise = fromIntegral (lowerBound edges (vals VU.! i))

-- | Cut points giving bins of roughly equal row counts, never inside a run.
cutEdges :: Int -> Int -> [(Double, Int)] -> [Double]
cutEdges maxBins n = go 0 0
  where
    perBin = fromIntegral n / fromIntegral maxBins :: Double
    go :: Int -> Int -> [(Double, Int)] -> [Double]
    go !nCuts !acc ((v, c) : rest@((v', _) : _))
        | nCuts >= maxBins - 1 = []
        | fromIntegral (acc + c) >= perBin = splitMidpoint v v' : go (nCuts + 1) 0 rest
        | otherwise = go nCuts (acc + c) rest
    go _ _ _ = []

runLengths :: VU.Vector Double -> [(Double, Int)]
runLengths v = go 0
  where
    n = VU.length v
    go i
        | i >= n = []
        | otherwise =
            let x = v VU.! i
                j = until (\k -> k >= n || v VU.! k /= x) (+ 1) i
             in (x, j - i) : go j

-- | Number of edges below @x@: the bin @x@ falls in.
lowerBound :: VU.Vector Double -> Double -> Int
lowerBound edges x = go 0 (VU.length edges)
  where
    go lo hi
        | lo >= hi = lo
        | VU.unsafeIndex edges m < x = go (m + 1) hi
        | otherwise = go lo m
      where
        m = (lo + hi) `div` 2

-- | The stopping rules of 'DataFrame.DecisionTree.Regression.RegTreeConfig'.
data TreeLimits = TreeLimits
    { tlMaxDepth :: !Int
    , tlMinSamplesSplit :: !Int
    , tlMinLeafSize :: !Int
    , tlMinImpurityDecrease :: !Double
    , tlL2 :: !Double
    -- ^ Added to every node's total weight: shrinks leaf values and split gains.
    , tlMinChildWeight :: !Double
    -- ^ Smallest total weight a child may have.
    }

{- | Per-bin Σw, Σwy, Σwy² and row count, four slots per bin, null bin last.
Built strictly: left lazy, the histograms of a deep tree pile up as thunks.
-}
type Hist = VU.Vector Double

{- | Every feature's histogram for a node, in one pass over its rows. Each
feature's slice of the result is its 'Hist'.
-}
buildHists ::
    BinnedFeatures ->
    VU.Vector Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Int ->
    V.Vector Hist
buildHists bf allowed w y idxs = V.imap slice (bfFeatures bf)
  where
    nf = V.length (bfFeatures bf)
    na = VU.length allowed
    slice j b = VU.slice (VU.unsafeIndex (bfOffsets bf) j) (4 * (bnNBins b + 1)) flat
    flat = runST $ do
        m <- VUM.replicate (bfSlots bf) 0
        VU.forM_ idxs $ \i -> do
            let wi = VU.unsafeIndex w i
                yi = VU.unsafeIndex y i
                wy = wi * yi
                wyy = wy * yi
                row = i * nf
                go !k
                    | k >= na = pure ()
                    | otherwise = do
                        let j = VU.unsafeIndex allowed k
                            bin = fromIntegral (VU.unsafeIndex (bfRows bf) (row + j))
                            s = VU.unsafeIndex (bfOffsets bf) j + 4 * bin
                        VUM.unsafeModify m (+ wi) s
                        VUM.unsafeModify m (+ wy) (s + 1)
                        VUM.unsafeModify m (+ wyy) (s + 2)
                        VUM.unsafeModify m (+ 1) (s + 3)
                        go (k + 1)
            go 0
        VU.unsafeFreeze m

data Node
    = NLeaf !Double !(VU.Vector Int)
    | NBranch !(Expr Bool) !Node !Node

{- | Fit a weighted-SSE regression tree on binned features; returns the tree
and its in-sample predictions.
-}
fitBinnedTree ::
    TreeLimits ->
    BinnedFeatures ->
    VU.Vector Double ->
    Maybe (VU.Vector Double) ->
    (Tree Double, VU.Vector Double)
fitBinnedTree lim bf = fitBinnedTreeOn (VU.enumFromN 0 (V.length (bfFeatures bf))) lim bf

-- | 'fitBinnedTree' splitting only on the features at the given indices.
fitBinnedTreeOn ::
    VU.Vector Int ->
    TreeLimits ->
    BinnedFeatures ->
    VU.Vector Double ->
    Maybe (VU.Vector Double) ->
    (Tree Double, VU.Vector Double)
fitBinnedTreeOn allowed lim bf y mw = (toTree root, inSample)
  where
    binned = bfFeatures bf
    n = VU.length y
    w = Data.Maybe.fromMaybe (VU.replicate n 1) mw
    allIdx = VU.enumFromN 0 n
    root = node 0 allIdx (hists allIdx)
    inSample = VU.update (VU.replicate n 0) (VU.concat (leafRows root))
    leafRows (NLeaf v idxs) = [VU.map (,v) idxs]
    leafRows (NBranch _ l r) = leafRows l ++ leafRows r

    hists = buildHists bf allowed w y
    -- Every allowed feature's histogram covers all of a node's rows.
    totalsOf hs = histTotals (hs V.! VU.head allowed)

    node depth idxs hs
        | depth >= tlMaxDepth lim || VU.length idxs < tlMinSamplesSplit lim || V.null hs =
            leaf
        | otherwise = maybe leaf split (bestSplit lim binned allowed hs)
      where
        Totals tw tsy _ _ = totalsOf hs
        leaf = NLeaf (if tw + tlL2 lim == 0 then 0 else tsy / (tw + tlL2 lim)) idxs
        split (fj, b, nullLeft) =
            forceNode l `par` (forceNode r `pseq` NBranch cond l r)
          where
            bn = binned V.! fj
            goesLeft i =
                let k = fromIntegral (VU.unsafeIndex (bnBins bn) i)
                 in if k == bnNBins bn then nullLeft else k <= b
            lefts = VU.filter goesLeft idxs
            rights = VU.filter (not . goesLeft) idxs
            -- Histogram the smaller child; the larger is parent minus it.
            smallIsLeft = VU.length lefts <= VU.length rights
            smallH = hists (if smallIsLeft then lefts else rights)
            bigH = V.zipWith (VU.zipWith (-)) hs smallH
            (lh, rh) = if smallIsLeft then (smallH, bigH) else (bigH, smallH)
            l = node (depth + 1) lefts lh
            r = node (depth + 1) rights rh
            side = if nullLeft then NullsLeft else NullsRight
            cond = cfSplit (bnFeature bn) (bnUpper bn VU.! b) side

data Totals = Totals !Double !Double !Double !Double

histTotals :: Hist -> Totals
histTotals h = go 0 0 0 0 0
  where
    slots = VU.length h `div` 4
    go !k !a !b !c !d
        | k >= slots = Totals a b c d
        | otherwise =
            go
                (k + 1)
                (a + h VU.! (4 * k))
                (b + h VU.! (4 * k + 1))
                (c + h VU.! (4 * k + 2))
                (d + h VU.! (4 * k + 3))

{- | Best @(feature, bin, nulls left)@ by SSE reduction. Each bin boundary is
scored with the node's null rows on the right and then on the left; ties keep
the earliest candidate, as in the exact sweep.
-}
bestSplit ::
    TreeLimits ->
    V.Vector Binned ->
    VU.Vector Int ->
    V.Vector Hist ->
    Maybe (Int, Int, Bool)
bestSplit lim binned allowed hs
    | null candidates = Nothing
    | red > 0 && red >= tlMinImpurityDecrease lim = Just sp
    | otherwise = Nothing
  where
    Totals totW totSY totSY2 totC = histTotals (hs V.! VU.head allowed)
    nNode = round totC :: Int
    sse = penalisedSse (tlL2 lim)
    nodeSSE = sse totSY totSY2 totW
    candidates = concat [featBest fj (hs V.! fj) | fj <- VU.toList allowed]
    (red, sp) = maximumByFst candidates
    featBest fj h = [(r, (fj, b, nl)) | (b, nl, r) <- maybeToList (scan 0 0 0 0 0 Nothing)]
      where
        nb = bnNBins (binned V.! fj)
        nW = h VU.! (4 * nb)
        nSY = h VU.! (4 * nb + 1)
        nSY2 = h VU.! (4 * nb + 2)
        nC = round (h VU.! (4 * nb + 3)) :: Int
        hasNulls = nC > 0
        lastB = if hasNulls then nb - 1 else nb - 2
        score nl wl syl syl2
            | nl >= tlMinLeafSize lim
            , nNode - nl >= tlMinLeafSize lim
            , wl > 0
            , wr > 0
            , wl >= tlMinChildWeight lim
            , wr >= tlMinChildWeight lim =
                Just (nodeSSE - (sse syl syl2 wl + sse (totSY - syl) (totSY2 - syl2) wr))
            | otherwise = Nothing
          where
            wr = totW - wl
        scan ::
            Int ->
            Double ->
            Double ->
            Double ->
            Int ->
            Maybe (Int, Bool, Double) ->
            Maybe (Int, Bool, Double)
        scan !b !wl !syl !syl2 !cl best
            | b > lastB = best
            | otherwise = scan (b + 1) wl' syl' syl2' cl' best'
          where
            wl' = wl + h VU.! (4 * b)
            syl' = syl + h VU.! (4 * b + 1)
            syl2' = syl2 + h VU.! (4 * b + 2)
            cl' = cl + round (h VU.! (4 * b + 3))
            right = (,) False <$> score cl' wl' syl' syl2'
            left
                | hasNulls =
                    (,) True <$> score (cl' + nC) (wl' + nW) (syl' + nSY) (syl2' + nSY2)
                | otherwise = Nothing
            consider bst (dir, r)
                | maybe True (\(_, _, rb) -> r > rb) bst = Just (b, dir, r)
                | otherwise = bst
            best' = foldl consider best (maybeToList right ++ maybeToList left)

{- | Weighted SSE of a node from its Σy, Σy², and total weight, with @l2@ added
to the weight. Differences of these give the Newton split gain with an L2 leaf
penalty; at @l2 = 0@ it is the plain SSE.
-}
penalisedSse :: Double -> Double -> Double -> Double -> Double
penalisedSse l2 sumY sumSq wt = sumSq - (if wt + l2 == 0 then 0 else sumY * sumY / (wt + l2))

maximumByFst :: (Ord a) => [(a, b)] -> (a, b)
maximumByFst = foldr1 (\x@(a, _) y@(b, _) -> if a >= b then x else y)

toTree :: Node -> Tree Double
toTree (NLeaf v _) = Leaf v
toTree (NBranch c l r) = Branch c (toTree l) (toTree r)

-- | Force a subtree so the sibling's spark has real work (cf. 'Regression').
forceNode :: Node -> ()
forceNode (NLeaf v _) = v `seq` ()
forceNode (NBranch _ l r) = forceNode l `seq` forceNode r
