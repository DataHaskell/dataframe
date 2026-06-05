{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Candidate-pool scoring and boolean expansion: penalized scoring, diverse
-- top-K selection, AND/OR saturation, and structural/truth-vector dedup. The
-- per-node scoring scans run in parallel chunks.
module DataFrame.DecisionTree.Pool (
    evalWithPenaltyVec,
    primaryColExpr,
    primaryColCV,
    takeDiverse,
    candidateParChunk,
    bestDiscreteCandidate,
    boolExprsVec,
    DedupMode (..),
    saturateCandidates,
    roundProducts,
    admitKeys,
    admitVecs,
    dedupCVByExpr,
    nubByExpr,
) where

import DataFrame.DecisionTree.CondVec (CondVec (..), combineAndVec, combineOrVec, countErrorsByVec)
import DataFrame.DecisionTree.Types (CarePoint, SynthConfig (..), TreeConfig (..))
import DataFrame.Internal.Expression (Expr, compareExpr, eSize, eqExpr, getColumns, normalize)

import Control.Parallel.Strategies (parListChunk, rdeepseq, using)
import Data.Function (on)
import Data.List (minimumBy, sortBy)
import qualified Data.Map.Strict as M
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU

-- | Penalized score of a candidate: care-point errors plus a complexity
-- penalty, tie-broken by expression size.
evalWithPenaltyVec :: TreeConfig -> [CarePoint] -> CondVec -> (Int, Int)
evalWithPenaltyVec cfg carePoints cv = (countErrorsByVec (cvVec cv) carePoints + penalty, sz)
  where
    sz = eSize (cvExpr cv)
    penalty = floor (complexityPenalty (synthConfig cfg) * fromIntegral sz)

-- | First referenced column of a condition (a sentinel for literal-only ones),
-- used by 'takeDiverse' to enforce per-column diversity.
primaryColExpr :: Expr Bool -> T.Text
primaryColExpr e = case getColumns e of
    [] -> "<noncol>"
    (c : _) -> c

primaryColCV :: CondVec -> T.Text
primaryColCV = primaryColExpr . cvExpr

-- | Keep the first @k@ of an already-sorted list, admitting at most @quota@ per
-- primary column (@Nothing@ disables the per-column cap).
takeDiverse :: Int -> Maybe Int -> (a -> T.Text) -> [a] -> [a]
takeDiverse k Nothing _ = take k
takeDiverse k (Just quota) primary = go M.empty 0
  where
    go !_ !_ [] = []
    go !seen !n (x : xs)
        | n >= k = []
        | M.findWithDefault 0 col seen >= quota = go seen n xs
        | otherwise = x : go (M.insertWith (+) col 1 seen) (n + 1) xs
      where
        !col = primary x

-- | Chunk size for the parallel per-node candidate scans; tuned by an -N
-- sweep, not correctness-affecting.
candidateParChunk :: Int
candidateParChunk = 64

-- | Decorate candidates with their penalty in parallel chunks, forcing only
-- the @(Int, Int)@ key so the order (hence later sorts/minima) is preserved.
decorate :: (CondVec -> (Int, Int)) -> [CondVec] -> [((Int, Int), CondVec)]
decorate penaltyCV xs = zip (map penaltyCV xs `using` parListChunk candidateParChunk rdeepseq) xs

-- | The diverse top-@expressionPairs@ valid candidates by penalty.
sortedTopK :: TreeConfig -> (CondVec -> (Int, Int)) -> [CondVec] -> [CondVec]
sortedTopK cfg penaltyCV validCondVecs =
    map snd (takeDiverse (expressionPairs cfg) (perColumnQuota (synthConfig cfg)) (primaryColCV . snd) sorted)
  where
    sorted = sortBy (compare `on` fst) (decorate penaltyCV validCondVecs)

-- | Lowest-penalty candidate after boolean saturation of the diverse top-K.
bestDiscreteCandidate :: TreeConfig -> (CondVec -> (Int, Int)) -> [CondVec] -> Maybe CondVec
bestDiscreteCandidate _ _ [] = Nothing
bestDiscreteCandidate cfg penaltyCV validCondVecs =
    case saturateCandidates Structural (boolExpansion (synthConfig cfg)) (sortedTopK cfg penaltyCV validCondVecs) of
        [] -> Nothing
        xs -> Just (snd (minimumBy (compare `on` fst) (decorate penaltyCV xs)))

-- | AND/OR expansion of cached conditions to depth @maxDepth@ (each
-- combination is a single vector op, not an interpret).
boolExprsVec :: [CondVec] -> [CondVec] -> Int -> Int -> [CondVec]
boolExprsVec baseExprs prevExprs depth maxDepth
    | depth == 0 = baseExprs ++ boolExprsVec baseExprs prevExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise = combined ++ boolExprsVec baseExprs combined (depth + 1) maxDepth
  where
    combined = roundProducts prevExprs baseExprs

data DedupMode = Structural | TruthVector
    deriving (Eq, Show)

-- | Saturate the pool with AND/OR combinations, deduplicating structurally
-- (byte-identical, first occurrence kept) or by truth vector (opt-in).
saturateCandidates :: DedupMode -> Int -> [CondVec] -> [CondVec]
saturateCandidates Structural maxDepth base = base' ++ go 1 base' seen0
  where
    (base', seen0) = admitKeys Set.empty base
    go !depth frontier seen
        | depth >= maxDepth || null frontier = []
        | otherwise = let (admitted, seen') = admitKeys seen (roundProducts frontier base) in admitted ++ go (depth + 1) admitted seen'
saturateCandidates TruthVector maxDepth base = M.elems (go 1 frontier0 reps0)
  where
    (reps0, frontier0) = admitVecs M.empty base
    go !depth frontier reps
        | depth >= maxDepth || null frontier = reps
        | otherwise = let (reps', admitted) = admitVecs reps (roundProducts frontier base) in go (depth + 1) admitted reps'

-- | One combination round: @frontier × base@ via AND then OR, skipping
-- self-pairs (mirrors 'boolExprsVec' for byte-identical structural output).
roundProducts :: [CondVec] -> [CondVec] -> [CondVec]
roundProducts frontier base =
    [c | e1 <- frontier, e2 <- base, not (eqExpr (cvExpr e1) (cvExpr e2)), c <- [combineAndVec e1 e2, combineOrVec e1 e2]]

-- | Admit candidates with a not-yet-seen normalized form, preserving order.
admitKeys :: Set.Set String -> [CondVec] -> ([CondVec], Set.Set String)
admitKeys = go []
  where
    go acc seen [] = (reverse acc, seen)
    go acc !seen (c : cs)
        | structuralKey c `Set.member` seen = go acc seen cs
        | otherwise = go (c : acc) (Set.insert (structuralKey c) seen) cs

structuralKey :: CondVec -> String
structuralKey = show . normalize . cvExpr

-- | Admit candidates by distinct truth vector, keeping the smallest-expression
-- representative per vector.
admitVecs :: M.Map (VU.Vector Bool) CondVec -> [CondVec] -> (M.Map (VU.Vector Bool) CondVec, [CondVec])
admitVecs = go []
  where
    go acc reps [] = (reps, reverse acc)
    go acc !reps (c : cs) = case M.lookup (cvVec c) reps of
        Nothing -> go (c : acc) (M.insert (cvVec c) c reps) cs
        Just r -> go acc (M.insert (cvVec c) (smaller r c) reps) cs

smaller :: CondVec -> CondVec -> CondVec
smaller a b = case compare (eSize (cvExpr a)) (eSize (cvExpr b)) of
    LT -> a
    GT -> b
    EQ -> if compareExpr (cvExpr a) (cvExpr b) /= GT then a else b

-- | Deduplicate 'CondVec's by normalized 'cvExpr', keeping the first.
dedupCVByExpr :: [CondVec] -> [CondVec]
dedupCVByExpr = go Set.empty
  where
    go _ [] = []
    go seen (cv : cvs)
        | structuralKey cv `Set.member` seen = go seen cvs
        | otherwise = cv : go (Set.insert (structuralKey cv) seen) cvs

-- | Deduplicate expressions by normalized form, keeping the first.
nubByExpr :: [Expr Bool] -> [Expr Bool]
nubByExpr = go Set.empty
  where
    go _ [] = []
    go seen (e : es)
        | k `Set.member` seen = go seen es
        | otherwise = e : go (Set.insert k seen) es
      where
        k = show (normalize e)
