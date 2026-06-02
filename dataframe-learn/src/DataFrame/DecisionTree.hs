{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.DecisionTree where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnNames,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (
    Expr (..),
    binaryName,
    compareExpr,
    eSize,
    eqExpr,
    getColumns,
    normalize,
 )
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Simplify (PredFact, entails, factFalse, factTrue)
import DataFrame.Internal.Statistics (percentileOrd')
import DataFrame.Internal.Types
import qualified DataFrame.LinearSolver as LS
import DataFrame.Operations.Core (nRows)
import DataFrame.Operations.Subset (exclude, filterWhere)

import Control.Exception (throw)
import Control.Monad (guard)
import Control.Monad.ST (ST, runST)
import Data.Function (on)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.List (
    foldl',
    inits,
    maximumBy,
    minimumBy,
    nub,
    nubBy,
    sort,
    sortBy,
    subsequences,
 )
import qualified Data.Map.Strict as M
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Proxy (Proxy (..))
import qualified Data.Set as Set
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import qualified Debug.Trace as Trace
import System.Environment (lookupEnv)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (SomeTypeRep (..), typeRep)

import DataFrame.Operators

{-# NOINLINE taoTraceEnabled #-}

{- | Profiling toggle. Set @TAO_TRACE=1@ in the environment to enable
the temporary instrumentation calls below. No overhead when unset.
-}
taoTraceEnabled :: Bool
taoTraceEnabled = unsafePerformIO $ do
    v <- lookupEnv "TAO_TRACE"
    pure (v == Just "1")

ttrace :: String -> a -> a
ttrace msg x
    | taoTraceEnabled = Trace.trace ("[TAO] " ++ msg) x
    | otherwise = x

data CondVec = CondVec
    { cvExpr :: !(Expr Bool)
    , cvVec :: !(VU.Vector Bool)
    }

{- | Build a 'CondVec' by interpreting the expression once over the full
DataFrame.  'Nothing' on type/interpret failure (so the candidate is
silently dropped from the cache).
-}
materializeCondVec :: DataFrame -> Expr Bool -> Maybe CondVec
materializeCondVec df cond =
    case interpret @Bool df cond of
        Left _ -> Nothing
        Right (TColumn column) -> case toVector @Bool @VU.Vector column of
            Left _ -> Nothing
            Right vec -> Just (CondVec cond vec)

{- | Partition @indices@ by the cached Bool vector: True → left, False → right.
Uses 'VU.unsafeIndex'; safe because the vector spans the full DataFrame so
every entry in @indices@ is in range by construction.
-}
partitionByVec ::
    VU.Vector Bool -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionByVec boolVals = V.partition (boolVals VU.!)

-- | Count misrouted care points for a condition whose Bool vector is cached.
countErrorsByVec :: VU.Vector Bool -> [CarePoint] -> Int
countErrorsByVec boolVals =
    length . filter isMis
  where
    isMis cp =
        let goesLeft = boolVals VU.! cpIndex cp
            shouldGoLeft = cpCorrectDir cp == GoLeft
         in goesLeft /= shouldGoLeft

consolidateThreshold ::
    Bool -> -- True = AND, False = OR
    Expr Bool ->
    Expr Bool ->
    Maybe (Expr Bool)
consolidateThreshold isAnd ea eb = case (ea, eb) of
    ( Binary op1 (Col c1 :: Expr c1) (Lit (t1 :: t1))
        , Binary op2 (Col c2 :: Expr c2) (Lit (t2 :: t2))
        ) ->
            case ( testEquality (typeRep @c1) (typeRep @Double)
                 , testEquality (typeRep @c2) (typeRep @Double)
                 , testEquality (typeRep @t1) (typeRep @Double)
                 , testEquality (typeRep @t2) (typeRep @Double)
                 ) of
                (Just Refl, Just Refl, Just Refl, Just Refl)
                    | c1 == c2
                    , binaryName op1 == binaryName op2
                    , let name = binaryName op1
                    , name `elem` (["lt", "leq", "gt", "geq"] :: [T.Text]) ->
                        -- For same-direction same-column thresholds:
                        --   '<' / '<=' (left-half-spaces):
                        --     AND = tighter = min(t1, t2)
                        --     OR  = looser  = max(t1, t2)
                        --   '>' / '>=' (right-half-spaces):
                        --     AND = tighter = max(t1, t2)
                        --     OR  = looser  = min(t1, t2)
                        let leftDir = name == "lt" || name == "leq"
                            chosen
                                | leftDir, isAnd = min (t1 :: Double) t2
                                | leftDir = max t1 t2
                                | isAnd = max t1 t2
                                | otherwise = min t1 t2
                         in Just (Binary op1 (Col c1) (Lit chosen))
                _ -> Nothing
    _ -> Nothing

{- | AND-combine two cached conditions: the resulting Bool vector is the
elementwise conjunction; the Expr is @F.and a b@.

Applies (in order):

  1. Idempotence: if both operands are 'eqExpr'-equal, return the left
     operand unchanged. Catches @(x > c) ∧ (x > c)@ tautologies that
     'nubBy eqExpr' upstream missed.

  2. Threshold consolidation (see 'consolidateThreshold'): same-column
     same-direction strict-Double comparisons collapse to a single
     comparison. The cached Bool vector is then the elementwise
     conjunction of the two inputs — bit-exact because @(x>a) ∧ (x>b)@
     is true iff both @x>a@ and @x>b@ are true regardless of how we
     express the consolidated form.

These rewrites cut the K² blow-up in 'boolExprsVec' driven by correlated
thresholds (the BCW-regression mechanism diagnosed in Pre-flight §B). The
generic @F.and@ fallback fires only when neither shortcut applies.
-}
combineAndVec :: CondVec -> CondVec -> CondVec
combineAndVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | Just consolidated <- consolidateThreshold True (cvExpr a) (cvExpr b) =
        CondVec consolidated (VU.zipWith (&&) (cvVec a) (cvVec b))
    | otherwise =
        CondVec
            (F.and (cvExpr a) (cvExpr b))
            (VU.zipWith (&&) (cvVec a) (cvVec b))

{- | OR-combine two cached conditions. Idempotence + threshold consolidation
applied as in 'combineAndVec' (the AND/OR direction differs; see
'consolidateThreshold').
-}
combineOrVec :: CondVec -> CondVec -> CondVec
combineOrVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | Just consolidated <- consolidateThreshold False (cvExpr a) (cvExpr b) =
        CondVec consolidated (VU.zipWith (||) (cvVec a) (cvVec b))
    | otherwise =
        CondVec
            (F.or (cvExpr a) (cvExpr b))
            (VU.zipWith (||) (cvVec a) (cvVec b))

{- | Declares which column types support ordering for decision tree splits.

Use 'orderable' to register a type, and '<>' to combine:

@
defaultTreeConfig
    { columnOrdering = defaultColumnOrdering <> orderable \@MyCustomType
    }
@
-}
newtype ColumnOrdering = ColumnOrdering (M.Map SomeTypeRep OrdDict)

instance Semigroup ColumnOrdering where
    ColumnOrdering a <> ColumnOrdering b = ColumnOrdering (a <> b)

instance Monoid ColumnOrdering where
    mempty = ColumnOrdering M.empty

-- | Register a type as orderable for decision tree splits.
orderable :: forall a. (Columnable a, Ord a) => ColumnOrdering
orderable = ColumnOrdering (M.singleton (SomeTypeRep (typeRep @a)) (OrdDict (Proxy @a)))

-- | All standard numeric, text, and primitive types.
defaultColumnOrdering :: ColumnOrdering
defaultColumnOrdering =
    mconcat
        [ orderable @Int
        , orderable @Int8
        , orderable @Int16
        , orderable @Int32
        , orderable @Int64
        , orderable @Word
        , orderable @Word8
        , orderable @Word16
        , orderable @Word32
        , orderable @Word64
        , orderable @Integer
        , orderable @Double
        , orderable @Float
        , orderable @Bool
        , orderable @Char
        , orderable @T.Text
        , orderable @String
        ]

-- Internal: existential Ord dictionary.
data OrdDict where
    OrdDict :: (Columnable a, Ord a) => Proxy a -> OrdDict

-- Internal: look up Ord for type @a@.
withOrdFrom ::
    forall a r. (Columnable a) => ColumnOrdering -> ((Ord a) => r) -> Maybe r
withOrdFrom (ColumnOrdering m) k = case M.lookup (SomeTypeRep (typeRep @a)) m of
    Just (OrdDict (_ :: Proxy b)) -> case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Just k
        Nothing -> Nothing
    Nothing -> Nothing

data TreeConfig = TreeConfig
    { maxTreeDepth :: Int
    , minSamplesSplit :: Int
    , minLeafSize :: Int
    , percentiles :: [Int]
    , expressionPairs :: Int
    , synthConfig :: SynthConfig
    , taoIterations :: Int
    , taoConvergenceTol :: Double
    , columnOrdering :: ColumnOrdering
    , useLinearSolver :: Bool
    {- ^ When 'True', each internal-node TAO update also fits an L1-LR on care
    points; the resulting oblique hyperplane competes with the discrete
    pool (the current condition is always included in the competition).
    -}
    , linearSolverConfig :: LS.SolverConfig
    {- ^ Hyper-parameters for the L1-LR solver. Only used when
    'useLinearSolver' is 'True'.
    -}
    , minCarePointsForLinear :: Int
    {- ^ Skip the linear path when fewer than this many care points are
    available; fitting on tiny sample sizes is dominated by noise.
    -}
    , pureReplacementLinear :: Bool
    {- ^ When 'True', a valid linear candidate replaces the current condition
    unconditionally (paper-faithful pure replacement), bypassing the
    competition. Default 'False' keeps the existing compete behavior.
    -}
    }

data SynthConfig = SynthConfig
    { maxExprDepth :: Int
    , boolExpansion :: Int
    , disallowedCombinations :: [(T.Text, T.Text)]
    , complexityPenalty :: Double
    , enableStringOps :: Bool
    , enableCrossCols :: Bool
    , enableArithOps :: Bool
    , maxCategoricalSubsetCardinality :: Int
    {- ^ Above this many distinct values, multi-class targets fall back from
    2^k subset enumeration to k singletons. Binary targets always use
    Breiman's O(k log k) sort-by-positive-rate regardless of cardinality.
    -}
    , perColumnQuota :: Maybe Int
    {- ^ Cap on how many top-K candidates may share the same primary column
    (first 'getColumns' entry). 'Nothing' = no cap (legacy behaviour, useful
    for A/B). 'Just q' = at most @q@ conditions per column survive the
    gain-sort + take-K step in 'bestDiscreteCandidate'. Forces diversity in the
    @sortedConditions@ pool fed to 'boolExprsVec'/'saturateCandidates'; empirically guards
    small-n folds (BCW, Wine) from correlated-threshold tautologies.
    Equivalent in spirit to Random Forest @mtry@ column sampling
    (Breiman 2001, §3) and RuleFit rule-diversity (Friedman & Popescu 2008).
    -}
    }
    deriving (Eq, Show)

defaultSynthConfig :: SynthConfig
defaultSynthConfig =
    SynthConfig
        { maxExprDepth = 2
        , boolExpansion = 2
        , disallowedCombinations = []
        , complexityPenalty = 0.05
        , enableStringOps = True
        , enableCrossCols = True
        , enableArithOps = True
        , maxCategoricalSubsetCardinality = 4
        , perColumnQuota = Just 3
        }

defaultTreeConfig :: TreeConfig
defaultTreeConfig =
    TreeConfig
        { maxTreeDepth = 4
        , minSamplesSplit = 5
        , minLeafSize = 1
        , percentiles = [0, 10 .. 100]
        , expressionPairs = 10
        , synthConfig = defaultSynthConfig
        , taoIterations = 10
        , taoConvergenceTol = 1e-6
        , columnOrdering = defaultColumnOrdering
        , useLinearSolver = True
        , linearSolverConfig = LS.defaultSolverConfig
        , minCarePointsForLinear = 10
        , pureReplacementLinear = False
        }

data Tree a
    = Leaf !a
    | Branch !(Expr Bool) !(Tree a) !(Tree a)
    deriving (Show)

treeDepth :: Tree a -> Int
treeDepth (Leaf _) = 0
treeDepth (Branch _ l r) = 1 + max (treeDepth l) (treeDepth r)

treeToExpr :: (Columnable a) => Tree a -> Expr a
treeToExpr (Leaf v) = Lit v
treeToExpr (Branch cond left right) =
    F.ifThenElse cond (treeToExpr left) (treeToExpr right)

-- | Fit a TAO decision tree
fitDecisionTree ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    Expr a ->
    DataFrame ->
    Expr a
fitDecisionTree cfg (Col target) df =
    let
        !numConds = numericConditions cfg (exclude [target] df)
        !targetInfo = case mkTargetInfo @a target df of
            Nothing -> TargetInfo False Nothing V.empty
            Just ti -> ti
        !oldConds = discreteConditions targetInfo cfg (exclude [target] df)
        !rawCount = length numConds + length oldConds
        !conds = nubByExpr (numConds ++ oldConds)
        !nConds = length conds
        -- Pre-evaluate every candidate condition to a Bool vector ONCE over
        -- the full DataFrame.  All downstream node-level scoring uses these
        -- cached vectors instead of calling 'interpret' per (node, candidate).
        !condVecs = mapMaybe (materializeCondVec df) conds

        !initialTree =
            ttrace
                ( "fitDecisionTree: rows="
                    ++ show (nRows df)
                    ++ " rawConds="
                    ++ show rawCount
                    ++ " (numeric="
                    ++ show (length numConds)
                    ++ " old="
                    ++ show (length oldConds)
                    ++ ") deduped="
                    ++ show nConds
                    ++ " cached="
                    ++ show (length condVecs)
                )
                -- Seed TAO from a faithful sklearn-style CART (axis-aligned, exact
                -- Gini). One-time cost per fit; the inner TAO loop is the hot path.
                (buildCartTree @a cfg target df)

        indices = V.enumFromN 0 (nRows df)

        optimizedTree =
            ttrace "fitDecisionTree: built CART init tree, starting TAO" $
                -- Go straight to the internal CV worker — no need to re-marshal
                -- through the public [Expr Bool] wrapper.
                taoOptimizeCV @a cfg target condVecs df indices initialTree
        finalExpr = pruneExpr (treeToExpr optimizedTree)
     in
        ttrace
            ( "P1/tree: useLinear="
                ++ show (useLinearSolver cfg)
                ++ " pureReplace="
                ++ show (pureReplacementLinear cfg)
                ++ " final="
                ++ show finalExpr
            )
            $ ttrace "fitDecisionTree: TAO done" finalExpr
fitDecisionTree _ expr _ = error $ "Cannot create tree for compound expression: " ++ show expr

{- | Public TAO entry point.  Accepts a raw list of candidate split
conditions for ergonomic use from outside (tests, ad-hoc scripts).
Materializes each condition's Bool vector once against @df@ and delegates
to the internal worker 'taoOptimizeCV', which does the real work over
pre-evaluated 'CondVec's.
-}
taoOptimize ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
taoOptimize cfg target conds df rootIndices initialTree =
    let !condVecs = mapMaybe (materializeCondVec df) conds
     in taoOptimizeCV @a cfg target condVecs df rootIndices initialTree

{- | Internal TAO outer loop — operates on pre-evaluated 'CondVec's so the
per-candidate scoring never re-interprets the same expression.
-}
taoOptimizeCV ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text -> -- Target column name
    [CondVec] -> -- Pre-evaluated candidate conditions
    DataFrame -> -- Full dataset
    V.Vector Int -> -- Indices of points reaching the root
    Tree a -> -- Current tree
    Tree a
taoOptimizeCV cfg target condVecs df rootIndices initialTree =
    go 0 initialTree (computeTreeLoss @a target df rootIndices initialTree)
  where
    go :: Int -> Tree a -> Double -> Tree a
    go iter tree prevLoss
        | iter >= taoIterations cfg = pruneDead tree
        | otherwise =
            let
                !tree' =
                    ttrace
                        ( "taoIteration: iter="
                            ++ show iter
                            ++ " prevLoss="
                            ++ show prevLoss
                        )
                        (taoIterationCV @a cfg target condVecs df rootIndices tree)

                !newLoss = computeTreeLoss @a target df rootIndices tree'
                improvement = prevLoss - newLoss
             in
                ttrace ("taoIteration: iter=" ++ show iter ++ " done newLoss=" ++ show newLoss) $
                    if improvement < taoConvergenceTol cfg
                        then pruneDead tree'
                        else go (iter + 1) tree' newLoss

{- | Public single-iteration entry point.  Same marshalling pattern as
'taoOptimize'.
-}
taoIteration ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
taoIteration cfg target conds df rootIndices tree =
    let !condVecs = mapMaybe (materializeCondVec df) conds
     in taoIterationCV @a cfg target condVecs df rootIndices tree

-- | Internal single-iteration worker over pre-evaluated 'CondVec's.
taoIterationCV ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [CondVec] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
taoIterationCV cfg target condVecs df rootIndices tree =
    let depth = treeDepth tree
     in foldl'
            (optimizeDepthLevel @a cfg target condVecs df rootIndices)
            tree
            [depth, depth - 1 .. 0] -- Bottom to top

optimizeDepthLevel ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [CondVec] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Int -> -- Target depth
    Tree a
optimizeDepthLevel cfg target condVecs df rootIndices tree = optimizeAtDepth @a cfg target condVecs df rootIndices tree 0

optimizeAtDepth ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [CondVec] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Int ->
    Int ->
    Tree a
optimizeAtDepth cfg target condVecs df indices tree currentDepth targetDepth
    | currentDepth == targetDepth =
        optimizeNode @a cfg target condVecs df indices tree
    | otherwise = case tree of
        Leaf v -> Leaf v
        Branch cond left right ->
            let
                -- Tree branch conditions aren't necessarily in our cache
                -- (they could be boolExprs combinations). Fall back to the
                -- old interpret-based partition here; it's at most one call
                -- per branch traversal, which is cheap compared to the
                -- per-candidate scoring inside optimizeNode.
                (indicesL, indicesR) = partitionIndices cond df indices
                left' =
                    optimizeAtDepth @a
                        cfg
                        target
                        condVecs
                        df
                        indicesL
                        left
                        (currentDepth + 1)
                        targetDepth
                right' =
                    optimizeAtDepth @a
                        cfg
                        target
                        condVecs
                        df
                        indicesR
                        right
                        (currentDepth + 1)
                        targetDepth
             in
                Branch cond left' right'

optimizeNode ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    T.Text ->
    [CondVec] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
optimizeNode cfg target condVecs df indices tree
    | V.null indices = tree
    | otherwise = case tree of
        Leaf _ -> Leaf (majorityValueFromIndices @a target df indices)
        Branch oldCond left right ->
            let
                newCond = findBestSplitTAO @a cfg target condVecs df indices left right oldCond

                -- New cond may be a cached candidate OR the unchanged
                -- oldCond — either way, partition once via interpret.
                (newIndicesL, newIndicesR) = partitionIndices newCond df indices
             in
                if V.length newIndicesL < minLeafSize cfg
                    || V.length newIndicesR < minLeafSize cfg
                    then Leaf (majorityValueFromIndices @a target df indices)
                    else Branch newCond left right

findBestSplitTAO ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text ->
    [CondVec] ->
    DataFrame ->
    V.Vector Int ->
    Tree a -> -- Left subtree (FIXED)
    Tree a -> -- Right subtree (FIXED)
    Expr Bool -> -- Current condition (fallback)
    Expr Bool
findBestSplitTAO cfg target condVecs df indices leftTree rightTree currentCond
    | V.null indices = currentCond
    | null carePoints =
        ttrace "findBestSplitTAO: no care points → keep current" currentCond
    | pureReplacementLinear cfg, Just c <- linearCandidate, validExpr c = c
    | null poolCV = currentCond
    | otherwise =
        ttrace
            ( "findBestSplitTAO: idx="
                ++ show (V.length indices)
                ++ " care="
                ++ show (length carePoints)
                ++ " valid="
                ++ show (length validCondVecs)
                ++ " conds="
                ++ show (length condVecs)
            )
            (cvExpr (minimumBy (compare `on` penaltyCV) poolCV))
  where
    !carePoints = identifyCarePoints @a target df indices leftTree rightTree
    penaltyCV = evalWithPenaltyVec cfg carePoints
    -- For a CondVec (cached), validity check is index-only.
    validVec cv =
        let (t, f) = partitionByVec (cvVec cv) indices
         in V.length t >= minLeafSize cfg && V.length f >= minLeafSize cfg
    -- For a free Expr Bool (e.g., currentCond, linear candidate), fall back
    -- to the interpret-based check.
    validExpr = isValidAtNode cfg df indices
    !validCondVecs = filter validVec condVecs
    discreteCandidate = bestDiscreteCandidate cfg penaltyCV validCondVecs
    linearCandidate = bestLinearCandidate cfg df carePoints
    -- The current condition and the linear candidate aren't in the cache, so
    -- we materialize them into CondVecs here (one interpret each, per node).
    currentCV = materializeCondVec df currentCond
    linearCV =
        case linearCandidate of
            Nothing -> Nothing
            Just c ->
                let
                    valid = validExpr c
                    (tCnt, fCnt) =
                        let (t, f) = partitionIndices c df indices
                         in (V.length t, V.length f)
                 in
                    ttrace
                        ( "P1/poolCV: linear valid="
                            ++ show valid
                            ++ " split=("
                            ++ show tCnt
                            ++ ","
                            ++ show fCnt
                            ++ ") min="
                            ++ show (minLeafSize cfg)
                        )
                        (materializeCondVec df c)
    discreteCV = discreteCandidate
    poolCV =
        filter
            (validExpr . cvExpr)
            (catMaybes [currentCV, discreteCV, linearCV])

{- | Penalized score of a CondVec: care-point errors plus a complexity penalty,
tie-broken by expression size. Uses the cached Bool vector directly.
-}
evalWithPenaltyVec ::
    TreeConfig -> [CarePoint] -> CondVec -> (Int, Int)
evalWithPenaltyVec cfg carePoints cv =
    let errors = countErrorsByVec (cvVec cv) carePoints
        sz = eSize (cvExpr cv)
        penalty =
            floor (complexityPenalty (synthConfig cfg) * fromIntegral sz)
     in (errors + penalty, sz)

{- | Does a condition split @indices@ with both sides at least 'minLeafSize'?
Free-expression variant; used for the currentCond / linear-candidate paths
that don't have a cached vector.
-}
isValidAtNode :: TreeConfig -> DataFrame -> V.Vector Int -> Expr Bool -> Bool
isValidAtNode cfg df indices c =
    let (t, f) = partitionIndices c df indices
     in V.length t >= minLeafSize cfg && V.length f >= minLeafSize cfg

{- | Primary column of a candidate expression: the first @getColumns@ entry.
Used by the per-column quota in 'takeDiverse' to enforce diversity over
the sorted candidate pool. Returns a sentinel @"<noncol>"@ when the
expression has no column references (vanishingly rare; literal-only
predicates).
-}
primaryColExpr :: Expr Bool -> T.Text
primaryColExpr e = case getColumns e of
    [] -> T.pack "<noncol>"
    (c : _) -> c

primaryColCV :: CondVec -> T.Text
primaryColCV = primaryColExpr . cvExpr

{- | Greedy diverse top-K selector. Walks the (already-sorted) input and keeps
each candidate until its primary column has been picked @quota@ times.
-}
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

{- | Best discrete candidate. Takes the top-scoring valid CondVecs by penalty,
expands them with boolean combinations (cached AND/OR over Bool vectors),
returns the lowest-penalty result as a CondVec.

The top-K selection enforces the optional 'perColumnQuota' to keep the
sorted-and-truncated pool diverse across columns — without this, on
small-n folds the K² expansion in 'boolExprsVec' degenerates to
near-tautologies among correlated thresholds.
-}
bestDiscreteCandidate ::
    TreeConfig ->
    (CondVec -> (Int, Int)) ->
    [CondVec] ->
    Maybe CondVec
bestDiscreteCandidate _ _ [] = Nothing
bestDiscreteCandidate cfg penaltyCV validCondVecs =
    case saturateCandidates Structural (boolExpansion (synthConfig cfg)) sortedCondVecs of
        [] -> Nothing
        xs -> Just (minimumBy (compare `on` penaltyCV) xs)
  where
    sortedCondVecs =
        takeDiverse
            (expressionPairs cfg)
            (perColumnQuota (synthConfig cfg))
            primaryColCV
            (sortBy (compare `on` penaltyCV) validCondVecs)

{- | AND/OR expansion of CondVecs to depth @maxDepth@. Mirrors the old
@boolExprs@ but operates on cached Bool vectors so combinations cost a
single 'VU.zipWith' each instead of an 'interpret'.
-}
boolExprsVec :: [CondVec] -> [CondVec] -> Int -> Int -> [CondVec]
boolExprsVec baseExprs prevExprs depth maxDepth
    | depth == 0 =
        baseExprs ++ boolExprsVec baseExprs prevExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise =
        combinedExprs ++ boolExprsVec baseExprs combinedExprs (depth + 1) maxDepth
  where
    combinedExprs = do
        e1 <- prevExprs
        e2 <- baseExprs
        guard (not (eqExpr (cvExpr e1) (cvExpr e2)))
        [combineAndVec e1 e2, combineOrVec e1 e2]

data DedupMode = Structural | TruthVector
    deriving (Eq, Show)

saturateCandidates :: DedupMode -> Int -> [CondVec] -> [CondVec]
saturateCandidates Structural maxDepth base =
    base' ++ go 1 base' seen0
  where
    (base', seen0) = admitKeys Set.empty base
    go !depth frontier seen
        | depth >= maxDepth || null frontier = []
        | otherwise =
            let (admitted, seen') = admitKeys seen (roundProducts frontier base)
             in admitted ++ go (depth + 1) admitted seen'
saturateCandidates TruthVector maxDepth base =
    M.elems (go 1 frontier0 reps0)
  where
    (reps0, frontier0) = admitVecs M.empty base
    go !depth frontier reps
        | depth >= maxDepth || null frontier = reps
        | otherwise =
            let (reps', admitted) = admitVecs reps (roundProducts frontier base)
             in go (depth + 1) admitted reps'

{- | One combination round: @frontier × base@ via AND then OR, skipping self-pairs.
Mirrors 'boolExprsVec'\'s @combinedExprs@ so structural output is byte-identical.
-}
roundProducts :: [CondVec] -> [CondVec] -> [CondVec]
roundProducts frontier base =
    [ c
    | e1 <- frontier
    , e2 <- base
    , not (eqExpr (cvExpr e1) (cvExpr e2))
    , c <- [combineAndVec e1 e2, combineOrVec e1 e2]
    ]

admitKeys :: Set.Set String -> [CondVec] -> ([CondVec], Set.Set String)
admitKeys = go []
  where
    go acc seen [] = (reverse acc, seen)
    go acc !seen (c : cs)
        | k `Set.member` seen = go acc seen cs
        | otherwise = go (c : acc) (Set.insert k seen) cs
      where
        k = show (normalize (cvExpr c))

admitVecs ::
    M.Map (VU.Vector Bool) CondVec ->
    [CondVec] ->
    (M.Map (VU.Vector Bool) CondVec, [CondVec])
admitVecs = go []
  where
    go acc reps [] = (reps, reverse acc)
    go acc !reps (c : cs) =
        case M.lookup (cvVec c) reps of
            Nothing -> go (c : acc) (M.insert (cvVec c) c reps) cs
            Just r -> go acc (M.insert (cvVec c) (smaller r c) reps) cs
    smaller a b = case compare (eSize (cvExpr a)) (eSize (cvExpr b)) of
        LT -> a
        GT -> b
        EQ -> if compareExpr (cvExpr a) (cvExpr b) /= GT then a else b

{- | Best linear candidate, or 'Nothing' when the linear path is disabled or
there are too few care points to fit on.
-}
bestLinearCandidate ::
    TreeConfig -> DataFrame -> [CarePoint] -> Maybe (Expr Bool)
bestLinearCandidate cfg df carePoints
    | not (useLinearSolver cfg) =
        ttrace "P1/linear: disabled by useLinearSolver=False" Nothing
    | length carePoints < minCarePointsForLinear cfg =
        ttrace
            ( "P1/linear: skipped — care="
                ++ show (length carePoints)
                ++ " < min="
                ++ show (minCarePointsForLinear cfg)
            )
            Nothing
    | otherwise =
        ttrace
            ("P1/linear: called care=" ++ show (length carePoints))
            (fitLinearCandidate cfg df carePoints)

{- | Fit an L1-LR (Elastic-Net regularised) to the
care points and convert the hyperplane to an 'Expr Bool'. Class-balanced
sample weights are constructed from the care-point label distribution to
prevent the FISTA intercept from polarising under tree-shape imbalance.

Returns 'Nothing' when:
  * there are no numeric columns,
  * the solver returns an all-zero model, or
  * the resulting hyperplane scores every care row on the same side of 0
    (degenerate-hyperplane detector: PR 3 safety net; equivalent to
    'isValidAtNode' rejection but caught upstream).
-}
fitLinearCandidate ::
    TreeConfig -> DataFrame -> [CarePoint] -> Maybe (Expr Bool)
fitLinearCandidate cfg df carePoints =
    case mapMaybe (materializeFeatureForCare df carePoints) (numericCols df) of
        [] -> ttrace "P1/fit: empty-numeric — no NumExpr cols materialized" Nothing
        mats ->
            let nCare = length carePoints
                rows = careRowsFromFeatures nCare mats
                labels = careLabels carePoints
                !nPos = VU.length (VU.filter (> 0) labels)
                !nNeg = nCare - nPos
                -- Class-balanced sklearn-form weights: w_i = N / (2 · N_class)
                -- with N = nCare so weights have mean 1 (Σ w_i = N). This
                -- preserves the existing @1/N@ gradient normalisation
                -- exactly; the Lipschitz bound stays @(d+1)/4 + λ₂@.
                !classWeights =
                    if nPos > 0 && nNeg > 0
                        then Just $ VU.generate nCare $ \i ->
                            let !y = VU.unsafeIndex labels i
                             in if y > 0
                                    then
                                        fromIntegral nCare
                                            / (2 * fromIntegral nPos)
                                    else
                                        fromIntegral nCare
                                            / (2 * fromIntegral nNeg)
                        else Nothing -- degenerate one-class case; uniform OK
                solverCfg =
                    (linearSolverConfig cfg)
                        { LS.scSampleWeights = classWeights
                        }
                model =
                    LS.fitL1Logistic
                        solverCfg
                        rows
                        labels
                        (V.fromList (map fst mats))
                weights = LS.lmWeights model
                nnz = VU.length (VU.filter (/= 0) weights)
                l1 = VU.sum (VU.map abs weights)
             in if VU.all (== 0) weights
                    then
                        ttrace
                            ( "P1/fit: all-zero — d="
                                ++ show (VU.length weights)
                                ++ " care="
                                ++ show nCare
                                ++ " imbalance="
                                ++ show
                                    ( fromIntegral (max nPos nNeg)
                                        / fromIntegral (max 1 nCare) ::
                                        Double
                                    )
                            )
                            Nothing
                    else
                        let !bias = LS.lmIntercept model
                            !nFull = V.length (V.fromList mats)
                            -- Score each care row: w · x_i + b
                            scoreAt i =
                                let !row = V.unsafeIndex rows i
                                 in VU.sum (VU.zipWith (*) weights row) + bias
                            scores =
                                VU.generate nCare scoreAt
                            !sMin = VU.minimum scores
                            !sMax = VU.maximum scores
                            degenerate =
                                nCare > 0 && (sMin > 0 || sMax < 0)
                            _ = nFull -- silence unused
                         in if degenerate
                                then
                                    ttrace
                                        ( "P1/fit: degenerate — scores ["
                                            ++ show sMin
                                            ++ ","
                                            ++ show sMax
                                            ++ "] all same-side; rejecting"
                                        )
                                        Nothing
                                else
                                    ttrace
                                        ( "P1/fit: ok nnz="
                                            ++ show nnz
                                            ++ "/"
                                            ++ show (VU.length weights)
                                            ++ " |w|1="
                                            ++ show l1
                                            ++ " imbalance="
                                            ++ show
                                                ( fromIntegral (max nPos nNeg)
                                                    / fromIntegral (max 1 nCare) ::
                                                    Double
                                                )
                                        )
                                        (Just (LS.modelToExpr model))

{- | Build per-care-point feature rows from materialized columns. Each column
has length @nCare@ by construction, so the indexing is in range.
-}
careRowsFromFeatures ::
    Int -> [(T.Text, VU.Vector Double)] -> V.Vector (VU.Vector Double)
careRowsFromFeatures nCare mats =
    let matsVec = V.fromList mats
        nFeat = V.length matsVec
     in V.generate nCare $ \i ->
            VU.generate nFeat $ \j -> snd (matsVec V.! j) VU.! i

-- | Solver labels: @+1@ when 'GoLeft' is the correct direction, @-1@ otherwise.
careLabels :: [CarePoint] -> VU.Vector Double
careLabels carePoints =
    VU.fromList [if cpCorrectDir cp == GoLeft then 1.0 else -1.0 | cp <- carePoints]

-- | First column referenced by an expression, or a placeholder when none.
featName :: Expr b -> T.Text
featName expr = case getColumns expr of
    (c : _) -> c
    [] -> T.pack "<feat>"

{- | Replace missing values with the mean of present ones; 'Nothing' overall
when nothing is present, so the caller can drop the feature.
-}
imputeMean :: [Maybe Double] -> Maybe (VU.Vector Double)
imputeMean careRaw =
    case catMaybes careRaw of
        [] -> Nothing
        present ->
            let m = sum present / fromIntegral (length present)
             in Just (VU.fromList [fromMaybe m mv | mv <- careRaw])

{- | Materialize a 'NumExpr' over the care-point rows. 'Nothing' when the
expression fails to evaluate, or (for nullable columns) when no care point
has a present value; otherwise missing values are mean-imputed.
-}
materializeFeatureForCare ::
    DataFrame -> [CarePoint] -> NumExpr -> Maybe (T.Text, VU.Vector Double)
materializeFeatureForCare df carePoints (NDouble expr) =
    case interpret @Double df expr of
        Left _ -> Nothing
        Right (TColumn column) -> case toVector @Double column of
            Left _ -> Nothing
            Right vals ->
                Just (featName expr, VU.fromList [vals V.! cpIndex cp | cp <- carePoints])
materializeFeatureForCare df carePoints (NMaybeDouble expr) =
    case interpret @(Maybe Double) df expr of
        Left _ -> Nothing
        Right (TColumn column) -> case toVector @(Maybe Double) column of
            Left _ -> Nothing
            Right vals ->
                let careRaw = [vals V.! cpIndex cp | cp <- carePoints]
                 in (,) (featName expr) <$> imputeMean careRaw

-- | A care point with its index and which direction leads to correct classification
data CarePoint = CarePoint
    { cpIndex :: !Int
    , cpCorrectDir :: !Direction -- Which child classifies this point correctly
    }
    deriving (Eq, Show)

data Direction = GoLeft | GoRight
    deriving (Eq, Show)

{- | Identify care points: points where exactly one subtree classifies correctly

   For each point reaching the node:
   1. Compute what label the left subtree would predict
   2. Compute what label the right subtree would predict
   3. If exactly one matches the true label, it's a care point
   4. Record which direction leads to correct classification
-}
identifyCarePoints ::
    forall a.
    (Columnable a) =>
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    Tree a -> -- Left subtree
    Tree a -> -- Right subtree
    [CarePoint]
identifyCarePoints target df indices leftTree rightTree =
    case interpret @a df (Col target) of
        Left _ -> []
        Right (TColumn column) ->
            case toVector @a column of
                Left _ -> []
                Right targetVals ->
                    let
                        !leftPreds = predictManyWithTree leftTree df indices
                        !rightPreds = predictManyWithTree rightTree df indices
                     in
                        V.toList $
                            V.imapMaybe
                                (checkPoint targetVals leftPreds rightPreds)
                                indices
  where
    checkPoint ::
        V.Vector a -> V.Vector a -> V.Vector a -> Int -> Int -> Maybe CarePoint
    checkPoint targetVals leftPreds rightPreds k idx =
        let
            trueLabel = targetVals V.! idx
            leftPred = leftPreds V.! k
            rightPred = rightPreds V.! k
            leftCorrect = leftPred == trueLabel
            rightCorrect = rightPred == trueLabel
         in
            case (leftCorrect, rightCorrect) of
                (True, False) -> Just $ CarePoint idx GoLeft
                (False, True) -> Just $ CarePoint idx GoRight
                _ -> Nothing -- Don't-care point (both correct or both wrong)

-- | Predict the label for a single point using a fixed tree
predictWithTree ::
    forall a.
    (Columnable a) =>
    T.Text ->
    DataFrame ->
    Int -> -- Row index
    Tree a ->
    a
predictWithTree _target _df _idx (Leaf v) = v
predictWithTree target df idx (Branch cond left right) =
    case interpret @Bool df cond of
        Left _ -> predictWithTree @a target df idx left -- Default to left on error
        Right (TColumn column) ->
            -- Unboxed Bool: 'toVector @Bool @VU.Vector' resolves to an
            -- @VG.convert :: VU.Vector Bool -> VU.Vector Bool@ no-op when
            -- the column is already an 'UnboxedColumn Bool' (the common
            -- case from predicate interpretation), avoiding the boxed
            -- round-trip the default @toVector @Bool@ would force.
            case toVector @Bool @VU.Vector column of
                Left _ -> predictWithTree @a target df idx left
                Right boolVals ->
                    if boolVals VU.! idx
                        then predictWithTree @a target df idx left
                        else predictWithTree @a target df idx right

{- | Batched predictions across many rows. Each branch condition is
materialized at most once per call (instead of once per row), so the
cost is O(branches · rows) interpret calls collapsed into
O(branches) — the dominant TAO inner-loop saving (Phase 6, hot path
per the profile: 'identifyCarePoints' invoking 'predictWithTree'
previously consumed >60% of total time).

Returns predictions aligned with the input index vector:
@result V.! k = predictWithTree (indices V.! k) tree@.
-}
predictManyWithTree ::
    forall a.
    (Columnable a) =>
    Tree a -> DataFrame -> V.Vector Int -> V.Vector a
predictManyWithTree tree df indices = V.create $ do
    mv <- VM.new (V.length indices)
    let positionsAndRows = V.zip (V.enumFromN 0 (V.length indices)) indices
    fill mv positionsAndRows tree
    pure mv
  where
    fill :: VM.MVector s a -> V.Vector (Int, Int) -> Tree a -> ST s ()
    fill mv prs (Leaf v) = V.mapM_ (\(p, _) -> VM.write mv p v) prs
    fill mv prs (Branch cond left right) =
        case interpret @Bool df cond of
            Left _ -> fill mv prs left
            Right (TColumn column) -> case toVector @Bool @VU.Vector column of
                Left _ -> fill mv prs left
                Right boolVals ->
                    let
                        (leftPrs, rightPrs) =
                            V.partition (\(_, i) -> boolVals VU.! i) prs
                     in
                        fill mv leftPrs left >> fill mv rightPrs right

countCarePointErrors :: Expr Bool -> DataFrame -> [CarePoint] -> Int
countCarePointErrors cond df carePoints =
    case interpret @Bool df cond of
        Left _ -> length carePoints
        Right (TColumn column) ->
            case toVector @Bool @VU.Vector column of
                Left _ -> length carePoints
                Right boolVals ->
                    length $ filter (isMisclassified boolVals) carePoints
  where
    isMisclassified :: VU.Vector Bool -> CarePoint -> Bool
    isMisclassified boolVals cp =
        let goesLeft = boolVals VU.! cpIndex cp
            shouldGoLeft = cpCorrectDir cp == GoLeft
         in goesLeft /= shouldGoLeft

partitionIndices ::
    Expr Bool -> DataFrame -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionIndices cond df indices =
    case interpret @Bool df cond of
        Left _ -> (indices, V.empty)
        Right (TColumn column) ->
            case toVector @Bool @VU.Vector column of
                Left _ -> (indices, V.empty)
                Right boolVals ->
                    V.partition (boolVals VU.!) indices

majorityValueFromIndices ::
    forall a.
    (Columnable a, Ord a) =>
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    a
majorityValueFromIndices target df indices =
    case interpret @a df (Col target) of
        Left e -> throw e
        Right (TColumn column) ->
            case toVector @a column of
                Left e -> throw e
                Right vals ->
                    let counts =
                            V.foldl'
                                (\acc i -> M.insertWith (+) (vals V.! i) (1 :: Int) acc)
                                M.empty
                                indices
                     in if M.null counts
                            then error "Empty indices in majorityValueFromIndices"
                            else fst $ maximumBy (compare `on` snd) (M.toList counts)

computeTreeLoss ::
    forall a.
    (Columnable a) =>
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Double
computeTreeLoss target df indices tree
    | V.null indices = 0
    | otherwise =
        case interpret @a df (Col target) of
            Left _ -> 1.0
            Right (TColumn column) ->
                case toVector @a column of
                    Left _ -> 1.0
                    Right targetVals ->
                        let
                            !preds = predictManyWithTree tree df indices
                            n = V.length indices
                            errors =
                                V.length $
                                    V.ifilter
                                        ( \k _ ->
                                            targetVals V.! (indices V.! k)
                                                /= preds V.! k
                                        )
                                        preds
                         in
                            fromIntegral errors / fromIntegral n

{- | Semantics-preserving fitted-tree pruning, run once post-convergence:
* path-condition entailment — if the conditions taken to reach a node force its
  test true\/false, splice in the live child (true-edge facts always usable;
  false-edge facts only for non-NaN integral columns);
* same-branch collapse — @Branch c t t@ becomes @t@, cascading upward.
Decidable only for same-column Double\/Int threshold tests; cross-column, oblique
and nullable tests are left untouched.
-}
pruneDead :: forall a. (Columnable a) => Tree a -> Tree a
pruneDead = go []
  where
    go :: [PredFact] -> Tree a -> Tree a
    go _ (Leaf v) = Leaf v
    go facts (Branch cond left right) =
        case entails facts cond of
            Just True -> go facts left
            Just False -> go facts right
            Nothing ->
                let left' = go (addFact (factTrue cond) facts) left
                    right' = go (addFact (factFalse cond) facts) right
                 in if treeEq left' right' then left' else Branch cond left' right'
    addFact (Just f) fs = f : fs
    addFact Nothing fs = fs

treeEq :: (Columnable a) => Tree a -> Tree a -> Bool
treeEq (Leaf x) (Leaf y) = x == y
treeEq (Branch c1 l1 r1) (Branch c2 l2 r2) = eqExpr c1 c2 && treeEq l1 l2 && treeEq r1 r2
treeEq _ _ = False

pruneExpr :: forall a. (Columnable a) => Expr a -> Expr a
pruneExpr (If cond trueBranch falseBranch) =
    let t = pruneExpr trueBranch
        f = pruneExpr falseBranch
     in if eqExpr t f
            then t
            else case (t, f) of
                (If condInner tInner _, _) | eqExpr cond condInner -> If cond tInner f
                (_, If condInner _ fInner) | eqExpr cond condInner -> If cond t fInner
                _ -> If cond t f
pruneExpr (Unary op e) = Unary op (pruneExpr e)
pruneExpr (Binary op l r) = Binary op (pruneExpr l) (pruneExpr r)
pruneExpr e = e

-- ----------------------------------------------------------------------------
-- Faithful sklearn CART (for an aligned TAO baseline)
-- ----------------------------------------------------------------------------

-- | A one-hot feature column: its per-row Double values plus the sklearn LEFT
-- predicate (@x <= threshold@) expressed over the ORIGINAL DataFrame.
data CartFeature = CartFeature
    { cfValues :: !(VU.Vector Double)
    , cfPred :: !(Double -> Expr Bool)
    }

-- | Pre-`Tree` CART node: leaf class id, or split on feature @j@ at @threshold@.
data CartNode = CLeaf !Int | CSplit !Int !Double !CartNode !CartNode

{- | sklearn-faithful CART: one-hot-encodes categoricals like
@pd.get_dummies(drop_first=False)@ (numeric columns pass through in CSV order,
each categorical replaced in place by sorted @col_value@ dummies), then builds a
tree with exact (unsmoothed) Gini, axis-aligned best splits over all midpoint
thresholds (@<=@ ⇒ left), sklearn stop rules, and the smallest-class-index
argmax leaf. Produces a @Tree T.Text@ (string class labels via @astype(str)@)
that predicts identically to @DecisionTreeClassifier(criterion='gini')@ on
continuous features. @min_samples_split@ is fixed at 2; @max_depth@ /
@min_samples_leaf@ come from 'maxTreeDepth' / 'minLeafSize'. Tie-broken by
feature order (no RNG permutation), so it matches sklearn only where gains do
not tie (i.e. not the seeded tie-break sklearn applies to one-hot columns).
-}
buildCartTree :: forall a. (Columnable a, Ord a) => TreeConfig -> T.Text -> DataFrame -> Tree a
buildCartTree cfg target df = cartToTree (buildCartNode 0 (VU.enumFromN 0 nAll))
  where
    feats = V.fromList (cartFeatures target df)
    nFeats = V.length feats
    labels :: V.Vector a
    labels = case interpret @a df (Col target) of
        Right (TColumn column) -> case toVector @a column of
            Right v -> v
            Left _ -> error "buildCartTree: target column type mismatch"
        _ -> error "buildCartTree: cannot interpret target column"
    classes = V.fromList (Set.toList (Set.fromList (V.toList labels)))
    nClasses = V.length classes
    classIx = M.fromList (zip (V.toList classes) [0 ..])
    codes = VU.generate nAll (\i -> M.findWithDefault 0 (labels V.! i) classIx)
    nAll = nRows df
    maxD = maxTreeDepth cfg
    minLeaf = max 1 (minLeafSize cfg)

    classCounts idxs =
        VU.accumulate (+) (VU.replicate nClasses 0) (VU.map (\i -> (codes VU.! i, 1)) idxs)

    -- Gini over a class-count list of total m: 1 - Σ (c/m)².
    giniL cs m
        | m == 0 = 0
        | otherwise = 1 - sum [let p = fromIntegral c / fromIntegral m in p * p | c <- cs]

    buildCartNode :: Int -> VU.Vector Int -> CartNode
    buildCartNode depth idxs
        | n < 2 || depth >= maxD || isPure = CLeaf (VU.maxIndex counts)
        | otherwise = case bestSplit idxs counts n of
            Nothing -> CLeaf (VU.maxIndex counts)
            Just (fj, thr) ->
                let !vals = cfValues (feats V.! fj)
                    leftIdx = VU.filter (\i -> vals VU.! i <= thr) idxs
                    rightIdx = VU.filter (\i -> vals VU.! i > thr) idxs
                 in CSplit fj thr (buildCartNode (depth + 1) leftIdx) (buildCartNode (depth + 1) rightIdx)
      where
        n = VU.length idxs
        counts = classCounts idxs
        isPure = VU.length (VU.filter (> 0) counts) <= 1

    -- Min weighted-child-Gini (feature, threshold); first feature wins ties; Nothing
    -- if no feature has a min_samples_leaf-respecting threshold (⇒ leaf).
    bestSplit :: VU.Vector Int -> VU.Vector Int -> Int -> Maybe (Int, Double)
    bestSplit idxs counts n = pick Nothing 0
      where
        total = VU.toList counts
        pick acc fj
            | fj >= nFeats = fmap (\(_, j, t) -> (j, t)) acc
            | otherwise = case featureBest (feats V.! fj) of
                Nothing -> pick acc (fj + 1)
                Just (g, thr) -> case acc of
                    Just (gBest, _, _) | gBest <= g -> pick acc (fj + 1)
                    _ -> pick (Just (g, fj, thr)) (fj + 1)
          where
            -- placeholder so the where binds to the right fj
            featureBest feat = sweep Nothing (replicate nClasses 0) (0 :: Int) (0 / 0) sorted
              where
                vals = cfValues feat
                sorted = sortBy (compare `on` fst) [(vals VU.! i, codes VU.! i) | i <- VU.toList idxs]
                sweep best _ _ _ [] = best
                sweep best leftAcc moved prevV ((v, c) : more) =
                    let best'
                            | moved >= minLeaf, n - moved >= minLeaf, v > prevV + 1e-7 =
                                let nl = moved
                                    nr = n - nl
                                    gl = giniL leftAcc nl
                                    gr = giniL (zipWith (-) total leftAcc) nr
                                    wg = (fromIntegral nl * gl + fromIntegral nr * gr) / fromIntegral n
                                    thr = (prevV + v) / 2
                                 in case best of
                                        Just (wb, _) | wb <= wg -> best
                                        _ -> Just (wg, thr)
                            | otherwise = best
                        leftAcc' = zipWith (\j x -> if j == c then x + 1 else x) [0 ..] leftAcc
                     in sweep best' leftAcc' (moved + 1) v more

    cartToTree (CLeaf cid) = Leaf (classes V.! cid)
    cartToTree (CSplit fj thr l r) = Branch (cfPred (feats V.! fj) thr) (cartToTree l) (cartToTree r)

-- | One-hot features in @pd.get_dummies(drop_first=False)@ column order.
cartFeatures :: T.Text -> DataFrame -> [CartFeature]
cartFeatures target df = concatMap featOf (filter (/= target) (columnNames df))
  where
    nAll = nRows df
    featOf :: T.Text -> [CartFeature]
    featOf c = case unsafeGetColumn c df of
        UnboxedColumn _ (v :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [CartFeature v (\t -> F.col @Double c .<=. F.lit t)]
                Nothing -> case sIntegral @b of
                    STrue -> [CartFeature (VU.map fromIntegral v) (\t -> F.toDouble (F.col @b c) .<=. F.lit t)]
                    SFalse -> []
        BoxedColumn _ (v :: V.Vector b) ->
            case testEquality (typeRep @b) (typeRep @T.Text) of
                Just Refl ->
                    [ CartFeature
                        (VU.generate nAll (\i -> if v V.! i == cat then 1 else 0))
                        (\_ -> F.col @T.Text c ./=. F.lit cat)
                    | cat <- Set.toList (Set.fromList (V.toList v))
                    ]
                Nothing -> []
        _ -> []

-- | Target column as string labels (matches pandas @y.astype(str)@).
cartTargetLabels :: T.Text -> DataFrame -> V.Vector T.Text
cartTargetLabels target df = case unsafeGetColumn target df of
    BoxedColumn _ (v :: V.Vector b) ->
        case testEquality (typeRep @b) (typeRep @T.Text) of
            Just Refl -> v
            Nothing -> V.map (T.pack . show) v
    UnboxedColumn _ (v :: VU.Vector b) -> V.map (T.pack . show) (V.convert v)
    _ -> error "cartTargetLabels: unsupported target column type"

-- | Unifies non-nullable and nullable Double expressions for feature generation.
data NumExpr
    = NDouble !(Expr Double)
    | NMaybeDouble !(Expr (Maybe Double))

numExprCols :: NumExpr -> [T.Text]
numExprCols (NDouble e) = getColumns e
numExprCols (NMaybeDouble e) = getColumns e

numExprEq :: NumExpr -> NumExpr -> Bool
numExprEq (NDouble e1) (NDouble e2) = eqExpr e1 e2
numExprEq (NMaybeDouble e1) (NMaybeDouble e2) = eqExpr e1 e2
numExprEq _ _ = False

combineNumExprs :: NumExpr -> NumExpr -> [NumExpr]
combineNumExprs (NDouble e1) (NDouble e2) =
    [ NDouble (e1 .+ e2)
    , NDouble (e1 .- e2)
    , NDouble (e1 .* e2)
    , NDouble
        (F.ifThenElse (e2 ./= F.lit (0 :: Double)) (e1 ./ e2) (F.lit (0 :: Double)))
    ]
combineNumExprs (NDouble e1) (NMaybeDouble e2) =
    [ NMaybeDouble (e1 .+ e2)
    , NMaybeDouble (e1 .- e2)
    , NMaybeDouble (e1 .* e2)
    , NMaybeDouble
        ( F.ifThenElse
            (F.fromMaybe False (e2 ./= F.lit (0 :: Double)))
            (e1 ./ e2)
            (F.lit (Nothing :: Maybe Double))
        )
    ]
combineNumExprs (NMaybeDouble e1) (NDouble e2) =
    [ NMaybeDouble (e1 .+ e2)
    , NMaybeDouble (e1 .- e2)
    , NMaybeDouble (e1 .* e2)
    , NMaybeDouble
        ( F.ifThenElse
            (e2 ./= F.lit (0 :: Double))
            (e1 ./ e2)
            (F.lit (Nothing :: Maybe Double))
        )
    ]
combineNumExprs (NMaybeDouble e1) (NMaybeDouble e2) =
    [ NMaybeDouble (e1 .+ e2)
    , NMaybeDouble (e1 .- e2)
    , NMaybeDouble (e1 .* e2)
    , NMaybeDouble
        ( F.ifThenElse
            (F.fromMaybe False (e2 ./= F.lit (0 :: Double)))
            (e1 ./ e2)
            (F.lit (Nothing :: Maybe Double))
        )
    ]

numericConditions :: TreeConfig -> DataFrame -> [Expr Bool]
numericConditions = generateNumericConds

generateNumericConds :: TreeConfig -> DataFrame -> [Expr Bool]
generateNumericConds cfg df = do
    expr <- numericExprsWithTerms (synthConfig cfg) df
    let thresholds = numericThresholds expr
    threshold <- thresholds
    numericCondsFromExpr expr threshold
  where
    -- Materialize each numeric expression once, sort once, then index for
    -- every requested percentile (was: separate interpret + sort per p).
    numericThresholds (NDouble e) = thresholdsForExpr e
    numericThresholds (NMaybeDouble e) = thresholdsForExpr (F.fromMaybe 0 e)

    thresholdsForExpr e = case interpret @Double df e of
        Left _ -> []
        Right (TColumn col) -> case toVector @Double col of
            Left _ -> []
            Right vals ->
                let !sortedV = V.fromList (sort (V.toList vals))
                    !n = V.length sortedV
                 in if n == 0
                        then []
                        else
                            map (\p -> sortedV V.! min (n - 1) (max 0 (p * n `div` 100))) (percentiles cfg)

    numericCondsFromExpr (NDouble e) t =
        [e .<= F.lit t, e .>= F.lit t, e .< F.lit t, e .> F.lit t]
    numericCondsFromExpr (NMaybeDouble e) t =
        [ F.fromMaybe False (e .<= F.lit t)
        , F.fromMaybe False (e .>= F.lit t)
        , F.fromMaybe False (e .< F.lit t)
        , F.fromMaybe False (e .> F.lit t)
        ]

{- | Dedup a candidate-expression list by normalized structural form, keeping the
first occurrence. O(n log n) — the keyed replacement for @nubBy eqExpr@ on large
candidate pools (`show . normalize` is faithful to 'eqExpr', see the worklist).
-}
nubByExpr :: [Expr Bool] -> [Expr Bool]
nubByExpr = go Set.empty
  where
    go _ [] = []
    go seen (e : es)
        | k `Set.member` seen = go seen es
        | otherwise = e : go (Set.insert k seen) es
      where
        k = show (normalize e)

{- | Arithmetic candidate expansion, generated already-deduped: each round
combines @frontier × base@ and admits only candidates whose normalized form is
novel, so the frontier never carries duplicates (no generate-all + 'nubBy').
This is the numeric analog of the bool 'saturateCandidates' worklist; without it,
@maxExprDepth >= 3@ blows the candidate list up combinatorially and the old
@nubBy eqExpr@ pass was O(n²). Produces base + @maxExprDepth-1@ combination
rounds (matching the previous depth semantics, deduped).
-}
numericExprsWithTerms :: SynthConfig -> DataFrame -> [NumExpr]
numericExprsWithTerms cfg df
    | not (enableArithOps cfg) = base
    | otherwise = base ++ go (max 0 (maxExprDepth cfg - 1)) base seen0
  where
    base = numericCols df
    keyNum (NDouble e) = show (normalize e)
    keyNum (NMaybeDouble e) = show (normalize e)
    seen0 = Set.fromList (map keyNum base)
    disallowed e1 e2 =
        let cols = numExprCols e1 <> numExprCols e2
         in any (\(l, r) -> l `elem` cols && r `elem` cols) (disallowedCombinations cfg)
    go 0 _ _ = []
    go d frontier seen =
        let products =
                [ c
                | e1 <- frontier
                , e2 <- base
                , not (numExprEq e1 e2)
                , not (disallowed e1 e2)
                , c <- combineNumExprs e1 e2
                ]
            (admitted, seen') = admit seen [] products
         in if null admitted then [] else admitted ++ go (d - 1) admitted seen'
    admit seen acc [] = (reverse acc, seen)
    admit seen acc (c : cs)
        | k `Set.member` seen = admit seen acc cs
        | otherwise = admit (Set.insert k seen) (c : acc) cs
      where
        k = keyNum c

numericCols :: DataFrame -> [NumExpr]
numericCols df = concatMap extract (columnNames df)
  where
    extract colName = case unsafeGetColumn colName df of
        UnboxedColumn Nothing (_ :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [NDouble (Col colName)]
                Nothing -> case sIntegral @b of
                    STrue -> [NDouble (F.toDouble (Col @b colName))]
                    SFalse -> []
        BoxedColumn (Just _) (_ :: V.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [NMaybeDouble (Col @(Maybe b) colName)]
                Nothing -> case sIntegral @b of
                    STrue ->
                        [NMaybeDouble (F.whenPresent (realToFrac @b @Double) (Col @(Maybe b) colName))]
                    SFalse -> []
        UnboxedColumn (Just _) (_ :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [NMaybeDouble (Col @(Maybe b) colName)]
                Nothing -> case sIntegral @b of
                    STrue ->
                        [NMaybeDouble (F.whenPresent (realToFrac @b @Double) (Col @(Maybe b) colName))]
                    SFalse -> []
        _ -> []

{- | Valid-slot view of a nullable boxed column: null slots hold crash-thunks,
so callers that force every element (e.g. percentile sorting) must filter
through the bitmap first.
-}
validBoxedValues :: Bitmap -> V.Vector a -> V.Vector a
validBoxedValues bm = V.ifilter (\i _ -> bitmapTestBit bm i)

{- | Summary of the target column used to drive the categorical condition
generator (Breiman for binary, subsets/singletons for multi-class).
Computed once per 'fitDecisionTree' invocation and threaded through.
-}
data TargetInfo target = TargetInfo
    { tiIsBinary :: !Bool
    , tiPositiveClass :: !(Maybe target)
    -- ^ Just for binary; Nothing for multi-class.
    , tiValues :: !(V.Vector target)
    }

{- | Compute 'TargetInfo' from the target column. The positive-class for
binary targets is deterministically chosen as the lexicographically-first
distinct value, so re-runs produce identical candidate pools.
-}
mkTargetInfo ::
    forall target.
    (Columnable target, Ord target) =>
    T.Text -> DataFrame -> Maybe (TargetInfo target)
mkTargetInfo target df = case interpret @target df (Col target) of
    Left _ -> Nothing
    Right (TColumn col) -> case toVector @target col of
        Left _ -> Nothing
        Right vals ->
            let distinct = Set.toAscList (Set.fromList (V.toList vals))
                isBinary = length distinct == 2
                posClass = case distinct of
                    (p : _) | isBinary -> Just p
                    _ -> Nothing
             in Just (TargetInfo isBinary posClass vals)

{- | Enumerate distinct values in @values@ with a hard cap. Returns
@Right vs@ (sorted ascending) when the column has at most @cap@ distinct
values, @Left actualCountSoFar@ when the cap is exceeded — the count
lets the caller route to the high-cardinality path.
-}
distinctValuesUpTo :: (Ord a) => Int -> V.Vector a -> Either Int [a]
distinctValuesUpTo cap values = go Set.empty 0
  where
    n = V.length values
    go !s !i
        | i >= n = Right (Set.toAscList s)
        | Set.size s > cap = Left (Set.size s)
        | otherwise =
            let v = V.unsafeIndex values i
                s' = Set.insert v s
             in go s' (i + 1)

{- | Subset enumeration for multi-class targets at low cardinality. Produces
one @Expr Bool@ per non-empty proper subset of @vs@: @col == v_1 .||. ... .||.
col == v_k@. For @|vs| = k@ this is @2^k - 2@ candidates.
-}
subsetSplits :: (a -> Expr Bool) -> [a] -> [Expr Bool]
subsetSplits eqLit vs =
    [ foldr1 (.||.) (map eqLit xs)
    | xs <- drop 1 (init (subsequences vs))
    ]

{- | Singleton fallback: one @col == v@ per distinct value. Used for
multi-class targets with cardinality above the subset cap.
-}
singletonSplits :: (a -> Expr Bool) -> [a] -> [Expr Bool]
singletonSplits = map

{- | Breiman's algorithm for binary classification (BFOS 1984, §4.2, Theorem
4.5): the optimal @col ∈ S@ split is found by sorting distinct levels by
per-level positive-class rate, then trying each contiguous prefix as the
split. This yields @k - 1@ candidates regardless of cardinality —
strictly preferable to enumeration for binary targets.

Uses Laplace smoothing @(pos + 1) / (n + 2)@ on the per-level rate so that
low-count levels don't sort adjacent to genuinely high-positive ones under
sample noise (the optimality theorem assumes consistent ordering).
Secondary sort key is the level value itself for deterministic tie-break.
-}
breimanPrefixSplits ::
    forall a target.
    (Ord a, Ord target) =>
    target -> -- positiveClass (deterministically picked)
    V.Vector a -> -- column values (training-fold slice)
    V.Vector target -> -- target labels (training-fold slice, aligned to values)
    [a] -> -- distinct levels (we'll sort these)
    (a -> Expr Bool) -> -- equality builder
    [Expr Bool]
breimanPrefixSplits positiveClass values targetVals distinctVals eqLit =
    let perLevelCounts :: M.Map a (Int, Int)
        perLevelCounts =
            V.ifoldl'
                ( \acc i v ->
                    let !y = V.unsafeIndex targetVals i
                        !p = if y == positiveClass then 1 else 0
                     in M.insertWith
                            (\(p1, n1) (p2, n2) -> (p1 + p2, n1 + n2))
                            v
                            (p, 1)
                            acc
                )
                M.empty
                values
        rate v = case M.lookup v perLevelCounts of
            Nothing -> 0.5 -- value not observed in this fold: neutral
            Just (pos, n) ->
                (fromIntegral pos + 1) / (fromIntegral n + 2 :: Double)
        sorted = sortBy (compare `on` (\v -> (rate v, v))) distinctVals
        -- Proper non-empty prefixes: drop empty (head of inits) and drop the
        -- full set (last). For k levels, this yields k-1 prefixes.
        prefixes = tail (init (inits sorted))
     in map (foldr1 (.||.) . map eqLit) prefixes

{- | All equality-based candidate splits derived from non-numeric columns:

  * Per-column /categorical/ conditions. For each Boxed column the
    appropriate generator is chosen by ('tiIsBinary' targetInfo,
    cardinality): Breiman's O(/k/ log /k/) prefix splits for binary
    targets, @2^k - 2@ subset enumeration when multi-class with cardinality
    at most 'maxCategoricalSubsetCardinality', and one @col == v@ per
    level when multi-class and above the cap.

  * Cross-column equality / order conditions. For pairs of same-typed
    Boxed columns, @col_l == col_r@ (and @<=@ for orderable @Maybe@
    types).

Numeric splits are produced separately by 'numericConditions'.
-}
discreteConditions ::
    forall target.
    (Columnable target, Ord target) =>
    TargetInfo target ->
    TreeConfig ->
    DataFrame ->
    [Expr Bool]
discreteConditions targetInfo cfg df =
    let
        ords = columnOrdering cfg
        subsetCap = maxCategoricalSubsetCardinality (synthConfig cfg)
        isBinary = tiIsBinary targetInfo
        posClass = tiPositiveClass targetInfo
        targetVals = tiValues targetInfo

        -- Categorical condition generator. Replaces the percentile sampling
        -- with Breiman's algorithm for binary targets and subset/singleton
        -- enumeration for multi-class. Dispatched by (target arity,
        -- cardinality).
        catConds ::
            forall a.
            (Ord a, Columnable a) =>
            (a -> Expr Bool) -> V.Vector a -> [Expr Bool]
        catConds eqLit values
            | V.null values = []
            | isBinary
            , Just pc <- posClass =
                let distinct = case distinctValuesUpTo 64 values of
                        Right vs -> vs
                        Left _ -> Set.toAscList (Set.fromList (V.toList values))
                 in if length distinct < 2
                        then []
                        else
                            breimanPrefixSplits @a @target
                                pc
                                values
                                targetVals
                                distinct
                                eqLit
            | otherwise = case distinctValuesUpTo subsetCap values of
                Right vs
                    | length vs >= 2 -> subsetSplits eqLit vs
                    | otherwise -> []
                Left _ ->
                    singletonSplits
                        eqLit
                        (Set.toAscList (Set.fromList (V.toList values)))

        genConds :: T.Text -> [Expr Bool]
        genConds colName = case unsafeGetColumn colName df of
            (BoxedColumn Nothing (column :: V.Vector a)) ->
                fromMaybe
                    []
                    ( withOrdFrom
                        @a
                        ords
                        (catConds @a (\v -> Col @a colName .==. Lit v) column)
                    )
            (BoxedColumn (Just bm) (column :: V.Vector a)) -> case sFloating @a of
                STrue -> [] -- handled by numericCols / numericExprs
                SFalse -> case sIntegral @a of
                    STrue -> [] -- handled by numericCols / numericExprs
                    SFalse ->
                        let !valid = validBoxedValues bm column
                         in if V.null valid
                                then []
                                else
                                    fromMaybe
                                        []
                                        ( withOrdFrom
                                            @a
                                            ords
                                            ( catConds
                                                @a
                                                (\v -> Col @(Maybe a) colName .==. Lit (Just v))
                                                valid
                                            )
                                        )
            (UnboxedColumn _ (_ :: VU.Vector a)) -> []

        columnConds =
            concatMap
                colConds
                [ (l, r)
                | l <- columnNames df
                , r <- columnNames df
                , l /= r -- self-pairs would produce always-true tautologies
                , not
                    ( any
                        (\(l', r') -> sort [l', r'] == sort [l, r])
                        (disallowedCombinations (synthConfig cfg))
                    )
                ]
          where
            colConds (!l, !r) = case (unsafeGetColumn l df, unsafeGetColumn r df) of
                ( BoxedColumn Nothing (_col1 :: V.Vector a)
                    , BoxedColumn Nothing (_ :: V.Vector b)
                    ) ->
                        case testEquality (typeRep @a) (typeRep @b) of
                            Nothing -> []
                            Just Refl -> [Col @a l .==. Col @a r]
                (UnboxedColumn _ (_ :: VU.Vector a), UnboxedColumn _ (_ :: VU.Vector b)) -> []
                ( BoxedColumn (Just _) (_ :: V.Vector a)
                    , BoxedColumn (Just _) (_ :: V.Vector b)
                    ) -> case testEquality (typeRep @a) (typeRep @b) of
                        Nothing -> []
                        Just Refl -> case testEquality (typeRep @a) (typeRep @T.Text) of
                            Nothing ->
                                case withOrdFrom @a ords [Col @(Maybe a) l .<=. Col @(Maybe a) r] of
                                    Just leExprs ->
                                        leExprs ++ [Col @(Maybe a) l .==. Col @(Maybe a) r]
                                    Nothing -> [Col @(Maybe a) l .==. Col @(Maybe a) r]
                            Just Refl -> [Col @(Maybe a) l .==. Col @(Maybe a) r]
                _ -> []
     in
        concatMap genConds (columnNames df) ++ columnConds

partitionDataFrame :: Expr Bool -> DataFrame -> (DataFrame, DataFrame)
partitionDataFrame cond df = (filterWhere cond df, filterWhere (F.not cond) df)

calculateGini ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> Double
calculateGini target df =
    let n = fromIntegral $ nRows df
        counts = getCounts @a target df
        numClasses = fromIntegral $ M.size counts
        probs = map (\c -> (fromIntegral c + 1) / (n + numClasses)) (M.elems counts)
     in if n == 0 then 0 else 1 - sum (map (^ (2 :: Int)) probs)

majorityValue :: forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> a
majorityValue target df =
    let counts = getCounts @a target df
     in if M.null counts
            then error "Empty DataFrame in leaf"
            else fst $ maximumBy (compare `on` snd) (M.toList counts)

getCounts ::
    forall a. (Columnable a, Ord a) => T.Text -> DataFrame -> M.Map a Int
getCounts target df =
    case interpret @a df (Col target) of
        Left e -> throw e
        Right (TColumn column) ->
            case toVector @a column of
                Left e -> throw e
                Right vals -> foldl' (\acc x -> M.insertWith (+) x 1 acc) M.empty (V.toList vals)

percentile :: Int -> Expr Double -> DataFrame -> Double
percentile p expr df =
    case interpret @Double df expr of
        Left _ -> 0
        Right (TColumn column) ->
            case toVector @Double column of
                Left _ -> 0
                Right vals ->
                    let sorted = V.fromList $ sort $ V.toList vals
                        n = V.length sorted
                        idx = min (n - 1) $ max 0 $ (p * n) `div` 100
                     in if n == 0 then 0 else sorted V.! idx

buildTree ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    Int ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    Expr a
buildTree cfg depth target conds df =
    let
        tree = buildCartTree @a cfg{maxTreeDepth = depth} target df
        indices = V.enumFromN 0 (nRows df)
        optimized = taoOptimize @a cfg target conds df indices tree
     in
        pruneExpr (treeToExpr optimized)

pruneTree :: forall a. (Columnable a) => Expr a -> Expr a
pruneTree = pruneExpr

-- | A tree where each leaf stores a class-probability distribution.
type ProbTree a = Tree (M.Map a Double)

-- | Compute normalised class probabilities from a subset of training rows.
probsFromIndices ::
    forall a.
    (Columnable a, Ord a) =>
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    M.Map a Double
probsFromIndices target df indices =
    case interpret @a df (Col target) of
        Left _ -> M.empty
        Right (TColumn column) ->
            case toVector @a column of
                Left _ -> M.empty
                Right vals ->
                    let counts =
                            V.foldl'
                                (\acc i -> M.insertWith (+) (vals V.! i) (1 :: Int) acc)
                                M.empty
                                indices
                        total = fromIntegral (V.length indices) :: Double
                     in M.map (\c -> fromIntegral c / total) counts

{- | Annotate a fitted 'Tree a' with class distributions by routing the
  training data through it.  The split conditions are preserved; only the
  leaf values change from a majority label to a probability map.
-}
buildProbTree ::
    forall a.
    (Columnable a, Ord a) =>
    Tree a ->
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    ProbTree a
buildProbTree (Leaf _) target df indices =
    Leaf (probsFromIndices @a target df indices)
buildProbTree (Branch cond left right) target df indices =
    let (indicesL, indicesR) = partitionIndices cond df indices
     in Branch
            cond
            (buildProbTree @a left target df indicesL)
            (buildProbTree @a right target df indicesR)

{- | Fit a TAO decision tree and return one @Expr Double@ per class.

  Each @(c, e)@ pair in the result map means: evaluate @e@ on a 'DataFrame'
  row to get the predicted probability of class @c@.  You can insert these
  as new columns with 'derive' or evaluate them with 'interpret'.

  Example:
  @
  let pes = fitProbTree \@T.Text cfg (Col \"species\") trainDf
  -- pes M.! \"setosa\" :: Expr Double
  df' = M.foldlWithKey' (\\d cls e -> D.derive (cls <> \"_prob\") e d) testDf pes
  @
-}
fitProbTree ::
    forall a.
    (Columnable a, Ord a) =>
    TreeConfig ->
    Expr a -> -- target column, e.g. @Col \"label\"@
    DataFrame ->
    M.Map a (Expr Double)
fitProbTree cfg (Col target) df =
    let
        !targetInfo = case mkTargetInfo @a target df of
            Nothing -> TargetInfo False Nothing V.empty
            Just ti -> ti
        conds =
            nubByExpr $
                numericConditions cfg (exclude [target] df)
                    ++ discreteConditions targetInfo cfg (exclude [target] df)
        initialTree = buildCartTree @a cfg target df
        indices = V.enumFromN 0 (nRows df)
        optimizedTree = taoOptimize @a cfg target conds df indices initialTree
        pruned = pruneDead optimizedTree
     in
        probExprs (buildProbTree @a pruned target df indices)
fitProbTree _ expr _ =
    error $ "Cannot create prob tree for compound expression: " ++ show expr

{- | Convert a 'ProbTree' into one 'Expr Double' per class.

  Each @(c, e)@ pair means: evaluate @e@ on a 'DataFrame' row to get the
  predicted probability of class @c@.  You can insert these as new columns
  with 'derive' or evaluate them with 'interpret'.

  Example:
  @
  let pt  = fitProbTree \@T.Text cfg (Col \"species\") trainDf
      pes = probExprs pt
  -- pes M.! \"setosa\" :: Expr Double
  df' = M.foldlWithKey' (\\d cls e -> D.derive (cls <> \"_prob\") e d) testDf pes
  @
-}
probExprs ::
    forall a.
    (Columnable a, Ord a) =>
    ProbTree a ->
    M.Map a (Expr Double)
probExprs tree =
    let classes = nub (allClasses tree)
     in M.fromList [(c, classExpr c tree) | c <- classes]
  where
    allClasses :: ProbTree a -> [a]
    allClasses (Leaf m) = M.keys m
    allClasses (Branch _ l r) = allClasses l ++ allClasses r

    classExpr :: a -> ProbTree a -> Expr Double
    classExpr c (Leaf m) = Lit (M.findWithDefault 0.0 c m)
    classExpr c (Branch cond l r) =
        F.ifThenElse cond (classExpr c l) (classExpr c r)
