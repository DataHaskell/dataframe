{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Up-front (TDD) spec for the saturation worklist 'saturateCandidates' that
will replace the lazy generate-all 'boolExprsVec'. The existing depth-bounded
'boolExprsVec' is kept as the behaviour-preservation oracle.

State: 'saturateCandidates' is the identity stub, so the structural same-set
and truth-vector floor/collapse cases FAIL (red) — that is the spec PR1 must
meet; base-inclusion / dedup / determinism hold under the stub.
-}
module Worklist (tests, props) where

import qualified DataFrame as D
import DataFrame.DecisionTree (
    CondVec,
    DedupMode (Structural, TruthVector),
    boolExprsVec,
    combineAndVec,
    combineOrVec,
    cvExpr,
    cvVec,
    materializeCondVec,
    saturateCandidates,
 )
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (
    Expr,
    compareExpr,
    eSize,
    eqExpr,
    normalize,
 )
import DataFrame.Operators

import Data.Function (on)
import Data.List (minimumBy, nubBy)
import qualified Data.Maybe
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as VU
import Test.HUnit
import Test.QuickCheck

-- Fixture: x = 0..5, y = 5..0 (anti-correlated), z scrambled (independent of both).
-- Note x>2 and y<3 share the truth vector [F,F,F,T,T,T], so the truth-vector mode
-- must collapse them; z gives a third column for non-consolidating cross-column combos.
fixtureDF :: D.DataFrame
fixtureDF =
    D.fromNamedColumns
        [ ("x", DI.fromList ([0, 1, 2, 3, 4, 5] :: [Double]))
        , ("y", DI.fromList ([5, 4, 3, 2, 1, 0] :: [Double]))
        , ("z", DI.fromList ([2, 5, 1, 4, 0, 3] :: [Double]))
        ]

mat :: Expr Bool -> CondVec
mat e =
    Data.Maybe.fromMaybe
        (error "Worklist.mat: could not materialize")
        (materializeCondVec fixtureDF e)

xGt, xLt, yGt, yLt, zGt, zLt :: Double -> CondVec
xGt n = mat (F.col Double "x" .>. F.lit n)
xLt n = mat (F.col Double "x" .<. F.lit n)
yGt n = mat (F.col Double "y" .>. F.lit n)
yLt n = mat (F.col Double "y" .<. F.lit n)
zGt n = mat (F.col Double "z" .>. F.lit n)
zLt n = mat (F.col Double "z" .<. F.lit n)

-- Same truth vector as 'xGt 2' ([F,F,F,T,T,T]) but eSize 4 vs 3 — a non-degenerate
-- truth-vector collision for the min-eSize representative rule.
notLe2 :: CondVec
notLe2 = mat (F.not (F.col Double "x" .<=. F.lit 2))

litTrue :: CondVec
litTrue = mat (F.lit True)

keyOf :: CondVec -> String
keyOf = show . normalize . cvExpr

keySet :: [CondVec] -> Set.Set String
keySet = Set.fromList . map keyOf

truthSet :: [CondVec] -> Set.Set [Bool]
truthSet = Set.fromList . map (VU.toList . cvVec)

-- Mirrors 'evalWithPenaltyVec' (DecisionTree.hs): score = (#care-point errors, eSize),
-- depending only on the cached vector + size, so distinct same-vector same-size atoms tie.
penBy :: [Bool] -> CondVec -> (Int, Int)
penBy lbls cv =
    ( length (filter id (zipWith (/=) lbls (VU.toList (cvVec cv))))
    , eSize (cvExpr cv)
    )

-- The candidate 'bestDiscreteCandidate' would select: the first 'minimumBy penalty' winner.
argminKey :: [Bool] -> [CondVec] -> String
argminKey lbls = keyOf . minimumBy (compare `on` penBy lbls)

-- Oracle: the current depth-bounded generate-all.
ref :: Int -> [CondVec] -> [CondVec]
ref d base = boolExprsVec base base 0 d

base3 :: [CondVec]
base3 = [xGt 2, xGt 4, yGt 2]

-- x>2 and y<3 share the truth vector [F,F,F,T,T,T], so truth-vector mode collapses
-- this 3-atom base to 2 distinct vectors while structural mode keeps all three.
collBase :: [CondVec]
collBase = [xGt 2, yLt 3, yGt 2]

-- Wider fixture (3 independent-ish columns, 10 rows) yielding many distinct truth
-- vectors — broader coverage for the truth-vector floor / dedup than the 6-row x/y fixture.
wideDF :: D.DataFrame
wideDF =
    D.fromNamedColumns
        [ ("a", DI.fromList ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9] :: [Double]))
        , ("b", DI.fromList ([9, 7, 5, 3, 1, 8, 6, 4, 2, 0] :: [Double]))
        , ("c", DI.fromList ([1, 1, 2, 2, 3, 3, 4, 4, 5, 5] :: [Double]))
        ]

matW :: Expr Bool -> CondVec
matW e =
    Data.Maybe.fromMaybe
        (error "Worklist.matW: could not materialize")
        (materializeCondVec wideDF e)

wideBase :: [CondVec]
wideBase =
    [ matW (F.col Double "a" .>. F.lit 3)
    , matW (F.col Double "b" .<. F.lit 5)
    , matW (F.col Double "c" .>=. F.lit 3)
    ]

------------------------------------------------------------------------
-- HUnit cases
------------------------------------------------------------------------

tests :: [Test]
tests =
    [ TestLabel "structural: same distinct set as oracle" . TestCase $
        assertEqual
            "keySet"
            (keySet (ref 2 base3))
            (keySet (saturateCandidates Structural 2 base3))
    , TestLabel "structural: output deduped (length == distinct keys)" . TestCase $
        let out = saturateCandidates Structural 2 base3
         in assertEqual "no eqExpr duplicates" (Set.size (keySet out)) (length out)
    , TestLabel "structural: base atoms all present" . TestCase $
        assertBool "base subset of output" $
            keySet base3 `Set.isSubsetOf` keySet (saturateCandidates Structural 1 base3)
    , TestLabel "structural: deterministic" . TestCase $
        assertEqual
            "two runs identical"
            (map keyOf (saturateCandidates Structural 2 base3))
            (map keyOf (saturateCandidates Structural 2 base3))
    , TestLabel "structural: consolidation flows through the worklist" . TestCase $
        -- x>2 ∧ x>4 ↦ x>4, x>2 ∨ x>4 ↦ x>2, so the distinct set is just {x>2, x>4}.
        assertEqual
            "consolidated set"
            (keySet [xGt 2, xGt 4])
            (keySet (saturateCandidates Structural 2 [xGt 2, xGt 4]))
    , TestLabel "truth-vector: all output truth vectors distinct" . TestCase $
        let out = saturateCandidates TruthVector 2 [xGt 2, yLt 3]
         in assertEqual "distinct cvVecs" (Set.size (truthSet out)) (length out)
    , TestLabel "truth-vector: collapses same-truth atoms (x>2, y<3)" . TestCase $
        -- x>2 and y<3 are identical on the data; one representative survives.
        assertEqual
            "collapsed to one"
            1
            (length (saturateCandidates TruthVector 1 [xGt 2, yLt 3]))
    , TestLabel "truth-vector: reaches the semantic floor (no split dropped)"
        . TestCase
        $ assertEqual
            "same distinct truth vectors as oracle"
            (truthSet (ref 2 base3))
            (truthSet (saturateCandidates TruthVector 2 base3))
    , TestLabel "truth-vector: strictly fewer candidates when a collision exists"
        . TestCase
        $
        -- collBase has x>2 ≡ y<3, so the truth-vector floor is strictly below the structural set.
        assertBool "|truth| < |structural|"
        $ length (saturateCandidates TruthVector 2 collBase)
            < length (saturateCandidates Structural 2 collBase)
    , TestLabel "truth-vector: keeps the minimum-eSize representative" . TestCase $
        -- x>2 (eSize 3) and not(x<=2) (eSize 4) share a truth vector; the smaller survives.
        let out = saturateCandidates TruthVector 1 [xGt 2, notLe2]
         in do
                assertEqual "min eSize survivor" [3] (map (eSize . cvExpr) out)
                assertEqual "survivor is x>2" [keyOf (xGt 2)] (map keyOf out)
    , TestLabel "truth-vector: tie-break independent of input order" . TestCase $
        -- x>2 and y<3 tie on eSize 3; whichever survives must not depend on base order.
        assertEqual
            "order-independent survivor"
            (map keyOf (saturateCandidates TruthVector 1 [xGt 2, yLt 3]))
            (map keyOf (saturateCandidates TruthVector 1 [yLt 3, xGt 2]))
    , TestLabel "edge: empty base" . TestCase $
        assertEqual
            "empty in, empty out"
            Set.empty
            (keySet (saturateCandidates Structural 2 []))
    , TestLabel "edge: singleton base" . TestCase $
        assertEqual
            "same as oracle"
            (keySet (ref 2 [xGt 2]))
            (keySet (saturateCandidates Structural 2 [xGt 2]))
    , TestLabel "edge: maxDepth 0 is the base only" . TestCase $
        assertEqual
            "no expansion"
            (keySet base3)
            (keySet (saturateCandidates Structural 0 base3))
    , TestLabel "edge: duplicate-seeded base" . TestCase $
        let base = [xGt 2, xGt 2, yGt 2]
         in assertEqual
                "dedups seed, same as oracle"
                (keySet (ref 2 base))
                (keySet (saturateCandidates Structural 2 base))
    , TestLabel "edge: literal operand" . TestCase $
        let base = [xGt 2, litTrue]
         in assertEqual
                "same as oracle"
                (keySet (ref 2 base))
                (keySet (saturateCandidates Structural 2 base))
    , TestLabel "law: cvVec is a homomorphism over AND" . TestCase $
        -- the law justifying one-representative-per-truth-class dedup.
        assertEqual
            "cvVec(a∧b) == cvVec a && cvVec b"
            (VU.toList (VU.zipWith (&&) (cvVec (xGt 2)) (cvVec (yGt 2))))
            (VU.toList (cvVec (combineAndVec (xGt 2) (yGt 2))))
    , TestLabel "law: cvVec is a homomorphism over OR" . TestCase $
        assertEqual
            "cvVec(a∨b) == cvVec a || cvVec b"
            (VU.toList (VU.zipWith (||) (cvVec (xGt 2)) (cvVec (yGt 2))))
            (VU.toList (cvVec (combineOrVec (xGt 2) (yGt 2))))
    , TestLabel "law: consolidated expr re-interprets to its cached vector" . TestCase $
        -- x>2 ∧ x>4 consolidates to x>4; the cached vector must match re-materializing it.
        let c = combineAndVec (xGt 2) (xGt 4)
         in assertEqual
                "cached == re-interpreted"
                (VU.toList (cvVec c))
                (VU.toList (cvVec (mat (cvExpr c))))
    , TestLabel "structural: output order matches the deduped oracle" . TestCase $
        -- byte-identical to today's boolExprsVec, with eqExpr-duplicates removed (first kept):
        -- this is what lets the consumer's first-wins minimumBy pick the same candidate.
        assertEqual
            "deduped-oracle order"
            (map keyOf (nubBy ((==) `on` keyOf) (ref 2 base3)))
            (map keyOf (saturateCandidates Structural 2 base3))
    , TestLabel
        "structural: matches oracle set+order at depth 3+4 (non-consolidating)"
        . TestCase
        $
        -- cross-column base whose closure GROWS with depth (no consolidation); this is where the
        -- frontier:=admitted optimisation could diverge from the oracle's frontier:=all-products.
        let b = [xGt 2, yGt 2, zGt 1]
            deduped d = map keyOf (nubBy ((==) `on` keyOf) (ref d b))
            out d = map keyOf (saturateCandidates Structural d b)
         in do
                assertEqual "set d3" (Set.fromList (deduped 3)) (Set.fromList (out 3))
                assertEqual "order d3" (deduped 3) (out 3)
                assertEqual "set d4" (Set.fromList (deduped 4)) (Set.fromList (out 4))
                assertEqual "order d4" (deduped 4) (out 4)
    , TestLabel "structural: stabilizes at fixpoint (depth cap is a no-op past it)"
        . TestCase
        $
        -- all AND/OR consolidate back into the base ⇒ a genuine fixpoint at round 1, so deeper
        -- depth caps add nothing. (For a non-consolidating base the closure grows with depth,
        -- since 'normalize' does not flatten associativity — there the cap always binds.)
        assertEqual
            "depth 2 == depth 5"
            (keySet (saturateCandidates Structural 2 [xGt 1, xGt 2, xGt 3, xGt 4]))
            (keySet (saturateCandidates Structural 5 [xGt 1, xGt 2, xGt 3, xGt 4]))
    , TestLabel "truth-vector: reaches the floor on a wider fixture" . TestCase $
        assertEqual
            "same distinct truth vectors as oracle"
            (truthSet (ref 2 wideBase))
            (truthSet (saturateCandidates TruthVector 2 wideBase))
    , TestLabel "selection: surfaces the oracle's winning combination" . TestCase $
        -- labels = x>2 ∧ x<5; the unique min-penalty split is that band (not in the base), so
        -- 'minimumBy penalty' over the worklist must pick it just as it does over the oracle.
        let lbls = [False, False, False, True, True, False]
            base = [xGt 2, xLt 5, yGt 2]
         in assertEqual
                "same argmin as oracle"
                (argminKey lbls (ref 2 base))
                (argminKey lbls (saturateCandidates Structural 2 base))
    , TestLabel "selection: tie-winner tracks input order, matching the oracle"
        . TestCase
        $
        -- x>2 and y<3 both score the min (0,3); 'minimumBy' keeps the first, so the winner must
        -- flip with input order exactly as the oracle does. A worklist that imposes its own
        -- (eSize, exprKey) order would pick the same atom for both orders and fail one. (byte-identical)
        let lbls = [False, False, False, True, True, True]
         in do
                assertEqual
                    "x>2-first order"
                    (argminKey lbls (ref 2 [xGt 2, yLt 3]))
                    (argminKey lbls (saturateCandidates Structural 2 [xGt 2, yLt 3]))
                assertEqual
                    "y<3-first order"
                    (argminKey lbls (ref 2 [yLt 3, xGt 2]))
                    (argminKey lbls (saturateCandidates Structural 2 [yLt 3, xGt 2]))
    , TestLabel
        "bounded: output is the distinct closure, below the oracle's materialized count"
        . TestCase
        $
        -- Same-direction thresholds: every AND/OR consolidates, so the closure stays these 4 while
        -- the oracle materializes far more. (Peak residency is the +RTS -s integration check.)
        let base = [xGt 1, xGt 2, xGt 3, xGt 4]
            gen = ref 3 base
         in do
                assertEqual
                    "output bounded to the distinct closure"
                    (Set.size (keySet gen))
                    (length (saturateCandidates Structural 3 base))
                assertBool
                    "oracle materializes more than the closure (the explosion the worklist avoids)"
                    (Set.size (keySet gen) < length gen)
    , TestLabel "structural: maxDepth 1 is base-only (no combination round)"
        . TestCase
        $
        -- boolExprsVec does no combining until depth 2; the worklist must match it depth-for-depth.
        assertEqual
            "no combination at depth 1"
            (keySet (ref 1 [xGt 2, yGt 2]))
            (keySet (saturateCandidates Structural 1 [xGt 2, yGt 2]))
    , TestLabel "structural: base atoms survive the combination round" . TestCase $
        -- a base atom regenerated by a combination must not be dropped.
        -- a base atom regenerated by a combination must not be dropped.
        -- a base atom regenerated by a combination must not be dropped.
        -- a base atom regenerated by a combination must not be dropped.
        -- a base atom regenerated by a combination must not be dropped.
        assertBool "base subset of output at depth 2" $
            keySet base3 `Set.isSubsetOf` keySet (saturateCandidates Structural 2 base3)
    , TestLabel
        "structural: re-saturating a closed base is stable (fixpoint idempotence)"
        . TestCase
        $
        -- on a base whose closure is itself, re-saturating changes nothing. (Depth-bounded
        -- saturation is NOT idempotent on a growing closure — re-feeding goes one round deeper.)
        let b = [xGt 1, xGt 2, xGt 3, xGt 4]
         in assertEqual
                "saturate ∘ saturate == saturate"
                (keySet (saturateCandidates Structural 2 b))
                (keySet (saturateCandidates Structural 2 (saturateCandidates Structural 2 b)))
    , TestLabel "law: combiner key is order-independent (congruence basis)" . TestCase $
        -- combining respects 'normalize', so deduping before combining is sound; also exercises
        -- consolidation in both operand orders.
        do
            assertEqual
                "AND consolidation commutes at the key"
                (keyOf (combineAndVec (xGt 2) (xGt 4)))
                (keyOf (combineAndVec (xGt 4) (xGt 2)))
            assertEqual
                "OR consolidation commutes at the key"
                (keyOf (combineOrVec (xGt 2) (xGt 4)))
                (keyOf (combineOrVec (xGt 4) (xGt 2)))
            assertEqual
                "cross-column AND commutes at the key"
                (keyOf (combineAndVec (xGt 2) (yGt 2)))
                (keyOf (combineAndVec (yGt 2) (xGt 2)))
    , TestLabel
        "truth-vector: section is the (eSize, compareExpr)-minimum of the fiber"
        . TestCase
        $
        -- not merely order-independent: the survivor is the deterministic min, never the max.
        let fiber = [xGt 2, yLt 3]
            cmp a b =
                compare (eSize (cvExpr a)) (eSize (cvExpr b))
                    <> compareExpr (cvExpr a) (cvExpr b)
            want = keyOf (minimumBy cmp fiber)
         in assertEqual
                "min-section survivor"
                [want]
                (map keyOf (saturateCandidates TruthVector 1 fiber))
    ]

------------------------------------------------------------------------
-- QuickCheck properties (over generated base pools and depths)
------------------------------------------------------------------------

genAtom :: Gen CondVec
genAtom =
    elements
        [xGt 1, xGt 2, xGt 3, xLt 2, xLt 4, yGt 1, yGt 3, yLt 3, zGt 1, zGt 3, zLt 4]

genBase :: Gen [CondVec]
genBase = choose (2, 5) >>= \k -> vectorOf k genAtom

-- Random label vector of the fixture's length (6 rows), for selection-preservation.
genLabels :: Gen [Bool]
genLabels = vectorOf 6 (elements [False, True])

prop_structuralSameSet :: Property
prop_structuralSameSet =
    forAllBlind genBase $ \base ->
        forAll (choose (1, 3)) $ \d ->
            counterexample (show (map keyOf base, d)) $
                keySet (saturateCandidates Structural d base) === keySet (ref d base)

prop_truthVectorFloor :: Property
prop_truthVectorFloor =
    forAllBlind genBase $ \base ->
        forAll (choose (1, 3)) $ \d ->
            counterexample (show (map keyOf base, d)) $
                truthSet (saturateCandidates TruthVector d base) === truthSet (ref d base)

-- The candidate *set* depends only on the base as a set, not its input order. (Output

-- * order* tracks input order — that is the byte-identity contract, see the selection tests.)
prop_orderInvariant :: Property
prop_orderInvariant =
    forAllBlind genBase $ \base ->
        forAllBlind (shuffle base) $ \base' ->
            forAll (choose (1, 3)) $ \d ->
                counterexample (show (map keyOf base, map keyOf base', d)) $
                    keySet (saturateCandidates Structural d base)
                        === keySet (saturateCandidates Structural d base')

-- The candidate the consumer's 'minimumBy penaltyCV' selects is byte-identical to the oracle's,
-- for any label vector (d >= 2 so combinations exist). This is the model-preservation contract.
prop_selectionPreserved :: Property
prop_selectionPreserved =
    forAllBlind genBase $ \base ->
        forAllBlind genLabels $ \lbls ->
            forAll (choose (2, 3)) $ \d ->
                counterexample (show (map keyOf base, lbls, d)) $
                    argminKey lbls (saturateCandidates Structural d base)
                        === argminKey lbls (ref d base)

-- The full output (order included) is byte-identical to the deduped oracle, at every depth.
-- Subsumes selection-preservation for ANY (cvVec,eSize)-penalty, and stresses the
-- frontier:=admitted optimisation past the depth where it could first diverge.
prop_orderMatchesOracle :: Property
prop_orderMatchesOracle =
    forAllBlind genBase $ \base ->
        forAll (choose (2, 3)) $ \d ->
            counterexample (show (map keyOf base, d)) $
                map keyOf (saturateCandidates Structural d base)
                    === map keyOf (nubBy ((==) `on` keyOf) (ref d base))

-- The structural key faithfully represents the 'eqExpr' quotient on the candidate domain
-- (atoms and their AND/OR products): show.normalize merges exactly what eqExpr merges.
genCand :: Gen CondVec
genCand =
    oneof
        [ genAtom
        , combineAndVec <$> genAtom <*> genAtom
        , combineOrVec <$> genAtom <*> genAtom
        ]

prop_keyFaithful :: Property
prop_keyFaithful =
    forAllBlind genCand $ \a ->
        forAllBlind genCand $ \b ->
            counterexample (keyOf a ++ "  vs  " ++ keyOf b) $
                (keyOf a == keyOf b) === eqExpr (cvExpr a) (cvExpr b)

props :: [Property]
props =
    [ prop_structuralSameSet
    , prop_truthVectorFloor
    , prop_orderInvariant
    , prop_selectionPreserved
    , prop_orderMatchesOracle
    , prop_keyFaithful
    ]
