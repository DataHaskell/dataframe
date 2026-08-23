{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module DecisionTree where

import DataFrame.DecisionTree
import DataFrame.DecisionTree.Cart (buildCartTree)
import DataFrame.DecisionTree.Categorical (
    TargetInfo,
    discreteConditions,
    mkTargetInfo,
 )
import DataFrame.DecisionTree.CondVec (
    CondVec (..),
    combineAndVec,
    combineOrVec,
    materializeCondVec,
 )
import DataFrame.DecisionTree.Fit (
    ProbTree,
    buildProbTree,
    fitDecisionTree,
    fitProbTree,
    probExprs,
    probsFromIndices,
 )
import DataFrame.DecisionTree.Numeric (
    NumExpr (NMaybeDouble),
    generateNumericConds,
    missingnessConditions,
    numericCols,
    numericCondVecs,
    numericExprsWithTerms,
 )
import DataFrame.DecisionTree.Predict (
    computeTreeLoss,
    countCarePointErrors,
    identifyCarePoints,
    majorityValueFromIndices,
    partitionIndices,
    predictWithTree,
 )
import DataFrame.DecisionTree.Tao (taoIteration, taoOptimize)
import DataFrame.DecisionTree.Types (CarePoint (..), Direction (..))
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.Column.Bitmap as DI
import DataFrame.Internal.Expression (Expr (..), eqExpr, getColumns)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.PackedText (mkPackedContiguous)
import qualified DataFrame.LinearSolver
import DataFrame.Operators
import qualified DataFrameApi as D

import Control.Monad (zipWithM_)
import qualified Data.ByteString as B
import Data.Function (on)
import Data.List (maximumBy, sort)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)
import Test.HUnit

------------------------------------------------------------------------
-- Shared fixtures
------------------------------------------------------------------------

{- | Build a 'TargetInfo' or fail loudly; the test fixtures always satisfy
'mkTargetInfo', so a 'Nothing' here is a broken test, not a runtime case.
-}
requireTargetInfo :: T.Text -> D.DataFrame -> TargetInfo T.Text
requireTargetInfo target df = case mkTargetInfo @T.Text target df of
    Just ti -> ti
    Nothing -> error ("requireTargetInfo: no target info for " <> T.unpack target)

-- 4 rows: label = ["A","B","A","C"], x = [1.0,2.0,3.0,4.0]
fixtureDF :: D.DataFrame
fixtureDF =
    D.fromNamedColumns
        [ ("label", DI.fromList (["A", "B", "A", "C"] :: [T.Text]))
        , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
        ]

allIndices :: V.Vector Int
allIndices = V.fromList [0, 1, 2, 3]

leftTree :: Tree T.Text
leftTree = Leaf "A"

rightTree :: Tree T.Text
rightTree = Leaf "B"

-- x <= 2.5: True for idx 0,1 (→ left); False for idx 2,3 (→ right)
splitCond :: Expr Bool
splitCond = F.col @Double "x" .<= F.lit (2.5 :: Double)

-- Pre-computed care points for the full fixture
carePoints3 :: [CarePoint]
carePoints3 =
    identifyCarePoints @T.Text "label" fixtureDF allIndices leftTree rightTree

------------------------------------------------------------------------
-- Unit tests: identifyCarePoints
------------------------------------------------------------------------

carePointsBothWrong :: Test
carePointsBothWrong =
    TestCase $
        assertBool
            "idx 3 (label=C, neither A nor B) should not be a care point"
            (3 `notElem` map cpIndex carePoints3)

carePointsLeftCorrect :: Test
carePointsLeftCorrect = TestCase $ do
    let cp0 = filter ((== 0) . cpIndex) carePoints3
    case cp0 of
        (c : _) ->
            assertEqual
                "idx 0 (label=A matches left Leaf A) should route GoLeft"
                GoLeft
                (cpCorrectDir c)
        [] -> assertFailure "idx 0 should be a care point"

carePointsRightCorrect :: Test
carePointsRightCorrect = TestCase $ do
    let cp1 = filter ((== 1) . cpIndex) carePoints3
    case cp1 of
        (c : _) ->
            assertEqual
                "idx 1 (label=B matches right Leaf B) should route GoRight"
                GoRight
                (cpCorrectDir c)
        [] -> assertFailure "idx 1 should be a care point"

carePointsMixed :: Test
carePointsMixed = TestCase $ do
    assertEqual "exactly 3 care points" 3 (length carePoints3)
    let idxs = map cpIndex carePoints3
    assertBool "idx 0 present" (0 `elem` idxs)
    assertBool "idx 1 present" (1 `elem` idxs)
    assertBool "idx 2 present" (2 `elem` idxs)
    assertBool "idx 3 absent" (3 `notElem` idxs)

carePointsBothCorrect :: Test
carePointsBothCorrect = TestCase $ do
    let df2 =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0] :: [Double]))
                ]
        cps =
            identifyCarePoints @T.Text
                "label"
                df2
                (V.fromList [0, 1])
                (Leaf "A")
                (Leaf "A")
    assertEqual "no care points when both subtrees agree" 0 (length cps)

------------------------------------------------------------------------
-- Unit tests: majorityValueFromIndices
------------------------------------------------------------------------

majorityVoteTest :: Test
majorityVoteTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["cat", "dog", "cat", "cat"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
                ]
    assertEqual
        "majority is cat (3 votes)"
        "cat"
        (majorityValueFromIndices @T.Text "label" df (V.fromList [0, 1, 2, 3]))

majorityVoteSubset :: Test
majorityVoteSubset = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["cat", "dog", "cat", "cat"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
                ]
        result = majorityValueFromIndices @T.Text "label" df (V.fromList [0, 1, 3])
    assertEqual "majority from subset [0,1,3] is cat" "cat" result

------------------------------------------------------------------------
-- Unit tests: computeTreeLoss
------------------------------------------------------------------------

computeLossZero :: Test
computeLossZero = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A", "B", "B"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
                ]
        stump = Branch splitCond (Leaf "A") (Leaf "B") :: Tree T.Text
        loss = computeTreeLoss @T.Text "label" df (V.fromList [0, 1, 2, 3]) stump
    assertEqual "perfect stump has zero loss" 0.0 loss

computeLossHalf :: Test
computeLossHalf = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A", "B", "B"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
                ]
        constTree = Leaf "A" :: Tree T.Text
        loss = computeTreeLoss @T.Text "label" df (V.fromList [0, 1, 2, 3]) constTree
    assertEqual "constant leaf misclassifies half of balanced data" 0.5 loss

------------------------------------------------------------------------
-- Unit tests: partitionIndices
------------------------------------------------------------------------

partitionDisjoint :: Test
partitionDisjoint = TestCase $ do
    let (lft, rgt) = partitionIndices splitCond fixtureDF allIndices
        leftSet = V.toList lft
        rightSet = V.toList rgt
        intersection = filter (`elem` rightSet) leftSet
    assertEqual "left and right partitions are disjoint" [] intersection

partitionUnion :: Test
partitionUnion = TestCase $ do
    let (lft, rgt) = partitionIndices splitCond fixtureDF allIndices
        combined = sort (V.toList lft ++ V.toList rgt)
    assertEqual
        "union of partitions equals the original index set"
        [0, 1, 2, 3]
        combined

------------------------------------------------------------------------
-- Unit tests: countCarePointErrors
------------------------------------------------------------------------

countErrorsAllCorrect :: Test
countErrorsAllCorrect = TestCase $ do
    let cps = [CarePoint 0 GoLeft, CarePoint 1 GoRight]
        cond = F.col @Double "x" .<= F.lit (1.5 :: Double)
        errs = countCarePointErrors cond fixtureDF cps
    assertEqual "condition routes all care points correctly" 0 errs

countErrorsAllWrong :: Test
countErrorsAllWrong = TestCase $ do
    let cps = [CarePoint 0 GoLeft, CarePoint 1 GoRight]
        cond = F.col @Double "x" .> F.lit (1.5 :: Double)
        errs = countCarePointErrors cond fixtureDF cps
    assertEqual "reversed condition misroutes all care points" 2 errs

------------------------------------------------------------------------
-- Unit tests: predictWithTree
------------------------------------------------------------------------

predictLeaf :: Test
predictLeaf =
    TestCase $
        assertEqual
            "leaf prediction ignores row index"
            "Z"
            (predictWithTree @T.Text "label" fixtureDF 0 (Leaf "Z"))

predictBranch :: Test
predictBranch = TestCase $ do
    let stump = Branch splitCond (Leaf "A") (Leaf "B") :: Tree T.Text
    assertEqual
        "idx 0 (x=1.0 <= 2.5) routes left -> A"
        "A"
        (predictWithTree @T.Text "label" fixtureDF 0 stump)
    assertEqual
        "idx 3 (x=4.0 > 2.5) routes right -> B"
        "B"
        (predictWithTree @T.Text "label" fixtureDF 3 stump)

------------------------------------------------------------------------
-- Integration tests
------------------------------------------------------------------------

-- 20-row, linearly separable: x in [1..10] -> "pos", x in [11..20] -> "neg"
sepDF :: D.DataFrame
sepDF =
    let xs = map fromIntegral [1 .. 20 :: Int] :: [Double]
        labels = map (\x -> if x <= 10.0 then "pos" else "neg") xs :: [T.Text]
     in D.fromNamedColumns
            [ ("label", DI.fromList labels)
            , ("x", DI.fromList xs)
            ]

-- Candidate conditions that bracket the decision boundary
sepConds :: [Expr Bool]
sepConds =
    [ F.col @Double "x" .<= F.lit (10.5 :: Double)
    , F.col @Double "x" .> F.lit (10.5 :: Double)
    ]

testCfg :: TreeConfig
testCfg =
    defaultTreeConfig
        { taoIterations = 5
        , expressionPairs = 4
        , minLeafSize = 1
        }

-- Initial tree deliberately wrong: routes "pos" rows to the "neg" leaf
wrongStump :: Tree T.Text
wrongStump =
    Branch
        (F.col @Double "x" .> F.lit (10.5 :: Double))
        (Leaf "pos")
        (Leaf "neg")

taoNoDegradation :: Test
taoNoDegradation = TestCase $ do
    let indices = V.enumFromN 0 20
        initialLoss = computeTreeLoss @T.Text "label" sepDF indices wrongStump
        optimized =
            taoOptimize @T.Text testCfg "label" sepConds sepDF indices wrongStump
        finalLoss = computeTreeLoss @T.Text "label" sepDF indices optimized
    assertBool
        "taoOptimize must not increase loss"
        (finalLoss <= initialLoss + 1e-9)

taoMonotone :: Test
taoMonotone = TestCase $ do
    let indices = V.enumFromN 0 20
        initLoss = computeTreeLoss @T.Text "label" sepDF indices wrongStump
        stepTree = taoIteration @T.Text testCfg "label" sepConds sepDF indices
        step (tree, _) =
            let tree' = stepTree tree
             in (tree', computeTreeLoss @T.Text "label" sepDF indices tree')
        snapshots = take 6 $ iterate step (wrongStump, initLoss)
        losses = map snd snapshots
        pairs = zip losses (drop 1 losses)
    assertBool
        "loss must be non-increasing across taoIteration steps"
        (all (\(a, b) -> b <= a + 1e-9) pairs)

taoConvergesPureLabels :: Test
taoConvergesPureLabels = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (replicate 10 ("A" :: T.Text)))
                , ("x", DI.fromList ([1.0 .. 10.0] :: [Double]))
                ]
        indices = V.enumFromN 0 10
        initTree = Leaf "A" :: Tree T.Text
        initLoss = computeTreeLoss @T.Text "label" df indices initTree
        result =
            taoOptimize @T.Text testCfg "label" sepConds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertEqual "pure-label initial loss must be zero" 0.0 initLoss
    assertEqual "pure-label final loss must still be zero" 0.0 finalLoss

taoDeadBranchNoCrash :: Test
taoDeadBranchNoCrash = TestCase $ do
    let badCond = F.col @Double "x" .<= F.lit (0.5 :: Double)
        indices = V.enumFromN 0 20
        initTree = Branch badCond (Leaf "pos") (Leaf "neg") :: Tree T.Text
        result =
            taoOptimize @T.Text testCfg "label" [badCond] sepDF indices initTree
        finalLoss = computeTreeLoss @T.Text "label" sepDF indices result
    assertBool
        "dead-branch tree must produce a valid loss in [0,1]"
        (finalLoss >= 0.0 && finalLoss <= 1.0)

gridPairs :: [(Double, Double)]
gridPairs = [(x, y) | y <- [1 .. 4], x <- [1 .. 4]]

gridBaseDF :: D.DataFrame
gridBaseDF =
    D.fromNamedColumns
        [ ("x", DI.fromList (map fst gridPairs))
        , ("y", DI.fromList (map snd gridPairs))
        ]

taoRecoversSingleObliqueDerived :: Test
taoRecoversSingleObliqueDerived = TestCase $ do
    let labelExpr =
            F.ifThenElse
                ((F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double))
                (F.lit ("pos" :: T.Text))
                (F.lit ("neg" :: T.Text))
        df = D.derive @T.Text "label" labelExpr gridBaseDF
        indices = V.enumFromN 0 16
        initTree =
            Branch
                (F.col @Double "x" .<= F.lit (2.5 :: Double))
                (Leaf "pos")
                (Leaf "neg") ::
                Tree T.Text
        conds =
            [ (F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double)
            , (F.col @Double "x" + F.col @Double "y") .> F.lit (4.5 :: Double)
            ]
        cfg = defaultTreeConfig{taoIterations = 5, expressionPairs = 4, minLeafSize = 1}
        result = taoOptimize @T.Text cfg "label" conds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertEqual
        "TAO recovers single oblique (x+y) split with zero loss"
        0.0
        finalLoss

taoRecoversNestedObliqueDerived :: Test
taoRecoversNestedObliqueDerived = TestCase $ do
    let labelExpr =
            F.ifThenElse
                ((F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double))
                (F.lit ("low" :: T.Text))
                ( F.ifThenElse
                    ((F.col @Double "x" - F.col @Double "y") .<= F.lit (0.5 :: Double))
                    (F.lit "mid")
                    (F.lit "high")
                )
        df = D.derive @T.Text "label" labelExpr gridBaseDF
        indices = V.enumFromN 0 16
        initTree =
            Branch
                (F.col @Double "x" .<= F.lit (1.5 :: Double))
                (Leaf "low")
                ( Branch
                    (F.col @Double "y" .<= F.lit (3.5 :: Double))
                    (Leaf "mid")
                    (Leaf "high")
                ) ::
                Tree T.Text
        conds =
            [ (F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double)
            , (F.col @Double "x" + F.col @Double "y") .> F.lit (4.5 :: Double)
            , (F.col @Double "x" - F.col @Double "y") .<= F.lit (0.5 :: Double)
            , (F.col @Double "x" - F.col @Double "y") .> F.lit (0.5 :: Double)
            ]
        cfg = defaultTreeConfig{taoIterations = 5, expressionPairs = 4, minLeafSize = 1}
        result = taoOptimize @T.Text cfg "label" conds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertEqual
        "TAO recovers nested oblique (x+y)/(x-y) tree with zero loss"
        0.0
        finalLoss

obliqueAxisAlignedFixture ::
    (D.DataFrame, V.Vector Int, [Expr Bool], Tree T.Text)
obliqueAxisAlignedFixture =
    let labelExpr =
            F.ifThenElse
                ((F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double))
                (F.lit ("pos" :: T.Text))
                (F.lit ("neg" :: T.Text))
        df = D.derive @T.Text "label" labelExpr gridBaseDF
        indices = V.enumFromN 0 16
        axisConds =
            [F.col @Double "x" .<= F.lit (t :: Double) | t <- [1.5, 2.5, 3.5]]
                ++ [F.col @Double "y" .<= F.lit (t :: Double) | t <- [1.5, 2.5, 3.5]]
        initTree =
            Branch
                (F.col @Double "x" .<= F.lit (2.5 :: Double))
                (Leaf "pos")
                (Leaf "neg") ::
                Tree T.Text
     in (df, indices, axisConds, initTree)

taoAxisAlignedInsufficientForObliqueDiscreteOnly :: Test
taoAxisAlignedInsufficientForObliqueDiscreteOnly = TestCase $ do
    let (df, indices, axisConds, initTree) = obliqueAxisAlignedFixture
        cfg =
            defaultTreeConfig
                { taoIterations = 10
                , expressionPairs = 6
                , minLeafSize = 1
                , useLinearSolver = False
                }
        result = taoOptimize @T.Text cfg "label" axisConds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertBool
        "axis-aligned stump cannot recover oblique label without linear solver (loss > 0.1)"
        (finalLoss > 0.1)

taoLinearRecoversObliqueFromAxisAlignedPool :: Test
taoLinearRecoversObliqueFromAxisAlignedPool = TestCase $ do
    let (df, indices, axisConds, initTree) = obliqueAxisAlignedFixture
        cfg =
            defaultTreeConfig
                { taoIterations = 10
                , expressionPairs = 6
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                }
        result = taoOptimize @T.Text cfg "label" axisConds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertEqual
        "linear solver recovers oblique split from axis-aligned-only pool"
        0.0
        finalLoss

-- Cleanly separable nullable column (no actual nulls): Just 1..6 -> "pos",
-- Just 7..12 -> "neg". Exercises the nullable numeric path.
nullableSepDF :: D.DataFrame
nullableSepDF =
    D.fromNamedColumns
        [ ("label", DI.fromList (replicate 6 "pos" ++ replicate 6 "neg" :: [T.Text]))
        ,
            ( "x"
            , DI.fromVector
                ( V.fromList $
                    map (Just . fromIntegral) ([1 .. 6] :: [Int])
                        ++ map (Just . fromIntegral) ([7 .. 12] :: [Int]) ::
                    V.Vector (Maybe Double)
                )
            )
        ]

-- DF with genuine nulls interspersed.
nullsMixedDF :: D.DataFrame
nullsMixedDF =
    D.fromNamedColumns
        [ ("label", DI.fromList (["pos", "pos", "pos", "neg", "neg", "neg"] :: [T.Text]))
        ,
            ( "x"
            , DI.fromVector
                ( V.fromList
                    [Just 1.0, Nothing, Just 3.0, Just 7.0, Nothing, Just 9.0] ::
                    V.Vector (Maybe Double)
                )
            )
        ]

-- numericCols picks up DI.fromVector (Maybe Double) as NMaybeDouble.
numericColsNullableDoubleTest :: Test
numericColsNullableDoubleTest = TestCase $ do
    let exprs = numericCols nullableSepDF
        hasMD = any (\case NMaybeDouble _ -> True; _ -> False) exprs
    assertBool
        "numericCols finds NMaybeDouble for DI.fromVector (Maybe Double)"
        hasMD

-- numericCols picks up DI.fromVector (Maybe Int) as NMaybeDouble (via whenPresent).
numericColsNullableIntTest :: Test
numericColsNullableIntTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["pos", "neg"] :: [T.Text]))
                ,
                    ( "n"
                    , DI.fromVector (V.fromList [Just (1 :: Int), Just 2] :: V.Vector (Maybe Int))
                    )
                ]
        hasMD = any (\case NMaybeDouble _ -> True; _ -> False) (numericCols df)
    assertBool "numericCols finds NMaybeDouble for DI.fromVector (Maybe Int)" hasMD

-- generateNumericConds is non-empty for a DF with an DI.fromVector (Maybe Double).
numericCondsNullableNonEmptyTest :: Test
numericCondsNullableNonEmptyTest =
    TestCase $
        assertBool
            "generateNumericConds non-empty for DI.fromVector (Maybe Double)"
            (not (null (generateNumericConds defaultTreeConfig nullableSepDF)))

-- Null values evaluate to False for threshold conditions (null rows route right).
nullValueRoutesFalseTest :: Test
nullValueRoutesFalseTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "B"] :: [T.Text]))
                ,
                    ( "x"
                    , DI.fromVector
                        (V.fromList [Nothing, Just (5.0 :: Double)] :: V.Vector (Maybe Double))
                    )
                ]
        cond = F.fromMaybe False (F.col @(Maybe Double) "x" .<= F.lit (6.0 :: Double))
        (lft, rgt) = partitionIndices cond df (V.fromList [0, 1])
    assertBool "null row (idx 0) routes to right (false) partition" (0 `V.elem` rgt)
    assertBool "Just 5.0 <= 6.0 routes to left (true) partition" (1 `V.elem` lft)

-- Nullable feature (no actual nulls) achieves zero loss on cleanly separable data.
nullableFitZeroLossTest :: Test
nullableFitZeroLossTest = TestCase $ do
    let cfg = defaultTreeConfig{taoIterations = 5, expressionPairs = 4, minLeafSize = 1}
        featureDf = D.exclude ["label"] nullableSepDF
        conds = generateNumericConds cfg featureDf
        initTree = buildCartTree @T.Text cfg "label" nullableSepDF
        indices = V.enumFromN 0 12
        result = taoOptimize @T.Text cfg "label" conds nullableSepDF indices initTree
        loss = computeTreeLoss @T.Text "label" nullableSepDF indices result
    assertEqual "zero loss on cleanly separable OptionalColumn data" 0.0 loss

-- fitDecisionTree with genuine nulls: loss is a valid probability and no crash.
nullableFitWithNullsNoCrashTest :: Test
nullableFitWithNullsNoCrashTest = TestCase $ do
    let cfg = defaultTreeConfig{taoIterations = 3, expressionPairs = 4, minLeafSize = 1}
        featureDf = D.exclude ["label"] nullsMixedDF
        conds = generateNumericConds cfg featureDf
        initTree = buildCartTree @T.Text cfg "label" nullsMixedDF
        indices = V.enumFromN 0 6
        result = taoOptimize @T.Text cfg "label" conds nullsMixedDF indices initTree
        loss = computeTreeLoss @T.Text "label" nullsMixedDF indices result
    assertBool
        "loss is in [0,1] with null values present"
        (loss >= 0.0 && loss <= 1.0)

-- numericExprsWithTerms produces cross-column combinations when one col is
-- DI.fromVector (Maybe Double) and another is a plain UnboxedColumn Double.
numericExprsWithTermsMixedTest :: Test
numericExprsWithTermsMixedTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [
                    ( "x"
                    , DI.fromVector
                        (V.fromList [Just 1.0, Just 2.0, Just 3.0] :: V.Vector (Maybe Double))
                    )
                , ("y", DI.fromList ([4.0, 5.0, 6.0] :: [Double]))
                ]
        cfg = defaultSynthConfig{maxExprDepth = 2, enableArithOps = True}
        exprs = numericExprsWithTerms cfg df
    assertBool
        "more than 2 expressions: base cols + combinations"
        (length exprs > 2)
    assertBool
        "combined exprs include NMaybeDouble (nullable arithmetic)"
        (any (\case NMaybeDouble _ -> True; _ -> False) exprs)

missingnessCondsTest :: Test
missingnessCondsTest = TestCase $ do
    let conds = missingnessConditions (D.exclude ["label"] nullsMixedDF)
    assertEqual "one nullable column -> one missingness cond" 1 (length conds)
    assertBool
        "cond is isNothing x"
        (eqExpr (head conds) (F.isNothing (F.col @(Maybe Double) "x")))
    assertBool
        "no missingness conds for a non-nullable DataFrame"
        (null (missingnessConditions fixtureDF))

poolContainsMissingnessTest :: Test
poolContainsMissingnessTest =
    TestCase $
        assertBool
            "generateNumericConds contains isNothing x"
            ( any
                (`eqExpr` F.isNothing (F.col @(Maybe Double) "x"))
                (generateNumericConds defaultTreeConfig (D.exclude ["label"] nullsMixedDF))
            )

missingnessCondVecTest :: Test
missingnessCondVecTest = TestCase $ do
    let cvs =
            numericCondVecs
                defaultTreeConfig
                (D.exclude ["label"] nullsMixedDF)
                nullsMixedDF
        isMissingCV cv = eqExpr (cvExpr cv) (F.isNothing (F.col @(Maybe Double) "x"))
        expected = VU.fromList [False, True, False, False, True, False]
    case filter isMissingCV cvs of
        (cv : _) -> assertEqual "isNothing vector marks null slots" expected (cvVec cv)
        [] -> assertFailure "no isNothing CondVec in pool"

observedOnlyThresholdsTest :: Test
observedOnlyThresholdsTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [
                    ( "x"
                    , DI.fromVector
                        ( V.fromList
                            (replicate 5 Nothing ++ map Just [10, 20, 30, 40, 50]) ::
                            V.Vector (Maybe Double)
                        )
                    )
                ]
        conds = generateNumericConds defaultTreeConfig{percentiles = [50]} df
        leqAt t = F.fromMaybe False (F.col @(Maybe Double) "x" .<= F.lit (t :: Double))
    assertBool
        "median threshold from observed values (30)"
        (any (`eqExpr` leqAt 30) conds)
    assertBool "no null-skewed threshold (10)" (not (any (`eqExpr` leqAt 10) conds))

packedFromTexts :: Maybe [Int] -> [T.Text] -> DI.Column
packedFromTexts nullIdxs ts =
    DI.PackedText bm (mkPackedContiguous arr (VU.fromList offs))
  where
    bytess = map (B.unpack . encodeUtf8) ts
    offs = scanl (+) 0 (map length bytess)
    arr = arrayFromBytes (concat bytess)
    bm = DI.buildBitmapFromNulls (length ts) <$> nullIdxs

arrayFromBytes :: [Word8] -> A.Array
arrayFromBytes ws = A.run $ do
    m <- A.new (length ws)
    zipWithM_ (A.unsafeWrite m) [0 ..] ws
    pure m

-- G5: nullable PackedText columns (what CSV ingest emits for nullable text)
-- get a missingness candidate; non-nullable PackedText does not.
packedMissingnessTest :: Test
packedMissingnessTest = TestCase $ do
    let df =
            D.fromNamedColumns
                [("s", packedFromTexts (Just [1, 3]) ["yes", "", "no", ""])]
        isMissingS = F.isNothing (F.col @(Maybe T.Text) "s")
        conds = missingnessConditions df
    assertEqual "one missingness cond for nullable PackedText" 1 (length conds)
    assertBool "cond is isNothing s" (eqExpr (head conds) isMissingS)
    assertBool
        "no missingness cond for non-nullable PackedText"
        ( null
            ( missingnessConditions
                (D.fromNamedColumns [("t", packedFromTexts Nothing ["a", "b"])])
            )
        )
    case filter (eqExpr isMissingS . cvExpr) (numericCondVecs defaultTreeConfig df df) of
        (cv : _) ->
            assertEqual
                "isNothing vector marks null slots"
                (VU.fromList [False, True, False, True])
                (cvVec cv)
        [] -> assertFailure "no isNothing CondVec for PackedText column"

probsFromIndicesBasic :: Test
probsFromIndicesBasic = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A", "B"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0] :: [Double]))
                ]
        probs = probsFromIndices @T.Text "label" df (V.fromList [0, 1, 2])
    assertBool "A prob ≈ 2/3" (abs (probs M.! "A" - 2 / 3) < 1e-9)
    assertBool "B prob ≈ 1/3" (abs (probs M.! "B" - 1 / 3) < 1e-9)

-- probsFromIndices: only a subset of rows counted
probsFromIndicesSubset :: Test
probsFromIndicesSubset = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A", "B", "B"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0, 4.0] :: [Double]))
                ]
        probs = probsFromIndices @T.Text "label" df (V.fromList [0, 1])
    assertEqual "only rows 0,1 → A:1.0" (M.fromList [("A", 1.0)]) probs

-- probsFromIndices: single class → probability 1.0
probsFromIndicesSingleClass :: Test
probsFromIndicesSingleClass = TestCase $ do
    let probs = probsFromIndices @T.Text "label" fixtureDF (V.fromList [0, 2])
    assertEqual "rows 0,2 both A → A:1.0" (M.fromList [("A", 1.0)]) probs

-- buildProbTree: Leaf preserves distribution
buildProbTreeLeaf :: Test
buildProbTreeLeaf = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("label", DI.fromList (["A", "A", "A"] :: [T.Text]))
                , ("x", DI.fromList ([1.0, 2.0, 3.0] :: [Double]))
                ]
        pt = buildProbTree @T.Text (Leaf "A") "label" df (V.fromList [0, 1, 2])
    case pt of
        Leaf m -> assertEqual "pure-A leaf → {A:1.0}" (M.fromList [("A", 1.0)]) m
        _ -> assertFailure "expected Leaf"

-- buildProbTree: Branch distributes rows to left/right leaves correctly
buildProbTreeBranch :: Test
buildProbTreeBranch = TestCase $ do
    let stump = Branch splitCond (Leaf "A") (Leaf "B") :: Tree T.Text
        pt = buildProbTree @T.Text stump "label" fixtureDF allIndices
    case pt of
        Branch _ (Leaf lm) (Leaf rm) -> do
            assertBool "left leaf has A:0.5" (abs (M.findWithDefault 0 "A" lm - 0.5) < 1e-9)
            assertBool "left leaf has B:0.5" (abs (M.findWithDefault 0 "B" lm - 0.5) < 1e-9)
            assertBool
                "right leaf has A:0.5"
                (abs (M.findWithDefault 0 "A" rm - 0.5) < 1e-9)
            assertBool
                "right leaf has C:0.5"
                (abs (M.findWithDefault 0 "C" rm - 0.5) < 1e-9)
        _ -> assertFailure "expected Branch with two Leaves"

-- probExprs: leaf tree produces Lit values
probExprsLeaf :: Test
probExprsLeaf = TestCase $ do
    let pt = Leaf (M.fromList [("A", 0.75), ("B", 0.25)]) :: ProbTree T.Text
        pe = probExprs pt
    assertBool "A expr is Lit 0.75" (eqExpr (Lit 0.75) (pe M.! "A"))
    assertBool "B expr is Lit 0.25" (eqExpr (Lit 0.25) (pe M.! "B"))

-- probExprs: class absent from one leaf gets Lit 0.0 on that side
probExprsMissingClass :: Test
probExprsMissingClass = TestCase $ do
    let pt =
            Branch
                splitCond
                (Leaf (M.fromList [("A", 1.0)]))
                (Leaf (M.fromList [("B", 1.0)])) ::
                ProbTree T.Text
        pe = probExprs pt
    assertBool
        "A expr: If cond (Lit 1.0) (Lit 0.0)"
        (eqExpr (F.ifThenElse splitCond (Lit 1.0) (Lit 0.0)) (pe M.! "A"))
    assertBool
        "B expr: If cond (Lit 0.0) (Lit 1.0)"
        (eqExpr (F.ifThenElse splitCond (Lit 0.0) (Lit 1.0)) (pe M.! "B"))

-- probExprs: keys equal all classes that appear across any leaf
probExprsAllClasses :: Test
probExprsAllClasses = TestCase $ do
    let pt =
            Branch
                splitCond
                (Leaf (M.fromList [("A", 1.0)]))
                (Leaf (M.fromList [("B", 0.6), ("C", 0.4)])) ::
                ProbTree T.Text
        pe = probExprs pt
    assertEqual "three classes in result" (sort ["A", "B", "C"]) (sort (M.keys pe))

-- Probabilities sum to 1.0 at every row after applying probExprs
probsSumToOne :: Test
probsSumToOne = TestCase $ do
    let stump = Branch splitCond (Leaf "A") (Leaf "B") :: Tree T.Text
        pt = buildProbTree @T.Text stump "label" fixtureDF allIndices
        pe = probExprs pt
        sumExpr = foldl1 (.+) (M.elems pe)
    case interpret @Double fixtureDF sumExpr of
        Left e -> assertFailure (show e)
        Right (DI.TColumn sumCol) ->
            case DI.toVector @Double sumCol of
                Left e2 -> assertFailure (show e2)
                Right vals ->
                    mapM_
                        (\v -> assertBool ("sum ≈ 1.0, got " ++ show v) (abs (v - 1.0) < 1e-9))
                        (V.toList vals)

-- argmax of probExprs agrees with fitDecisionTree on sepDF
probArgmaxMatchesClassifier :: Test
probArgmaxMatchesClassifier = TestCase $ do
    let cfg = defaultTreeConfig{taoIterations = 5, expressionPairs = 4, minLeafSize = 1}
        hardExpr = fitDecisionTree @T.Text cfg (Col "label") sepDF
        pe = fitProbTree @T.Text cfg (Col "label") sepDF
        indices = [0 .. D.nRows sepDF - 1]
    case interpret @T.Text sepDF hardExpr of
        Left e -> assertFailure (show e)
        Right (DI.TColumn hardCol) ->
            case DI.toVector @T.Text hardCol of
                Left e2 -> assertFailure (show e2)
                Right hardVals -> do
                    probCols <-
                        mapM
                            ( \(cls, expr) -> case interpret @Double sepDF expr of
                                Left e3 -> assertFailure (show e3) >> return (cls, V.empty)
                                Right (DI.TColumn col2) -> case DI.toVector @Double col2 of
                                    Left e4 -> assertFailure (show e4) >> return (cls, V.empty)
                                    Right v -> return (cls, v)
                            )
                            (M.toList pe)
                    mapM_
                        ( \i ->
                            let argmax = fst $ maximumBy (compare `on` (V.! i) . snd) probCols
                                hard = hardVals V.! i
                             in assertEqual ("row " ++ show i) hard argmax
                        )
                        indices

taoRecoversNestedObliqueWithoutHint :: Test
taoRecoversNestedObliqueWithoutHint = TestCase $ do
    let labelExpr =
            F.ifThenElse
                ((F.col @Double "x" + F.col @Double "y") .<= F.lit (4.5 :: Double))
                (F.lit ("low" :: T.Text))
                ( F.ifThenElse
                    ((F.col @Double "x" - F.col @Double "y") .<= F.lit (0.5 :: Double))
                    (F.lit "mid")
                    (F.lit "high")
                )
        df = D.derive @T.Text "label" labelExpr gridBaseDF
        indices = V.enumFromN 0 16
        initTree =
            Branch
                (F.col @Double "x" .<= F.lit (1.5 :: Double))
                (Leaf "low")
                ( Branch
                    (F.col @Double "y" .<= F.lit (3.5 :: Double))
                    (Leaf "mid")
                    (Leaf "high")
                ) ::
                Tree T.Text
        axisOnlyConds =
            [F.col @Double "x" .<= F.lit (t :: Double) | t <- [1.5, 2.5, 3.5]]
                ++ [F.col @Double "y" .<= F.lit (t :: Double) | t <- [1.5, 2.5, 3.5]]
        cfg =
            defaultTreeConfig
                { taoIterations = 20
                , expressionPairs = 6
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                }
        result = taoOptimize @T.Text cfg "label" axisOnlyConds df indices initTree
        finalLoss = computeTreeLoss @T.Text "label" df indices result
    assertEqual
        "linear solver recovers nested oblique tree from axis-aligned-only pool"
        0.0
        finalLoss

taoMonotoneWithLinear :: Test
taoMonotoneWithLinear = TestCase $ do
    let indices = V.enumFromN 0 20
        cfg = defaultTreeConfig{taoIterations = 5, expressionPairs = 4, minLeafSize = 1}
        initLoss = computeTreeLoss @T.Text "label" sepDF indices wrongStump
        stepTree = taoIteration @T.Text cfg "label" sepConds sepDF indices
        step (tree, _) =
            let tree' = stepTree tree
             in (tree', computeTreeLoss @T.Text "label" sepDF indices tree')
        snapshots = take 6 $ iterate step (wrongStump, initLoss)
        losses = map snd snapshots
        pairs = zip losses (drop 1 losses)
    assertBool
        ("loss must be non-increasing across iterations (got " ++ show losses ++ ")")
        (all (\(a, b) -> b <= a + 1e-9) pairs)

taoLinearVsDiscreteCompetition :: Test
taoLinearVsDiscreteCompetition = TestCase $ do
    let indices = V.enumFromN 0 20
        cfg =
            defaultTreeConfig
                { taoIterations = 5
                , expressionPairs = 4
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                }
        result = taoOptimize @T.Text cfg "label" sepConds sepDF indices wrongStump
        finalLoss = computeTreeLoss @T.Text "label" sepDF indices result
    assertEqual
        "axis-aligned separable data should fit to zero loss"
        0.0
        finalLoss

taoLinearProducesSparsity :: Test
taoLinearProducesSparsity = TestCase $ do
    let n = 50 :: Int
        xs = [fromIntegral i / 10 - 2.5 :: Double | i <- [0 .. n - 1]]
        avals = xs
        bs = map (* 0.7) xs
        cs = [fromIntegral ((i * 7) `mod` 11) / 5 - 1 :: Double | i <- [0 .. n - 1]]
        ds = [fromIntegral ((i * 13) `mod` 7) / 3 - 1 :: Double | i <- [0 .. n - 1]]
        labels =
            [ if (avals !! i) + (bs !! i) > 0 then "pos" else "neg" :: T.Text
            | i <- [0 .. n - 1]
            ]
        df =
            D.fromNamedColumns
                [ ("label", DI.fromList labels)
                , ("a", DI.fromList avals)
                , ("b", DI.fromList bs)
                , ("c", DI.fromList cs)
                , ("d", DI.fromList ds)
                ]
        cfg =
            defaultTreeConfig
                { maxTreeDepth = 1
                , taoIterations = 10
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                , linearSolverConfig =
                    (linearSolverConfig defaultTreeConfig)
                        { DataFrame.LinearSolver.scL1Lambda = 0.05
                        }
                }
        result = fitDecisionTree @T.Text cfg (Col "label") df
        rootCols = getColumns result
    assertBool
        ( "informative columns 'a' or 'b' must appear in the fitted Expr (got "
            ++ show rootCols
            ++ ")"
        )
        ("a" `elem` rootCols || "b" `elem` rootCols)

taoLinearDoesNotUseTarget :: Test
taoLinearDoesNotUseTarget = TestCase $ do
    let n = 40 :: Int
        as = [fromIntegral (i `div` 4) :: Double | i <- [0 .. n - 1]]
        bs = [fromIntegral (i `mod` 3) :: Double | i <- [0 .. n - 1]]
        targets = [if even i then 1.0 else 0.0 :: Double | i <- [0 .. n - 1]]
        df =
            D.fromNamedColumns
                [ ("target", DI.fromList targets)
                , ("a", DI.fromList as)
                , ("b", DI.fromList bs)
                ]
        cfg =
            defaultTreeConfig
                { maxTreeDepth = 1
                , taoIterations = 10
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                }
        result = fitDecisionTree @Double cfg (Col "target") df
        rootCols = getColumns result
    assertBool
        ("target must not appear in the fitted Expr (got " ++ show result ++ ")")
        ("target" `notElem` rootCols)

taoLinearDeterministic :: Test
taoLinearDeterministic = TestCase $ do
    let cfg =
            defaultTreeConfig
                { taoIterations = 5
                , expressionPairs = 4
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 2
                }
        r1 = fitDecisionTree @T.Text cfg (Col "label") sepDF
        r2 = fitDecisionTree @T.Text cfg (Col "label") sepDF
    assertBool "fitDecisionTree is deterministic on the same input" (eqExpr r1 r2)

-- D1: One care point — solver must not crash; integration should fall back
-- gracefully (via minCarePointsForLinear) and rely on the discrete path.
taoLinearTinyCareSet :: Test
taoLinearTinyCareSet = TestCase $ do
    let cfg =
            defaultTreeConfig
                { taoIterations = 5
                , expressionPairs = 4
                , minLeafSize = 1
                , useLinearSolver = True
                , minCarePointsForLinear = 100
                }
        result = fitDecisionTree @T.Text cfg (Col "label") sepDF
        cfgOff = cfg{useLinearSolver = False}
        resultOff = fitDecisionTree @T.Text cfgOff (Col "label") sepDF
    assertBool
        "skipping linear solver yields same expression as linear-off baseline"
        (eqExpr result resultOff)

breimanBinaryDF :: D.DataFrame
breimanBinaryDF =
    let n = 100 :: Int
        mkLabel "a" = "neg"
        mkLabel "b" = "neg"
        mkLabel "c" = "pos"
        mkLabel "d" = "pos"
        mkLabel "e" = "pos"
        mkLabel _ = "neg"
        levels = cycle ["a", "b", "c", "d", "e"]
        feats = take n levels
        labs = map mkLabel feats
     in D.fromUnnamedColumns
            [ DI.fromList (map T.pack feats :: [T.Text])
            , DI.fromList (map T.pack labs :: [T.Text])
            ]
            |> D.rename "0" "feat"
            |> D.rename "1" "label"

testCategoricalBreimanBinary :: Test
testCategoricalBreimanBinary = TestCase $ do
    let ti = requireTargetInfo "label" breimanBinaryDF
        conds =
            discreteConditions @T.Text
                ti
                defaultTreeConfig
                (D.exclude ["label"] breimanBinaryDF)
        feat = "feat"
        feats = filter (\c -> feat `elem` getColumns c) conds
    assertEqual "Breiman emits k-1 prefixes" 4 (length feats)

testCategoricalSubsetsMulticlassLowCard :: Test
testCategoricalSubsetsMulticlassLowCard = TestCase $ do
    let n = 30 :: Int
        feats = take n (cycle ["x", "y", "z"])
        labs = take n (cycle ["A", "B", "C"])
        df =
            D.fromUnnamedColumns
                [ DI.fromList (map T.pack feats :: [T.Text])
                , DI.fromList (map T.pack labs :: [T.Text])
                ]
                |> D.rename "0" "feat"
                |> D.rename "1" "label"
        ti = requireTargetInfo "label" df
        conds = discreteConditions @T.Text ti defaultTreeConfig (D.exclude ["label"] df)
        feat = "feat"
        feats' = filter (\c -> feat `elem` getColumns c) conds
    assertEqual "subsets at low cardinality" 6 (length feats')

testCategoricalSingletonsMulticlassHighCard :: Test
testCategoricalSingletonsMulticlassHighCard = TestCase $ do
    let n = 60 :: Int
        feats = take n (cycle ["a", "b", "c", "d", "e", "f"])
        labs = take n (cycle ["A", "B", "C"])
        df =
            D.fromUnnamedColumns
                [ DI.fromList (map T.pack feats :: [T.Text])
                , DI.fromList (map T.pack labs :: [T.Text])
                ]
                |> D.rename "0" "feat"
                |> D.rename "1" "label"
        ti = requireTargetInfo "label" df
        conds = discreteConditions @T.Text ti defaultTreeConfig (D.exclude ["label"] df)
        feat = "feat"
        feats' = filter (\c -> feat `elem` getColumns c) conds
    assertEqual "singletons at high cardinality" 6 (length feats')

testCategoricalCardZero :: Test
testCategoricalCardZero = TestCase $ do
    let df =
            D.fromUnnamedColumns
                [ DI.fromList ([] :: [T.Text])
                , DI.fromList ([] :: [T.Text])
                ]
                |> D.rename "0" "feat"
                |> D.rename "1" "label"
        ti = requireTargetInfo "label" df
        conds = discreteConditions @T.Text ti defaultTreeConfig (D.exclude ["label"] df)
        feat = "feat"
        feats' = filter (\c -> feat `elem` getColumns c) conds
    assertEqual "no candidates on empty column" 0 (length feats')

testCategoricalNullableBinary :: Test
testCategoricalNullableBinary = TestCase $ do
    let feats =
            [ Just "a"
            , Just "b"
            , Just "c"
            , Nothing
            , Just "a"
            , Just "b"
            , Just "c"
            , Nothing
            , Just "a"
            , Just "b"
            , Just "c"
            , Just "a"
            , Just "b"
            , Just "c"
            , Just "a"
            , Just "b"
            ]
        labs =
            [ "neg"
            , "neg"
            , "pos"
            , "neg"
            , "neg"
            , "neg"
            , "pos"
            , "neg"
            , "neg"
            , "neg"
            , "pos"
            , "neg"
            , "neg"
            , "pos"
            , "neg"
            , "pos"
            ]
        df =
            D.fromUnnamedColumns
                [ DI.fromList (feats :: [Maybe T.Text])
                , DI.fromList (map T.pack labs :: [T.Text])
                ]
                |> D.rename "0" "feat"
                |> D.rename "1" "label"
        ti = requireTargetInfo "label" df
        conds = discreteConditions @T.Text ti defaultTreeConfig (D.exclude ["label"] df)
        feat = "feat" :: T.Text
        feats' = filter (\c -> feat `elem` getColumns c) conds
    assertEqual "Breiman prefixes on nullable column ignore nulls" 2 (length feats')

-- A small synthetic DataFrame to materialize CondVecs against.
threshFixtureDF :: D.DataFrame
threshFixtureDF =
    D.fromNamedColumns
        [ ("x", DI.fromList ([0.0, 1.0, 2.0, 3.0, 4.0, 5.0] :: [Double]))
        , ("y", DI.fromList ([5.0, 4.0, 3.0, 2.0, 1.0, 0.0] :: [Double]))
        ]

materializeOrFail :: Expr Bool -> CondVec
materializeOrFail e = case materializeCondVec threshFixtureDF e of
    Just cv -> cv
    Nothing -> error "materializeOrFail: condition could not be materialized"

-- | Helper: assert that two `Expr Bool`s agree by 'eqExpr'.
assertEqExpr :: String -> Expr Bool -> Expr Bool -> Assertion
assertEqExpr msg expected actual =
    assertBool
        (msg ++ "\n  expected: " ++ show expected ++ "\n  actual:   " ++ show actual)
        (eqExpr expected actual)

-- Eight positive cases.

threshAndLeq :: Test
threshAndLeq = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .<=. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .<=. F.lit (1.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "AND of x≤3 and x≤1 collapses to x≤1"
        (F.col @Double "x" .<=. F.lit (1.0 :: Double))
        (cvExpr r)

threshOrLeq :: Test
threshOrLeq = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .<=. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .<=. F.lit (1.0 :: Double))
        r = combineOrVec a b
    assertEqExpr
        "OR of x≤3 and x≤1 collapses to x≤3"
        (F.col @Double "x" .<=. F.lit (3.0 :: Double))
        (cvExpr r)

threshAndLt :: Test
threshAndLt = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .<. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .<. F.lit (1.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "AND of x<3 and x<1 collapses to x<1"
        (F.col @Double "x" .<. F.lit (1.0 :: Double))
        (cvExpr r)

threshOrLt :: Test
threshOrLt = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .<. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .<. F.lit (1.0 :: Double))
        r = combineOrVec a b
    assertEqExpr
        "OR of x<3 and x<1 collapses to x<3"
        (F.col @Double "x" .<. F.lit (3.0 :: Double))
        (cvExpr r)

threshAndGeq :: Test
threshAndGeq = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>=. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>=. F.lit (3.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "AND of x≥1 and x≥3 collapses to x≥3"
        (F.col @Double "x" .>=. F.lit (3.0 :: Double))
        (cvExpr r)

threshOrGeq :: Test
threshOrGeq = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>=. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>=. F.lit (3.0 :: Double))
        r = combineOrVec a b
    assertEqExpr
        "OR of x≥1 and x≥3 collapses to x≥1"
        (F.col @Double "x" .>=. F.lit (1.0 :: Double))
        (cvExpr r)

threshAndGt :: Test
threshAndGt = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>. F.lit (3.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "AND of x>1 and x>3 collapses to x>3"
        (F.col @Double "x" .>. F.lit (3.0 :: Double))
        (cvExpr r)

threshOrGt :: Test
threshOrGt = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>. F.lit (3.0 :: Double))
        r = combineOrVec a b
    assertEqExpr
        "OR of x>1 and x>3 collapses to x>1"
        (F.col @Double "x" .>. F.lit (1.0 :: Double))
        (cvExpr r)

-- Six negative cases: rewrite must NOT fire.

threshNegMixedDirection :: Test
threshNegMixedDirection = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .<. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>=. F.lit (1.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "mixed-direction AND keeps generic F.and form"
        (F.and (cvExpr a) (cvExpr b))
        (cvExpr r)

threshNegCrossColumn :: Test
threshNegCrossColumn = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "y" .>. F.lit (3.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "cross-column AND keeps generic F.and form"
        (F.and (cvExpr a) (cvExpr b))
        (cvExpr r)

threshNegMixedOpFamily :: Test
threshNegMixedOpFamily = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .<. F.lit (4.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "different-op-family AND keeps generic F.and form"
        (F.and (cvExpr a) (cvExpr b))
        (cvExpr r)

threshNegEqualityOp :: Test
threshNegEqualityOp = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .==. F.lit (3.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .==. F.lit (1.0 :: Double))
        r = combineOrVec a b
    assertEqExpr
        "equality OR keeps generic F.or form"
        (F.or (cvExpr a) (cvExpr b))
        (cvExpr r)

threshNegLitOnLeft :: Test
threshNegLitOnLeft = TestCase $ do
    let a = materializeOrFail (F.lit (1.0 :: Double) .<. F.col @Double "x")
        b = materializeOrFail (F.lit (3.0 :: Double) .<. F.col @Double "x")
        r = combineAndVec a b
    assertEqExpr
        "Lit-on-left AND keeps generic F.and form"
        (F.and (cvExpr a) (cvExpr b))
        (cvExpr r)

threshNegNonLiteralRhs :: Test
threshNegNonLiteralRhs = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.col @Double "y")
        b = materializeOrFail (F.col @Double "x" .>. F.lit (3.0 :: Double))
        r = combineAndVec a b
    assertEqExpr
        "non-literal RHS AND keeps generic F.and form"
        (F.and (cvExpr a) (cvExpr b))
        (cvExpr r)

-- Semantic-preservation spot check: the consolidated cvVec matches the
-- elementwise AND/OR of the inputs at every row of a synthetic DataFrame.
threshSemanticPreservation :: Test
threshSemanticPreservation = TestCase $ do
    let a = materializeOrFail (F.col @Double "x" .>. F.lit (1.0 :: Double))
        b = materializeOrFail (F.col @Double "x" .>. F.lit (3.0 :: Double))
        rAnd = combineAndVec a b
        rOr = combineOrVec a b
        expectedAnd = VU.zipWith (&&) (cvVec a) (cvVec b)
        expectedOr = VU.zipWith (||) (cvVec a) (cvVec b)
    assertEqual
        "consolidated AND vec matches elementwise &&"
        expectedAnd
        (cvVec rAnd)
    assertEqual
        "consolidated OR vec matches elementwise ||"
        expectedOr
        (cvVec rOr)

------------------------------------------------------------------------
-- Test list
------------------------------------------------------------------------

tests :: [Test]
tests =
    [ TestLabel "carePointsBothWrong" carePointsBothWrong
    , TestLabel "carePointsLeftCorrect" carePointsLeftCorrect
    , TestLabel "carePointsRightCorrect" carePointsRightCorrect
    , TestLabel "carePointsMixed" carePointsMixed
    , TestLabel "carePointsBothCorrect" carePointsBothCorrect
    , TestLabel "majorityVoteTest" majorityVoteTest
    , TestLabel "majorityVoteSubset" majorityVoteSubset
    , TestLabel "computeLossZero" computeLossZero
    , TestLabel "computeLossHalf" computeLossHalf
    , TestLabel "partitionDisjoint" partitionDisjoint
    , TestLabel "partitionUnion" partitionUnion
    , TestLabel "countErrorsAllCorrect" countErrorsAllCorrect
    , TestLabel "countErrorsAllWrong" countErrorsAllWrong
    , TestLabel "predictLeaf" predictLeaf
    , TestLabel "predictBranch" predictBranch
    , TestLabel "taoNoDegradation" taoNoDegradation
    , TestLabel "taoMonotone" taoMonotone
    , TestLabel "taoConvergesPureLabels" taoConvergesPureLabels
    , TestLabel "taoDeadBranchNoCrash" taoDeadBranchNoCrash
    , TestLabel "taoRecoversSingleObliqueDerived" taoRecoversSingleObliqueDerived
    , TestLabel "taoRecoversNestedObliqueDerived" taoRecoversNestedObliqueDerived
    , TestLabel
        "C2a taoAxisAlignedInsufficientForObliqueDiscreteOnly"
        taoAxisAlignedInsufficientForObliqueDiscreteOnly
    , TestLabel
        "C2b taoLinearRecoversObliqueFromAxisAlignedPool"
        taoLinearRecoversObliqueFromAxisAlignedPool
    , TestLabel "numericColsNullableDouble" numericColsNullableDoubleTest
    , TestLabel "numericColsNullableInt" numericColsNullableIntTest
    , TestLabel "numericCondsNullableNonEmpty" numericCondsNullableNonEmptyTest
    , TestLabel "nullValueRoutesFalse" nullValueRoutesFalseTest
    , TestLabel "nullableFitZeroLoss" nullableFitZeroLossTest
    , TestLabel "nullableFitWithNullsNoCrash" nullableFitWithNullsNoCrashTest
    , TestLabel "numericExprsWithTermsMixed" numericExprsWithTermsMixedTest
    , TestLabel "G1 missingnessConds" missingnessCondsTest
    , TestLabel "G2 poolContainsMissingness" poolContainsMissingnessTest
    , TestLabel "G3 missingnessCondVec" missingnessCondVecTest
    , TestLabel "G4 observedOnlyThresholds" observedOnlyThresholdsTest
    , TestLabel "G5 packedMissingness" packedMissingnessTest
    , TestLabel "probsFromIndicesBasic" probsFromIndicesBasic
    , TestLabel "probsFromIndicesSubset" probsFromIndicesSubset
    , TestLabel "probsFromIndicesSingleClass" probsFromIndicesSingleClass
    , TestLabel "buildProbTreeLeaf" buildProbTreeLeaf
    , TestLabel "buildProbTreeBranch" buildProbTreeBranch
    , TestLabel "probExprsLeaf" probExprsLeaf
    , TestLabel "probExprsMissingClass" probExprsMissingClass
    , TestLabel "probExprsAllClasses" probExprsAllClasses
    , TestLabel "probsSumToOne" probsSumToOne
    , TestLabel "probArgmaxMatchesClassifier" probArgmaxMatchesClassifier
    , TestLabel
        "C4 taoRecoversNestedObliqueWithoutHint"
        taoRecoversNestedObliqueWithoutHint
    , TestLabel "C5 taoMonotoneWithLinear" taoMonotoneWithLinear
    , TestLabel "C6 taoLinearVsDiscreteCompetition" taoLinearVsDiscreteCompetition
    , TestLabel "C8 taoLinearProducesSparsity" taoLinearProducesSparsity
    , TestLabel "C8b taoLinearDoesNotUseTarget" taoLinearDoesNotUseTarget
    , TestLabel "C9 taoLinearDeterministic" taoLinearDeterministic
    , TestLabel "D1 taoLinearTinyCareSet" taoLinearTinyCareSet
    , TestLabel "E1 categoricalBreimanBinary" testCategoricalBreimanBinary
    , TestLabel
        "E2 categoricalSubsetsMulticlassLowCard"
        testCategoricalSubsetsMulticlassLowCard
    , TestLabel
        "E3 categoricalSingletonsMulticlassHighCard"
        testCategoricalSingletonsMulticlassHighCard
    , TestLabel "E4 categoricalCardZero" testCategoricalCardZero
    , TestLabel "E5 categoricalNullableBinary" testCategoricalNullableBinary
    , -- PR 2 extended: threshold-consolidation rewrite (positive cases).
      TestLabel "F1 threshAndLeq" threshAndLeq
    , TestLabel "F2 threshOrLeq" threshOrLeq
    , TestLabel "F3 threshAndLt" threshAndLt
    , TestLabel "F4 threshOrLt" threshOrLt
    , TestLabel "F5 threshAndGeq" threshAndGeq
    , TestLabel "F6 threshOrGeq" threshOrGeq
    , TestLabel "F7 threshAndGt" threshAndGt
    , TestLabel "F8 threshOrGt" threshOrGt
    , -- PR 2 extended: negative cases (rewrite must NOT fire).
      TestLabel "F9 threshNegMixedDirection" threshNegMixedDirection
    , TestLabel "F10 threshNegCrossColumn" threshNegCrossColumn
    , TestLabel "F11 threshNegMixedOpFamily" threshNegMixedOpFamily
    , TestLabel "F12 threshNegEqualityOp" threshNegEqualityOp
    , TestLabel "F13 threshNegLitOnLeft" threshNegLitOnLeft
    , TestLabel "F14 threshNegNonLiteralRhs" threshNegNonLiteralRhs
    , TestLabel "F15 threshSemanticPreservation" threshSemanticPreservation
    ]
