{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.DecisionTree where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), eSize, getColumns)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Statistics (percentile', percentileOrd')
import DataFrame.Internal.Types
import DataFrame.Operations.Core (columnNames, nRows)
import DataFrame.Operations.Subset (exclude, filterWhere)

import Control.Exception (throw)
import Control.Monad (guard)
import Data.Containers.ListUtils (nubOrd)
import Data.Function (on)
import Data.List (foldl', maximumBy, minimumBy, sort, sortBy)
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

import DataFrame.Functions ((./=), (.<), (.<=), (.==), (.>), (.>=))

data TreeConfig = TreeConfig
    { maxTreeDepth :: Int
    , minSamplesSplit :: Int
    , minLeafSize :: Int
    , percentiles :: [Int]
    , expressionPairs :: Int
    , synthConfig :: SynthConfig
    , taoIterations :: Int
    , taoConvergenceTol :: Double
    }
    deriving (Eq, Show)

data SynthConfig = SynthConfig
    { maxExprDepth :: Int
    , boolExpansion :: Int
    , disallowedCombinations :: [(T.Text, T.Text)]
    , complexityPenalty :: Double
    , enableStringOps :: Bool
    , enableCrossCols :: Bool
    , enableArithOps :: Bool
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
        }

data Tree a
    = Leaf !a
    | Branch !(Expr Bool) !(Tree a) !(Tree a)
    deriving (Eq, Show)

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
    (Columnable a) =>
    TreeConfig ->
    Expr a ->
    DataFrame ->
    Expr a
fitDecisionTree cfg (Col target) df =
    let
        conds =
            nubOrd $
                numericConditions cfg (exclude [target] df)
                    ++ generateConditionsOld cfg (exclude [target] df)

        initialTree = buildGreedyTree @a cfg (maxTreeDepth cfg) target conds df

        indices = V.enumFromN 0 (nRows df)

        optimizedTree = taoOptimize @a cfg target conds df indices initialTree
     in
        pruneExpr (treeToExpr optimizedTree)
fitDecisionTree _ expr _ = error $ "Cannot create tree for compound expression: " ++ show expr

taoOptimize ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text -> -- Target column name
    [Expr Bool] -> -- Candidate conditions
    DataFrame -> -- Full dataset
    V.Vector Int -> -- Indices of points reaching the root
    Tree a -> -- Current tree
    Tree a
taoOptimize cfg target conds df rootIndices initialTree =
    go 0 initialTree (computeTreeLoss @a target df rootIndices initialTree)
  where
    go :: Int -> Tree a -> Double -> Tree a
    go iter tree prevLoss
        | iter >= taoIterations cfg = pruneDead tree
        | otherwise =
            let
                tree' = taoIteration @a cfg target conds df rootIndices tree

                newLoss = computeTreeLoss @a target df rootIndices tree'
                improvement = prevLoss - newLoss
             in
                if improvement < taoConvergenceTol cfg
                    then pruneDead tree'
                    else go (iter + 1) tree' newLoss

taoIteration ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
taoIteration cfg target conds df rootIndices tree =
    let depth = treeDepth tree
     in foldl'
            (optimizeDepthLevel @a cfg target conds df rootIndices)
            tree
            [depth, depth - 1 .. 0] -- Bottom to top

optimizeDepthLevel ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Int -> -- Target depth
    Tree a
optimizeDepthLevel cfg target conds df rootIndices tree = optimizeAtDepth @a cfg target conds df rootIndices tree 0

optimizeAtDepth ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Int ->
    Int ->
    Tree a
optimizeAtDepth cfg target conds df indices tree currentDepth targetDepth
    | currentDepth == targetDepth =
        optimizeNode @a cfg target conds df indices tree
    | otherwise = case tree of
        Leaf v -> Leaf v
        Branch cond left right ->
            let
                (indicesL, indicesR) = partitionIndices cond df indices
                left' =
                    optimizeAtDepth @a
                        cfg
                        target
                        conds
                        df
                        indicesL
                        left
                        (currentDepth + 1)
                        targetDepth
                right' =
                    optimizeAtDepth @a
                        cfg
                        target
                        conds
                        df
                        indicesR
                        right
                        (currentDepth + 1)
                        targetDepth
             in
                Branch cond left' right'

optimizeNode ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a ->
    Tree a
optimizeNode cfg target conds df indices tree
    | V.null indices = tree
    | otherwise = case tree of
        Leaf _ -> Leaf (majorityValueFromIndices @a target df indices)
        Branch oldCond left right ->
            let
                newCond = findBestSplitTAO @a cfg target conds df indices left right oldCond

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
    [Expr Bool] ->
    DataFrame ->
    V.Vector Int ->
    Tree a -> -- Left subtree (FIXED)
    Tree a -> -- Right subtree (FIXED)
    Expr Bool -> -- Current condition (fallback)
    Expr Bool
findBestSplitTAO cfg target conds df indices leftTree rightTree currentCond
    | V.null indices = currentCond
    | null validConds = currentCond
    | otherwise =
        let
            carePoints = identifyCarePoints @a target df indices leftTree rightTree
         in
            if null carePoints
                then currentCond
                else
                    let
                        evalSplit :: Expr Bool -> Int
                        evalSplit cond = countCarePointErrors cond df carePoints

                        evalWithPenalty c =
                            let errors = evalSplit c
                                penalty =
                                    floor
                                        ( complexityPenalty (synthConfig cfg)
                                            * fromIntegral (eSize c)
                                        )
                             in errors + penalty

                        sortedConds =
                            take (expressionPairs cfg) $
                                sortBy (compare `on` evalWithPenalty) validConds

                        expandedConds =
                            boolExprs
                                df
                                sortedConds
                                sortedConds
                                0
                                (boolExpansion (synthConfig cfg))
                     in
                        if null expandedConds
                            then currentCond
                            else minimumBy (compare `on` evalWithPenalty) expandedConds
  where
    validConds = filter isValidSplit conds
    isValidSplit c =
        let (t, f) = partitionIndices c df indices
         in V.length t >= minLeafSize cfg && V.length f >= minLeafSize cfg

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
        Right (TColumn col) ->
            case toVector @a col of
                Left _ -> []
                Right targetVals ->
                    V.toList $ V.mapMaybe (checkPoint targetVals) indices
  where
    checkPoint :: V.Vector a -> Int -> Maybe CarePoint
    checkPoint targetVals idx =
        let
            trueLabel = targetVals V.! idx
            leftPred = predictWithTree @a target df idx leftTree
            rightPred = predictWithTree @a target df idx rightTree
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
predictWithTree target df idx (Leaf v) = v
predictWithTree target df idx (Branch cond left right) =
    case interpret @Bool df cond of
        Left _ -> predictWithTree @a target df idx left -- Default to left on error
        Right (TColumn col) ->
            case toVector @Bool col of
                Left _ -> predictWithTree @a target df idx left
                Right boolVals ->
                    if boolVals V.! idx
                        then predictWithTree @a target df idx left
                        else predictWithTree @a target df idx right

countCarePointErrors :: Expr Bool -> DataFrame -> [CarePoint] -> Int
countCarePointErrors cond df carePoints =
    case interpret @Bool df cond of
        Left _ -> length carePoints
        Right (TColumn col) ->
            case toVector @Bool col of
                Left _ -> length carePoints
                Right boolVals ->
                    length $ filter (isMisclassified boolVals) carePoints
  where
    isMisclassified :: V.Vector Bool -> CarePoint -> Bool
    isMisclassified boolVals cp =
        let goesLeft = boolVals V.! cpIndex cp
            shouldGoLeft = cpCorrectDir cp == GoLeft
         in goesLeft /= shouldGoLeft

partitionIndices ::
    Expr Bool -> DataFrame -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionIndices cond df indices =
    case interpret @Bool df cond of
        Left _ -> (indices, V.empty)
        Right (TColumn col) ->
            case toVector @Bool col of
                Left _ -> (indices, V.empty)
                Right boolVals ->
                    V.partition (boolVals V.!) indices

majorityValueFromIndices ::
    forall a.
    (Columnable a) =>
    T.Text ->
    DataFrame ->
    V.Vector Int ->
    a
majorityValueFromIndices target df indices =
    case interpret @a df (Col target) of
        Left e -> throw e
        Right (TColumn col) ->
            case toVector @a col of
                Left e -> throw e
                Right vals ->
                    let counts =
                            V.foldl'
                                (\acc i -> M.insertWith (+) (vals V.! i) 1 acc)
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
            Right (TColumn col) ->
                case toVector @a col of
                    Left _ -> 1.0
                    Right targetVals ->
                        let
                            n = V.length indices
                            errors =
                                V.length $
                                    V.filter
                                        (\i -> targetVals V.! i /= predictWithTree @a target df i tree)
                                        indices
                         in
                            fromIntegral errors / fromIntegral n

pruneDead :: Tree a -> Tree a
pruneDead (Leaf v) = Leaf v
pruneDead (Branch cond left right) =
    let
        left' = pruneDead left
        right' = pruneDead right
     in
        Branch cond left' right'

pruneExpr :: forall a. (Columnable a, Eq a) => Expr a -> Expr a
pruneExpr (If cond trueBranch falseBranch) =
    let t = pruneExpr trueBranch
        f = pruneExpr falseBranch
     in if t == f
            then t
            else case (t, f) of
                (If condInner tInner _, _) | cond == condInner -> If cond tInner f
                (_, If condInner _ fInner) | cond == condInner -> If cond t fInner
                _ -> If cond t f
pruneExpr (Unary op e) = Unary op (pruneExpr e)
pruneExpr (Binary op l r) = Binary op (pruneExpr l) (pruneExpr r)
pruneExpr e = e

buildGreedyTree ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    Int ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    Tree a
buildGreedyTree cfg depth target conds df
    | depth <= 0 || nRows df <= minSamplesSplit cfg =
        Leaf (majorityValue @a target df)
    | otherwise =
        case findBestGreedySplit @a cfg target conds df of
            Nothing -> Leaf (majorityValue @a target df)
            Just bestCond ->
                let (dfTrue, dfFalse) = partitionDataFrame bestCond df
                 in if nRows dfTrue < minLeafSize cfg || nRows dfFalse < minLeafSize cfg
                        then Leaf (majorityValue @a target df)
                        else
                            Branch
                                bestCond
                                (buildGreedyTree @a cfg (depth - 1) target conds dfTrue)
                                (buildGreedyTree @a cfg (depth - 1) target conds dfFalse)

findBestGreedySplit ::
    forall a.
    (Columnable a) =>
    TreeConfig -> T.Text -> [Expr Bool] -> DataFrame -> Maybe (Expr Bool)
findBestGreedySplit cfg target conds df =
    let
        initialImpurity = calculateGini @a target df
        calculateComplexity c = complexityPenalty (synthConfig cfg) * fromIntegral (eSize c)

        evalGain :: Expr Bool -> (Double, Int)
        evalGain cond =
            let (t, f) = partitionDataFrame cond df
                n = fromIntegral @Int @Double (nRows df)
                weightT = fromIntegral @Int @Double (nRows t) / n
                weightF = fromIntegral @Int @Double (nRows f) / n
                newImpurity =
                    weightT * calculateGini @a target t
                        + weightF * calculateGini @a target f
             in ( (initialImpurity - newImpurity) - calculateComplexity cond
                , negate (eSize cond)
                )

        validConds =
            filter
                ( \c ->
                    let (t, f) = partitionDataFrame c df
                     in nRows t >= minLeafSize cfg && nRows f >= minLeafSize cfg
                )
                conds

        sortedConditions =
            map fst $
                take
                    (expressionPairs cfg)
                    ( filter
                        (\(c, v) -> ((> negate (calculateComplexity c)) . fst) v)
                        (sortBy (flip compare `on` snd) (map (\c -> (c, evalGain c)) validConds))
                    )
     in
        if null sortedConditions
            then Nothing
            else
                Just $
                    maximumBy
                        (compare `on` evalGain)
                        ( boolExprs
                            df
                            sortedConditions
                            sortedConditions
                            0
                            (boolExpansion (synthConfig cfg))
                        )

numericConditions :: TreeConfig -> DataFrame -> [Expr Bool]
numericConditions = generateNumericConds

generateNumericConds :: TreeConfig -> DataFrame -> [Expr Bool]
generateNumericConds cfg df = do
    expr <- numericExprsWithTerms (synthConfig cfg) df
    let thresholds = map (\p -> percentile p expr df) (percentiles cfg)
    threshold <- thresholds
    [ expr .<= F.lit threshold
        , expr .>= F.lit threshold
        , expr .< F.lit threshold
        , expr .> F.lit threshold
        ]

numericExprsWithTerms :: SynthConfig -> DataFrame -> [Expr Double]
numericExprsWithTerms cfg df =
    concatMap (numericExprs cfg df [] 0) [0 .. maxExprDepth cfg]

numericCols :: DataFrame -> [Expr Double]
numericCols df = concatMap extract (columnNames df)
  where
    extract col = case unsafeGetColumn col df of
        UnboxedColumn (_ :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [Col col]
                Nothing -> case sIntegral @b of
                    STrue -> [F.toDouble (Col @b col)]
                    SFalse -> []
        _ -> []

numericExprs ::
    SynthConfig -> DataFrame -> [Expr Double] -> Int -> Int -> [Expr Double]
numericExprs cfg df prevExprs depth maxDepth
    | depth == 0 = baseExprs ++ numericExprs cfg df baseExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise =
        combinedExprs ++ numericExprs cfg df combinedExprs (depth + 1) maxDepth
  where
    baseExprs = numericCols df
    combinedExprs
        | not (enableArithOps cfg) = []
        | otherwise = do
            e1 <- prevExprs
            e2 <- baseExprs
            let cols = getColumns e1 <> getColumns e2
            guard
                ( e1 /= e2
                    && not
                        ( any
                            (\(l, r) -> l `elem` cols && r `elem` cols)
                            (disallowedCombinations cfg)
                        )
                )
            [e1 + e2, e1 - e2, e1 * e2, F.ifThenElse (e2 ./= 0) (e1 / e2) 0]

boolExprs ::
    DataFrame -> [Expr Bool] -> [Expr Bool] -> Int -> Int -> [Expr Bool]
boolExprs df baseExprs prevExprs depth maxDepth
    | depth == 0 =
        baseExprs ++ boolExprs df baseExprs prevExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise =
        combinedExprs ++ boolExprs df baseExprs combinedExprs (depth + 1) maxDepth
  where
    combinedExprs = do
        e1 <- prevExprs
        e2 <- baseExprs
        guard (e1 /= e2)
        [F.and e1 e2, F.or e1 e2]

generateConditionsOld :: TreeConfig -> DataFrame -> [Expr Bool]
generateConditionsOld cfg df =
    let
        genConds :: T.Text -> [Expr Bool]
        genConds colName = case unsafeGetColumn colName df of
            (BoxedColumn (col :: V.Vector a)) ->
                let ps = map (Lit . (`percentileOrd'` col)) [1, 25, 75, 99]
                 in map (Col @a colName .==) ps
            (OptionalColumn (col :: V.Vector (Maybe a))) -> case sFloating @a of
                STrue ->
                    let doubleCol =
                            VU.convert
                                (V.map fromJust (V.filter isJust (V.map (fmap (realToFrac @a @Double)) col)))
                     in zipWith
                            ($)
                            [ (Col @(Maybe a) colName .==)
                            , (Col @(Maybe a) colName .<=)
                            , (Col @(Maybe a) colName .>=)
                            ]
                            ( Lit Nothing
                                : map
                                    (Lit . Just . realToFrac . (`percentile'` doubleCol))
                                    (percentiles cfg)
                            )
                SFalse -> case sIntegral @a of
                    STrue ->
                        let doubleCol =
                                VU.convert
                                    (V.map fromJust (V.filter isJust (V.map (fmap (fromIntegral @a @Double)) col)))
                         in zipWith
                                ($)
                                [ (Col @(Maybe a) colName .==)
                                , (Col @(Maybe a) colName .<=)
                                , (Col @(Maybe a) colName .>=)
                                ]
                                ( Lit Nothing
                                    : map
                                        (Lit . Just . round . (`percentile'` doubleCol))
                                        (percentiles cfg)
                                )
                    SFalse ->
                        map
                            ((Col @(Maybe a) colName .==) . Lit . (`percentileOrd'` col))
                            [1, 25, 75, 99]
            (UnboxedColumn (_ :: VU.Vector a)) -> []

        columnConds =
            concatMap
                colConds
                [ (l, r)
                | l <- columnNames df
                , r <- columnNames df
                , not
                    ( any
                        (\(l', r') -> sort [l', r'] == sort [l, r])
                        (disallowedCombinations (synthConfig cfg))
                    )
                ]
          where
            colConds (!l, !r) = case (unsafeGetColumn l df, unsafeGetColumn r df) of
                (BoxedColumn (col1 :: V.Vector a), BoxedColumn (_ :: V.Vector b)) ->
                    case testEquality (typeRep @a) (typeRep @b) of
                        Nothing -> []
                        Just Refl -> [Col @a l .== Col @a r]
                (UnboxedColumn (_ :: VU.Vector a), UnboxedColumn (_ :: VU.Vector b)) -> []
                ( OptionalColumn (_ :: V.Vector (Maybe a))
                    , OptionalColumn (_ :: V.Vector (Maybe b))
                    ) -> case testEquality (typeRep @a) (typeRep @b) of
                        Nothing -> []
                        Just Refl -> case testEquality (typeRep @a) (typeRep @T.Text) of
                            Nothing -> [Col @(Maybe a) l .<= Col r, Col @(Maybe a) l .== Col r]
                            Just Refl -> [Col @(Maybe a) l .== Col r]
                _ -> []
     in
        concatMap genConds (columnNames df) ++ columnConds

partitionDataFrame :: Expr Bool -> DataFrame -> (DataFrame, DataFrame)
partitionDataFrame cond df = (filterWhere cond df, filterWhere (F.not cond) df)

calculateGini :: forall a. (Columnable a) => T.Text -> DataFrame -> Double
calculateGini target df =
    let n = fromIntegral $ nRows df
        counts = getCounts @a target df
        numClasses = fromIntegral $ M.size counts
        probs = map (\c -> (fromIntegral c + 1) / (n + numClasses)) (M.elems counts)
     in if n == 0 then 0 else 1 - sum (map (^ 2) probs)

majorityValue :: forall a. (Columnable a) => T.Text -> DataFrame -> a
majorityValue target df =
    let counts = getCounts @a target df
     in if M.null counts
            then error "Empty DataFrame in leaf"
            else fst $ maximumBy (compare `on` snd) (M.toList counts)

getCounts :: forall a. (Columnable a) => T.Text -> DataFrame -> M.Map a Int
getCounts target df =
    case interpret @a df (Col target) of
        Left e -> throw e
        Right (TColumn col) ->
            case toVector @a col of
                Left e -> throw e
                Right vals -> foldl' (\acc x -> M.insertWith (+) x 1 acc) M.empty (V.toList vals)

percentile :: Int -> Expr Double -> DataFrame -> Double
percentile p expr df =
    case interpret @Double df expr of
        Left _ -> 0
        Right (TColumn col) ->
            case toVector @Double col of
                Left _ -> 0
                Right vals ->
                    let sorted = V.fromList $ sort $ V.toList vals
                        n = V.length sorted
                        idx = min (n - 1) $ max 0 $ (p * n) `div` 100
                     in if n == 0 then 0 else sorted V.! idx

buildTree ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    Int ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    Expr a
buildTree cfg depth target conds df =
    let
        tree = buildGreedyTree @a cfg depth target conds df
        indices = V.enumFromN 0 (nRows df)
        optimized = taoOptimize @a cfg target conds df indices tree
     in
        pruneExpr (treeToExpr optimized)

findBestSplit ::
    forall a.
    (Columnable a) =>
    TreeConfig -> T.Text -> [Expr Bool] -> DataFrame -> Maybe (Expr Bool)
findBestSplit = findBestGreedySplit @a

pruneTree :: forall a. (Columnable a, Eq a) => Expr a -> Expr a
pruneTree = pruneExpr
