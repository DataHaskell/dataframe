{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Lazy.Internal.Optimizer (optimize) where

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Schema (Schema (..), elements)
import DataFrame.Lazy.Internal.LogicalPlan
import DataFrame.Lazy.Internal.PhysicalPlan

{- | Optimise a logical plan and lower it to a physical plan.

Rules applied bottom-up (in order):
  1. Filter fusion       — merge consecutive Filter nodes into a conjunction
  2. Predicate pushdown  — move Filter past Derive/Project toward Scan
  3. Dead column elim    — drop Derive nodes whose output is never referenced

After rule application @toPhysical@ selects concrete operators.
-}
optimize :: Int -> LogicalPlan -> PhysicalPlan
optimize batchSz =
    toPhysical batchSz
        . eliminateDeadColumns
        . pushPredicates
        . fuseFilters

-- ---------------------------------------------------------------------------
-- Rule 1: Filter fusion
-- ---------------------------------------------------------------------------

-- | Merge @Filter p1 (Filter p2 child)@ into @Filter (p1 && p2) child@.
fuseFilters :: LogicalPlan -> LogicalPlan
fuseFilters (Filter p1 (Filter p2 child)) =
    fuseFilters (Filter (andExpr p1 p2) (fuseFilters child))
fuseFilters (Filter p child) = Filter p (fuseFilters child)
fuseFilters (Project cols child) = Project cols (fuseFilters child)
fuseFilters (Derive name expr child) = Derive name expr (fuseFilters child)
fuseFilters (Join jt l r left right) =
    Join jt l r (fuseFilters left) (fuseFilters right)
fuseFilters (Aggregate keys aggs child) =
    Aggregate keys aggs (fuseFilters child)
fuseFilters (Sort cols child) = Sort cols (fuseFilters child)
fuseFilters (Limit n child) = Limit n (fuseFilters child)
fuseFilters leaf = leaf

-- | Logical AND of two @Bool@ expressions.
andExpr :: E.Expr Bool -> E.Expr Bool -> E.Expr Bool
andExpr =
    E.Binary
        ( E.MkBinaryOp
            { E.binaryFn = (&&)
            , E.binaryName = "and"
            , E.binarySymbol = Just "&&"
            , E.binaryCommutative = True
            , E.binaryPrecedence = 3
            }
        )

-- ---------------------------------------------------------------------------
-- Rule 2: Predicate pushdown
-- ---------------------------------------------------------------------------

{- | Push Filter nodes as close to the Scan as possible.

* Past a @Derive@ when the predicate doesn't reference the derived column.
* Past a @Project@ when all predicate columns are in the projected set.
* Into @ScanConfig.scanPushdownPredicate@ when the child is a @Scan@.
-}
pushPredicates :: LogicalPlan -> LogicalPlan
pushPredicates (Filter p (Derive name expr child))
    | name `notElem` E.getColumns p =
        Derive name expr (pushPredicates (Filter p child))
    | otherwise =
        Filter p (Derive name expr (pushPredicates child))
pushPredicates (Filter p (Project cols child))
    | all (`elem` cols) (E.getColumns p) =
        Project cols (pushPredicates (Filter p child))
    | otherwise =
        Filter p (Project cols (pushPredicates child))
pushPredicates (Filter p child) = Filter p (pushPredicates child)
pushPredicates (Project cols child) = Project cols (pushPredicates child)
pushPredicates (Derive name expr child) = Derive name expr (pushPredicates child)
pushPredicates (Join jt l r left right) =
    Join jt l r (pushPredicates left) (pushPredicates right)
pushPredicates (Aggregate keys aggs child) =
    Aggregate keys aggs (pushPredicates child)
pushPredicates (Sort cols child) = Sort cols (pushPredicates child)
pushPredicates (Limit n child) = Limit n (pushPredicates child)
pushPredicates leaf = leaf

-- ---------------------------------------------------------------------------
-- Rule 3: Dead column elimination
-- ---------------------------------------------------------------------------

{- | Collect every column name that is explicitly referenced somewhere in the
plan (in filter predicates, sort keys, aggregate keys, projection lists,
join keys, and derived expressions).  Returns Nothing when "all columns
are needed" (i.e. no Project restricts the output).
-}
referencedCols :: LogicalPlan -> Maybe (S.Set T.Text)
referencedCols (Scan _ schema) = Just (S.fromList (M.keys (elements schema)))
referencedCols (Project cols _) = Just (S.fromList cols)
referencedCols (Filter p child) =
    fmap (S.union (S.fromList (E.getColumns p))) (referencedCols child)
referencedCols (Derive _ expr child) =
    fmap (S.union (S.fromList (uExprCols expr))) (referencedCols child)
referencedCols (Join _ l r left right) =
    let keySet = S.fromList [l, r]
        lRef = fmap (S.union keySet) (referencedCols left)
        rRef = fmap (S.union keySet) (referencedCols right)
     in liftMaybe2 S.union lRef rRef
referencedCols (Aggregate keys aggs child) =
    let aggCols = S.fromList (keys <> concatMap (uExprCols . snd) aggs)
     in fmap (S.union aggCols) (referencedCols child)
referencedCols (Sort cols child) =
    fmap (S.union (S.fromList (fmap fst cols))) (referencedCols child)
referencedCols (Limit _ child) = referencedCols child
referencedCols (SourceDF _) = Nothing

liftMaybe2 :: (a -> b -> c) -> Maybe a -> Maybe b -> Maybe c
liftMaybe2 f (Just a) (Just b) = Just (f a b)
liftMaybe2 _ _ _ = Nothing

uExprCols :: E.UExpr -> [T.Text]
uExprCols (E.UExpr expr) = E.getColumns expr

-- | Drop @Derive@ nodes whose output column is never consumed downstream.
eliminateDeadColumns :: LogicalPlan -> LogicalPlan
eliminateDeadColumns plan = go (referencedCols plan) plan
  where
    go needed (Derive name expr child) =
        case needed of
            Nothing -> Derive name expr (go needed child)
            Just cols
                | name `S.notMember` cols -> go needed child
                | otherwise ->
                    Derive
                        name
                        expr
                        (go (Just (S.union cols (S.fromList (uExprCols expr)))) child)
    go needed (Filter p child) =
        Filter p (go (fmap (S.union (S.fromList (E.getColumns p))) needed) child)
    go _needed (Project cols child) =
        Project cols (go (Just (S.fromList cols)) child)
    go needed (Join jt l r left right) =
        let keySet = fmap (S.union (S.fromList [l, r])) needed
         in Join jt l r (go keySet left) (go keySet right)
    go needed (Aggregate keys aggs child) =
        let aggCols = fmap (S.union (S.fromList (keys <> concatMap (uExprCols . snd) aggs))) needed
         in Aggregate keys aggs (go aggCols child)
    go needed (Sort cols child) =
        Sort cols (go (fmap (S.union (S.fromList (fmap fst cols))) needed) child)
    go needed (Limit n child) = Limit n (go needed child)
    go needed (Scan ds schema) =
        case needed of
            Nothing -> Scan ds schema
            Just cols ->
                Scan ds (Schema (M.filterWithKey (\k _ -> k `S.member` cols) (elements schema)))
    go _ (SourceDF df) = SourceDF df

-- ---------------------------------------------------------------------------
-- Logical → Physical lowering
-- ---------------------------------------------------------------------------

{- | Lower the (already-optimised) logical plan to a physical plan.

Join strategy: always HashJoin (the executor can fall back to SortMerge
at runtime once statistics are available).
-}
toPhysical :: Int -> LogicalPlan -> PhysicalPlan
-- Special case: Filter directly on a Scan → push into ScanConfig.
toPhysical batchSz (Filter p (Scan (CsvSource path sep) schema)) =
    PhysicalScan
        (CsvSource path sep)
        (ScanConfig batchSz sep schema (Just p))
toPhysical batchSz (Scan (CsvSource path sep) schema) =
    PhysicalScan
        (CsvSource path sep)
        (ScanConfig batchSz sep schema Nothing)
toPhysical batchSz (Filter p (Scan (ParquetSource path) schema)) =
    PhysicalScan
        (ParquetSource path)
        (ScanConfig batchSz ',' schema (Just p))
toPhysical batchSz (Scan (ParquetSource path) schema) =
    PhysicalScan
        (ParquetSource path)
        (ScanConfig batchSz ',' schema Nothing)
toPhysical batchSz (Project cols child) =
    PhysicalProject cols (toPhysical batchSz child)
toPhysical batchSz (Filter p child) =
    PhysicalFilter p (toPhysical batchSz child)
toPhysical batchSz (Derive name expr child) =
    PhysicalDerive name expr (toPhysical batchSz child)
toPhysical batchSz (Join jt l r left right) =
    PhysicalHashJoin
        jt
        l
        r
        (toPhysical batchSz left)
        (toPhysical batchSz right)
toPhysical batchSz (Aggregate keys aggs child) =
    PhysicalHashAggregate keys aggs (toPhysical batchSz child)
toPhysical batchSz (Sort cols child) =
    PhysicalSort cols (toPhysical batchSz child)
toPhysical batchSz (Limit n child) =
    PhysicalLimit n (toPhysical batchSz child)
toPhysical _ (SourceDF df) = PhysicalSourceDF df
