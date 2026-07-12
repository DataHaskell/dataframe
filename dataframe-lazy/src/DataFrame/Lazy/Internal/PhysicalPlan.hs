module DataFrame.Lazy.Internal.PhysicalPlan where

import qualified Data.Text as T
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Lazy.Internal.LogicalPlan (DataSource, SortOrder)
import DataFrame.Operations.Join (JoinType)
import DataFrame.Schema (Schema)

-- | Scan-level configuration: batch size, separator, optional pushdowns.
data ScanConfig = ScanConfig
    { scanBatchSize :: !Int
    , scanSeparator :: !Char
    , scanSchema :: !Schema
    , scanPushdownPredicate :: !(Maybe (E.Expr Bool))
    }
    deriving (Show)

{- | Physical plan: every node carries enough information for the executor
to allocate resources and choose algorithms without further analysis.
-}
data PhysicalPlan
    = PhysicalScan DataSource ScanConfig
    | PhysicalProject [T.Text] PhysicalPlan
    | PhysicalFilter (E.Expr Bool) PhysicalPlan
    | PhysicalDerive T.Text E.UExpr PhysicalPlan
    | PhysicalHashJoin JoinType T.Text T.Text PhysicalPlan PhysicalPlan
    | PhysicalSortMergeJoin JoinType T.Text T.Text PhysicalPlan PhysicalPlan
    | PhysicalHashAggregate [T.Text] [(T.Text, E.UExpr)] PhysicalPlan
    | PhysicalSort [(T.Text, SortOrder)] PhysicalPlan
    | PhysicalLimit Int PhysicalPlan
    | -- | Materialize child to a binary file on disk (used for build sides).
      PhysicalSpill PhysicalPlan FilePath
    | -- | Emit an already-loaded DataFrame as a stream of batches of size @n@.
      PhysicalSourceDF Int D.DataFrame
    deriving (Show)
