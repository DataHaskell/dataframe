{-# LANGUAGE GADTs #-}

module DataFrame.Lazy.Internal.LogicalPlan where

import qualified Data.Text as T
import DataFrame.IO.CSV (CsvReader)
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Schema (Schema)
import DataFrame.Operations.Join (JoinType)

-- | Data source for a scan node.
data DataSource
    = -- | path, separator, CSV reader (e.g. attoparsec or SIMD)
      CsvSource FilePath Char CsvReader
    | ParquetSource FilePath

instance Show DataSource where
    show (CsvSource path sep _) =
        "CsvSource " ++ show path ++ " " ++ show sep ++ " <reader>"
    show (ParquetSource path) = "ParquetSource " ++ show path

-- | Sort direction used in Sort nodes and the public API.
data SortOrder = Ascending | Descending
    deriving (Show, Eq, Ord)

{- | Relational-algebra tree that represents what the query computes.
No physical decisions (batch size, join strategy) are made here.
-}
data LogicalPlan
    = -- | Read columns described by the schema from a source.
      Scan DataSource Schema
    | -- | Retain only the listed columns.
      Project [T.Text] LogicalPlan
    | -- | Keep rows matching the predicate.
      Filter (E.Expr Bool) LogicalPlan
    | -- | Add or overwrite a column via an expression.
      Derive T.Text E.UExpr LogicalPlan
    | -- | Join two sub-plans on the given key columns.
      Join JoinType T.Text T.Text LogicalPlan LogicalPlan
    | -- | Group then aggregate.
      Aggregate [T.Text] [(T.Text, E.UExpr)] LogicalPlan
    | -- | Sort by a list of (column, direction) pairs.
      Sort [(T.Text, SortOrder)] LogicalPlan
    | -- | Retain at most N rows.
      Limit Int LogicalPlan
    | -- | Lift an already-loaded DataFrame into the lazy plan.
      SourceDF D.DataFrame
    deriving (Show)
