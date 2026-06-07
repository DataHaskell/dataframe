{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Lazy.Internal.DataFrame where

import qualified Data.Text as T
import DataFrame.IO.CSV (CsvReader, readCsvWithSchema)
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Schema (Schema)
import DataFrame.Lazy.Internal.Executor (execute)
import DataFrame.Lazy.Internal.LogicalPlan (
    DataSource (..),
    LogicalPlan (..),
    SortOrder (..),
 )
import qualified DataFrame.Lazy.Internal.Optimizer as Opt
import DataFrame.Operations.Join (JoinType)

{- | A lazy query that has not been executed yet.

The query is represented as a 'LogicalPlan' tree; execution is deferred
until 'runDataFrame' is called.
-}
data LazyDataFrame = LazyDataFrame
    { plan :: LogicalPlan
    , batchSize :: Int
    }

instance Show LazyDataFrame where
    show ldf =
        "LazyDataFrame { batchSize = "
            <> (show (batchSize ldf) <> (", plan = " <> (show (plan ldf) <> " }")))

-- ---------------------------------------------------------------------------
-- Entry point
-- ---------------------------------------------------------------------------

{- | Execute the lazy query: optimise the logical plan, then stream-execute
the resulting physical plan, returning a fully-materialised 'D.DataFrame'.
The CSV reader (default: attoparsec) is set per scan via 'scanCsv' /
'scanCsvWith'.
-}
runDataFrame :: LazyDataFrame -> IO D.DataFrame
runDataFrame ldf = execute (Opt.optimize (batchSize ldf) (plan ldf))

-- ---------------------------------------------------------------------------
-- Builders that construct the logical plan tree
-- ---------------------------------------------------------------------------

-- | Lift an already-loaded eager 'D.DataFrame' into the lazy plan.
fromDataFrame :: D.DataFrame -> LazyDataFrame
fromDataFrame df = LazyDataFrame{plan = SourceDF df, batchSize = 1_000_000}

{- | Scan a CSV file with the default comma separator and the in-tree
attoparsec reader.  For the SIMD reader use 'scanCsvWith'.
-}
scanCsv :: Schema -> T.Text -> LazyDataFrame
scanCsv = scanCsvWith readCsvWithSchema

{- | Like 'scanCsv' but with an explicit CSV reader (e.g. the SIMD reader
@fastReadCsvWithSchema@ from @dataframe-fastcsv@).
-}
scanCsvWith :: CsvReader -> Schema -> T.Text -> LazyDataFrame
scanCsvWith reader schema path =
    LazyDataFrame
        { plan = Scan (CsvSource (T.unpack path) ',' reader) schema
        , batchSize = 1_000_000
        }

-- | Scan a character-separated file with the default attoparsec reader.
scanSeparated :: Char -> Schema -> T.Text -> LazyDataFrame
scanSeparated = scanSeparatedWith readCsvWithSchema

-- | Like 'scanSeparated' but with an explicit CSV reader.
scanSeparatedWith ::
    CsvReader -> Char -> Schema -> T.Text -> LazyDataFrame
scanSeparatedWith reader sep schema path =
    LazyDataFrame
        { plan = Scan (CsvSource (T.unpack path) sep reader) schema
        , batchSize = 1_000_000
        }

-- | Scan a Parquet file, directory of files, or glob pattern.
scanParquet :: Schema -> T.Text -> LazyDataFrame
scanParquet schema path =
    LazyDataFrame
        { plan = Scan (ParquetSource (T.unpack path)) schema
        , batchSize = 1_000_000
        }

-- | Add a computed column (or overwrite an existing one).
derive ::
    (C.Columnable a) => T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr ldf =
    ldf{plan = Derive name (E.UExpr expr) (plan ldf)}

-- | Retain only the listed columns.
select :: [T.Text] -> LazyDataFrame -> LazyDataFrame
select cols ldf = ldf{plan = Project cols (plan ldf)}

-- | Keep rows that satisfy the predicate.
filter :: E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond ldf = ldf{plan = Filter cond (plan ldf)}

-- | Join two lazy queries on the given key columns.
join ::
    JoinType ->
    -- | Left join key column name
    T.Text ->
    -- | Right join key column name
    T.Text ->
    -- | Left sub-query
    LazyDataFrame ->
    -- | Right sub-query
    LazyDataFrame ->
    LazyDataFrame
join jt leftKey rightKey left right =
    LazyDataFrame
        { plan = Join jt leftKey rightKey (plan left) (plan right)
        , batchSize = batchSize left
        }

{- | Group by a set of columns and compute aggregate expressions.

Each aggregate expression should use an 'Agg' node (e.g. @sumOf@, @meanOf@).
-}
groupBy ::
    -- | Group-by key columns
    [T.Text] ->
    -- | @[(outputName, aggregateExpr)]@
    [(T.Text, E.UExpr)] ->
    LazyDataFrame ->
    LazyDataFrame
groupBy keys aggs ldf = ldf{plan = Aggregate keys aggs (plan ldf)}

-- | Sort the result by the given @(column, direction)@ pairs.
sortBy :: [(T.Text, SortOrder)] -> LazyDataFrame -> LazyDataFrame
sortBy cols ldf = ldf{plan = Sort cols (plan ldf)}

-- | Retain at most @n@ rows.
take :: Int -> LazyDataFrame -> LazyDataFrame
take n ldf = ldf{plan = Limit n (plan ldf)}
