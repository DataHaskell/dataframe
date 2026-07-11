{- | The umbrella subset these internal tests use. The suite cannot depend on
the meta @dataframe@ package (that would be a package-level cycle), so the
names the tests reach for are re-exported under one alias here.
-}
module DataFrameApi (
    DataFrame,
    fromNamedColumns,
    fromUnnamedColumns,
    dimensions,
    nRows,
    rename,
    exclude,
    randomSplit,
    derive,
    readCsv,
) where

import DataFrame.Core (DataFrame, fromNamedColumns)
import DataFrame.IO.CSV (readCsv)
import DataFrame.Operations.Core (dimensions, fromUnnamedColumns, nRows, rename)
import DataFrame.Operations.Subset (exclude, randomSplit)
import DataFrame.Operations.Transformations (derive)
