{- | The curated public surface of @dataframe-core@: the interchange types
('DataFrame', 'Column', 'Row', 'Expr'), element constraints, and the
rendering/serialization verbs. Internal plumbing stays in @dataframe-core:internal@.
-}
module DataFrame.Core (
    -- * The DataFrame
    DataFrame,
    GroupedDataFrame,
    empty,
    fromNamedColumns,
    insertColumn,
    columnNames,
    null,

    -- * Columns
    Column,
    fromList,
    fromVector,
    fromUnboxedVector,
    mkRandom,
    toList,
    toVector,
    hasElemType,
    hasMissing,
    isNumeric,

    -- * Element constraints
    Columnable,
    Columnable',

    -- * Rows
    Row,
    Any,
    toAny,
    fromAny,
    rowValue,
    toRowList,
    toRowVector,

    -- * Expressions
    Expr,
    NamedExpr,
    toNamedExpr,
    toUExpr,
    fromUExpr,
    eSize,
    prettyPrint,
    prettyPrintWidth,

    -- * Rendering & serialization
    TruncateConfig (..),
    defaultTruncateConfig,
    toCsv,
    toCsv',
    toSeparated,
    toMarkdown,
    toMarkdown',
) where

import Prelude hiding (null)

import DataFrame.Internal.Column (
    Column,
    Columnable,
    fromList,
    fromUnboxedVector,
    fromVector,
    hasElemType,
    hasMissing,
    isNumeric,
    mkRandom,
    toList,
    toVector,
 )
import DataFrame.Internal.DataFrame (
    DataFrame,
    GroupedDataFrame,
    TruncateConfig (..),
    columnNames,
    defaultTruncateConfig,
    empty,
    fromNamedColumns,
    insertColumn,
    null,
    toCsv,
    toCsv',
    toMarkdown,
    toMarkdown',
    toSeparated,
 )
import DataFrame.Internal.Expression (
    Expr,
    NamedExpr,
    toNamedExpr,
    toUExpr,
    fromUExpr,
    eSize,
    prettyPrint,
    prettyPrintWidth,
 )
import DataFrame.Internal.Row (
    Any,
    Row,
    fromAny,
    rowValue,
    toAny,
    toRowList,
    toRowVector,
 )
import DataFrame.Internal.Types (Columnable')
