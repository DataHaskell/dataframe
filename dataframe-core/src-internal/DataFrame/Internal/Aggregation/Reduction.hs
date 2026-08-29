{-# LANGUAGE ScopedTypeVariables #-}

{- | The vocabulary shared by the aggregation planner and the aggregation
kernels: the set of recognised reductions, and the admission gate that decides
whether a column may enter a Double-valued fast path.

This module holds no kernel and no policy, so the planner can depend on it
without depending on an implementation.
-}
module DataFrame.Internal.Aggregation.Reduction (
    Reduction (..),
    cleanDoubleVector,
) where

import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.Column (
    Column (..),
    materializePacked,
 )
import DataFrame.Internal.Column.Conversion (toDoubleVector)

{- | A recognised fast-path reduction over a single value column. The element
type (Int vs Double) is resolved at scatter time; sum/min/max preserve the
column's element type, everything else produces a Double column.
-}
data Reduction
    = RSum
    | RCount
    | RMin
    | RMax
    | RMean
    | RStd
    | RVar
    | RTop2Sum
    deriving (Eq, Show)

cleanDoubleVector :: Column -> Maybe (VU.Vector Double)
cleanDoubleVector col = case col of
    UnboxedColumn Nothing _ -> either (const Nothing) Just (toDoubleVector col)
    BoxedColumn Nothing _ -> either (const Nothing) Just (toDoubleVector col)
    p@(PackedText _ _) -> cleanDoubleVector (materializePacked p)
    _ -> Nothing
