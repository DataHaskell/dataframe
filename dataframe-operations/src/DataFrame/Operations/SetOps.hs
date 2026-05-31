{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : DataFrame.Operations.SetOps
Description : Set-theoretic ("topos") row operations.

These treat a 'DataFrame' as a /set/ of rows and implement the subobject
lattice from relational algebra: 'union', 'intersect', 'difference', and
'symmetricDifference'. Every result is deduplicated, so each operation has the
schema-preserving shape @DataFrame -> DataFrame -> DataFrame@.

Row equality is the same hash-based notion used by 'distinct' (see
"DataFrame.Operations.Aggregation"), so these operations and 'distinct' agree
on what "the same row" means. Both inputs are expected to share a schema; the
typed layer ('DataFrame.Typed') enforces that statically.
-}
module DataFrame.Operations.SetOps (
    union,
    intersect,
    difference,
    symmetricDifference,
) where

import Prelude hiding (filter)

import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
 )
import DataFrame.Operations.Aggregation (distinct, groupBy, selectIndices)
import DataFrame.Operations.Merge ()

{- | All rows that appear in either dataframe, deduplicated.

@union a b@ is @distinct (a <> b)@: the set union of the two row sets.
-}
union :: DataFrame -> DataFrame -> DataFrame
union a b = distinct (a <> b)

{- | Rows that appear in both dataframes, deduplicated.

A row survives iff an equal row is present in each input.
-}
intersect :: DataFrame -> DataFrame -> DataFrame
intersect = setOp (&&)

{- | Rows present in the left dataframe but absent from the right, deduplicated
(the relational @EXCEPT@; the subobject complement of @a@ by @b@).
-}
difference :: DataFrame -> DataFrame -> DataFrame
difference = setOp (\inLeft inRight -> inLeft && not inRight)

{- | Rows present in exactly one of the two dataframes, deduplicated.

@symmetricDifference a b@ is @union (difference a b) (difference b a)@.
-}
symmetricDifference :: DataFrame -> DataFrame -> DataFrame
symmetricDifference a b = difference a b `union` difference b a

{- | Core engine for 'intersect' and 'difference'.

Concatenate the inputs, group by every column, and decide each group from
whether it has a member on the left side (original-row index @< nRows a@) and/or
the right side. The first member of a qualifying group is emitted as the
representative row, which keeps the result deduplicated and, because group
members come out in ascending row order, prefers the left dataframe's row.
-}
setOp :: (Bool -> Bool -> Bool) -> DataFrame -> DataFrame -> DataFrame
setOp keep a b =
    selectIndices (VU.fromList chosen) combined
  where
    leftRows = fst (dataframeDimensions a)
    combined = a <> b
    Grouped _ _ vis offs _ = groupBy (columnNames combined) combined
    nGroups = VU.length offs - 1
    chosen =
        [ VU.head members
        | k <- [0 .. nGroups - 1]
        , let s = VU.unsafeIndex offs k
              e = VU.unsafeIndex offs (k + 1)
              members = VU.slice s (e - s) vis
              inLeft = VU.any (< leftRows) members
              inRight = VU.any (>= leftRows) members
        , keep inLeft inRight
        ]
