{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.Typed.Join (
    -- * Typed joins
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,
) where

import GHC.TypeLits (Symbol)

import qualified DataFrame.Operations.Join as DJ

import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types (TypedDataFrame (..))

-- | Typed inner join on one or more key columns.
innerJoin ::
    forall (keys :: [Symbol]) left right.
    (AllKnownSymbol keys) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (InnerJoinSchema keys left right)
innerJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.innerJoin keyNames r l)
  where
    keyNames = symbolVals @keys

-- | Typed left join.
leftJoin ::
    forall (keys :: [Symbol]) left right.
    (AllKnownSymbol keys) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (LeftJoinSchema keys left right)
leftJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.leftJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Typed right join.
rightJoin ::
    forall (keys :: [Symbol]) left right.
    (AllKnownSymbol keys) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (RightJoinSchema keys left right)
rightJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.rightJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Typed full outer join.
fullOuterJoin ::
    forall (keys :: [Symbol]) left right.
    (AllKnownSymbol keys) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (FullOuterJoinSchema keys left right)
fullOuterJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.fullOuterJoin keyNames r l)
  where
    keyNames = symbolVals @keys
