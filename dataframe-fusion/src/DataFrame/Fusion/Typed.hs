{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Typed DataFusion-backed query API. Mirrors 'DataFrame.Typed.Lazy' but runs
on Rust-side DataFusion via FFI.
-}
module DataFrame.Fusion.Typed (
    -- * Carrier
    DataFrame,

    -- * Sources
    scanCsv,

    -- * Operators
    filter,
    take,
    select,
    derive,
    sortBy,
    SortOrder (..),

    -- * Aggregation
    Grouped,
    groupBy,
    aggregate,

    -- * Joins
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,

    -- * Materialization
    run,

    -- * Re-exports
    module DataFrame.Typed.Expr,
    module DataFrame.Typed.Types,
    module DataFrame.Fusion.Plan,

    -- ** Aggregation builders (re-exported from DataFrame.Typed.Aggregate)
    AGG.as,
) where

import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Foreign.C.String (CString, withCString)
import Foreign.Ptr (nullPtr)
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude hiding (filter, take)

import qualified DataFrame.Internal.Column as IC
import qualified DataFrame.Internal.Expression as IE
import DataFrame.Lazy (SortOrder (..))
import qualified DataFrame.Typed.Aggregate as AGG

import qualified DataFrame.Fusion.FFI as F
import DataFrame.Fusion.Plan
import DataFrame.IR.ExprJson (encodeExprToBytes)
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types

{- | A query plan whose row schema is tracked at the type level. The
underlying handle is owned by Rust-side DataFusion; Haskell holds a
'ForeignPtr' that frees it on garbage collection.
-}
newtype DataFrame (cols :: [Type]) = DF {unDataFrame :: PlanHandle}

{- | Scan a CSV file. The schema comes from the @cols@ phantom via 'KnownSchema';
the user does not pass it separately. DataFusion currently infers column types
from the file, so the constraint is reserved for future wire-format derivation.
-}
scanCsv ::
    forall cols.
    (KnownSchema cols) =>
    Context ->
    T.Text ->
    IO (DataFrame cols)
scanCsv ctx path =
    withContext ctx $ \cp ->
        withCString (T.unpack path) $ \cpath -> do
            ph <- runPlanOp (F.df_scan_csv cp cpath nullPtr)
            return (DF ph)

-- | Keep rows that satisfy the predicate.
filter ::
    TExpr cols Bool ->
    DataFrame cols ->
    IO (DataFrame cols)
filter (TExpr expr) (DF plan) = do
    bytes <- case encodeExprToBytes expr of
        Right bs -> return bs
        Left e -> error ("DataFrame.Fusion.Typed.filter: " <> e)
    withPlan plan $ \pp ->
        withCStringBS bytes $ \cbytes -> do
            ph <- runPlanOp (F.df_plan_filter pp cbytes)
            return (DF ph)

-- | Retain at most @n@ rows.
take ::
    Int ->
    DataFrame cols ->
    IO (DataFrame cols)
take n (DF plan) = withPlan plan $ \pp -> do
    ph <- runPlanOp (F.df_plan_take pp (fromIntegral n))
    return (DF ph)

-- | Project to the named columns. Result schema computed by 'SubsetSchema'.
select ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names, AssertAllPresent names cols) =>
    DataFrame cols ->
    IO (DataFrame (SubsetSchema names cols))
select (DF plan) = do
    let names = symbolVals @names
        json = BL.toStrict (Aeson.encode names)
    withPlan plan $ \pp ->
        withCStringBS json $ \cjson -> do
            ph <- runPlanOp (F.df_plan_select pp cjson)
            return (DF ph)

{- | Add a computed column, appended to the input schema (mirroring
'DataFrame.Typed.Lazy.derive'). The expression is lowered to JSON and decoded
into a DataFusion 'Expr' on the Rust side.
-}
derive ::
    forall name a cols.
    (KnownSymbol name, IC.Columnable a, AssertAbsent name cols) =>
    TExpr cols a ->
    DataFrame cols ->
    IO (DataFrame (Snoc cols (Column name a)))
derive (TExpr expr) (DF plan) = do
    bytes <- case encodeExprToBytes expr of
        Right bs -> return bs
        Left e -> error ("DataFrame.Fusion.Typed.derive: " <> e)
    let nameStr = symbolVal (Proxy @name)
    withPlan plan $ \pp ->
        withCString nameStr $ \cname ->
            withCStringBS bytes $ \cbytes -> do
                ph <- runPlanOp (F.df_plan_derive pp cname cbytes)
                return (DF ph)

-- | A grouped query: an 'DataFrame' tagged with the group-by key list.
data Grouped (keys :: [Symbol]) (cols :: [Type]) = GD
    { gdKeys :: ![T.Text]
    , gdPlan :: !PlanHandle
    }

-- | Partition rows by the named keys.
groupBy ::
    forall (keys :: [Symbol]) cols.
    (AllKnownSymbol keys, AssertAllPresent keys cols) =>
    DataFrame cols ->
    Grouped keys cols
groupBy (DF plan) = GD (symbolVals @keys) plan

{- | Aggregate a grouped query. The first argument is a chain of 'AGG.as'
entries composed with @(.)@; the empty composition (@id@) yields just the
group keys.
-}
aggregate ::
    forall keys cols aggs.
    (TAgg keys cols '[] -> TAgg keys cols aggs) ->
    Grouped keys cols ->
    IO (DataFrame (Append (GroupKeyColumns keys cols) (Reverse aggs)))
aggregate build (GD keys plan) = do
    let keysJson = BL.toStrict (Aeson.encode keys)
    aggEntries <- traverse encodeAggEntry (taggToNamedExprs (build TAggNil))
    let aggsJson = BL.toStrict (Aeson.encode aggEntries)
    withPlan plan $ \pp ->
        withCStringBS keysJson $ \cKeys ->
            withCStringBS aggsJson $ \cAggs -> do
                ph <- runPlanOp (F.df_plan_groupby_aggregate pp cKeys cAggs)
                return (DF ph)
  where
    encodeAggEntry :: IE.NamedExpr -> IO Aeson.Value
    encodeAggEntry (name, IE.UExpr e) = case encodeExprToBytes e of
        Right bs -> case Aeson.decode (BL.fromStrict bs) :: Maybe Aeson.Value of
            Just v ->
                return $
                    Aeson.object
                        [ "name" Aeson..= name
                        , "expr" Aeson..= v
                        ]
            Nothing ->
                error
                    "DataFrame.Fusion.Typed.aggregate: unparseable JSON from encodeExprToBytes"
        Left err -> error ("DataFrame.Fusion.Typed.aggregate: " <> err)

{- | Inner join on a single key pair. The result schema is currently the left
schema (matching 'DataFrame.Typed.Lazy.join'); a sharper 'InnerJoinSchema'
result is on the v1.5 list.
-}
innerJoin ::
    T.Text -> T.Text -> DataFrame left -> DataFrame right -> IO (DataFrame left)
innerJoin = joinWith "inner"

leftJoin ::
    T.Text -> T.Text -> DataFrame left -> DataFrame right -> IO (DataFrame left)
leftJoin = joinWith "left"

rightJoin ::
    T.Text -> T.Text -> DataFrame left -> DataFrame right -> IO (DataFrame left)
rightJoin = joinWith "right"

fullOuterJoin ::
    T.Text -> T.Text -> DataFrame left -> DataFrame right -> IO (DataFrame left)
fullOuterJoin = joinWith "outer"

joinWith ::
    T.Text ->
    T.Text ->
    T.Text ->
    DataFrame left ->
    DataFrame right ->
    IO (DataFrame left)
joinWith how leftKey rightKey (DF leftPlan) (DF rightPlan) = do
    let onJson = BL.toStrict (Aeson.encode [[leftKey, rightKey]])
    withPlan leftPlan $ \lp ->
        withPlan rightPlan $ \rp ->
            withCString (T.unpack how) $ \cHow ->
                withCStringBS onJson $ \cOn -> do
                    ph <- runPlanOp (F.df_plan_join lp rp cHow cOn)
                    return (DF ph)

{- | Sort the result by a list of (column, direction) pairs.
The 'SortOrder' is reused from "DataFrame.Lazy.Internal.LogicalPlan" so
this signature lines up exactly with 'DataFrame.Typed.Lazy.sortBy'.
-}
sortBy ::
    [(T.Text, SortOrder)] ->
    DataFrame cols ->
    IO (DataFrame cols)
sortBy orders (DF plan) = do
    let json = BL.toStrict (Aeson.encode (map encodeOrder orders))
    withPlan plan $ \pp ->
        withCStringBS json $ \cjson -> do
            ph <- runPlanOp (F.df_plan_sort_by pp cjson)
            return (DF ph)
  where
    encodeOrder (c, o) =
        Aeson.object
            [ "col" Aeson..= c
            , "asc" Aeson..= isAsc o
            ]
    isAsc Ascending = True
    isAsc Descending = False

-- | Execute the plan and import the result as a 'TypedDataFrame'.
run ::
    forall cols.
    DataFrame cols ->
    IO (TypedDataFrame cols)
run (DF plan) = unsafeFreeze <$> collectArrow plan

{- | Pass a strict 'BS.ByteString' to a C function expecting a
NUL-terminated UTF-8 string. The bytes must not contain interior NULs;
JSON output never does.
-}
withCStringBS :: BS.ByteString -> (CString -> IO a) -> IO a
withCStringBS bs k =
    BS.useAsCString bs $ \cs ->
        if cs == nullPtr then k nullPtr else k cs
