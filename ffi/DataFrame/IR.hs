{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Intermediate Representation for DataFrame query plans.
  JSON-decodable plan tree + interpreter.
-}
module DataFrame.IR (
    PlanNode (..),
    AggSpec (..),
    executePlan,
) where

import Data.Aeson (FromJSON (..), withObject, (.:))
import qualified Data.Aeson as Aeson
import Data.Aeson.Types (Parser)
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.Text as T
import Data.Type.Equality (
    TestEquality (testEquality),
    type (:~:) (Refl),
    type (:~~:) (HRefl),
 )
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import Foreign (wordPtrToPtr)
import Type.Reflection (SomeTypeRep (..), eqTypeRep, typeRep)

import DataFrame.Functions (count, mean, meanMaybe, sumMaybe)
import qualified DataFrame.Functions as Functions
import DataFrame.IO.Arrow (arrowToDataframe)
import DataFrame.IO.CSV (
    CsvReader,
    defaultReadOptions,
    readSeparated,
    readTsv,
    writeCsv,
 )
import DataFrame.IO.JSON (readJSON)
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.IR.ExprJson (SomeExpr (..), decodeExprAny, decodeExprAt)
import DataFrame.Internal.Column (Column (..), Columnable)
import DataFrame.Internal.DataFrame (DataFrame, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), NamedExpr)
import DataFrame.Internal.Schema (Schema, makeSchema, schemaType)
import qualified DataFrame.Lazy as Lazy
import DataFrame.Operations.Aggregation (aggregate, distinct, groupBy)
import DataFrame.Operations.Core (insertVector, renameMany)
import DataFrame.Operations.Join (JoinType (..), join)
import DataFrame.Operations.Permutation (SortOrder (..), sortBy)
import qualified DataFrame.Operations.Statistics as Stats
import DataFrame.Operations.Subset (exclude, filterWhere, range, select)
import qualified DataFrame.Operations.Subset as Subset
import DataFrame.Operations.Transformations (derive)
import DataFrame.Operators ((.=))

-- ---------------------------------------------------------------------------
-- IR types
-- ---------------------------------------------------------------------------

data AggSpec = AggSpec
    { aggName :: T.Text
    , aggFn :: T.Text
    , aggCol :: T.Text
    }
    deriving (Show)

data PlanNode
    = ReadCsv FilePath
    | ReadTsv FilePath
    | -- | schema_addr array_addr
      FromArrow Word64 Word64
    | Select [T.Text] PlanNode
    | GroupBy [T.Text] [AggSpec] PlanNode
    | Sort [T.Text] Bool PlanNode
    | Limit Int PlanNode
    | -- | predicate JSON, child plan
      Filter Aeson.Value PlanNode
    | -- | column name, expr JSON, child plan
      Derive T.Text Aeson.Value PlanNode
    | Exclude [T.Text] PlanNode
    | Rename [(T.Text, T.Text)] PlanNode
    | Distinct PlanNode
    | TakeLast Int PlanNode
    | Drop Int PlanNode
    | DropLast Int PlanNode
    | Range Int Int PlanNode
    | -- | joinType ("inner"|"left"|"right"|"outer"), shared key columns, left, right
      Join T.Text [T.Text] PlanNode PlanNode
    | Describe PlanNode
    | -- | first column, second column, child plan
      Correlation T.Text T.Text PlanNode
    | Frequencies T.Text PlanNode
    | ReadParquet FilePath
    | ReadJson FilePath
    | -- | path, separator (single character), child plan; runs as a terminal op
      WriteCsv FilePath PlanNode
    | {- | path, schema (column name → type-tag map). Reads via the lazy
      engine with predicate / projection pushdown; subsequent ops
      currently still run eagerly on the materialized result.
      -}
      ScanCsv FilePath [(T.Text, T.Text)]
    | ScanParquet FilePath [(T.Text, T.Text)]
    deriving (Show)

-- ---------------------------------------------------------------------------
-- JSON decoding
-- ---------------------------------------------------------------------------

instance FromJSON AggSpec where
    parseJSON = withObject "AggSpec" $ \o ->
        AggSpec
            <$> o .: "name"
            <*> o .: "agg"
            <*> o .: "col"

instance FromJSON PlanNode where
    parseJSON = withObject "PlanNode" $ \o -> do
        op <- o .: "op" :: Parser T.Text
        case op of
            "ReadCsv" -> ReadCsv <$> o .: "path"
            "ReadTsv" -> ReadTsv <$> o .: "path"
            "FromArrow" -> FromArrow <$> o .: "schema" <*> o .: "array"
            "Select" -> Select <$> o .: "cols" <*> o .: "input"
            "GroupBy" -> GroupBy <$> o .: "keys" <*> o .: "aggregations" <*> o .: "input"
            "Sort" -> Sort <$> o .: "cols" <*> o .: "ascending" <*> o .: "input"
            "Limit" -> Limit <$> o .: "n" <*> o .: "input"
            "Filter" -> Filter <$> o .: "predicate" <*> o .: "input"
            "Derive" -> Derive <$> o .: "name" <*> o .: "expr" <*> o .: "input"
            "Exclude" -> Exclude <$> o .: "cols" <*> o .: "input"
            "Rename" -> Rename <$> o .: "pairs" <*> o .: "input"
            "Distinct" -> Distinct <$> o .: "input"
            "TakeLast" -> TakeLast <$> o .: "n" <*> o .: "input"
            "Drop" -> Drop <$> o .: "n" <*> o .: "input"
            "DropLast" -> DropLast <$> o .: "n" <*> o .: "input"
            "Range" -> Range <$> o .: "start" <*> o .: "end" <*> o .: "input"
            "Join" ->
                Join
                    <$> o .: "how"
                    <*> o .: "on"
                    <*> o .: "left"
                    <*> o .: "right"
            "Describe" -> Describe <$> o .: "input"
            "Correlation" ->
                Correlation
                    <$> o .: "first"
                    <*> o .: "second"
                    <*> o .: "input"
            "Frequencies" -> Frequencies <$> o .: "col" <*> o .: "input"
            "ReadParquet" -> ReadParquet <$> o .: "path"
            "ReadJson" -> ReadJson <$> o .: "path"
            "WriteCsv" -> WriteCsv <$> o .: "path" <*> o .: "input"
            "ScanCsv" -> ScanCsv <$> o .: "path" <*> o .: "schema"
            "ScanParquet" -> ScanParquet <$> o .: "path" <*> o .: "schema"
            _ -> fail $ "DataFrame.IR: unknown op: " ++ T.unpack op

executePlan :: CsvReader -> PlanNode -> IO DataFrame
executePlan _reader (ReadCsv path) =
    readSeparated defaultReadOptions path
executePlan _reader (ReadTsv path) =
    readTsv path
executePlan _reader (FromArrow schemaAddr arrayAddr) =
    arrowToDataframe
        (wordPtrToPtr (fromIntegral schemaAddr))
        (wordPtrToPtr (fromIntegral arrayAddr))
executePlan reader (Select cols node) =
    select cols <$> executePlan reader node
executePlan reader (GroupBy keys aggs node) = do
    df <- executePlan reader node
    nes <- mapM (buildNamedExpr df) aggs
    return $ aggregate nes (groupBy keys df)
executePlan reader (Sort cols ascending node) = do
    df <- executePlan reader node
    let orders = map (\c -> mkSortOrder ascending c (unsafeGetColumn c df)) cols
    return $ sortBy orders df
executePlan reader (Limit k node) =
    Subset.take k <$> executePlan reader node
executePlan reader (Filter predJson node) = do
    df <- executePlan reader node
    case decodeExprAt @Bool predJson of
        Right pred_ -> return $ filterWhere pred_ df
        Left err -> ioError $ userError $ "DataFrame.IR.Filter: " <> err
executePlan reader (Derive name exprJson node) = do
    df <- executePlan reader node
    case decodeExprAny exprJson of
        Right (SomeExpr _trep expr) -> return $ derive name expr df
        Left err -> ioError $ userError $ "DataFrame.IR.Derive: " <> err
executePlan reader (Exclude cols node) =
    exclude cols <$> executePlan reader node
executePlan reader (Rename pairs node) =
    renameMany pairs <$> executePlan reader node
executePlan reader (Distinct node) =
    distinct <$> executePlan reader node
executePlan reader (TakeLast n node) =
    Subset.takeLast n <$> executePlan reader node
executePlan reader (Drop n node) =
    Subset.drop n <$> executePlan reader node
executePlan reader (DropLast n node) =
    Subset.dropLast n <$> executePlan reader node
executePlan reader (Range start end node) =
    range (start, end) <$> executePlan reader node
executePlan reader (Join how on leftPlan rightPlan) = do
    left <- executePlan reader leftPlan
    right <- executePlan reader rightPlan
    jt <- case how of
        "inner" -> return INNER
        "left" -> return LEFT
        "right" -> return RIGHT
        "outer" -> return FULL_OUTER
        "full_outer" -> return FULL_OUTER
        other ->
            ioError . userError $
                "DataFrame.IR.Join: unknown join type " <> T.unpack other
    return $ join jt on left right
executePlan reader (Describe node) = Stats.summarize <$> executePlan reader node
executePlan reader (Correlation a b node) = do
    df <- executePlan reader node
    let r = Stats.correlation a b df
        valueCol = case r of
            Just d -> V.singleton d
            Nothing -> V.singleton (0 / 0 :: Double) -- NaN when correlation is undefined
            -- Return a single-row frame: { first, second, correlation }
    return $
        insertVector "first" (V.singleton a) $
            insertVector "second" (V.singleton b) $
                insertVector "correlation" valueCol mempty
executePlan reader (Frequencies colName node) = do
    df <- executePlan reader node
    runFrequencies colName df
executePlan _reader (ReadParquet path) = Parquet.readParquet path
executePlan _reader (ReadJson path) = readJSON path
executePlan reader (WriteCsv path node) = do
    df <- executePlan reader node
    writeCsv path df
    -- Return the same frame so callers that pipe through .collect() get the
    -- written data back too. Callers that don't care can discard.
    return df
executePlan reader (ScanCsv path schemaPairs) = do
    schema <- buildSchema schemaPairs
    Lazy.runDataFrame (Lazy.scanCsvWith reader schema (T.pack path))
executePlan _reader (ScanParquet path schemaPairs) = do
    schema <- buildSchema schemaPairs
    Lazy.runDataFrame (Lazy.scanParquet schema (T.pack path))

{- | Build a SortOrder from a column's runtime type.
Uses type dispatch to recover Ord for known column types.
-}
mkSortOrder :: Bool -> T.Text -> Column -> SortOrder
mkSortOrder isAsc name col = dispatchType (columnTypeRep col)
  where
    columnTypeRep :: Column -> SomeTypeRep
    columnTypeRep (UnboxedColumn _ (_ :: VU.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (BoxedColumn _ (_ :: V.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (PackedText _ _) = SomeTypeRep (typeRep @T.Text)
    mk :: (Columnable a, Ord a) => Expr a -> SortOrder
    mk = if isAsc then Asc else Desc
    dispatchType (SomeTypeRep tr)
        | Just HRefl <- eqTypeRep tr (typeRep @Int) = mk (Col @Int name)
        | Just HRefl <- eqTypeRep tr (typeRep @Int8) = mk (Col @Int8 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Int16) = mk (Col @Int16 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Int32) = mk (Col @Int32 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Int64) = mk (Col @Int64 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Word) = mk (Col @Word name)
        | Just HRefl <- eqTypeRep tr (typeRep @Word8) = mk (Col @Word8 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Word16) = mk (Col @Word16 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Word32) = mk (Col @Word32 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Word64) = mk (Col @Word64 name)
        | Just HRefl <- eqTypeRep tr (typeRep @Integer) = mk (Col @Integer name)
        | Just HRefl <- eqTypeRep tr (typeRep @Double) = mk (Col @Double name)
        | Just HRefl <- eqTypeRep tr (typeRep @Float) = mk (Col @Float name)
        | Just HRefl <- eqTypeRep tr (typeRep @Bool) = mk (Col @Bool name)
        | Just HRefl <- eqTypeRep tr (typeRep @Char) = mk (Col @Char name)
        | Just HRefl <- eqTypeRep tr (typeRep @T.Text) = mk (Col @T.Text name)
        | Just HRefl <- eqTypeRep tr (typeRep @String) = mk (Col @String name)
        | Just HRefl <- eqTypeRep tr (typeRep @BS.ByteString) =
            mk (Col @BS.ByteString name)
        | otherwise = error $ "mkSortOrder: unsupported column type: " ++ show tr

-- | Dispatch aggregation by fn name and runtime column type.
buildNamedExpr :: DataFrame -> AggSpec -> IO NamedExpr
buildNamedExpr df (AggSpec name fn colName) =
    case fn of
        "count" -> countExpr name colName (unsafeGetColumn colName df)
        "sum" -> sumExpr name colName (unsafeGetColumn colName df)
        "mean" -> meanExpr name colName (unsafeGetColumn colName df)
        "min" -> minMaxExpr Functions.minimum name colName (unsafeGetColumn colName df)
        "max" -> minMaxExpr Functions.maximum name colName (unsafeGetColumn colName df)
        "median" -> doubleStatExpr Functions.median name colName (unsafeGetColumn colName df)
        "variance" -> doubleStatExpr Functions.variance name colName (unsafeGetColumn colName df)
        "std" -> doubleStatExpr stdDevExpr name colName (unsafeGetColumn colName df)
        other ->
            ioError $
                userError $
                    "DataFrame.IR: unknown aggregation '" ++ T.unpack other ++ "'"

-- | Variance → standard deviation; sqrt of the underlying variance Expr.
stdDevExpr :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stdDevExpr e = sqrt (Functions.variance e)

-- | Build a 'Schema' from a list of (col, type-tag) pairs sent over the wire.
buildSchema :: [(T.Text, T.Text)] -> IO Schema
buildSchema pairs = do
    sch <- mapM resolve pairs
    return (makeSchema sch)
  where
    resolve (name, tag) = case tag of
        "int" -> return (name, schemaType @Int)
        "int8" -> return (name, schemaType @Int8)
        "int16" -> return (name, schemaType @Int16)
        "int32" -> return (name, schemaType @Int32)
        "int64" -> return (name, schemaType @Int64)
        "double" -> return (name, schemaType @Double)
        "float" -> return (name, schemaType @Float)
        "bool" -> return (name, schemaType @Bool)
        "text" -> return (name, schemaType @T.Text)
        "string" -> return (name, schemaType @String)
        other ->
            ioError . userError $
                "DataFrame.IR.buildSchema: unsupported schema type tag '"
                    ++ T.unpack other
                    ++ "' for column '"
                    ++ T.unpack name
                    ++ "'"

-- | Dispatch 'frequencies' on the column's runtime element type.
runFrequencies :: T.Text -> DataFrame -> IO DataFrame
runFrequencies colName df = dispatchType (columnTypeRep (unsafeGetColumn colName df))
  where
    columnTypeRep :: Column -> SomeTypeRep
    columnTypeRep (UnboxedColumn _ (_ :: VU.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (BoxedColumn _ (_ :: V.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (PackedText _ _) = SomeTypeRep (typeRep @T.Text)

    fr :: forall a. (Columnable a, Ord a) => IO DataFrame
    fr = return $ Stats.frequencies (Col @a colName) df

    dispatchType :: SomeTypeRep -> IO DataFrame
    dispatchType (SomeTypeRep tr)
        | Just HRefl <- eqTypeRep tr (typeRep @Int) = fr @Int
        | Just HRefl <- eqTypeRep tr (typeRep @Int8) = fr @Int8
        | Just HRefl <- eqTypeRep tr (typeRep @Int16) = fr @Int16
        | Just HRefl <- eqTypeRep tr (typeRep @Int32) = fr @Int32
        | Just HRefl <- eqTypeRep tr (typeRep @Int64) = fr @Int64
        | Just HRefl <- eqTypeRep tr (typeRep @Word) = fr @Word
        | Just HRefl <- eqTypeRep tr (typeRep @Word8) = fr @Word8
        | Just HRefl <- eqTypeRep tr (typeRep @Word16) = fr @Word16
        | Just HRefl <- eqTypeRep tr (typeRep @Word32) = fr @Word32
        | Just HRefl <- eqTypeRep tr (typeRep @Word64) = fr @Word64
        | Just HRefl <- eqTypeRep tr (typeRep @Integer) = fr @Integer
        | Just HRefl <- eqTypeRep tr (typeRep @Double) = fr @Double
        | Just HRefl <- eqTypeRep tr (typeRep @Float) = fr @Float
        | Just HRefl <- eqTypeRep tr (typeRep @Bool) = fr @Bool
        | Just HRefl <- eqTypeRep tr (typeRep @Char) = fr @Char
        | Just HRefl <- eqTypeRep tr (typeRep @T.Text) = fr @T.Text
        | Just HRefl <- eqTypeRep tr (typeRep @String) = fr @String
        | otherwise =
            ioError . userError $
                "DataFrame.IR.Frequencies: unsupported column type for '"
                    ++ T.unpack colName
                    ++ "'"

countExpr :: T.Text -> T.Text -> Column -> IO NamedExpr
countExpr name colName (UnboxedColumn Nothing (_ :: VU.Vector a)) = return $ name .= count (Col @a colName)
countExpr name colName (UnboxedColumn (Just _) (_ :: VU.Vector a)) = return $ name .= count (Col @(Maybe a) colName)
countExpr name colName (BoxedColumn Nothing (_ :: V.Vector a)) = return $ name .= count (Col @a colName)
countExpr name colName (BoxedColumn (Just _) (_ :: V.Vector a)) = return $ name .= count (Col @(Maybe a) colName)
countExpr name colName (PackedText Nothing _) = return $ name .= count (Col @T.Text colName)
countExpr name colName (PackedText (Just _) _) = return $ name .= count (Col @(Maybe T.Text) colName)

sumExpr :: T.Text -> T.Text -> Column -> IO NamedExpr
sumExpr name colName (UnboxedColumn Nothing (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= Functions.sum (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= Functions.sum (Col @Double colName)
sumExpr name colName (UnboxedColumn (Just _) (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= sumMaybe (Col @(Maybe Int) colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= sumMaybe (Col @(Maybe Double) colName)
sumExpr name colName (BoxedColumn Nothing (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= Functions.sum (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= Functions.sum (Col @Double colName)
sumExpr name colName (BoxedColumn (Just _) (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= sumMaybe (Col @(Maybe Int) colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= sumMaybe (Col @(Maybe Double) colName)
sumExpr _ colName _ =
    ioError $
        userError $
            "DataFrame.IR: sum: unsupported column type for '" ++ T.unpack colName ++ "'"

meanExpr :: T.Text -> T.Text -> Column -> IO NamedExpr
meanExpr name colName (UnboxedColumn Nothing (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= mean (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= mean (Col @Double colName)
meanExpr name colName (UnboxedColumn (Just _) (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= meanMaybe (Col @(Maybe Double) colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= meanMaybe (Col @(Maybe Int) colName)
meanExpr name colName (BoxedColumn Nothing (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= mean (Col @Double colName)
meanExpr name colName (BoxedColumn (Just _) (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= meanMaybe (Col @(Maybe Double) colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= meanMaybe (Col @(Maybe Int) colName)
meanExpr _ colName _ =
    ioError $
        userError $
            "DataFrame.IR: mean: unsupported column type for '" ++ T.unpack colName ++ "'"

-- | min / max — preserve column type, require Ord.
minMaxExpr ::
    (forall a. (Columnable a, Ord a) => Expr a -> Expr a) ->
    T.Text ->
    T.Text ->
    Column ->
    IO NamedExpr
minMaxExpr op name colName (UnboxedColumn Nothing (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= op (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= op (Col @Double colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Float) =
        return $ name .= op (Col @Float colName)
minMaxExpr op name colName (BoxedColumn Nothing (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) =
        return $ name .= op (Col @T.Text colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= op (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= op (Col @Double colName)
minMaxExpr _ _ colName _ =
    ioError . userError $
        "DataFrame.IR: min/max: unsupported column type for '"
            ++ T.unpack colName
            ++ "'"

-- | median / variance / std — return Double, require Real + Unbox.
doubleStatExpr ::
    (forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double) ->
    T.Text ->
    T.Text ->
    Column ->
    IO NamedExpr
doubleStatExpr op name colName (UnboxedColumn Nothing (_ :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= op (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= op (Col @Double colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Float) =
        return $ name .= op (Col @Float colName)
doubleStatExpr op name colName (BoxedColumn Nothing (_ :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        return $ name .= op (Col @Int colName)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        return $ name .= op (Col @Double colName)
doubleStatExpr _ _ colName _ =
    ioError . userError $
        "DataFrame.IR: median/variance/std: unsupported column type for '"
            ++ T.unpack colName
            ++ "'"
