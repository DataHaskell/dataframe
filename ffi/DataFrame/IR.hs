{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
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
import Data.Word (Word, Word16, Word32, Word64, Word8)
import Foreign (wordPtrToPtr)
import Type.Reflection (SomeTypeRep (..), eqTypeRep, typeRep)

import DataFrame.Functions (count, mean, meanMaybe, sumMaybe)
import qualified DataFrame.Functions as Functions
import DataFrame.IO.Arrow (arrowToDataframe)
import DataFrame.IO.CSV (
    defaultReadOptions,
    readSeparated,
    readTsv,
 )
import DataFrame.Internal.Column (Column (..), Columnable)
import DataFrame.Internal.DataFrame (DataFrame, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), NamedExpr)
import DataFrame.Operations.Aggregation (aggregate, groupBy)
import DataFrame.Operations.Permutation (SortOrder (..), sortBy)
import DataFrame.Operations.Subset (select)
import qualified DataFrame.Operations.Subset as Subset
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
            _ -> fail $ "DataFrame.IR: unknown op: " ++ T.unpack op

executePlan :: PlanNode -> IO DataFrame
executePlan (ReadCsv path) =
    readSeparated defaultReadOptions path
executePlan (ReadTsv path) =
    readTsv path
executePlan (FromArrow schemaAddr arrayAddr) =
    arrowToDataframe
        (wordPtrToPtr (fromIntegral schemaAddr))
        (wordPtrToPtr (fromIntegral arrayAddr))
executePlan (Select cols node) =
    select cols <$> executePlan node
executePlan (GroupBy keys aggs node) = do
    df <- executePlan node
    nes <- mapM (buildNamedExpr df) aggs
    return $ aggregate nes (groupBy keys df)
executePlan (Sort cols ascending node) = do
    df <- executePlan node
    let orders = map (\c -> mkSortOrder ascending c (unsafeGetColumn c df)) cols
    return $ sortBy orders df
executePlan (Limit k node) =
    Subset.take k <$> executePlan node

{- | Build a SortOrder from a column's runtime type.
Uses type dispatch to recover Ord for known column types.
-}
mkSortOrder :: Bool -> T.Text -> Column -> SortOrder
mkSortOrder isAsc name col = dispatchType (columnTypeRep col)
  where
    columnTypeRep :: Column -> SomeTypeRep
    columnTypeRep (UnboxedColumn _ (_ :: VU.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (BoxedColumn _ (_ :: V.Vector a)) = SomeTypeRep (typeRep @a)
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
        other ->
            ioError $
                userError $
                    "DataFrame.IR: unknown aggregation '" ++ T.unpack other ++ "'"

countExpr :: T.Text -> T.Text -> Column -> IO NamedExpr
countExpr name colName (UnboxedColumn Nothing (_ :: VU.Vector a)) = return $ name .= count (Col @a colName)
countExpr name colName (UnboxedColumn (Just _) (_ :: VU.Vector a)) = return $ name .= count (Col @(Maybe a) colName)
countExpr name colName (BoxedColumn Nothing (_ :: V.Vector a)) = return $ name .= count (Col @a colName)
countExpr name colName (BoxedColumn (Just _) (_ :: V.Vector a)) = return $ name .= count (Col @(Maybe a) colName)

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
sumExpr name colName _ =
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
meanExpr name colName _ =
    ioError $
        userError $
            "DataFrame.IR: mean: unsupported column type for '" ++ T.unpack colName ++ "'"
