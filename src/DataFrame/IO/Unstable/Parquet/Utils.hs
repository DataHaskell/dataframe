{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Unstable.Parquet.Utils (
    ParquetType (..),
    parquetTypeFromInt,
    ColumnDescription (..),
    PageDescription (..),
    generateColumnDescriptions,
    foldColumns,
) where

import Control.Monad.IO.Class (MonadIO (..))
import qualified Data.ByteString as BS
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import DataFrame.IO.Parquet.Types (DictVals, ParquetType (..), parquetTypeFromInt)
import DataFrame.IO.Unstable.Parquet.Thrift (
    CompressionCodec,
    ConvertedType (..),
    FieldRepetitionType (..),
    LogicalType (..),
    PageHeader,
    SchemaElement (..),
    unField,
 )
import DataFrame.IO.Utils.RandomAccess (RandomAccess)
import DataFrame.Internal.Column (
    Column (..),
    MutableColumn (..),
    columnLength,
    copyIntoMutableColumn,
    freezeMutableColumn,
    newMutableColumn,
 )
import qualified Streamly.Data.Fold as Fold
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream

data ColumnDescription = ColumnDescription
    { colElementType :: !ParquetType
    , maxDefinitionLevel :: !Int32
    , maxRepetitionLevel :: !Int32
    , colLogicalType :: !(Maybe LogicalType)
    , colConvertedType :: !(Maybe ConvertedType)
    }
    deriving (Show, Eq)

data PageDescription
    = PageDescription
    { rawBytes :: BS.ByteString
    , header :: PageHeader
    , codec :: CompressionCodec
    , dictionary :: Maybe DictVals
    , parquetType :: Int
    }
    deriving (Eq, Show)

{- | How much each repetition type contributes to def/rep levels.
  REQUIRED contributes nothing; OPTIONAL adds a def level;
  REPEATED adds both a def and a rep level.
-}
levelContribution :: Maybe FieldRepetitionType -> (Int, Int)
levelContribution = \case
    Just (REPEATED _) -> (1, 1)
    Just (OPTIONAL _) -> (1, 0)
    _ -> (0, 0) -- REQUIRED or absent

{- | Build a forest from a flat, depth-first schema list,
  consuming elements and returning (tree, remaining).
-}
data SchemaTree = SchemaTree SchemaElement [SchemaTree]

buildForest :: [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildForest [] = ([], [])
buildForest (se : rest) =
    let n = fromIntegral $ fromMaybe 0 (unField (num_children se)) :: Int
        (children, rest') = buildChildren n rest
        (siblings, rest'') = buildForest rest'
     in (SchemaTree se children : siblings, rest'')

buildChildren :: Int -> [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildChildren 0 xs = ([], xs)
buildChildren n xs =
    let (child, rest') = buildForest xs -- one subtree
        (children, rest'') = buildChildren (n - 1) rest'
     in (take 1 child <> children, rest'') -- safe: buildForest >=1 result

{- | Recursively collect leaf ColumnDescriptions, threading
  accumulated def/rep levels down the path.
-}
collectLeaves :: Int -> Int -> SchemaTree -> [ColumnDescription]
collectLeaves defAcc repAcc (SchemaTree se children) =
    let (dInc, rInc) = levelContribution (unField (repetition_type se))
        defLevel = defAcc + dInc
        repLevel = repAcc + rInc
     in case children of
            [] ->
                -- leaf: emit a description
                let pType = case unField (schematype se) of
                        Just t -> parquetTypeFromInt (fromIntegral t)
                        Nothing -> PARQUET_TYPE_UNKNOWN
                 in [ ColumnDescription
                        pType
                        (fromIntegral defLevel)
                        (fromIntegral repLevel)
                        (unField (logicalType se))
                        (unField (converted_type se))
                    ]
            _ ->
                -- internal node: recurse into children
                concatMap (collectLeaves defLevel repLevel) children

{- | Entry point: skip the message-type root (first element),
  then walk the schema forest.
-}
generateColumnDescriptions :: [SchemaElement] -> [ColumnDescription]
generateColumnDescriptions [] = []
generateColumnDescriptions (_ : rest) =
    -- drop schema root
    let (forest, _) = buildForest rest
     in concatMap (collectLeaves 0 0) forest

foldColumns :: (RandomAccess r, MonadIO r) => Int -> Stream r Column -> r Column
foldColumns size stream = do
    chunk <- Stream.uncons stream
    case chunk of
        Nothing -> error "Empty Column Stream"
        Just (initialChunk, _) -> do
            foldStream <- foldStreamM initialChunk
            (mutableColumn, _) <- Stream.fold foldStream stream
            liftIO $ freezeMutableColumn mutableColumn
  where
    foldStreamM ::
        (RandomAccess r, MonadIO r) =>
        Column -> r (Fold.Fold r Column (MutableColumn, Int))
    foldStreamM initialChunk = do
        mutableColumn <- liftIO $ newMutableColumn size initialChunk
        return $ Fold.foldlM' f (pure (mutableColumn, 0))
    f ::
        (RandomAccess r, MonadIO r) =>
        (MutableColumn, Int) -> Column -> r (MutableColumn, Int)
    f (accumulator, offset) columnChunk = do
        liftIO $ copyIntoMutableColumn accumulator offset columnChunk
        return (accumulator, offset + columnLength columnChunk)
