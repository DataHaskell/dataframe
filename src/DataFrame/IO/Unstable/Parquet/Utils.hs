{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Unstable.Parquet.Utils (
    ParquetType (..),
    parquetTypeFromInt,
    ColumnDescription (..),
    generateColumnDescriptions,
    getColumnNames,
    foldNonNullable,
    foldNullable,
    foldRepeated,
) where

import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.ST (runST)
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word8)
import DataFrame.IO.Parquet.Types (
    ParquetType (..),
    parquetTypeFromInt,
 )
import DataFrame.IO.Unstable.Parquet.Levels (
    stitchList2V,
    stitchList3V,
    stitchListV,
 )
import DataFrame.IO.Unstable.Parquet.Thrift (
    ConvertedType (..),
    FieldRepetitionType (..),
    LogicalType (..),
    SchemaElement (..),
    ThriftType,
    unField,
 )
import DataFrame.IO.Utils.RandomAccess (RandomAccess)
import DataFrame.Internal.Column (
    Bitmap,
    Column (..),
    Columnable,
    buildBitmapFromValid,
    fromList,
    fromVector,
 )
import DataFrame.Internal.Types (SBool (..), sUnbox)
import Streamly.Data.Stream (Stream)
import qualified Streamly.Data.Stream as Stream

data ColumnDescription = ColumnDescription
    { colElementType :: !(Maybe ThriftType)
    , maxDefinitionLevel :: !Int32
    , maxRepetitionLevel :: !Int32
    , colLogicalType :: !(Maybe LogicalType)
    , colConvertedType :: !(Maybe ConvertedType)
    , typeLength :: !(Maybe Int32)
    }
    deriving (Show, Eq)

levelContribution :: Maybe FieldRepetitionType -> (Int, Int)
levelContribution = \case
    Just (REPEATED _) -> (1, 1)
    Just (OPTIONAL _) -> (1, 0)
    _ -> (0, 0) -- REQUIRED or absent

data SchemaTree = SchemaTree SchemaElement [SchemaTree]

buildTree :: [SchemaElement] -> (SchemaTree, [SchemaElement])
buildTree [] = error "buildTree: schema ended unexpectedly"
buildTree (se : rest) =
    let n = fromIntegral $ fromMaybe 0 (unField (num_children se)) :: Int
        (children, rest') = buildChildren n rest
     in (SchemaTree se children, rest')

-- | Build a forest of sibling trees from a flat depth-first element list.
buildForest :: [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildForest [] = ([], [])
buildForest xs =
    let (tree, rest') = buildTree xs
        (siblings, rest'') = buildForest rest'
     in (tree : siblings, rest'')

-- | Build exactly @n@ child trees, each consuming only its own subtree.
buildChildren :: Int -> [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildChildren 0 xs = ([], xs)
buildChildren n xs =
    let (child, rest') = buildTree xs
        (siblings, rest'') = buildChildren (n - 1) rest'
     in (child : siblings, rest'')

collectLeaves :: Int -> Int -> SchemaTree -> [ColumnDescription]
collectLeaves defAcc repAcc (SchemaTree se children) =
    let (dInc, rInc) = levelContribution (unField (repetition_type se))
        defLevel = defAcc + dInc
        repLevel = repAcc + rInc
     in case children of
            [] ->
                -- leaf: emit a description
                let pType = unField (schematype se)
                 in [ ColumnDescription
                        pType
                        (fromIntegral defLevel)
                        (fromIntegral repLevel)
                        (unField (logicalType se))
                        (unField (converted_type se))
                        (unField (type_length se))
                    ]
            _ ->
                -- internal node: recurse into children
                concatMap (collectLeaves defLevel repLevel) children

generateColumnDescriptions :: [SchemaElement] -> [ColumnDescription]
generateColumnDescriptions [] = []
generateColumnDescriptions (_ : rest) =
    -- drop schema root
    let (forest, _) = buildForest rest
     in concatMap (collectLeaves 0 0) forest

getColumnNames :: [SchemaElement] -> [Text]
getColumnNames [] = []
getColumnNames schemaElements =
    let (forest, _) = buildForest schemaElements
     in go forest [] False
  where
    isRepeated se = case unField (repetition_type se) of
        Just (REPEATED _) -> True
        _ -> False

    go [] _ _ = []
    go (SchemaTree se children : rest) path skipThis =
        case children of
            -- Leaf node
            [] ->
                let newPath = if skipThis then path else path ++ [unField (name se)]
                    fullName = T.intercalate "." newPath
                 in fullName : go rest path skipThis
            -- REPEATED intermediate: skip this name; skip single child too
            _
                | isRepeated se ->
                    let skipChildren = length children == 1
                        childLeaves = go children path skipChildren
                     in childLeaves ++ go rest path skipThis
            -- Name-skipped intermediate: recurse with skip cleared
            _
                | skipThis ->
                    let childLeaves = go children path False
                     in childLeaves ++ go rest path skipThis
            -- Normal intermediate: add name to path, recurse
            _ ->
                let subPath = path ++ [unField (name se)]
                    childLeaves = go children subPath False
                 in childLeaves ++ go rest path skipThis

{- | Fold a stream of value vectors into a non-nullable 'Column'.
Concatenates all vectors and calls 'fromVector'.
-}
foldNonNullable ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a) =>
    Stream m (VB.Vector a) ->
    m Column
foldNonNullable stream = do
    vecs <- Stream.toList stream
    return $ fromVector (VB.concat vecs)

foldNullable ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a) =>
    Int ->
    Stream m (VB.Vector a, VU.Vector Int) ->
    m Column
foldNullable maxDef stream = do
    chunks <- Stream.toList stream
    let allVals = VB.concat (map fst chunks)
        allDefs = VU.concat (map snd chunks)
        nRows = VU.length allDefs
        validVec :: VU.Vector Word8
        validVec = VU.map (\d -> if d == maxDef then 1 else 0) allDefs
        maybeBm :: Maybe Bitmap
        maybeBm =
            if VU.all (== 1) validVec
                then Nothing
                else Just (buildBitmapFromValid validVec)
    return $ case sUnbox @a of
        STrue ->
            -- Unboxed path: scatter present values to the right positions.
            -- Null slots keep the zero-initialised default; the bitmap
            -- guards them from being read.
            let dat = runST $ do
                    mv <- VUM.new nRows
                    let go i j
                            | i >= nRows = pure ()
                            | VU.unsafeIndex validVec i == 1 = do
                                VUM.unsafeWrite mv i (VB.unsafeIndex allVals j)
                                go (i + 1) (j + 1)
                            | otherwise = go (i + 1) j
                    go 0 0
                    VU.unsafeFreeze mv
             in UnboxedColumn maybeBm dat
        SFalse ->
            -- Boxed path: same scatter, null slots hold an error thunk
            -- that is never evaluated (guarded by the bitmap).
            let dat = runST $ do
                    mv <- VBM.replicate nRows (error "parquet: null slot accessed")
                    let go i j
                            | i >= nRows = pure ()
                            | VU.unsafeIndex validVec i == 1 = do
                                VBM.unsafeWrite mv i (VB.unsafeIndex allVals j)
                                go (i + 1) (j + 1)
                            | otherwise = go (i + 1) j
                    go 0 0
                    VB.unsafeFreeze mv
             in BoxedColumn maybeBm dat

{- | Fold a stream of (values, def-levels, rep-levels) triples into a
repeated (list) 'Column' using Dremel-style level stitching.

The stitching function is selected by @maxRep@:

  * @maxRep == 1@  →  'stitchListV'   → @[Maybe [Maybe a]]@
  * @maxRep == 2@  →  'stitchList2V'  → @[Maybe [Maybe [Maybe a]]]@
  * @maxRep >= 3@  →  'stitchList3V'  → @[Maybe [Maybe [Maybe [Maybe a]]]]@

Threshold formula: @defT_r = maxDef - 2 * (maxRep - r)@.
-}
foldRepeated ::
    forall m a.
    ( RandomAccess m
    , MonadIO m
    , Columnable a
    , Columnable (Maybe [Maybe a])
    , Columnable (Maybe [Maybe [Maybe a]])
    , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
    ) =>
    Int ->
    Int ->
    Stream m (VB.Vector a, VU.Vector Int, VU.Vector Int) ->
    m Column
foldRepeated maxRep maxDef stream = do
    chunks <- Stream.toList stream
    let allVals = VB.concat [vs | (vs, _, _) <- chunks]
        allDefs = VU.concat [ds | (_, ds, _) <- chunks]
        allReps = VU.concat [rs | (_, _, rs) <- chunks]
    return $ case maxRep of
        2 -> fromList (stitchList2V (maxDef - 2) maxDef allReps allDefs allVals)
        3 ->
            fromList (stitchList3V (maxDef - 4) (maxDef - 2) maxDef allReps allDefs allVals)
        _ -> fromList (stitchListV maxDef allReps allDefs allVals)
