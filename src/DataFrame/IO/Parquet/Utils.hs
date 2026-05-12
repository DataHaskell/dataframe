{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Parquet.Utils (
    ColumnDescription (..),
    generateColumnDescriptions,
    getColumnNames,
    foldNonNullable,
    foldNonNullableUnboxed,
    foldNullable,
    foldNullableUnboxed,
    foldRepeated,
    foldRepeatedUnboxed,
) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word8)
import DataFrame.IO.Parquet.Levels (
    stitchList,
    stitchList2,
    stitchList3,
 )
import DataFrame.IO.Parquet.Thrift (
    ConvertedType (..),
    FieldRepetitionType (..),
    LogicalType (..),
    SchemaElement (..),
    ThriftType,
    unField,
 )
import DataFrame.IO.Utils.RandomAccess (RandomAccess)
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    buildBitmapFromValid,
    fromList,
 )
import DataFrame.Internal.Types (SBool (..), sUnbox)
import qualified Streamly.Data.Fold as Fold
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

{- | Fold a stream of value chunks into a non-nullable 'Column'.

Pre-allocates a mutable vector of @totalRows@ and fills it chunk-by-chunk
using a single 'Fold.foldlM\'' pass, avoiding any intermediate list or
concatenation allocation.

For unboxable element types the chunks (which are always boxed) are
unboxed element-by-element directly into the pre-allocated unboxed
buffer, eliminating the boxing round-trip that a 'fromVector' call on a
boxed concat would otherwise require.
-}
foldNonNullable ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a) =>
    Int ->
    Stream m (VB.Vector a) ->
    m Column
foldNonNullable totalRows stream = do
    mv <- liftIO $ VBM.unsafeNew totalRows
    _ <-
        Stream.fold
            ( Fold.foldlM'
                ( \off chunk -> liftIO $ do
                    let n = VB.length chunk
                    VB.copy (VBM.unsafeSlice off n mv) chunk
                    return (off + n)
                )
                (return 0)
            )
            stream
    v <- liftIO $ VB.unsafeFreeze mv
    return (BoxedColumn Nothing v)

foldNonNullableUnboxed ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a, VU.Unbox a) =>
    Int ->
    Stream m (VU.Vector a) ->
    m Column
foldNonNullableUnboxed totalRows stream = do
    mv <- liftIO $ VUM.unsafeNew totalRows
    _ <-
        Stream.fold
            ( Fold.foldlM'
                ( \off chunk -> liftIO $ do
                    let n = VU.length chunk
                        go i
                            | i >= n = return ()
                            | otherwise = do
                                VUM.unsafeWrite
                                    mv
                                    (off + i)
                                    (VU.unsafeIndex chunk i)
                                go (i + 1)
                    go 0
                    return (off + n)
                )
                (return 0)
            )
            stream
    dat <- liftIO $ VU.unsafeFreeze mv
    return (UnboxedColumn Nothing dat)

{- | Fold a stream of (values, def-levels) pairs into a nullable 'Column'.

Pre-allocates the output buffer and a valid-mask vector of @totalRows@,
then scatters values inline during a single 'Fold.foldlM\'' pass.
This eliminates the @allVals@ intermediate vector that the old
'Stream.toList' + concat approach required.

A 'hasNull' flag is accumulated during the scatter so the
'buildBitmapFromValid' call (and the second 'VU.all' scan) is skipped
entirely when all values are present.
-}
foldNullable ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a) =>
    Int ->
    Int ->
    Stream m (VB.Vector a, VU.Vector Int) ->
    m Column
foldNullable maxDef totalRows stream = do
    -- null slots hold an error thunk, guarded by bitmap.
    --
    -- IMPORTANT: 'VBM.unsafeWrite' for boxed vectors stores a *pointer* to
    -- the value without evaluating it, so unsupported-encoding error thunks
    -- would be silently swallowed into the column data and only fire lazily
    -- when user code reads a cell. The '!v' bang pattern forces each value
    -- to WHNF before the write, surfacing decoder errors immediately.
    mvDat <-
        liftIO $ VBM.replicate totalRows (error "parquet: null slot accessed")
    mvValid <- liftIO (VUM.new totalRows :: IO (VUM.IOVector Word8))
    (_, hasNull) <-
        Stream.fold
            ( Fold.foldlM'
                ( \(rowOff, anyNull) (vals, defs) -> liftIO $ do
                    let nDefs = VU.length defs
                        go i j acc
                            | i >= nDefs = return acc
                            | VU.unsafeIndex defs i == maxDef = do
                                let !v = VB.unsafeIndex vals j
                                VBM.unsafeWrite mvDat (rowOff + i) v
                                VUM.unsafeWrite mvValid (rowOff + i) 1
                                go (i + 1) (j + 1) acc
                            | otherwise = go (i + 1) j True
                    newNull <- go 0 0 False
                    return (rowOff + nDefs, anyNull || newNull)
                )
                (return (0, False))
            )
            stream
    dat <- liftIO $ VB.unsafeFreeze mvDat
    maybeBm <-
        if hasNull
            then do
                validV <- liftIO $ VU.unsafeFreeze mvValid
                return (Just (buildBitmapFromValid validV))
            else return Nothing
    return (BoxedColumn maybeBm dat)

foldNullableUnboxed ::
    forall m a.
    (RandomAccess m, MonadIO m, Columnable a, VU.Unbox a) =>
    Int ->
    Int ->
    Stream m (VU.Vector a, VU.Vector Int) ->
    m Column
foldNullableUnboxed maxDef totalRows stream = do
    -- zero-init means null slots silently hold 0, guarded by bitmap.
    mvDat <- liftIO $ VUM.new totalRows
    mvValid <- liftIO (VUM.new totalRows :: IO (VUM.IOVector Word8))
    -- Drain the stream into a list once, then run a tight IO loop. This
    -- avoids per-page Streamly polymorphic-monad dispatch in the inner
    -- scatter loop.
    chunks <- Stream.toList stream
    hasNull <- liftIO $ scatterChunks mvDat mvValid maxDef chunks
    dat <- liftIO $ VU.unsafeFreeze mvDat
    maybeBm <-
        if hasNull
            then do
                validV <- liftIO $ VU.unsafeFreeze mvValid
                return (Just (buildBitmapFromValid validV))
            else return Nothing
    return (UnboxedColumn maybeBm dat)
  where
    scatterChunks ::
        VUM.IOVector a ->
        VUM.IOVector Word8 ->
        Int ->
        [(VU.Vector a, VU.Vector Int)] ->
        IO Bool
    scatterChunks mvDat mvValid !md = goChunks 0 False
      where
        goChunks !_ !anyNull [] = pure anyNull
        goChunks !rowOff !anyNull ((vals, defs) : rest) = do
            let !nDefs = VU.length defs
                go !i !j !acc
                    | i >= nDefs = pure acc
                    | VU.unsafeIndex defs i == md = do
                        VUM.unsafeWrite mvDat (rowOff + i) (VU.unsafeIndex vals j)
                        VUM.unsafeWrite mvValid (rowOff + i) 1
                        go (i + 1) (j + 1) acc
                    | otherwise = go (i + 1) j True
            !newNull <- go 0 0 False
            goChunks (rowOff + nDefs) (anyNull || newNull) rest
{-# INLINE foldNullableUnboxed #-}

{- | Fold a stream of (values, def-levels, rep-levels) triples into a
repeated (list) 'Column' using Dremel-style level stitching.

The stitching function is selected by @maxRep@:

  * @maxRep == 1@  →  'stitchList'   → @[Maybe [Maybe a]]@
  * @maxRep == 2@  →  'stitchList2'  → @[Maybe [Maybe [Maybe a]]]@
  * @maxRep >= 3@  →  'stitchList3'  → @[Maybe [Maybe [Maybe [Maybe a]]]]@

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
        2 -> fromList (stitchList2 (maxDef - 2) maxDef allReps allDefs allVals)
        3 ->
            fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef allReps allDefs allVals)
        _ -> fromList (stitchList maxDef allReps allDefs allVals)

foldRepeatedUnboxed ::
    forall m a.
    ( RandomAccess m
    , MonadIO m
    , Columnable a
    , VU.Unbox a
    , Columnable (Maybe [Maybe a])
    , Columnable (Maybe [Maybe [Maybe a]])
    , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
    ) =>
    Int ->
    Int ->
    Stream m (VU.Vector a, VU.Vector Int, VU.Vector Int) ->
    m Column
foldRepeatedUnboxed maxRep maxDef stream = do
    chunks <- Stream.toList stream
    let allVals = VB.convert $ VU.concat [vs | (vs, _, _) <- chunks]
        allDefs = VU.concat [ds | (_, ds, _) <- chunks]
        allReps = VU.concat [rs | (_, _, rs) <- chunks]
    return $ case maxRep of
        2 -> fromList (stitchList2 (maxDef - 2) maxDef allReps allDefs allVals)
        3 ->
            fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef allReps allDefs allVals)
        _ -> fromList (stitchList maxDef allReps allDefs allVals)
