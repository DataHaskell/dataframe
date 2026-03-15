{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}

module DataFrame.IO.Unstable.Parquet.PageParser (parsePage) where

import Streamly.Data.Unfold (Unfold)
import qualified Streamly.Internal.Data.Unfold as Unfold
import qualified Data.ByteString as BS
import DataFrame.IO.Unstable.Parquet.Thrift
import DataFrame.IO.Unstable.Parquet.Utils (ColumnDescription(..))
import DataFrame.IO.Parquet (decodePageData, applyLogicalType)
import DataFrame.IO.Parquet.Levels (readLevelsV1, readLevelsV2)
import DataFrame.IO.Parquet.Types (DictVals, parquetTypeFromInt)
import DataFrame.Internal.Column (Columnable, Column(..))
import DataFrame.IO.Utils.RandomAccess (RandomAccess)
import Control.Monad.IO.Class (MonadIO(liftIO))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import Data.Type.Equality (TestEquality(..), (:~:)(Refl))
import Type.Reflection (Typeable, typeRep)

import Debug.Trace

-- | We normalise all decoded column data into a boxed VB.Vector in the inject
-- phase. This avoids carrying a VU.Unbox constraint through the step function,
-- which the outer Columnable constraint does not guarantee. The conversion from
-- VU.Vector to VB.Vector is safe inside the UnboxedColumn GADT match where the
-- Unbox dictionary is in scope.
data PageState a = PageState !(VB.Vector a) !Int !Int

parsePage :: forall r a. (RandomAccess r, MonadIO r, Columnable a, Typeable a) => ColumnDescription -> Unfold r (BS.ByteString, PageHeader, CompressionCodec, Maybe DictVals, Int) a
parsePage description = Unfold.Unfold step inject
  where
    inject :: (BS.ByteString, PageHeader, CompressionCodec, Maybe DictVals, Int) -> r (PageState a)
    inject (pageBytes, header, _codec, dictValsM, pType') = do
      let maxDef = fromIntegral $ maxDefinitionLevel description
          maxRep = fromIntegral $ maxRepetitionLevel description
          -- We do not have type lengths threaded effectively for Fixed Len yet, assume Nothing for now
          -- unless handled correctly.
          logicalType = fmap pinchLogicalTypeToLogicalType $ colLogicalType description
          maybeTypeLen = Nothing
          pType = parquetTypeFromInt . fromIntegral $ pType'

      traceShowM (pType, description, header)
      column <- liftIO $ case unField (ph_data_page_header header) of
        Just dph -> do
          let n = fromIntegral $ unField (dph_num_values dph)
              enc = parquetEncodingFromPinch (unField (dph_encoding dph))
              (defLvls, repLvls, afterLvls) = readLevelsV1 n maxDef maxRep pageBytes
              nPresent = length (filter (== maxDef) defLvls)
          decodePageData dictValsM (maxDef, maxRep) pType maybeTypeLen enc defLvls repLvls nPresent afterLvls "v1"
        Nothing -> case unField (ph_data_page_header_v2 header) of
          Just dph2 -> do
            let n = fromIntegral $ unField (dph2_num_values dph2)
                enc = parquetEncodingFromPinch (unField (dph2_encoding dph2))
                (defLvls, repLvls, afterLvls) = readLevelsV2 n maxDef maxRep (unField $ dph2_definition_levels_byte_length dph2) (unField $ dph2_repetition_levels_byte_length dph2) pageBytes
                nPresent 
                  | unField (dph2_num_nulls dph2) > 0 = fromIntegral (unField (dph2_num_values dph2) - unField (dph2_num_nulls dph2))
                  | otherwise = length (filter (== maxDef) defLvls)
            column <- decodePageData dictValsM (maxDef, maxRep) pType maybeTypeLen enc defLvls repLvls nPresent afterLvls "v2"
            case logicalType of
              Nothing -> return column
              Just lt -> return $ applyLogicalType lt column
          Nothing -> error "Page header is neither v1 nor v2 data page"

      -- Cast the untyped Column to a VB.Vector a.
      -- Inside each GADT branch the relevant constraints (Unbox, etc.) are in
      -- scope, so VG.convert is safe for the UnboxedColumn case.
      return $ case column of
        BoxedColumn (v :: VB.Vector b) ->
          case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> PageState v 0 (VB.length v)
            Nothing   -> error $ "Type mismatch: expected " <> show (typeRep @a) <> ", got " <> show (typeRep @b)
        OptionalColumn (v :: VB.Vector (Maybe b)) ->
          case testEquality (typeRep @a) (typeRep @(Maybe b)) of
            Just Refl -> PageState v 0 (VB.length v)
            Nothing   -> error $ "Type mismatch: expected " <> show (typeRep @a) <> ", got Maybe " <> show (typeRep @b)
        UnboxedColumn (v :: VU.Vector b) ->
          -- Unbox b is in scope here from the GADT; after Refl we have Unbox a
          case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> let boxed = VG.convert v :: VB.Vector a
                         in PageState boxed 0 (VB.length boxed)
            Nothing   -> error $ "Type mismatch: expected " <> show (typeRep @a) <> ", got Unboxed " <> show (typeRep @b)

    step :: (RandomAccess r, MonadIO r) => PageState a -> r (Unfold.Step (PageState a) a)
    step (PageState v idx len)
      | idx >= len = return Unfold.Stop
      | otherwise  = return $ Unfold.Yield (v VB.! idx) (PageState v (idx + 1) len)
