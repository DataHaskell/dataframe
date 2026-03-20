{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Unstable.Parquet.PageParser (parsePage) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import DataFrame.IO.Parquet (applyLogicalType, decodePageData)
import DataFrame.IO.Parquet.Levels (readLevelsV1, readLevelsV2)
import DataFrame.IO.Parquet.Types (parquetTypeFromInt)
import DataFrame.IO.Unstable.Parquet.Thrift
import DataFrame.IO.Unstable.Parquet.Utils (
    ColumnDescription (..),
    PageDescription (..),
 )
import DataFrame.IO.Utils.RandomAccess (RandomAccess)
import DataFrame.Internal.Column (Column)

parsePage ::
    (RandomAccess r, MonadIO r) => ColumnDescription -> PageDescription -> r Column
parsePage description (PageDescription pageBytes header _ dictValsM pType') = do
    let maxDef = fromIntegral $ maxDefinitionLevel description
        maxRep = fromIntegral $ maxRepetitionLevel description
        -- We do not have type lengths threaded effectively for Fixed Len yet, assume Nothing for now
        -- unless handled correctly.
        logicalType = pinchLogicalTypeToLogicalType <$> colLogicalType description
        maybeTypeLen = Nothing
        pType = parquetTypeFromInt . fromIntegral $ pType'

    liftIO $ case unField (ph_data_page_header header) of
        Just dph -> do
            let n = fromIntegral $ unField (dph_num_values dph)
                enc = parquetEncodingFromPinch (unField (dph_encoding dph))
                (defLvls, repLvls, afterLvls) = readLevelsV1 n maxDef maxRep pageBytes
                nPresent = length (filter (== maxDef) defLvls)
            decodePageData
                dictValsM
                (maxDef, maxRep)
                pType
                maybeTypeLen
                enc
                defLvls
                repLvls
                nPresent
                afterLvls
                "v1"
        Nothing -> case unField (ph_data_page_header_v2 header) of
            Just dph2 -> do
                let n = fromIntegral $ unField (dph2_num_values dph2)
                    enc = parquetEncodingFromPinch (unField (dph2_encoding dph2))
                    (defLvls, repLvls, afterLvls) =
                        readLevelsV2
                            n
                            maxDef
                            maxRep
                            (unField $ dph2_definition_levels_byte_length dph2)
                            (unField $ dph2_repetition_levels_byte_length dph2)
                            pageBytes
                    nPresent
                        | unField (dph2_num_nulls dph2) > 0 =
                            fromIntegral (unField (dph2_num_values dph2) - unField (dph2_num_nulls dph2))
                        | otherwise = length (filter (== maxDef) defLvls)
                column <-
                    decodePageData
                        dictValsM
                        (maxDef, maxRep)
                        pType
                        maybeTypeLen
                        enc
                        defLvls
                        repLvls
                        nPresent
                        afterLvls
                        "v2"
                case logicalType of
                    Nothing -> return column
                    Just lt -> return $ applyLogicalType lt column
            Nothing -> error "Page header is neither v1 nor v2 data page"
