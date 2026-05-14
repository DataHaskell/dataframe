{-# LANGUAGE FlexibleContexts #-}

{- |
Module      : DataFrame.TH.Parquet
License     : MIT

Parquet-file-based 'DataFrame.TH' splices. Splits out the Parquet ingest
path so @dataframe-th@ stays IO-agnostic.
-}
module DataFrame.TH.Parquet (
    declareColumnsFromParquetFile,
) where

import Control.Monad (filterM)
import Control.Monad.IO.Class (liftIO)
import Data.Int (Int64)
import qualified Data.Maybe as Maybe
import qualified Data.Set as S
import qualified Data.Text as T

import Language.Haskell.TH
import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)

import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.IO.Parquet.Schema (schemaToEmptyDataFrame)
import DataFrame.IO.Parquet.Thrift (
    cc_meta_data,
    cmd_path_in_schema,
    cmd_statistics,
    rg_columns,
    row_groups,
    schema,
    stats_null_count,
    unField,
 )
import qualified DataFrame.Internal.DataFrame as DI
import DataFrame.TH.Records (declareColumns)
import Prelude as P

{- | Splice a binding for every column of a parquet file (or directory of
parquet files). The schema is read from each file's metadata and merged.
-}
declareColumnsFromParquetFile :: String -> DecsQ
declareColumnsFromParquetFile path = do
    isDir <- liftIO $ doesDirectoryExist path
    let pat = if isDir then path </> "*.parquet" else path
    matches <- liftIO $ glob pat
    files <- liftIO $ filterM (fmap P.not . doesDirectoryExist) matches
    metas <- liftIO $ mapM Parquet.readMetadataFromPath files
    let nullableCols :: S.Set T.Text
        nullableCols =
            S.fromList
                [ T.pack (last colPath)
                | meta <- metas
                , rg <- unField (row_groups meta)
                , cc <- unField (rg_columns rg)
                , Just cm <- [unField (cc_meta_data cc)]
                , let colPath = map T.unpack (unField (cmd_path_in_schema cm))
                , P.not (null colPath)
                , let nc :: Int64
                      nc = case unField (cmd_statistics cm) of
                        Nothing -> 0
                        Just stats ->
                            Maybe.fromMaybe 0 (unField $ stats_null_count stats)
                , nc > 0
                ]
    let df =
            foldl
                ( \acc meta ->
                    acc
                        <> schemaToEmptyDataFrame
                            nullableCols
                            (unField (schema meta))
                )
                DI.empty
                metas

    declareColumns df
