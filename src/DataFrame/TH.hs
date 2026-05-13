{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

{- |
Module      : DataFrame.TH
License     : MIT

Template Haskell splices for the untyped 'DataFrame' API.

These splices generate top-level @Expr a@ bindings — one per column of a
'DataFrame' — so you can refer to columns by name in GHCi or in code
without writing @F.col \@T \"name\"@ at every use site.

Most users will reach for these via the @:declareColumns@ GHCi macro
provided by @dataframe.ghci@.

@
ghci> :set -XTemplateHaskell
ghci> :declareColumns df
ghci> :type passengers
passengers :: Expr Int
@

The typed-API equivalents (which generate a schema type synonym) live in
"DataFrame.Typed.TH".
-}
module DataFrame.TH (
    -- * Declare one binding per column
    declareColumns,
    declareColumnsWithPrefix,
    declareColumnsWithPrefix',

    -- * From a file
    declareColumnsFromCsvFile,
    declareColumnsFromCsvWithOpts,
    declareColumnsFromParquetFile,

    -- * Type-string parser (exposed for testing)
    typeFromString,
) where

import Control.Monad (filterM, forM)
import Control.Monad.IO.Class (liftIO)
import Data.Function (on)
import Data.Functor ((<&>))
import Data.Int (Int64)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Maybe as Maybe
import qualified Data.Set as S
import qualified Data.Text as T

import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH

import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)

import DataFrame.Functions (sanitize)
import qualified DataFrame.IO.CSV as CSV
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
import DataFrame.Internal.Column (columnTypeString)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    unsafeGetColumn,
 )
import qualified DataFrame.Internal.DataFrame as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Operators (col)
import Prelude as P

typeFromString :: [String] -> Q Type
typeFromString [] = fail "No type specified"
typeFromString [t0] = do
    let t = trim t0
    case stripBrackets t of
        Just inner -> typeFromString [inner] <&> AppT ListT
        Nothing
            | t == "Text" || t == "Data.Text.Text" || t == "T.Text" ->
                pure (ConT ''T.Text)
            | otherwise -> do
                m <- lookupTypeName t
                case m of
                    Just tyName -> pure (ConT tyName)
                    Nothing -> fail $ "Unsupported type: " ++ t0
typeFromString [tycon, t1] = AppT <$> typeFromString [tycon] <*> typeFromString [t1]
typeFromString [tycon, t1, t2] =
    (\outer a b -> AppT (AppT outer a) b)
        <$> typeFromString [tycon]
        <*> typeFromString [t1]
        <*> typeFromString [t2]
typeFromString s = fail $ "Unsupported types: " ++ unwords s

trim :: String -> String
trim = dropWhile (== ' ') . reverse . dropWhile (== ' ') . reverse

stripBrackets :: String -> Maybe String
stripBrackets s =
    case s of
        ('[' : rest)
            | P.not (null rest) && last rest == ']' ->
                Just (init rest)
        _ -> Nothing

{- | Splice a binding for every column of the 'DataFrame' read from a CSV
file. Each binding has type @Expr T@ where @T@ is the inferred column
type.
-}
declareColumnsFromCsvFile :: String -> DecsQ
declareColumnsFromCsvFile path = do
    df <-
        liftIO
            ( CSV.readSeparated
                (CSV.defaultReadOptions{CSV.numColumns = Just 100})
                path
            )
    declareColumns df

-- | Like 'declareColumnsFromCsvFile' but with custom 'CSV.ReadOptions'.
declareColumnsFromCsvWithOpts :: CSV.ReadOptions -> String -> DecsQ
declareColumnsFromCsvWithOpts opts path = do
    df <- liftIO (CSV.readSeparated opts path)
    declareColumns df

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

{- | Splice a binding for every column of @df@, named after the column.
Column names that are not valid Haskell identifiers are sanitized
(see 'DataFrame.Functions.sanitize').
-}
declareColumns :: DataFrame -> DecsQ
declareColumns = declareColumnsWithPrefix' Nothing

-- | Like 'declareColumns' but prefixes every binding name with @prefix_@.
declareColumnsWithPrefix :: T.Text -> DataFrame -> DecsQ
declareColumnsWithPrefix prefix = declareColumnsWithPrefix' (Just prefix)

-- | Like 'declareColumnsWithPrefix' but takes an optional prefix.
declareColumnsWithPrefix' :: Maybe T.Text -> DataFrame -> DecsQ
declareColumnsWithPrefix' prefix df =
    let
        names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs =
            zipWith
                ( \colName type_ ->
                    ( colName
                    , maybe "" (sanitize . (<> "_")) prefix <> sanitize colName
                    , type_
                    )
                )
                names
                types
     in
        fmap concat $ forM specs $ \(raw, nm, tyStr) -> do
            ty <- typeFromString (words tyStr)
            let n = mkName (T.unpack nm)
            sig <- sigD n [t|Expr $(pure ty)|]
            val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
            pure [sig, val]
