{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

{- |
Module      : DataFrame.TH.Records
License     : MIT

Record-based 'DataFrame' splices — the IO-agnostic core of the
'DataFrame.TH' family. Splices that read CSV / Parquet files at compile
time live in @DataFrame.TH.CSV@ (in @dataframe-csv-th@) and
@DataFrame.TH.Parquet@ (in @dataframe-parquet-th@).
-}
module DataFrame.TH.Records (
    -- * Declare one binding per column
    declareColumns,
    declareColumnsWithPrefix,
    declareColumnsWithPrefix',

    -- * Type-string parser (exposed for testing)
    typeFromString,
) where

import Data.Function (on)
import Data.Functor ((<&>))
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T

import Control.Monad (forM)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH

import DataFrame.Functions (sanitize)
import DataFrame.Internal.Column (columnTypeString)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    unsafeGetColumn,
 )
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
            val <- valD (varP n) (normalB [| col $(pure $ TypeE ty) $(TH.lift raw) |]) []
            pure [sig, val]
