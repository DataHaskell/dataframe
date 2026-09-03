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

    -- * Declare a runtime schema alongside the column bindings
    declareSchemaValues,

    -- * Type-string parser (exposed for testing)
    typeFromString,
) where

import qualified Data.Char as Char
import Data.Function (on)
import Data.Functor ((<&>))
import Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import Data.Time (
    Day,
    DiffTime,
    LocalTime,
    NominalDiffTime,
    TimeOfDay,
    UTCTime,
    ZonedTime,
 )
import Data.Word (Word16, Word32, Word64, Word8)

import Control.Monad (forM)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH

import DataFrame.Expression.Operators (col)
import DataFrame.Functions (sanitize)
import DataFrame.Internal.Column (columnTypeString)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Schema.TH (schemaTypeE)
import DataFrame.Schema (Schema, makeSchema)
import Prelude as P

knownTypeNames :: M.Map String Name
knownTypeNames =
    M.fromList
        [ ("Bool", ''Bool)
        , ("Char", ''Char)
        , ("Day", ''Day)
        , ("DiffTime", ''DiffTime)
        , ("Double", ''Double)
        , ("Either", ''Either)
        , ("Float", ''Float)
        , ("Int", ''Int)
        , ("Int16", ''Int16)
        , ("Int32", ''Int32)
        , ("Int64", ''Int64)
        , ("Int8", ''Int8)
        , ("Integer", ''Integer)
        , ("LocalTime", ''LocalTime)
        , ("Maybe", ''Maybe)
        , ("NominalDiffTime", ''NominalDiffTime)
        , ("Ordering", ''Ordering)
        , ("TimeOfDay", ''TimeOfDay)
        , ("UTCTime", ''UTCTime)
        , ("Word", ''Word)
        , ("Word16", ''Word16)
        , ("Word32", ''Word32)
        , ("Word64", ''Word64)
        , ("Word8", ''Word8)
        , ("ZonedTime", ''ZonedTime)
        ]

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
                    Nothing -> case M.lookup t knownTypeNames of
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

columnSpecs :: DataFrame -> Q [(T.Text, Type)]
columnSpecs df =
    let names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
     in forM (zip names types) $ \(colName, tyStr) ->
            (,) colName <$> typeFromString (words tyStr)

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
declareColumnsWithPrefix' prefix df = do
    specs <- columnSpecs df
    fmap concat $ forM specs $ \(raw, ty) -> do
        let nm = maybe "" (sanitize . (<> "_")) prefix <> sanitize raw
            n = mkName (T.unpack nm)
        sig <- sigD n [t|Expr $(pure ty)|]
        val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
        pure [sig, val]

{- | Splice a runtime 'Schema' for @df@, along with 'Expr' refs
all named @<prefix><item>@.

The schema is bound to @\<prefix\>Schema@ and each accessor to
@\<prefix\>\<ColumnName\>@, where the column name is camel-cased.

@
\$(declareSchemaValues "housing" df)
-- emits:
--   housingSchema       :: Schema
--   housingSchema       = makeSchema [("median_income", SType (Proxy :: Proxy Double)), ...]
--   housingMedianIncome :: Expr Double
--   housingMedianIncome = col "median_income"
@
-}
declareSchemaValues :: String -> DataFrame -> DecsQ
declareSchemaValues prefix df = do
    validatePrefix prefix
    specs <- columnSpecs df
    let n = mkName (prefix ++ "Schema")
        entry (raw, ty) =
            TupE
                [ Just (AppE (VarE 'T.pack) (LitE (StringL (T.unpack raw))))
                , Just (schemaTypeE ty)
                ]
    sig <- sigD n [t|Schema|]
    val <-
        valD
            (varP n)
            (normalB (pure (AppE (VarE 'makeSchema) (ListE (map entry specs)))))
            []
    accessors <- fmap concat $ forM specs $ \(raw, ty) -> do
        let an = mkName (accessorName prefix raw)
        asig <- sigD an [t|Expr $(pure ty)|]
        aval <- valD (varP an) (normalB [|col $(TH.lift raw)|]) []
        pure [asig, aval]
    pure (sig : val : accessors)

validatePrefix :: String -> Q ()
validatePrefix prefix = case prefix of
    (c : _) | Char.isLower c || c == '_' -> pure ()
    _ ->
        fail $
            "declareSchemaValues: prefix "
                ++ show prefix
                ++ " must start with a lowercase letter or underscore"

accessorName :: String -> T.Text -> String
accessorName prefix raw =
    prefix ++ upperFirst (camelCase (T.unpack (sanitize raw)))

camelCase :: String -> String
camelCase s = case P.filter (P.not . P.null) (splitOn '_' s) of
    [] -> s
    (w : ws) -> w ++ P.concatMap upperFirst ws

splitOn :: Char -> String -> [String]
splitOn c = P.foldr step [[]]
  where
    step _ [] = []
    step x acc@(cur : rest)
        | x == c = [] : acc
        | otherwise = (x : cur) : rest

upperFirst :: String -> String
upperFirst [] = []
upperFirst (c : cs) = Char.toUpper c : cs
