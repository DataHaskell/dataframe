{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskellQuotes #-}

{- |
Module      : DataFrame.IO.Persistent.Schema.Common
License     : MIT

Shared options, name mangling, type mapping, and compile-time introspection used
by both the typed-schema splice (@declareTable@) and the entity splice
(@declareEntity@) in "DataFrame.IO.Persistent.Schema".
-}
module DataFrame.IO.Persistent.Schema.Common (
    -- * Options
    DeclareOptions (..),
    defaultDeclareOptions,

    -- * Compile-time introspection
    introspectQ,
    selectColumns,

    -- * Per-column resolution
    resolvedType,
    isNullable,

    -- * Type mapping
    thType,
    persistType,

    -- * Name helpers
    pascalCase,
    schemaTypeNameOf,
    entityNameOf,
    fieldIdentifier,

    -- * Warnings
    warnOnNumeric,
) where

import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.Char (isAlpha, isAlphaNum, toLower, toUpper)
import Data.Either (fromRight)
import Data.List (find, intercalate)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (Day, TimeOfDay, UTCTime)
import Language.Haskell.TH (Q, Type (ConT), reportWarning)
import Language.Haskell.TH.Syntax (addDependentFile, runIO)

import Data.ByteString (ByteString)
import DataFrame.IO.Persistent.Schema.Introspect

{- | Knobs shared by 'DataFrame.IO.Persistent.Schema.declareTable' and
'DataFrame.IO.Persistent.Schema.declareEntity'. Start from
'defaultDeclareOptions' and override fields with record syntax.
-}
data DeclareOptions = DeclareOptions
    { honorNotNull :: Bool
    {- ^ @True@ (default): @NOT NULL@ columns are non-null, others @Maybe@.
    @False@: every column is treated as nullable.
    -}
    , typeOverride :: Text -> Text -> Maybe HaskellType
    {- ^ Given @(rawColumnName, declaredSqliteType)@, optionally override the
    inferred Haskell type. Default: never override.
    -}
    , includeColumns :: Maybe [Text]
    -- ^ Restrict to these raw columns (@Nothing@ = all).
    , excludeColumns :: [Text]
    -- ^ Drop these raw columns (applied after 'includeColumns').
    , schemaTypeName :: Maybe String
    -- ^ Override the generated schema type-synonym name (default @\<Table\>Schema@).
    , entityName :: Maybe String
    -- ^ Override the generated entity name (default PascalCased table).
    }

-- | Sensible defaults: honor @NOT NULL@, no overrides, all columns, derived names.
defaultDeclareOptions :: DeclareOptions
defaultDeclareOptions =
    DeclareOptions
        { honorNotNull = True
        , typeOverride = \_ _ -> Nothing
        , includeColumns = Nothing
        , excludeColumns = []
        , schemaTypeName = Nothing
        , entityName = Nothing
        }

{- | Introspect a table at compile time: register the DB as a dependency (so a
schema change forces a rebuild), then fail helpfully if it can't be read.
-}
introspectQ :: FilePath -> String -> Q [ColumnInfo]
introspectQ path table = do
    addDependentFile path
    outcome <- runIO (try (introspectTable path table))
    case outcome of
        Left (e :: SomeException) -> fail (openError path e)
        Right [] -> failMissingTable path table
        Right cols -> pure cols

failMissingTable :: FilePath -> String -> Q a
failMissingTable path table = do
    names <-
        runIO (try (introspectTableNames path) :: IO (Either SomeException [Text]))
    fail (missingTableMessage path table (fromRight [] names))

openError :: FilePath -> SomeException -> String
openError path e =
    "declareTable/declareEntity: could not read " <> path <> ": " <> show e

missingTableMessage :: FilePath -> String -> [Text] -> String
missingTableMessage path table names =
    "declareTable/declareEntity: table "
        <> show table
        <> " not found in "
        <> path
        <> ".\n  Available tables: "
        <> intercalate ", " (map T.unpack names)
        <> maybe
            ""
            (\s -> "\n  (did you mean " <> show (T.unpack s) <> "?)")
            (didYouMean table names)

didYouMean :: String -> [Text] -> Maybe Text
didYouMean target = find ((== map toLower target) . map toLower . T.unpack)

-- | Apply 'includeColumns' / 'excludeColumns'.
selectColumns :: DeclareOptions -> [ColumnInfo] -> [ColumnInfo]
selectColumns opts = filter keep
  where
    keep c =
        maybe True (ciName c `elem`) (includeColumns opts)
            && ciName c `notElem` excludeColumns opts

-- | The Haskell type for a column, honoring 'typeOverride'.
resolvedType :: DeclareOptions -> ColumnInfo -> HaskellType
resolvedType opts c =
    fromMaybe
        (inferType (ciDeclType c))
        (typeOverride opts (ciName c) (ciDeclType c))

-- | Whether a column should be modelled as nullable.
isNullable :: DeclareOptions -> ColumnInfo -> Bool
isNullable opts c = not (honorNotNull opts) || not (ciNotNull c)

-- | The Template Haskell 'Type' for a 'HaskellType'.
thType :: HaskellType -> Type
thType HTInt = ConT ''Int
thType HTDouble = ConT ''Double
thType HTText = ConT ''Text
thType HTBool = ConT ''Bool
thType HTByteString = ConT ''ByteString
thType HTDay = ConT ''Day
thType HTUTCTime = ConT ''UTCTime
thType HTTimeOfDay = ConT ''TimeOfDay

-- | The @persistent@ DSL type name for a 'HaskellType'.
persistType :: HaskellType -> Text
persistType HTInt = "Int64"
persistType HTDouble = "Double"
persistType HTText = "Text"
persistType HTBool = "Bool"
persistType HTByteString = "ByteString"
persistType HTDay = "Day"
persistType HTUTCTime = "UTCTime"
persistType HTTimeOfDay = "TimeOfDay"

-- | Capitalize the first character (e.g. @"artists" -> "Artists"@).
pascalCase :: Text -> String
pascalCase t = case T.unpack t of
    (c : cs) -> toUpper c : cs
    [] -> []

schemaTypeNameOf :: DeclareOptions -> Text -> String
schemaTypeNameOf opts table = fromMaybe (pascalCase table <> "Schema") (schemaTypeName opts)

entityNameOf :: DeclareOptions -> Text -> String
entityNameOf opts table = fromMaybe (pascalCase table) (entityName opts)

-- | Turn a raw column name into a valid lowercase-initial @persistent@ field name.
fieldIdentifier :: Text -> Text
fieldIdentifier raw = T.pack (lowerFirst (ensureLetter cleaned))
  where
    cleaned = filter (\ch -> isAlphaNum ch || ch == '_') (T.unpack raw)
    lowerFirst (c : cs) = toLower c : cs
    lowerFirst [] = "field"
    ensureLetter s@(c : _) | isAlpha c = s
    ensureLetter s = 'f' : s

-- | Warn (at splice time) about @NUMERIC@/@DECIMAL@ columns mapped to 'Double'.
warnOnNumeric :: [ColumnInfo] -> Q ()
warnOnNumeric = mapM_ warn1
  where
    warn1 c = when (isNumeric (ciDeclType c)) (reportWarning (numericMessage c))
    isNumeric t = any (`T.isInfixOf` T.toUpper t) ["NUMERIC", "DECIMAL"]
    numericMessage c =
        "declareTable: column "
            <> show (T.unpack (ciName c))
            <> " has SQLite type "
            <> show (T.unpack (ciDeclType c))
            <> "; mapping to Double may lose precision "
            <> "(override with typeOverride)."
