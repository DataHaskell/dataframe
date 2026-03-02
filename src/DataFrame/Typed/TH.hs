{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskellQuotes #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Typed.TH (
    -- * Schema inference
    deriveSchema,
    deriveSchemaFromCsvFile,

    -- * Re-export for TH splices
    TypedDataFrame,
    Column,
) where

import Control.Monad.IO.Class
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T

import Language.Haskell.TH

import qualified DataFrame.IO.CSV as D
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Typed.Types (Column, TypedDataFrame)

{- | Generate a type synonym for a schema based on an existing 'DataFrame'.

@
-}

{- $(deriveSchema \"IrisSchema\" irisDF)
-- Generates: type IrisSchema = '[Column \"sepal_length\" Double, ...]
@
-}

deriveSchema :: String -> D.DataFrame -> DecsQ
deriveSchema typeName df = do
    let cols = getSchemaInfo df
    let names = map fst cols
    case findDuplicate names of
        Just dup -> fail $ "Duplicate column name in DataFrame: " ++ T.unpack dup
        Nothing -> pure ()
    colTypes <- mapM mkColumnType cols
    let schemaType = foldr (\t acc -> PromotedConsT `AppT` t `AppT` acc) PromotedNilT colTypes
    let synName = mkName typeName
    pure [TySynD synName [] schemaType]

deriveSchemaFromCsvFile :: String -> String -> DecsQ
deriveSchemaFromCsvFile typeName path = do
    df <- liftIO (D.readCsv path)
    deriveSchema typeName df

getSchemaInfo :: D.DataFrame -> [(T.Text, String)]
getSchemaInfo df =
    let orderedNames =
            map fst $
                L.sortBy (\(_, a) (_, b) -> compare a b) $
                    M.toList (D.columnIndices df)
     in map (\name -> (name, getColumnTypeStr name df)) orderedNames

getColumnTypeStr :: T.Text -> D.DataFrame -> String
getColumnTypeStr name df = case D.getColumn name df of
    Just col -> C.columnTypeString col
    Nothing -> error $ "Column not found: " ++ T.unpack name

mkColumnType :: (T.Text, String) -> Q Type
mkColumnType (name, tyStr) = do
    ty <- parseTypeString tyStr
    let nameLit = LitT (StrTyLit (T.unpack name))
    pure $ ConT ''Column `AppT` nameLit `AppT` ty

parseTypeString :: String -> Q Type
parseTypeString "Int" = pure $ ConT ''Int
parseTypeString "Double" = pure $ ConT ''Double
parseTypeString "Float" = pure $ ConT ''Float
parseTypeString "Bool" = pure $ ConT ''Bool
parseTypeString "Char" = pure $ ConT ''Char
parseTypeString "String" = pure $ ConT ''String
parseTypeString "Text" = pure $ ConT ''T.Text
parseTypeString "Integer" = pure $ ConT ''Integer
parseTypeString s
    | "Maybe " `L.isPrefixOf` s = do
        inner <- parseTypeString (L.drop 6 s)
        pure $ ConT ''Maybe `AppT` inner
parseTypeString s = fail $ "Unsupported column type in schema inference: " ++ s

findDuplicate :: (Eq a) => [a] -> Maybe a
findDuplicate [] = Nothing
findDuplicate (x : xs)
    | x `elem` xs = Just x
    | otherwise = findDuplicate xs
