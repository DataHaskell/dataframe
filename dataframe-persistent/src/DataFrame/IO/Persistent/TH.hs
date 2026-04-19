{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Persistent.TH (
    -- * Template Haskell derivation
    deriveEntityToDataFrame,
    deriveDataFrameToEntity,
    derivePersistentDataFrame,
) where

import Control.Monad (forM)
import Data.Char
import qualified Data.Text as T
import qualified Data.Vector as V
import DataFrame.Functions (col)
import DataFrame.IO.Persistent
import DataFrame.Internal.Expression
import Database.Persist
import Language.Haskell.TH
import Language.Haskell.TH.Syntax (lift)

import Debug.Trace (trace)

-- | Derive EntityToDataFrame instance using Template Haskell
deriveEntityToDataFrame :: Name -> Q [Dec]
deriveEntityToDataFrame entityName = do
    info <- reify entityName
    case info of
        TyConI (DataD _ _ _ _ [RecC conName fields] _) -> do
            -- Extract field names and remove entity prefix
            let entityNameStr = nameBase entityName
                entityPrefix = camelToSnake entityNameStr ++ "_"
                fieldNames = [nameBase fname | (fname, _, _) <- fields]
                -- Remove the entity prefix from field names
                cleanFieldNames = map (removePrefix entityPrefix . camelToSnake) fieldNames
                textFieldNames = map T.pack cleanFieldNames

            -- Generate entityColumnNames implementation
            colNamesImpl <- [|$(lift textFieldNames)|]
            let colNamesMethod =
                    FunD
                        'entityColumnNames
                        [Clause [WildP] (NormalB colNamesImpl) []]

            -- Generate entityToColumnData implementation
            entityDataLambda <- generateEntityToColumnData conName fields cleanFieldNames
            let entityVar = mkName "entity"
                entityDataImpl = AppE entityDataLambda (VarE entityVar)
                entityDataMethod =
                    FunD
                        'entityToColumnData
                        [Clause [VarP entityVar] (NormalB entityDataImpl) []]

            -- Create instance
            let instanceDec =
                    InstanceD
                        Nothing
                        []
                        (AppT (ConT ''EntityToDataFrame) (ConT entityName))
                        [colNamesMethod, entityDataMethod]
            dataframeExprs <- forM fields $ \(raw, _, ty) -> do
                let nm = camelToSnake (nameBase raw)
                let colName = camelToSnake (drop (length entityNameStr) (nameBase raw))
                trace (nm <> " :: Expr " <> show ty) pure ()
                let n = mkName nm
                sig <- sigD n [t|Expr $(pure ty)|]
                val <- valD (varP n) (normalB [|col $(lift colName)|]) []
                pure [sig, val]

            return (instanceDec : concat dataframeExprs)
        _ ->
            fail $
                "deriveEntityToDataFrame: " ++ show entityName ++ " must be a record type"

-- | Remove prefix from a string
removePrefix :: String -> String -> String
removePrefix prefix str
    | take (length prefix) str == prefix = drop (length prefix) str
    | otherwise = str

-- | Generate the entityToColumnData expression
generateEntityToColumnData :: Name -> [(Name, Bang, Type)] -> [String] -> Q Exp
generateEntityToColumnData conName fields cleanFieldNames = do
    entityVar <- newName "entity"
    keyVar <- newName "key"
    valVar <- newName "val"

    -- Pattern to destructure Entity
    let entityPat = ConP 'Entity [] [VarP keyVar, VarP valVar]

    -- Generate field extractors
    fieldExprs <-
        mapM (generateFieldExtractor valVar) (zip3 fields cleanFieldNames [0 ..])

    -- Build the final list expression with ID column if needed
    let idExpr =
            TupE
                [ Just (LitE (StringL "id"))
                , Just
                    ( AppE
                        (ConE 'SomeColumn)
                        ( AppE
                            (VarE 'V.singleton)
                            (VarE keyVar)
                        )
                    )
                ]
        allExprs = idExpr : fieldExprs

    -- Return lambda that pattern matches on Entity
    return $ LamE [entityPat] (ListE allExprs)

-- | Generate field extractor for a single field
generateFieldExtractor :: Name -> ((Name, Bang, Type), String, Int) -> Q Exp
generateFieldExtractor valVar ((fieldName, _, fieldType), cleanName, idx) = do
    let accessor = mkName (nameBase fieldName)
        value = AppE (VarE accessor) (VarE valVar)

    -- Wrap the value in SomeColumn after converting to vector
    return $
        TupE
            [ Just (LitE (StringL cleanName))
            , Just
                ( AppE
                    (ConE 'SomeColumn)
                    (AppE (VarE 'V.singleton) value)
                )
            ]

-- | Derive DataFrameToEntity instance
deriveDataFrameToEntity :: Name -> Q [Dec]
deriveDataFrameToEntity entityName = do
    info <- reify entityName
    case info of
        TyConI (DataD _ _ _ _ [RecC conName fields] _) -> do
            -- Generate rowToEntity implementation
            rowToEntityImpl <- generateRowToEntity conName fields
            let rowToEntityMethod =
                    FunD
                        'rowToEntity
                        [ Clause
                            [VarP (mkName "idx"), VarP (mkName "df")]
                            (NormalB rowToEntityImpl)
                            []
                        ]

            -- Create instance
            let instanceDec =
                    InstanceD
                        Nothing
                        []
                        (AppT (ConT ''DataFrameToEntity) (ConT entityName))
                        [rowToEntityMethod]

            return [instanceDec]
        _ ->
            fail $
                "deriveDataFrameToEntity: " ++ show entityName ++ " must be a record type"

-- | Generate the rowToEntity expression
generateRowToEntity :: Name -> [(Name, Bang, Type)] -> Q Exp
generateRowToEntity conName fields = do
    -- This is a simplified implementation
    -- In practice, you'd need to extract values from the DataFrame
    [|Left "Auto-generated rowToEntity not fully implemented"|]

-- | Derive both instances at once
derivePersistentDataFrame :: Name -> Q [Dec]
derivePersistentDataFrame name = do
    entityDecs <- deriveEntityToDataFrame name
    dfDecs <- deriveDataFrameToEntity name
    return (entityDecs ++ dfDecs)

-- | Convert camelCase to snake_case
camelToSnake :: String -> String
camelToSnake [] = []
camelToSnake (c : cs) = toLower c : go cs
  where
    go [] = []
    go (c : cs)
        | isUpper c = '_' : toLower c : go cs
        | otherwise = c : go cs
    isUpper = isAsciiUpper
    toLower c = if isUpper c then toEnum (fromEnum c + 32) else c
