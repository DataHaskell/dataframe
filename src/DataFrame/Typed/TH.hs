{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskellQuotes #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Typed.TH (
    -- * Schema inference
    deriveSchema,
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,

    -- * ADT-based schema derivation
    deriveSchemaFromType,
    deriveSchemaFromTypeWith,
    SchemaOptions (..),
    defaultSchemaOptions,
    camelToSnake,

    -- * Re-export for TH splices
    TypedDataFrame,
    Column,
) where

import Control.Monad (when)
import Control.Monad.IO.Class
import Data.Char (isUpper, toLower)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as VB

import Language.Haskell.TH

import qualified DataFrame.IO.CSV as D
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Typed.Record (
    HasSchema,
    Schema,
    fromColumns,
    requireColumn,
    toColumns,
 )
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
deriveSchemaFromCsvFile = deriveSchemaFromCsvFileWith D.defaultReadOptions

deriveSchemaFromCsvFileWith :: D.ReadOptions -> String -> String -> DecsQ
deriveSchemaFromCsvFileWith opts typeName path = do
    df <- liftIO (D.readSeparated opts path)
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

-- | Options controlling 'deriveSchemaFromTypeWith'.
data SchemaOptions = SchemaOptions
    { nameTransform :: String -> String
    -- ^ Map each record selector name to a column name. Default: 'camelToSnake'.
    , schemaTypeName :: Maybe String
    -- ^ Override the generated type synonym name. Default: @\<TypeName\>Schema@.
    , generateInstance :: Bool
    {- ^ When @True@ (default), also generate a 'HasSchema' instance so the
    record can be turned into a 'DataFrame' via 'fromRecords' / 'toRecords'.
    -}
    }

defaultSchemaOptions :: SchemaOptions
defaultSchemaOptions =
    SchemaOptions
        { nameTransform = camelToSnake
        , schemaTypeName = Nothing
        , generateInstance = True
        }

{- | Convert a camelCase identifier to snake_case.

>>> camelToSnake "orderId"
"order_id"
>>> camelToSnake "amountUS"
"amount_u_s"
>>> camelToSnake "region"
"region"
-}
camelToSnake :: String -> String
camelToSnake [] = []
camelToSnake (c : cs) = toLower c : go cs
  where
    go [] = []
    go (x : xs)
        | isUpper x = '_' : toLower x : go xs
        | otherwise = x : go xs

{- | Derive a schema type synonym and a 'HasSchema' instance from a single-record
ADT, using 'defaultSchemaOptions'.

@
data Order = Order
  { orderId :: Int64
  , region  :: Text
  , amount  :: Double
  } deriving (Show, Eq)

\$(deriveSchemaFromType ''Order)

-- expands to:
-- type OrderSchema =
--   '[Column "order_id" Int64, Column "region" Text, Column "amount" Double]
--
-- instance HasSchema Order OrderSchema where
--   toColumns rs = ...
--   fromColumns df = ...
@
-}
deriveSchemaFromType :: Name -> DecsQ
deriveSchemaFromType = deriveSchemaFromTypeWith defaultSchemaOptions

-- | Like 'deriveSchemaFromType' but accepts custom 'SchemaOptions'.
deriveSchemaFromTypeWith :: SchemaOptions -> Name -> DecsQ
deriveSchemaFromTypeWith opts tyName = do
    info <- reify tyName
    (conName, vbts) <- extractRecord tyName info
    when (Prelude.null vbts) $
        fail $
            "deriveSchemaFromType: record "
                ++ show tyName
                ++ " has no fields"
    let fields =
            [ (nameTransform opts (nameBase fName), fName, ty)
            | (fName, _bang, ty) <- vbts
            ]
        colNames = [c | (c, _, _) <- fields]
    case findDuplicate colNames of
        Just dup ->
            fail $
                "deriveSchemaFromType: duplicate transformed column name "
                    ++ show dup
                    ++ " (consider customizing nameTransform via deriveSchemaFromTypeWith)"
        Nothing -> pure ()
    let synName = case schemaTypeName opts of
            Just s -> mkName s
            Nothing -> mkName (nameBase tyName ++ "Schema")
    let columnTypes =
            [ ConT ''Column `AppT` LitT (StrTyLit colName) `AppT` ty
            | (colName, _, ty) <- fields
            ]
        schemaType =
            foldr
                (\t acc -> PromotedConsT `AppT` t `AppT` acc)
                PromotedNilT
                columnTypes
        synDec = TySynD synName [] schemaType
    if generateInstance opts
        then do
            inst <- mkHasSchemaInstance tyName schemaType conName fields
            pure [synDec, inst]
        else pure [synDec]

extractRecord :: Name -> Info -> Q (Name, [VarBangType])
extractRecord _ (TyConI dec) = case dec of
    DataD _ _ _ _ [RecC conName fs] _ -> pure (conName, fs)
    NewtypeD _ _ _ _ (RecC conName fs) _ -> pure (conName, fs)
    DataD _ name _ _ _ _ ->
        fail $
            "deriveSchemaFromType: "
                ++ show name
                ++ " must have exactly one record constructor"
    NewtypeD _ name _ _ _ _ ->
        fail $
            "deriveSchemaFromType: "
                ++ show name
                ++ " newtype must use record syntax"
    other ->
        fail $
            "deriveSchemaFromType: unsupported declaration: " ++ show other
extractRecord tyName _ =
    fail $
        "deriveSchemaFromType: " ++ show tyName ++ " is not a data/newtype declaration"

mkHasSchemaInstance ::
    Name -> Type -> Name -> [(String, Name, Type)] -> Q Dec
mkHasSchemaInstance tyName schemaType conName fields = do
    toClause <- mkToColumnsClause fields
    fromClause <- mkFromColumnsClause conName fields
    let instType = ConT ''HasSchema `AppT` ConT tyName
        schemaInst =
            TySynInstD
                (TySynEqn Nothing (ConT ''Schema `AppT` ConT tyName) schemaType)
    pure $
        InstanceD
            Nothing
            []
            instType
            [ schemaInst
            , FunD 'toColumns [toClause]
            , FunD 'fromColumns [fromClause]
            ]

mkToColumnsClause :: [(String, Name, Type)] -> Q Clause
mkToColumnsClause fields = do
    rs <- newName "rs"
    let mkPair (colName, fieldFn, _ty) =
            let nameE = AppE (VarE 'T.pack) (LitE (StringL colName))
                colE =
                    AppE
                        (VarE 'C.fromList)
                        ( AppE
                            (AppE (VarE 'map) (VarE fieldFn))
                            (VarE rs)
                        )
             in TupE [Just nameE, Just colE]
        listExp = ListE (map mkPair fields)
    pure $ Clause [VarP rs] (NormalB listExp) []

mkFromColumnsClause :: Name -> [(String, Name, Type)] -> Q Clause
mkFromColumnsClause conName fields = do
    df <- newName "df"
    iN <- newName "i"
    nN <- newName "n"
    vNames <- mapM (\k -> newName ("v" ++ show (k :: Int))) [0 .. length fields - 1]
    let mkBind v (colName, _, ty) =
            let nameE = AppE (VarE 'T.pack) (LitE (StringL colName))
                callE =
                    AppE
                        ( AppE
                            (AppTypeE (VarE 'requireColumn) ty)
                            nameE
                        )
                        (VarE df)
             in BindS (VarP v) callE
        binds = zipWith mkBind vNames fields
        firstV = case vNames of
            (v0 : _) -> v0
            [] -> error "mkFromColumnsClause: empty fields (should have failed earlier)"
        lengthBind =
            LetS
                [ ValD
                    (VarP nN)
                    (NormalB (AppE (VarE 'VB.length) (VarE firstV)))
                    []
                ]
        indexE v =
            AppE (AppE (VarE 'VB.unsafeIndex) (VarE v)) (VarE iN)
        elemE =
            foldl
                (\acc v -> AppE acc (indexE v))
                (ConE conName)
                vNames
        nMinus1E =
            InfixE
                (Just (VarE nN))
                (VarE '(-))
                (Just (LitE (IntegerL 1)))
        rangeE = ArithSeqE (FromToR (LitE (IntegerL 0)) nMinus1E)
        compExp = CompE [BindS (VarP iN) rangeE, NoBindS elemE]
        rightE = AppE (ConE 'Right) compExp
        body = DoE Nothing (binds ++ [lengthBind, NoBindS rightE])
    pure $ Clause [VarP df] (NormalB body) []
