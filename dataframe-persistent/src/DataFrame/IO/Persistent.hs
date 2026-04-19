{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.IO.Persistent (
    -- * Main functions
    fromPersistent,
    fromPersistentWith,
    toPersistent,

    -- * Configuration
    PersistentConfig (..),
    defaultPersistentConfig,

    -- * Type classes
    EntityToDataFrame (..),
    DataFrameToEntity (..),

    -- * Helper types
    SomeColumn (..),

    -- * Utilities
    entityToColumns,
    columnsToEntity,
    persistFieldsToColumns,
    nRows,
) where

import Control.Monad (forM)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Reader (ReaderT)
import qualified Data.Map.Strict as M
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame.Internal.Column as DFCol
import DataFrame.Internal.DataFrame (DataFrame (..))
import qualified DataFrame.Internal.DataFrame as DF
import Database.Persist
import Database.Persist.Sql hiding (Column)
import Unsafe.Coerce (unsafeCoerce)

-- | Get number of rows in a DataFrame
nRows :: DataFrame -> Int
nRows df = fst (DF.dataframeDimensions df)

-- | Configuration for Persistent DataFrame operations
data PersistentConfig = PersistentConfig
    { pcBatchSize :: Int
    -- ^ Number of records to fetch at once (default: 10000)
    , pcIncludeId :: Bool
    -- ^ Whether to include the entity ID as a column (default: True)
    , pcIdColumnName :: Text
    -- ^ Name for the ID column (default: "id")
    }

-- | Default configuration
defaultPersistentConfig :: PersistentConfig
defaultPersistentConfig =
    PersistentConfig
        { pcBatchSize = 10000
        , pcIncludeId = True
        , pcIdColumnName = "id"
        }

-- | Type class for converting Persistent entities to DataFrame columns
class (PersistEntity record) => EntityToDataFrame record where
    -- | Get column names for the entity
    entityColumnNames :: Proxy record -> [Text]

    -- | Convert entity fields to column data
    entityToColumnData :: Entity record -> [(Text, SomeColumn)]

-- | Type class for converting DataFrame rows back to Persistent entities
class (PersistEntity record) => DataFrameToEntity record where
    -- | Convert a row from DataFrame to entity
    rowToEntity :: Int -> DataFrame -> Either String (Entity record)

-- | Helper type for heterogeneous column data
data SomeColumn = forall a. (DFCol.Columnable a) => SomeColumn (V.Vector a)

-- | Read entities from database into a DataFrame
fromPersistent ::
    forall record m.
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , EntityToDataFrame record
    , Show record
    ) =>
    [Filter record] ->
    ReaderT SqlBackend m DataFrame
fromPersistent = fromPersistentWith @record defaultPersistentConfig

-- | Read entities from database into a DataFrame with custom config
fromPersistentWith ::
    forall record m.
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , EntityToDataFrame record
    , Show record
    ) =>
    PersistentConfig ->
    [Filter record] ->
    ReaderT SqlBackend m DataFrame
fromPersistentWith config filters = do
    entities <- selectList filters []
    liftIO $ entitiesToDataFrame @record config entities

-- | Convert entities to DataFrame
entitiesToDataFrame ::
    forall record.
    (PersistEntity record, EntityToDataFrame record, Show record) =>
    PersistentConfig ->
    [Entity record] ->
    IO DataFrame
entitiesToDataFrame config entities = do
    let proxy = Proxy @record
        baseColNames = entityColumnNames proxy
        colNames =
            if pcIncludeId config
                then pcIdColumnName config : baseColNames
                else baseColNames

    if null entities
        then do
            -- Create empty dataframe with proper column structure
            let indices = M.fromList $ zip colNames [0 ..]
                numCols = length colNames
                dimensions = (0, numCols)
                emptyColumns = replicate numCols (DFCol.fromList ([] :: [Text]))
            return $ DataFrame (V.fromList emptyColumns) indices dimensions M.empty
        else do
            -- Create columns from entity data
            columns <- createColumnsFromEntities @record config entities
            let indices = M.fromList $ zip colNames [0 ..]
                numRows = if null entities then 0 else length entities
                numCols = length colNames
                dimensions = (numRows, numCols)
            return $ DataFrame (V.fromList columns) indices dimensions M.empty

-- | Create columns from entities
createColumnsFromEntities ::
    forall record.
    (PersistEntity record, EntityToDataFrame record, Show record) =>
    PersistentConfig ->
    [Entity record] ->
    IO [DFCol.Column]
createColumnsFromEntities config entities = do
    -- Get column data for all entities
    let allColumnData = map entityToColumnData entities

    -- Determine column names (with or without ID)
    let proxy = Proxy @record
        baseColNames = entityColumnNames proxy
        allColNames =
            if pcIncludeId config
                then pcIdColumnName config : baseColNames
                else baseColNames

    -- For each column name, collect and combine data from all entities
    let columns = map (createColumnFromEntityData allColumnData) allColNames
    return columns

-- | Create a single column by extracting and combining data for a specific column name
createColumnFromEntityData :: [[(Text, SomeColumn)]] -> Text -> DFCol.Column
createColumnFromEntityData allEntityData colName =
    -- Find all matching columns for this name
    case [ someCol
         | entityData <- allEntityData
         , (name, someCol) <- entityData
         , name == colName
         ] of
        [] -> DFCol.fromList ([] :: [Text]) -- Empty column
        allSomeColumns ->
            -- Extract and concatenate vectors from SomeColumn wrappers
            concatSomeColumns allSomeColumns

-- | Helper to concatenate SomeColumn values into a single Column
concatSomeColumns :: [SomeColumn] -> DFCol.Column
concatSomeColumns [] = DFCol.fromList ([] :: [Text])
concatSomeColumns (SomeColumn (firstVec :: V.Vector a) : rest) =
    -- Since all columns for a given field should have the same type,
    -- we can safely extract and concatenate them
    let vectors = firstVec : map extractSameType rest
        extractSameType (SomeColumn v) = unsafeCoerce v :: V.Vector a
        combined = V.concat vectors
     in DFCol.fromVector combined

-- | Write DataFrame to database as entities
toPersistent ::
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , DataFrameToEntity record
    , SafeToInsert record
    ) =>
    DataFrame ->
    ReaderT SqlBackend m [Key record]
toPersistent df = do
    let rowCount = nRows df
    forM [0 .. rowCount - 1] $ \i -> do
        case rowToEntity i df of
            Left err -> error $ "Failed to convert row " <> show i <> ": " <> err
            Right entity -> insert (entityVal entity)

-- | Convert entity fields to columns (helper for implementations)
entityToColumns ::
    (PersistEntity record, ToBackendKey SqlBackend record) =>
    Entity record ->
    [(Text, SomeColumn)]
entityToColumns (Entity key val) =
    [("id", SomeColumn (V.singleton $ fromSqlKey key))]

-- | Convert persistent fields to columns
persistFieldsToColumns ::
    (PersistEntity record) => record -> [(Text, SomeColumn)]
persistFieldsToColumns record =
    let persistValues = toPersistFields record
        -- Get entity definition using proxy - pass the record value to get the correct type
        fieldNames =
            map (unFieldNameHS . fieldHaskell) $ getEntityFields $ entityDef (Just record)
        fieldPairs = zip fieldNames persistValues
     in map persistValueToColumn fieldPairs
  where
    persistValueToColumn :: (Text, PersistValue) -> (Text, SomeColumn)
    persistValueToColumn (name, pval) = (name, persistValueToSomeColumn pval)

-- | Convert a PersistValue to SomeColumn
persistValueToSomeColumn :: PersistValue -> SomeColumn
persistValueToSomeColumn pval = case pval of
    PersistText t -> SomeColumn (V.singleton t)
    PersistByteString bs -> SomeColumn (V.singleton bs)
    PersistInt64 i -> SomeColumn (V.singleton (fromIntegral i :: Int))
    PersistDouble d -> SomeColumn (V.singleton d)
    PersistRational r -> SomeColumn (V.singleton (fromRational r :: Double))
    PersistBool b -> SomeColumn (V.singleton b)
    PersistDay d -> SomeColumn (V.singleton d)
    PersistTimeOfDay tod -> SomeColumn (V.singleton tod)
    PersistUTCTime utc -> SomeColumn (V.singleton utc)
    PersistNull -> SomeColumn (V.singleton ("" :: Text)) -- Handle nulls as empty text
    PersistList vals -> SomeColumn (V.singleton (T.pack $ show vals)) -- Convert list to string representation
    PersistMap m -> SomeColumn (V.singleton (T.pack $ show m)) -- Convert map to string representation
    PersistObjectId oid -> SomeColumn (V.singleton (T.pack $ show oid)) -- Convert ObjectId to string
    PersistArray vals -> SomeColumn (V.singleton (T.pack $ show vals)) -- Convert array to string representation
    PersistLiteral_ _ bs -> SomeColumn (V.singleton bs) -- Use the raw bytes

-- | Convert columns back to entity (helper for implementations)
columnsToEntity ::
    (PersistEntity record) =>
    Int ->
    DataFrame ->
    Either String (Entity record)
columnsToEntity rowIdx df =
    -- This would need to be implemented per entity type
    Left "Not implemented: entity-specific conversion required"
