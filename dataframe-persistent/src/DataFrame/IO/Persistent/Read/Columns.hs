{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
-- 'buildNullableColumn' deliberately constrains @Columnable (Maybe a)@ (needed to
-- build a bitmap-backed nullable column); GHC flags that as simplifiable.
{-# OPTIONS_GHC -Wno-simplifiable-class-constraints #-}

{- |
Module      : DataFrame.IO.Persistent.Read.Columns
License     : MIT

Turning rows of 'PersistValue's into typed dataframe 'Column's: decoders driven
by a known 'HaskellType', value-sniffing inference for unknown columns, and the
typed-schema boundary. Shared by the runtime readers and the generated readers.
-}
module DataFrame.IO.Persistent.Read.Columns (
    ColumnReader,
    buildColumn,
    buildNullableColumn,
    columnReaderFor,
    inferColumn,
    freezeOrThrow,
) where

import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.List (find)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (Day, TimeOfDay, UTCTime)
import Data.Typeable (Proxy (..), typeRep)
import Database.Persist (PersistField (fromPersistValue), PersistValue (..))

import DataFrame.IO.Persistent.Schema.Introspect (HaskellType (..))
import DataFrame.Internal.Column (Column, Columnable, fromList)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Typed.Freeze (freezeWithError)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

-- | A function that turns one column's worth of 'PersistValue's into a 'Column'.
type ColumnReader = [PersistValue] -> Column

-- | Build a non-null column, decoding each value with 'fromPersistValue'.
buildColumn :: forall a -> (Columnable a, PersistField a) => ColumnReader
buildColumn a pvs = case traverse fromPersistValue pvs of
    Left err -> error (decodeError (typeName a) err)
    Right (xs :: [a]) -> fromList xs

-- | Build a nullable column (@PersistNull@ becomes @Nothing@).
buildNullableColumn ::
    forall a -> (Columnable (Maybe a), PersistField a) => ColumnReader
buildNullableColumn a pvs = case traverse fromPersistValue pvs of
    Left err -> error (decodeError (typeName (Maybe a)) err)
    Right (xs :: [Maybe a]) -> fromList xs

{- | Infer a column's type by sniffing the first non-null value; nullable if any
value is @PersistNull@. Used for arbitrary queries and generic entity loads.
-}
inferColumn :: [PersistValue] -> Column
inferColumn pvs = columnReaderFor (inferHaskellType pvs) (any isNull pvs) pvs

columnReaderFor :: HaskellType -> Bool -> ColumnReader
columnReaderFor ht True = nullableReaderFor ht
columnReaderFor ht False = readerFor ht

readerFor :: HaskellType -> ColumnReader
readerFor HTInt = buildColumn Int
readerFor HTDouble = buildColumn Double
readerFor HTText = buildColumn Text
readerFor HTBool = buildColumn Bool
readerFor HTByteString = buildColumn ByteString
readerFor HTDay = buildColumn Day
readerFor HTUTCTime = buildColumn UTCTime
readerFor HTTimeOfDay = buildColumn TimeOfDay

nullableReaderFor :: HaskellType -> ColumnReader
nullableReaderFor HTInt = buildNullableColumn Int
nullableReaderFor HTDouble = buildNullableColumn Double
nullableReaderFor HTText = buildNullableColumn Text
nullableReaderFor HTBool = buildNullableColumn Bool
nullableReaderFor HTByteString = buildNullableColumn ByteString
nullableReaderFor HTDay = buildNullableColumn Day
nullableReaderFor HTUTCTime = buildNullableColumn UTCTime
nullableReaderFor HTTimeOfDay = buildNullableColumn TimeOfDay

inferHaskellType :: [PersistValue] -> HaskellType
inferHaskellType = maybe HTText haskellTypeOf . find (not . isNull)

haskellTypeOf :: PersistValue -> HaskellType
haskellTypeOf (PersistInt64 _) = HTInt
haskellTypeOf (PersistDouble _) = HTDouble
haskellTypeOf (PersistBool _) = HTBool
haskellTypeOf (PersistByteString _) = HTByteString
haskellTypeOf (PersistDay _) = HTDay
haskellTypeOf (PersistUTCTime _) = HTUTCTime
haskellTypeOf (PersistTimeOfDay _) = HTTimeOfDay
haskellTypeOf _ = HTText

isNull :: PersistValue -> Bool
isNull PersistNull = True
isNull _ = False

typeName :: forall a -> (Columnable a) => String
typeName a = show (typeRep (Proxy @a))

decodeError :: String -> Text -> String
decodeError ty err = "buildColumn: failed to decode column as " <> ty <> ": " <> T.unpack err

{- | Validate an untyped 'DataFrame' against a typed schema and wrap it, throwing
an 'IOError' on mismatch. The generated @declareTable@ readers call this; the
schema came from the same database, so success is the normal case.
-}
freezeOrThrow ::
    forall cols m.
    (KnownSchema cols, MonadIO m) =>
    DataFrame -> m (TypedDataFrame cols)
freezeOrThrow df =
    either (liftIO . ioError . userError . T.unpack) pure (freezeWithError @cols df)
