{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.JSON (
    readJSON,
    readJSONEither,
) where

import Control.Monad (forM)
import Data.Aeson
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString.Lazy as LBS
import Data.Maybe (catMaybes)
import Data.Scientific (toRealFloat)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V

import qualified DataFrame.Internal.Column as D
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Core as D

readJSONEither :: LBS.ByteString -> Either String D.DataFrame
readJSONEither bs = do
    v <- note "Could not decode JSON" (decode @Value bs)
    rows <- toArrayOfObjects v
    let cols :: [Text]
        cols =
            uniq
                . concatMap (map K.toText . KM.keys)
                . V.toList
                $ rows

    columns <- forM cols $ \c -> do
        let col = buildColumn rows c
        pure (c, col)

    pure $ D.fromNamedColumns columns

readJSON :: FilePath -> IO D.DataFrame
readJSON path = do
    contents <- LBS.readFile path
    case readJSONEither contents of
        Left err -> fail $ "readJSON: " <> err
        Right df -> pure df

toArrayOfObjects :: Value -> Either String (V.Vector Object)
toArrayOfObjects (Array xs)
    | V.null xs = Left "Top-level JSON array is empty"
    | otherwise = traverse asObject xs
toArrayOfObjects _ =
    Left "Top-level JSON value must be a JSON array of objects"

asObject :: Value -> Either String Object
asObject (Object o) = Right o
asObject _ = Left "Expected each element of the array to be an object"

uniq :: (Ord a) => [a] -> [a]
uniq = go mempty
  where
    go _ [] = []
    go seen (x : xs)
        | x `elem` seen = go seen xs
        | otherwise = x : go (x : seen) xs

note :: e -> Maybe a -> Either e a
note e = maybe (Left e) Right

data ColType
    = CTString
    | CTNumber
    | CTBool
    | CTArray
    | CTMixed

buildColumn :: V.Vector Object -> Text -> D.Column
buildColumn rows colName =
    let key = K.fromText colName
        values :: V.Vector (Maybe Value)
        values = V.map (KM.lookup key) rows
        colType = detectColType values
     in case colType of
            CTString ->
                D.fromVector (fmap (fmap asText) values)
            CTNumber ->
                D.fromVector (fmap (fmap asDouble) values)
            CTBool ->
                D.fromVector (fmap (fmap asBool) values)
            CTArray ->
                D.fromVector (fmap (fmap asArray) values)
            CTMixed ->
                D.fromVector values

detectColType :: V.Vector (Maybe Value) -> ColType
detectColType vals =
    case nonMissing of
        [] -> CTMixed
        vs
            | all isString vs -> CTString
            | all isNumber vs -> CTNumber
            | all isBool vs -> CTBool
            | all isArray vs -> CTArray
            | otherwise -> CTMixed
  where
    nonMissing = catMaybes (V.toList vals)

    isString (String _) = True
    isString _ = False

    isNumber (Number _) = True
    isNumber _ = False

    isBool (Bool _) = True
    isBool _ = False

    isArray (Array _) = True
    isArray _ = False

asText :: Value -> Text
asText (String s) = s
asText v = T.pack (show v)

asDouble :: Value -> Double
asDouble (Number s) = toRealFloat @Double s
asDouble v = error $ "asDouble: non-number value: " <> show v

asBool :: Value -> Bool
asBool (Bool b) = b
asBool v = error $ "asBool: non-bool value: " <> show v

asArray :: Value -> V.Vector Value
asArray (Array a) = a
asArray v = error $ "asArray: non-array value: " <> show v
