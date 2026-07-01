{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Persist dataframe expressions and pipelines, and load them back for
inference in another process (or from Python — see "DataFrame.IR.ExprJson" for
the shared wire format).

  * A single 'Expr' round-trips via 'encodeExpr' / 'decodeExprAny' (or the
    type-asserting 'decodeExprAt') and the file helpers below.
  * A /pipeline/ is an ordered @['NamedExpr']@ — the same shape a fitted
    @Transform@ carries. 'encodeNamedExprs' / 'decodeNamedExprs' wrap it in a
    versioned envelope; replay a decoded pipeline for inference with
    @deriveMany@ / @applyTransform@.

All functions are pure 'Either'; the @save@ helpers report an encode failure
before touching the file (no partial write), and the @load@ helpers turn a
missing/unreadable file into a 'Left' rather than throwing.

Limitations inherited from the wire format: @CastWith@ / @CastExprWith@ (they
hold closures) and @collect@ (its list-typed output has no type tag) cannot be
serialized.
-}
module DataFrame.Expr.Serialize (
    -- * Single expressions (re-exported wire codec)
    SomeExpr (..),
    encodeExpr,
    encodeExprToBytes,
    decodeExprAny,
    decodeExprAt,
    decodeExprFromBytes,

    -- * Pipelines
    encodeNamedExprs,
    decodeNamedExprs,

    -- * File IO
    saveExprToFile,
    loadExprFromFile,
    loadExprAtFromFile,
    savePipelineToFile,
    loadPipelineFromFile,
) where

import Control.Exception (IOException, try)
import Control.Monad (when)
import Data.Aeson (object, (.:), (.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Bifunctor (first)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T

import DataFrame.IR.ExprJson (
    SomeExpr (..),
    decodeExprAny,
    decodeExprAt,
    encodeExpr,
    encodeExprToBytes,
    parseSomeExpr,
 )
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr, NamedExpr, UExpr (..))

-- | Decode a single expression from a strict JSON 'BS.ByteString'.
decodeExprFromBytes :: BS.ByteString -> Either String SomeExpr
decodeExprFromBytes bs = Aeson.eitherDecodeStrict bs >>= decodeExprAny

{- | Encode an ordered pipeline (named output expressions) into the versioned
envelope @{ "version": 1, "outputs": [ { "name", "expr" }, … ] }@.
-}
encodeNamedExprs :: [NamedExpr] -> Either String Aeson.Value
encodeNamedExprs nes = do
    outs <- traverse encodeOne nes
    Right $ object ["version" .= (1 :: Int), "outputs" .= outs]
  where
    encodeOne :: NamedExpr -> Either String Aeson.Value
    encodeOne (name, UExpr e) = do
        ev <- encodeExpr e
        Right $ object ["name" .= name, "expr" .= ev]

-- | Decode a pipeline produced by 'encodeNamedExprs'.
decodeNamedExprs :: Aeson.Value -> Either String [NamedExpr]
decodeNamedExprs = Aeson.parseEither parsePipeline
  where
    parsePipeline = Aeson.withObject "Pipeline" $ \o -> do
        ver <- o .: "version" :: Aeson.Parser Int
        when (ver /= 1) $
            fail $
                "DataFrame.Expr.Serialize: unsupported pipeline version " <> show ver
        outs <- o .: "outputs"
        traverse parseOne outs
    parseOne :: Aeson.Value -> Aeson.Parser NamedExpr
    parseOne = Aeson.withObject "PipelineOutput" $ \o -> do
        name <- o .: "name" :: Aeson.Parser T.Text
        rawExpr <- o .: "expr"
        SomeExpr _ e <- parseSomeExpr rawExpr
        return (name, UExpr e)

-- | Encode an expression and write it to a file. No file is written on failure.
saveExprToFile :: (Columnable a) => FilePath -> Expr a -> IO (Either String ())
saveExprToFile fp e = case encodeExprToBytes e of
    Left err -> pure (Left err)
    Right bs -> writeBytes fp bs

-- | Load an expression of unknown output type from a file.
loadExprFromFile :: FilePath -> IO (Either String SomeExpr)
loadExprFromFile fp = (>>= decodeExprFromBytes) <$> readBytes fp

{- | Load an expression from a file, asserting its output type. Fails with
'Left' if the stored expression has a different output type.
-}
loadExprAtFromFile ::
    forall a. (Columnable a) => FilePath -> IO (Either String (Expr a))
loadExprAtFromFile fp = (>>= decode) <$> readBytes fp
  where
    decode bs = Aeson.eitherDecodeStrict bs >>= decodeExprAt @a

-- | Encode a pipeline and write it to a file. No file is written on failure.
savePipelineToFile :: FilePath -> [NamedExpr] -> IO (Either String ())
savePipelineToFile fp nes = case encodeNamedExprs nes of
    Left err -> pure (Left err)
    Right v -> writeBytes fp (BL.toStrict (Aeson.encode v))

-- | Load a pipeline produced by 'savePipelineToFile'.
loadPipelineFromFile :: FilePath -> IO (Either String [NamedExpr])
loadPipelineFromFile fp = (>>= decode) <$> readBytes fp
  where
    decode bs = Aeson.eitherDecodeStrict bs >>= decodeNamedExprs

writeBytes :: FilePath -> BS.ByteString -> IO (Either String ())
writeBytes fp bs = first showIO <$> try (BS.writeFile fp bs)

readBytes :: FilePath -> IO (Either String BS.ByteString)
readBytes fp = first showIO <$> try (BS.readFile fp)

showIO :: IOException -> String
showIO = show
