{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | JSON wire format for 'Expr' values.

  The Python bindings build expression trees as JSON; this module decodes them
  back into 'Expr' GADTs (typed) and encodes them in the reverse direction
  (used for shipping fitted decision trees back to Python).

  Encoded shape:

  > { "node": "col"   , "out_type": "double", "name": "x" }
  > { "node": "lit"   , "out_type": "int"   , "value": 42 }
  > { "node": "unary" , "out_type": "double", "op": "toDouble"
  >                   , "arg_type": "int"   , "arg": <expr> }
  > { "node": "binary", "out_type": "bool"  , "op": "leq"
  >                   , "arg_type": "double", "lhs": <expr>, "rhs": <expr> }
  > { "node": "if"    , "out_type": "text"  , "cond": <expr>
  >                   , "then": <expr>, "else": <expr> }

  Operator names follow 'binaryName' / 'unaryName' from
  "DataFrame.Internal.Expression". Nullable variants ("nulladd", "nullor", …)
  are accepted on decode and treated as their non-null equivalents — Python
  data crosses the FFI boundary as non-null Arrow buffers, so this is lossless
  for the supported workflow.
-}
module DataFrame.IR.ExprJson (
    SomeExpr (..),
    encodeExpr,
    encodeExprToBytes,
    decodeExprAny,
    decodeExprAt,
    parseSomeExpr,
    typeTagOf,
    withTypeTag,
    withOrdTypeTag,
    withNumTypeTag,
    withFracTypeTag,
    withRealTypeTag,
) where

import Data.Aeson (object, (.:), (.=))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Word (Word16, Word32, Word64, Word8)
import Type.Reflection (TypeRep, Typeable, typeRep)

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    AggStrategy (..),
    BinaryOp (binaryName),
    Expr (..),
    UnaryOp (unaryName),
 )
import qualified DataFrame.Internal.Expression as F
import DataFrame.Operators (
    ifThenElse,
    (.&&.),
    (./=.),
    (.<.),
    (.<=.),
    (.==.),
    (.>.),
    (.>=.),
    (.||.),
 )

{- | Existential wrapper around a typed expression decoded from JSON.
TODO: mchavinda - Maybe consolidate with UExpr from the main package.
-}
data SomeExpr where
    SomeExpr :: (Columnable a) => TypeRep a -> Expr a -> SomeExpr

-- | Map a Haskell type to its wire-format tag string.
typeTagOf :: forall a. (Typeable a) => Maybe T.Text
typeTagOf
    | Just _ <- testEquality (typeRep @a) (typeRep @Int) = Just "int"
    | Just _ <- testEquality (typeRep @a) (typeRep @Int8) = Just "int8"
    | Just _ <- testEquality (typeRep @a) (typeRep @Int16) = Just "int16"
    | Just _ <- testEquality (typeRep @a) (typeRep @Int32) = Just "int32"
    | Just _ <- testEquality (typeRep @a) (typeRep @Int64) = Just "int64"
    | Just _ <- testEquality (typeRep @a) (typeRep @Word) = Just "word"
    | Just _ <- testEquality (typeRep @a) (typeRep @Word8) = Just "word8"
    | Just _ <- testEquality (typeRep @a) (typeRep @Word16) = Just "word16"
    | Just _ <- testEquality (typeRep @a) (typeRep @Word32) = Just "word32"
    | Just _ <- testEquality (typeRep @a) (typeRep @Word64) = Just "word64"
    | Just _ <- testEquality (typeRep @a) (typeRep @Integer) = Just "integer"
    | Just _ <- testEquality (typeRep @a) (typeRep @Double) = Just "double"
    | Just _ <- testEquality (typeRep @a) (typeRep @Float) = Just "float"
    | Just _ <- testEquality (typeRep @a) (typeRep @Bool) = Just "bool"
    | Just _ <- testEquality (typeRep @a) (typeRep @Char) = Just "char"
    | Just _ <- testEquality (typeRep @a) (typeRep @T.Text) = Just "text"
    | Just _ <- testEquality (typeRep @a) (typeRep @String) = Just "string"
    | otherwise = Nothing

-- | Dispatch on a type tag with a 'Columnable' continuation.
withTypeTag ::
    T.Text ->
    (forall a. (Columnable a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withTypeTag t k = case t of
    "int" -> k (Proxy @Int)
    "int8" -> k (Proxy @Int8)
    "int16" -> k (Proxy @Int16)
    "int32" -> k (Proxy @Int32)
    "int64" -> k (Proxy @Int64)
    "word" -> k (Proxy @Word)
    "word8" -> k (Proxy @Word8)
    "word16" -> k (Proxy @Word16)
    "word32" -> k (Proxy @Word32)
    "word64" -> k (Proxy @Word64)
    "integer" -> k (Proxy @Integer)
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    "bool" -> k (Proxy @Bool)
    "char" -> k (Proxy @Char)
    "text" -> k (Proxy @T.Text)
    "string" -> k (Proxy @String)
    _ -> fail $ "DataFrame.IR.ExprJson: unknown type tag: " <> T.unpack t

-- | Subset that supports 'Ord' (for comparison ops).
withOrdTypeTag ::
    T.Text ->
    (forall a. (Columnable a, Ord a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withOrdTypeTag t k = case t of
    "int" -> k (Proxy @Int)
    "int8" -> k (Proxy @Int8)
    "int16" -> k (Proxy @Int16)
    "int32" -> k (Proxy @Int32)
    "int64" -> k (Proxy @Int64)
    "word" -> k (Proxy @Word)
    "word8" -> k (Proxy @Word8)
    "word16" -> k (Proxy @Word16)
    "word32" -> k (Proxy @Word32)
    "word64" -> k (Proxy @Word64)
    "integer" -> k (Proxy @Integer)
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    "bool" -> k (Proxy @Bool)
    "char" -> k (Proxy @Char)
    "text" -> k (Proxy @T.Text)
    "string" -> k (Proxy @String)
    _ ->
        fail $ "DataFrame.IR.ExprJson: type does not support ordering: " <> T.unpack t

-- | Subset that supports 'Num' (arithmetic).
withNumTypeTag ::
    T.Text ->
    (forall a. (Columnable a, Num a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withNumTypeTag t k = case t of
    "int" -> k (Proxy @Int)
    "int8" -> k (Proxy @Int8)
    "int16" -> k (Proxy @Int16)
    "int32" -> k (Proxy @Int32)
    "int64" -> k (Proxy @Int64)
    "word" -> k (Proxy @Word)
    "word8" -> k (Proxy @Word8)
    "word16" -> k (Proxy @Word16)
    "word32" -> k (Proxy @Word32)
    "word64" -> k (Proxy @Word64)
    "integer" -> k (Proxy @Integer)
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    _ ->
        fail $ "DataFrame.IR.ExprJson: type does not support arithmetic: " <> T.unpack t

-- | Subset that supports 'Fractional' (division).
withFracTypeTag ::
    T.Text ->
    (forall a. (Columnable a, Fractional a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withFracTypeTag t k = case t of
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    _ ->
        fail $
            "DataFrame.IR.ExprJson: type does not support fractional division: "
                <> T.unpack t

-- | Subset that supports 'Real' (used by toDouble source types).
withRealTypeTag ::
    T.Text ->
    (forall a. (Columnable a, Real a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withRealTypeTag t k = case t of
    "int" -> k (Proxy @Int)
    "int8" -> k (Proxy @Int8)
    "int16" -> k (Proxy @Int16)
    "int32" -> k (Proxy @Int32)
    "int64" -> k (Proxy @Int64)
    "word" -> k (Proxy @Word)
    "word8" -> k (Proxy @Word8)
    "word16" -> k (Proxy @Word16)
    "word32" -> k (Proxy @Word32)
    "word64" -> k (Proxy @Word64)
    "integer" -> k (Proxy @Integer)
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    _ ->
        fail $
            "DataFrame.IR.ExprJson: type does not support Real (toDouble source): "
                <> T.unpack t

-- | Subset that supports 'Floating'.
withFloatingTypeTag ::
    T.Text ->
    (forall a. (Columnable a, Floating a) => Proxy a -> Aeson.Parser r) ->
    Aeson.Parser r
withFloatingTypeTag t k = case t of
    "double" -> k (Proxy @Double)
    "float" -> k (Proxy @Float)
    _ ->
        fail $ "DataFrame.IR.ExprJson: type does not support Floating: " <> T.unpack t

{- | Encode an 'Expr' to a JSON value. Returns 'Left' on unsupported
constructors (Agg, Over, CastWith, CastExprWith) or unsupported operator
names (binaryUdf, unaryUdf, …).
-}
encodeExpr :: forall a. (Columnable a) => Expr a -> Either String Aeson.Value
encodeExpr expr = case expr of
    Col name -> do
        outTag <- requireTypeTag @a
        Right $
            object
                [ "node" .= ("col" :: T.Text)
                , "out_type" .= outTag
                , "name" .= name
                ]
    Lit v -> do
        outTag <- requireTypeTag @a
        litVal <- encodeLit @a v
        Right $
            object
                [ "node" .= ("lit" :: T.Text)
                , "out_type" .= outTag
                , "value" .= litVal
                ]
    Unary op (arg :: Expr b) -> do
        outTag <- requireTypeTag @a
        argTag <- requireTypeTag @b
        opTag <- recognizeUnary (unaryName op)
        argEnc <- encodeExpr arg
        Right $
            object
                [ "node" .= ("unary" :: T.Text)
                , "out_type" .= outTag
                , "op" .= opTag
                , "arg_type" .= argTag
                , "arg" .= argEnc
                ]
    Binary op (lhs :: Expr c) (rhs :: Expr b) -> do
        outTag <- requireTypeTag @a
        argTag <- requireTypeTag @c
        opTag <- recognizeBinary (binaryName op)
        lEnc <- encodeExpr lhs
        rEnc <- encodeExpr rhs
        Right $
            object
                [ "node" .= ("binary" :: T.Text)
                , "out_type" .= outTag
                , "op" .= opTag
                , "arg_type" .= argTag
                , "lhs" .= lEnc
                , "rhs" .= rEnc
                ]
    If cond th el -> do
        outTag <- requireTypeTag @a
        cEnc <- encodeExpr cond
        tEnc <- encodeExpr th
        eEnc <- encodeExpr el
        Right $
            object
                [ "node" .= ("if" :: T.Text)
                , "out_type" .= outTag
                , "cond" .= cEnc
                , "then" .= tEnc
                , "else" .= eEnc
                ]
    CastWith{} ->
        Left
            "DataFrame.IR.ExprJson.encodeExpr: CastWith is not supported in the wire format"
    CastExprWith{} ->
        Left
            "DataFrame.IR.ExprJson.encodeExpr: CastExprWith is not supported in the wire format"
    Agg (strat :: F.AggStrategy a b) (inner :: F.Expr b) -> do
        outTag <- requireTypeTag @a
        argTag <- requireTypeTag @b
        innerEnc <- encodeExpr inner
        Right $
            object
                [ "node" .= ("agg" :: T.Text)
                , "out_type" .= outTag
                , "agg" .= aggStrategyName strat
                , "arg_type" .= argTag
                , "arg" .= innerEnc
                ]
    Over names inner -> do
        outTag <- requireTypeTag @a
        innerEnc <- encodeExpr inner
        Right $
            object
                [ "node" .= ("over" :: T.Text)
                , "out_type" .= outTag
                , "partition_by" .= names
                , "arg" .= innerEnc
                ]
  where
    requireTypeTag :: forall x. (Typeable x) => Either String T.Text
    requireTypeTag = case typeTagOf @x of
        Just t -> Right t
        Nothing ->
            Left $
                "DataFrame.IR.ExprJson.encodeExpr: unsupported type: " <> show (typeRep @x)

-- | Wire-format name for an aggregation strategy.
aggStrategyName :: AggStrategy a b -> T.Text
aggStrategyName (CollectAgg n _) = n
aggStrategyName (FoldAgg n _ _) = n
aggStrategyName (MergeAgg n _ _ _ _) = n

-- | Encode an 'Expr' to a strict 'BS.ByteString' of JSON.
encodeExprToBytes ::
    forall a. (Columnable a) => Expr a -> Either String BS.ByteString
encodeExprToBytes e = BL.toStrict . Aeson.encode <$> encodeExpr e

encodeLit :: forall a. (Columnable a) => a -> Either String Aeson.Value
encodeLit v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        Right $ Aeson.toJSON (v :: Int)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int8) =
        Right $ Aeson.toJSON (v :: Int8)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int16) =
        Right $ Aeson.toJSON (v :: Int16)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int32) =
        Right $ Aeson.toJSON (v :: Int32)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int64) =
        Right $ Aeson.toJSON (v :: Int64)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word) =
        Right $ Aeson.toJSON (v :: Word)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word8) =
        Right $ Aeson.toJSON (v :: Word8)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word16) =
        Right $ Aeson.toJSON (v :: Word16)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word32) =
        Right $ Aeson.toJSON (v :: Word32)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word64) =
        Right $ Aeson.toJSON (v :: Word64)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Integer) =
        Right $ Aeson.toJSON (v :: Integer)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        Right $ Aeson.toJSON (v :: Double)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Float) =
        Right $ Aeson.toJSON (v :: Float)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Bool) =
        Right $ Aeson.toJSON (v :: Bool)
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) =
        Right $ Aeson.toJSON (v :: T.Text)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Char) =
        Right $ Aeson.toJSON [v :: Char]
    | Just Refl <- testEquality (typeRep @a) (typeRep @String) =
        Right $ Aeson.toJSON (v :: String)
    | otherwise =
        Left $
            "DataFrame.IR.ExprJson.encodeLit: unsupported type: " <> show (typeRep @a)

-- | Map a 'binaryName' string to a wire-format op name. Errors on opaque UDF names.
recognizeBinary :: T.Text -> Either String T.Text
recognizeBinary n = case n of
    "add" -> Right "add"
    "sub" -> Right "sub"
    "mult" -> Right "mult"
    "divide" -> Right "divide"
    "eq" -> Right "eq"
    "neq" -> Right "neq"
    "lt" -> Right "lt"
    "leq" -> Right "leq"
    "gt" -> Right "gt"
    "geq" -> Right "geq"
    "and" -> Right "and"
    "or" -> Right "or"
    -- Nullable-aware aliases (lossless on non-null inputs)
    "nulladd" -> Right "add"
    "nullsub" -> Right "sub"
    "nullmul" -> Right "mult"
    "nulldiv" -> Right "divide"
    "nulland" -> Right "and"
    "nullor" -> Right "or"
    "exponentiate" -> Right "exponentiate"
    "logBase" -> Right "logBase"
    "div" -> Right "div"
    "mod" -> Right "mod"
    "pow" -> Right "pow"
    other ->
        Left $
            "DataFrame.IR.ExprJson: unsupported binary op (cannot serialize): "
                <> T.unpack other

-- | Map a 'unaryName' string to a wire-format op name. Errors on opaque UDF names.
recognizeUnary :: T.Text -> Either String T.Text
recognizeUnary n = case n of
    "not" -> Right "not"
    "negate" -> Right "negate"
    "abs" -> Right "abs"
    "signum" -> Right "signum"
    "exp" -> Right "exp"
    "sqrt" -> Right "sqrt"
    "log" -> Right "log"
    "sin" -> Right "sin"
    "cos" -> Right "cos"
    "tan" -> Right "tan"
    "asin" -> Right "asin"
    "acos" -> Right "acos"
    "atan" -> Right "atan"
    "sinh" -> Right "sinh"
    "cosh" -> Right "cosh"
    "asinh" -> Right "asinh"
    "acosh" -> Right "acosh"
    "atanh" -> Right "atanh"
    "toDouble" -> Right "toDouble"
    other ->
        Left $
            "DataFrame.IR.ExprJson: unsupported unary op (cannot serialize): "
                <> T.unpack other

{- | Decode a JSON value into a 'SomeExpr'. The output type comes from the
@out_type@ field on the root node.
-}
decodeExprAny :: Aeson.Value -> Either String SomeExpr
decodeExprAny = Aeson.parseEither parseSomeExpr

-- | Decode a JSON value, asserting the result type matches @a@.
decodeExprAt ::
    forall a. (Columnable a) => Aeson.Value -> Either String (Expr a)
decodeExprAt v = do
    SomeExpr trep expr <- decodeExprAny v
    case testEquality trep (typeRep @a) of
        Just Refl -> Right expr
        Nothing ->
            Left $
                "DataFrame.IR.ExprJson.decodeExprAt: expected "
                    <> show (typeRep @a)
                    <> " but got "
                    <> show trep

-- | The Aeson.Parser entry point — useful when composing with bigger parsers.
parseSomeExpr :: Aeson.Value -> Aeson.Parser SomeExpr
parseSomeExpr = Aeson.withObject "Expr" $ \o -> do
    node <- o .: "node" :: Aeson.Parser T.Text
    outType <- o .: "out_type" :: Aeson.Parser T.Text
    case node of
        "col" -> do
            name <- o .: "name" :: Aeson.Parser T.Text
            withTypeTag outType $ \(_ :: Proxy a) ->
                return $ SomeExpr (typeRep @a) (Col @a name)
        "lit" -> do
            rawVal <- o .: "value"
            withTypeTag outType $ \(_ :: Proxy a) -> do
                litVal <- decodeLit @a rawVal
                return $ SomeExpr (typeRep @a) (Lit @a litVal)
        "if" -> do
            rawCond <- o .: "cond"
            rawThen <- o .: "then"
            rawElse <- o .: "else"
            cond <- parseExprAt @Bool rawCond
            withTypeTag outType $ \(_ :: Proxy a) -> do
                thenE <- parseExprAt @a rawThen
                elseE <- parseExprAt @a rawElse
                return $ SomeExpr (typeRep @a) (ifThenElse cond thenE elseE)
        "unary" -> do
            op <- o .: "op" :: Aeson.Parser T.Text
            argType <- o .: "arg_type" :: Aeson.Parser T.Text
            rawArg <- o .: "arg"
            parseUnary op outType argType rawArg
        "binary" -> do
            op <- o .: "op" :: Aeson.Parser T.Text
            argType <- o .: "arg_type" :: Aeson.Parser T.Text
            rawLhs <- o .: "lhs"
            rawRhs <- o .: "rhs"
            parseBinary op outType argType rawLhs rawRhs
        other -> fail $ "DataFrame.IR.ExprJson: unknown node kind: " <> T.unpack other

parseExprAt :: forall a. (Columnable a) => Aeson.Value -> Aeson.Parser (Expr a)
parseExprAt v = do
    SomeExpr trep expr <- parseSomeExpr v
    case testEquality trep (typeRep @a) of
        Just Refl -> return expr
        Nothing ->
            fail $
                "DataFrame.IR.ExprJson: expected "
                    <> show (typeRep @a)
                    <> " but got "
                    <> show trep

decodeLit :: forall a. (Columnable a) => Aeson.Value -> Aeson.Parser a
decodeLit v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = parseAs @Int v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int8) = parseAs @Int8 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int16) = parseAs @Int16 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int32) = parseAs @Int32 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int64) = parseAs @Int64 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word) = parseAs @Word v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word8) = parseAs @Word8 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word16) = parseAs @Word16 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word32) = parseAs @Word32 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Word64) = parseAs @Word64 v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Integer) = parseAs @Integer v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = parseAs @Double v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Float) = parseAs @Float v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Bool) = parseAs @Bool v
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) = parseAs @T.Text v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Char) = parseChar v
    | Just Refl <- testEquality (typeRep @a) (typeRep @String) = parseAs @String v
    | otherwise =
        fail $
            "DataFrame.IR.ExprJson.decodeLit: unsupported type: " <> show (typeRep @a)

parseAs :: forall b. (Aeson.FromJSON b) => Aeson.Value -> Aeson.Parser b
parseAs = Aeson.parseJSON

parseChar :: Aeson.Value -> Aeson.Parser Char
parseChar v = do
    s <- Aeson.parseJSON v :: Aeson.Parser T.Text
    case T.unpack s of
        [c] -> return c
        _ ->
            fail $
                "DataFrame.IR.ExprJson: expected single-character string, got: " <> show s

requireTag :: T.Text -> T.Text -> Aeson.Parser ()
requireTag actual expected
    | actual == expected = return ()
    | otherwise =
        fail $
            "DataFrame.IR.ExprJson: type mismatch — expected "
                <> T.unpack expected
                <> " but got "
                <> T.unpack actual

requireSame :: T.Text -> T.Text -> Aeson.Parser ()
requireSame a b
    | a == b = return ()
    | otherwise =
        fail $
            "DataFrame.IR.ExprJson: type mismatch — "
                <> T.unpack a
                <> " vs "
                <> T.unpack b

parseUnary ::
    T.Text -> T.Text -> T.Text -> Aeson.Value -> Aeson.Parser SomeExpr
parseUnary op outType argType rawArg = case op of
    "not" -> do
        requireTag outType "bool"
        requireTag argType "bool"
        arg <- parseExprAt @Bool rawArg
        return $ SomeExpr (typeRep @Bool) (F.not arg)
    "negate" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        arg <- parseExprAt @a rawArg
        return $ SomeExpr (typeRep @a) (negate arg)
    "abs" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        arg <- parseExprAt @a rawArg
        return $ SomeExpr (typeRep @a) (abs arg)
    "signum" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        arg <- parseExprAt @a rawArg
        return $ SomeExpr (typeRep @a) (signum arg)
    "toDouble" -> do
        requireTag outType "double"
        withRealTypeTag argType $ \(_ :: Proxy a) -> do
            arg <- parseExprAt @a rawArg
            return $ SomeExpr (typeRep @Double) (F.toDouble arg)
    "exp" -> floatingUnary outType argType rawArg exp
    "sqrt" -> floatingUnary outType argType rawArg sqrt
    "log" -> floatingUnary outType argType rawArg log
    "sin" -> floatingUnary outType argType rawArg sin
    "cos" -> floatingUnary outType argType rawArg cos
    "tan" -> floatingUnary outType argType rawArg tan
    "asin" -> floatingUnary outType argType rawArg asin
    "acos" -> floatingUnary outType argType rawArg acos
    "atan" -> floatingUnary outType argType rawArg atan
    "sinh" -> floatingUnary outType argType rawArg sinh
    "cosh" -> floatingUnary outType argType rawArg cosh
    "asinh" -> floatingUnary outType argType rawArg asinh
    "acosh" -> floatingUnary outType argType rawArg acosh
    "atanh" -> floatingUnary outType argType rawArg atanh
    other -> fail $ "DataFrame.IR.ExprJson: unsupported unary op: " <> T.unpack other

floatingUnary ::
    T.Text ->
    T.Text ->
    Aeson.Value ->
    (forall x. (Columnable x, Floating x) => Expr x -> Expr x) ->
    Aeson.Parser SomeExpr
floatingUnary outType argType rawArg f = withFloatingTypeTag outType $ \(_ :: Proxy a) -> do
    requireSame outType argType
    arg <- parseExprAt @a rawArg
    return $ SomeExpr (typeRep @a) (f arg)

parseBinary ::
    T.Text ->
    T.Text ->
    T.Text ->
    Aeson.Value ->
    Aeson.Value ->
    Aeson.Parser SomeExpr
parseBinary op outType argType rawLhs rawRhs = case op of
    "eq" -> do
        requireTag outType "bool"
        withTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l .==. r)
    "neq" -> do
        requireTag outType "bool"
        withTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l ./=. r)
    "lt" -> do
        requireTag outType "bool"
        withOrdTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l .<. r)
    "leq" -> do
        requireTag outType "bool"
        withOrdTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l .<=. r)
    "gt" -> do
        requireTag outType "bool"
        withOrdTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l .>. r)
    "geq" -> do
        requireTag outType "bool"
        withOrdTypeTag argType $ \(_ :: Proxy a) -> do
            l <- parseExprAt @a rawLhs
            r <- parseExprAt @a rawRhs
            return $ SomeExpr (typeRep @Bool) (l .>=. r)
    "and" -> do
        requireTag outType "bool"
        l <- parseExprAt @Bool rawLhs
        r <- parseExprAt @Bool rawRhs
        return $ SomeExpr (typeRep @Bool) (l .&&. r)
    "or" -> do
        requireTag outType "bool"
        l <- parseExprAt @Bool rawLhs
        r <- parseExprAt @Bool rawRhs
        return $ SomeExpr (typeRep @Bool) (l .||. r)
    "add" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        l <- parseExprAt @a rawLhs
        r <- parseExprAt @a rawRhs
        return $ SomeExpr (typeRep @a) (l + r)
    "sub" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        l <- parseExprAt @a rawLhs
        r <- parseExprAt @a rawRhs
        return $ SomeExpr (typeRep @a) (l - r)
    "mult" -> withNumTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        l <- parseExprAt @a rawLhs
        r <- parseExprAt @a rawRhs
        return $ SomeExpr (typeRep @a) (l * r)
    "divide" -> withFracTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        l <- parseExprAt @a rawLhs
        r <- parseExprAt @a rawRhs
        return $ SomeExpr (typeRep @a) (l / r)
    "exponentiate" -> withFloatingTypeTag outType $ \(_ :: Proxy a) -> do
        requireSame outType argType
        l <- parseExprAt @a rawLhs
        r <- parseExprAt @a rawRhs
        return $ SomeExpr (typeRep @a) (l ** r)
    "nulladd" -> parseBinary "add" outType argType rawLhs rawRhs
    "nullsub" -> parseBinary "sub" outType argType rawLhs rawRhs
    "nullmul" -> parseBinary "mult" outType argType rawLhs rawRhs
    "nulldiv" -> parseBinary "divide" outType argType rawLhs rawRhs
    "nulland" -> parseBinary "and" outType argType rawLhs rawRhs
    "nullor" -> parseBinary "or" outType argType rawLhs rawRhs
    other -> fail $ "DataFrame.IR.ExprJson: unsupported binary op: " <> T.unpack other
