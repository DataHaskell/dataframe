{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.FFI where

import Control.Exception (SomeException, try)
import qualified Data.Aeson as Aeson
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Type.Equality (
    type (:~~:) (HRefl),
 )
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import Foreign (Ptr, castPtr, copyBytes, mallocBytes, poke, ptrToWordPtr)
import Foreign.C.String (CString)
import Foreign.C.Types (CChar, CInt (..))
import Foreign.Marshal.Alloc (free)
import System.IO (hPrint, stderr)
import Type.Reflection (SomeTypeRep (..), eqTypeRep, typeRep)

import DataFrame.DecisionTree (
    SynthConfig (..),
    TreeConfig (..),
    defaultColumnOrdering,
    defaultSynthConfig,
    defaultTreeConfig,
    fit,
    predict,
 )
import DataFrame.IO.Arrow (dataframeToArrow)
import DataFrame.IO.CSV.Fast (fastReadCsvWithOpts)
import DataFrame.IR (PlanNode, executePlan)
import DataFrame.IR.ExprJson (encodeExprToBytes)
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    materializePacked,
    mergedHead,
 )
import DataFrame.Internal.DataFrame (DataFrame, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (Col))

foreign export ccall "dfExecutePlan"
    dfExecutePlan :: CString -> Ptr Word64 -> Ptr Word64 -> IO CInt

{- | Execute a JSON-encoded query plan, returning Arrow C Data Interface
pointers to Python via the @schemaOut@/@arrayOut@ output parameters (each an
address as a 'Word64'). Returns 0 on success, -1 on error.
-}
dfExecutePlan :: CString -> Ptr Word64 -> Ptr Word64 -> IO CInt
dfExecutePlan planCS schemaOut arrayOut = do
    planBytes <- BS.packCString planCS
    result <- try @SomeException $ do
        node <-
            either
                fail
                return
                (Aeson.eitherDecode (BL.fromStrict planBytes))
        df <- executePlan fastReadCsvWithOpts node
        (sPtr, aPtr) <- dataframeToArrow df
        poke schemaOut (fromIntegral (ptrToWordPtr (castPtr sPtr)))
        poke arrayOut (fromIntegral (ptrToWordPtr (castPtr aPtr)))
    case result of
        Left ex -> hPrint stderr ex >> return (CInt (-1))
        Right _ -> return (CInt 0)

foreign export ccall "dfFitDecisionTree"
    dfFitDecisionTree ::
        CString ->
        CString ->
        CString ->
        CString ->
        Ptr (Ptr CChar) ->
        Ptr Word64 ->
        IO CInt

foreign export ccall "dfFreeModel"
    dfFreeModel :: Ptr CChar -> IO ()

{- | Fit a TAO decision tree on a plan's materialized result, predicting the
target column from the rest (@target_type@: @"auto"@ or a wire type tag;
@config_json@: 'TreeConfig' fields). Caller frees via 'dfFreeModel'; 0 ok, -1 err.
-}
dfFitDecisionTree ::
    CString ->
    CString ->
    CString ->
    CString ->
    Ptr (Ptr CChar) ->
    Ptr Word64 ->
    IO CInt
dfFitDecisionTree planCS targetCS targetTypeCS configCS modelOut lenOut = do
    result <- try @SomeException $ do
        planBytes <- BS.packCString planCS
        targetBytes <- BS.packCString targetCS
        targetTypeBytes <- BS.packCString targetTypeCS
        configBytes <- BS.packCString configCS
        let target = TE.decodeUtf8 targetBytes
            targetTypeStr = TE.decodeUtf8 targetTypeBytes

        cfgJson <-
            either
                fail
                return
                (Aeson.eitherDecodeStrict configBytes :: Either String TreeConfigJson)
        let treeCfg = treeConfigFromJson cfgJson

        node <-
            either
                fail
                return
                (Aeson.eitherDecodeStrict planBytes :: Either String PlanNode)
        df <- executePlan fastReadCsvWithOpts node

        let typeTag =
                if targetTypeStr == "auto"
                    then inferTargetType target df
                    else targetTypeStr

        treeBytes <- fitTreeWithType typeTag treeCfg target df

        let len = BS.length treeBytes
        bufPtr <- mallocBytes (max 1 len) :: IO (Ptr CChar)
        BS.useAsCStringLen treeBytes $ \(srcPtr, srcLen) ->
            copyBytes bufPtr (castPtr srcPtr) srcLen
        poke modelOut bufPtr
        poke lenOut (fromIntegral len :: Word64)
    case result of
        Left ex -> hPrint stderr ex >> return (CInt (-1))
        Right _ -> return (CInt 0)

dfFreeModel :: Ptr CChar -> IO ()
dfFreeModel = free

data TreeConfigJson = TreeConfigJson
    { jcMaxDepth :: Maybe Int
    , jcMinSamplesSplit :: Maybe Int
    , jcMinLeafSize :: Maybe Int
    , jcPercentiles :: Maybe [Int]
    , jcExpressionPairs :: Maybe Int
    , jcTaoIterations :: Maybe Int
    , jcTaoConvergenceTol :: Maybe Double
    , jcMaxExprDepth :: Maybe Int
    , jcBoolExpansion :: Maybe Int
    , jcComplexityPenalty :: Maybe Double
    , jcEnableStringOps :: Maybe Bool
    , jcEnableCrossCols :: Maybe Bool
    , jcEnableArithOps :: Maybe Bool
    }

instance Aeson.FromJSON TreeConfigJson where
    parseJSON = Aeson.withObject "TreeConfig" $ \o ->
        TreeConfigJson
            <$> o Aeson..:? "max_depth"
            <*> o Aeson..:? "min_samples_split"
            <*> o Aeson..:? "min_leaf_size"
            <*> o Aeson..:? "percentiles"
            <*> o Aeson..:? "expression_pairs"
            <*> o Aeson..:? "tao_iterations"
            <*> o Aeson..:? "tao_convergence_tol"
            <*> o Aeson..:? "max_expr_depth"
            <*> o Aeson..:? "bool_expansion"
            <*> o Aeson..:? "complexity_penalty"
            <*> o Aeson..:? "enable_string_ops"
            <*> o Aeson..:? "enable_cross_cols"
            <*> o Aeson..:? "enable_arith_ops"

treeConfigFromJson :: TreeConfigJson -> TreeConfig
treeConfigFromJson j =
    defaultTreeConfig
        { maxTreeDepth = pick jcMaxDepth maxTreeDepth
        , minSamplesSplit = pick jcMinSamplesSplit minSamplesSplit
        , minLeafSize = pick jcMinLeafSize minLeafSize
        , percentiles = pick jcPercentiles percentiles
        , expressionPairs = pick jcExpressionPairs expressionPairs
        , taoIterations = pick jcTaoIterations taoIterations
        , taoConvergenceTol = pick jcTaoConvergenceTol taoConvergenceTol
        , columnOrdering = defaultColumnOrdering
        , synthConfig =
            defaultSynthConfig
                { maxExprDepth = pickS jcMaxExprDepth maxExprDepth
                , boolExpansion = pickS jcBoolExpansion boolExpansion
                , disallowedCombinations = []
                , complexityPenalty = pickS jcComplexityPenalty complexityPenalty
                , enableStringOps = pickS jcEnableStringOps enableStringOps
                , enableCrossCols = pickS jcEnableCrossCols enableCrossCols
                , enableArithOps = pickS jcEnableArithOps enableArithOps
                }
        }
  where
    pick :: (TreeConfigJson -> Maybe a) -> (TreeConfig -> a) -> a
    pick getter dflt = Data.Maybe.fromMaybe (dflt defaultTreeConfig) (getter j)
    pickS :: (TreeConfigJson -> Maybe a) -> (SynthConfig -> a) -> a
    pickS getter dflt = Data.Maybe.fromMaybe (dflt defaultSynthConfig) (getter j)

{- | Need this so we don't specify types on the Python side
but it means out type universe is limited/falls back to string.
-}
inferTargetType :: T.Text -> DataFrame -> T.Text
inferTargetType target df = dispatchType (columnTypeRep (unsafeGetColumn target df))
  where
    columnTypeRep :: Column -> SomeTypeRep
    columnTypeRep (UnboxedColumn _ (_ :: VU.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep (BoxedColumn _ (_ :: V.Vector a)) = SomeTypeRep (typeRep @a)
    columnTypeRep c@(PackedText _ _) = columnTypeRep (materializePacked c)
    columnTypeRep c@(MergedColumn _ _) = columnTypeRep (mergedHead c)

    dispatchType :: SomeTypeRep -> T.Text
    dispatchType (SomeTypeRep tr)
        | Just HRefl <- eqTypeRep tr (typeRep @Int) = "int"
        | Just HRefl <- eqTypeRep tr (typeRep @Int8) = "int8"
        | Just HRefl <- eqTypeRep tr (typeRep @Int16) = "int16"
        | Just HRefl <- eqTypeRep tr (typeRep @Int32) = "int32"
        | Just HRefl <- eqTypeRep tr (typeRep @Int64) = "int64"
        | Just HRefl <- eqTypeRep tr (typeRep @Word) = "word"
        | Just HRefl <- eqTypeRep tr (typeRep @Word8) = "word8"
        | Just HRefl <- eqTypeRep tr (typeRep @Word16) = "word16"
        | Just HRefl <- eqTypeRep tr (typeRep @Word32) = "word32"
        | Just HRefl <- eqTypeRep tr (typeRep @Word64) = "word64"
        | Just HRefl <- eqTypeRep tr (typeRep @Integer) = "integer"
        | Just HRefl <- eqTypeRep tr (typeRep @Double) = "double"
        | Just HRefl <- eqTypeRep tr (typeRep @Float) = "float"
        | Just HRefl <- eqTypeRep tr (typeRep @Bool) = "bool"
        | Just HRefl <- eqTypeRep tr (typeRep @Char) = "char"
        | Just HRefl <- eqTypeRep tr (typeRep @T.Text) = "text"
        | Just HRefl <- eqTypeRep tr (typeRep @String) = "string"
        | otherwise =
            error $
                "DataFrame.FFI.inferTargetType: unsupported target column type: "
                    ++ show tr

fitTreeWithType ::
    T.Text -> TreeConfig -> T.Text -> DataFrame -> IO BS.ByteString
fitTreeWithType ttag cfg target df = case ttag of
    "int" -> fitEncode @Int
    "int8" -> fitEncode @Int8
    "int16" -> fitEncode @Int16
    "int32" -> fitEncode @Int32
    "int64" -> fitEncode @Int64
    "word" -> fitEncode @Word
    "word8" -> fitEncode @Word8
    "word16" -> fitEncode @Word16
    "word32" -> fitEncode @Word32
    "word64" -> fitEncode @Word64
    "integer" -> fitEncode @Integer
    "double" -> fitEncode @Double
    "float" -> fitEncode @Float
    "bool" -> fitEncode @Bool
    "char" -> fitEncode @Char
    "text" -> fitEncode @T.Text
    "string" -> fitEncode @String
    other ->
        ioError . userError $
            "DataFrame.FFI.fitTreeWithType: unsupported target type tag: "
                ++ T.unpack other
  where
    fitEncode :: forall a. (Columnable a, Ord a) => IO BS.ByteString
    fitEncode = do
        let expr = predict (fit cfg (Col @a target) df)
        case encodeExprToBytes expr of
            Right bs -> return bs
            Left err -> ioError (userError $ "encodeExprToBytes: " ++ err)
