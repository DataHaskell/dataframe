{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Functions (module DataFrame.Functions, module DataFrame.Operators) where

import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    empty,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression hiding (normalize)
import DataFrame.Internal.Statistics
import DataFrame.Operations.Core

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.Char as Char
import Data.Either
import Data.Function
import Data.Functor
import Data.Int
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Maybe as Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Time
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word
import qualified DataFrame.IO.CSV as CSV
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types (columnNullCount)
import DataFrame.Internal.Nullable (
    BaseType,
    NullLift1Op (applyNull1),
    NullLift1Result,
    NullLift2Op (applyNull2),
    NullLift2Result,
 )
import DataFrame.Operators
import Debug.Trace (trace)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import Text.Regex.TDFA
import Prelude hiding (maximum, minimum)
import Prelude as P

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift f =
    Unary (MkUnaryOp{unaryFn = f, unaryName = "unaryUdf", unarySymbol = Nothing})

lift2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) -> Expr c -> Expr b -> Expr a
lift2 f =
    Binary
        ( MkBinaryOp
            { binaryFn = f
            , binaryName = "binaryUdf"
            , binarySymbol = Nothing
            , binaryCommutative = False
            , binaryPrecedence = 0
            }
        )

{- | Lift a unary function over a nullable or non-nullable column expression.
When the input is @Maybe a@, 'Nothing' short-circuits (like 'fmap').
When the input is plain @a@, the function is applied directly.

The return type is inferred via 'NullLift1Result': no annotation needed.
-}
nullLift ::
    (NullLift1Op a r (NullLift1Result a r), Columnable (NullLift1Result a r)) =>
    (BaseType a -> r) ->
    Expr a ->
    Expr (NullLift1Result a r)
nullLift f =
    Unary
        (MkUnaryOp{unaryFn = applyNull1 f, unaryName = "nullLift", unarySymbol = Nothing})

{- | Lift a binary function over nullable or non-nullable column expressions.
Any 'Nothing' operand short-circuits to 'Nothing' in the result.

The return type is inferred via 'NullLift2Result': no annotation needed.
-}
nullLift2 ::
    (NullLift2Op a b r (NullLift2Result a b r), Columnable (NullLift2Result a b r)) =>
    (BaseType a -> BaseType b -> r) ->
    Expr a ->
    Expr b ->
    Expr (NullLift2Result a b r)
nullLift2 f =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 f
            , binaryName = "nullLift2"
            , binarySymbol = Nothing
            , binaryCommutative = False
            , binaryPrecedence = 0
            }
        )

{- | Lenient numeric \/ text coercion returning @Maybe a@.  Looks up column
@name@ and coerces its values to @a@.  Values that cannot be converted
(parse failures, type mismatches) become 'Nothing'; successfully converted
values are wrapped in 'Just'.  Existing 'Nothing' in optional source columns
stays as 'Nothing'.
-}
cast :: forall a. (Columnable a, Read a) => T.Text -> Expr (Maybe a)
cast name = CastWith name "cast" (either (const Nothing) Just)

{- | Lenient coercion that substitutes a default for unconvertible values.
Looks up column @name@, coerces its values to @a@, and uses @def@ wherever
conversion fails or the source value is 'Nothing'.
-}
castWithDefault :: forall a. (Columnable a, Read a) => a -> T.Text -> Expr a
castWithDefault def name =
    CastWith name ("castWithDefault:" <> T.pack (show def)) (fromRight def)

{- | Lenient coercion returning @Either T.Text a@.  Successfully converted
values are 'Right'; values that cannot be parsed are kept as 'Left' with
their original string representation, so the caller can inspect or handle
them downstream.  Existing 'Nothing' in optional source columns becomes
@Left \"null\"@.
-}
castEither ::
    forall a. (Columnable a, Read a) => T.Text -> Expr (Either T.Text a)
castEither name = CastWith name "castEither" (either (Left . T.pack) Right)

{- | Lenient coercion for assertedly non-nullable columns.
Substitutes @error@ for @Nothing@, so it will crash at evaluation time if
any @Nothing@ is actually encountered.  For non-nullable and
fully-populated nullable columns no cost is paid.
-}
unsafeCast :: forall a. (Columnable a, Read a) => T.Text -> Expr a
unsafeCast name =
    CastWith
        name
        "unsafeCast"
        (fromRight (error "unsafeCast: unexpected Nothing in column"))

castExpr ::
    forall b src.
    (Columnable b, Columnable src, Read b) => Expr src -> Expr (Maybe b)
castExpr = CastExprWith @b @(Maybe b) @src "castExpr" (either (const Nothing) Just)

castExprWithDefault ::
    forall b src. (Columnable b, Columnable src, Read b) => b -> Expr src -> Expr b
castExprWithDefault def =
    CastExprWith @b @b @src
        ("castExprWithDefault:" <> T.pack (show def))
        (fromRight def)

castExprEither ::
    forall b src.
    (Columnable b, Columnable src, Read b) => Expr src -> Expr (Either T.Text b)
castExprEither =
    CastExprWith @b @(Either T.Text b) @src
        "castExprEither"
        (either (Left . T.pack) Right)

unsafeCastExpr ::
    forall b src. (Columnable b, Columnable src, Read b) => Expr src -> Expr b
unsafeCastExpr =
    CastExprWith @b @b @src
        "unsafeCastExpr"
        (fromRight (error "unsafeCastExpr: unexpected Nothing in column"))

toDouble :: (Columnable a, Real a) => Expr a -> Expr Double
toDouble =
    Unary
        ( MkUnaryOp
            { unaryFn = realToFrac
            , unaryName = "toDouble"
            , unarySymbol = Nothing
            }
        )

infix 8 `div`
div :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
div = lift2Decorated Prelude.div "div" (Just "//") False 7

mod :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
mod = lift2Decorated Prelude.mod "mod" Nothing False 7

eq :: (Columnable a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
eq = lift2Decorated (==) "eq" (Just "==") True 4

lt :: (Columnable a, Ord a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
lt = lift2Decorated (<) "lt" (Just "<") False 4

gt :: (Columnable a, Ord a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
gt = lift2Decorated (>) "gt" (Just ">") False 4

leq ::
    (Columnable a, Ord a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
leq = lift2Decorated (<=) "leq" (Just "<=") False 4

geq ::
    (Columnable a, Ord a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
geq = lift2Decorated (>=) "geq" (Just ">=") False 4

and :: Expr Bool -> Expr Bool -> Expr Bool
and = (.&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = (.||)

not :: Expr Bool -> Expr Bool
not =
    Unary
        (MkUnaryOp{unaryFn = Prelude.not, unaryName = "not", unarySymbol = Just "~"})

count :: (Columnable a) => Expr a -> Expr Int
count = Agg (MergeAgg "count" (0 :: Int) (\c _ -> c + 1) (+) id)
{-# SPECIALIZE count :: Expr Double -> Expr Int #-}
{-# SPECIALIZE count :: Expr Float -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int8 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int16 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int32 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int64 -> Expr Int #-}
{-# INLINEABLE count #-}

collect :: (Columnable a) => Expr a -> Expr [a]
collect = Agg (FoldAgg "collect" (Just []) (flip (:)))
{-# SPECIALIZE collect :: Expr Double -> Expr [Double] #-}
{-# SPECIALIZE collect :: Expr Float -> Expr [Float] #-}
{-# SPECIALIZE collect :: Expr Int -> Expr [Int] #-}
{-# INLINEABLE collect #-}

mode :: (Ord a, Columnable a, Eq a) => Expr a -> Expr a
mode =
    Agg
        ( CollectAgg
            "mode"
            ( fst
                . L.maximumBy (compare `on` snd)
                . M.toList
                . V.foldl' (\m e -> M.insertWith (+) e 1 m) M.empty
            )
        )
{-# SPECIALIZE mode :: Expr Double -> Expr Double #-}
{-# SPECIALIZE mode :: Expr Float -> Expr Float #-}
{-# SPECIALIZE mode :: Expr Int -> Expr Int #-}
{-# SPECIALIZE mode :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE mode :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE mode :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE mode :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE mode #-}

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum = Agg (FoldAgg "minimum" Nothing Prelude.min)
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.minimum #-}

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum = Agg (FoldAgg "maximum" Nothing Prelude.max)
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.maximum #-}

sum :: forall a. (Columnable a, Num a) => Expr a -> Expr a
sum = Agg (FoldAgg "sum" Nothing (+))
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.sum #-}

sumMaybe :: forall a. (Columnable a, Num a) => Expr (Maybe a) -> Expr a
sumMaybe = Agg (CollectAgg "sumMaybe" (P.sum . Maybe.catMaybes . V.toList))
{-# SPECIALIZE sumMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Float) -> Expr Float #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int) -> Expr Int #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int8) -> Expr Int8 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int16) -> Expr Int16 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int32) -> Expr Int32 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int64) -> Expr Int64 #-}
{-# INLINEABLE sumMaybe #-}

mean :: (Columnable a, Real a) => Expr a -> Expr Double
mean =
    Agg
        ( MergeAgg
            "mean"
            (MeanAcc 0.0 0)
            (\(MeanAcc s c) x -> MeanAcc (s + realToFrac x) (c + 1))
            (\(MeanAcc s1 c1) (MeanAcc s2 c2) -> MeanAcc (s1 + s2) (c1 + c2))
            (\(MeanAcc s c) -> if c == 0 then 0 / 0 else s / fromIntegral c)
        )
{-# SPECIALIZE mean :: Expr Double -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Float -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE mean #-}

meanMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
meanMaybe = Agg (CollectAgg "meanMaybe" (mean' . optionalToDoubleVector))
{-# SPECIALIZE meanMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE meanMaybe #-}

variance :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
variance = Agg (CollectAgg "variance" variance')
{-# SPECIALIZE variance :: Expr Double -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Float -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE variance #-}

median :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
median = Agg (CollectAgg "median" median')
{-# SPECIALIZE median :: Expr Double -> Expr Double #-}
{-# SPECIALIZE median :: Expr Float -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE median #-}

medianMaybe :: (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
medianMaybe = Agg (CollectAgg "meanMaybe" (median' . optionalToDoubleVector))
{-# SPECIALIZE medianMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE medianMaybe #-}

optionalToDoubleVector :: (Real a) => V.Vector (Maybe a) -> VU.Vector Double
optionalToDoubleVector =
    VU.fromList
        . V.foldl'
            (\acc e -> if Maybe.isJust e then realToFrac (Maybe.fromMaybe 0 e) : acc else acc)
            []

percentile :: Int -> Expr Double -> Expr Double
percentile n =
    Agg
        ( CollectAgg
            (T.pack $ "percentile " ++ show n)
            (percentile' n)
        )

stddev :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stddev = Agg (CollectAgg "stddev" (sqrt . variance'))
{-# SPECIALIZE stddev :: Expr Double -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Float -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE stddev #-}

stddevMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
stddevMaybe = Agg (CollectAgg "stddevMaybe" (sqrt . variance' . optionalToDoubleVector))
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE stddevMaybe #-}

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Expr a -> Int -> Expr a
pow expr i = lift2Decorated (^) "pow" (Just "^") True 8 expr (Lit i)
{-# SPECIALIZE pow :: Expr Double -> Int -> Expr Double #-}
{-# SPECIALIZE pow :: Expr Float -> Int -> Expr Float #-}
{-# SPECIALIZE pow :: Expr Int -> Int -> Expr Int #-}
{-# INLINEABLE pow #-}

relu :: (Columnable a, Num a, Ord a) => Expr a -> Expr a
relu = liftDecorated (Prelude.max 0) "relu" Nothing
{-# SPECIALIZE relu :: Expr Double -> Expr Double #-}
{-# SPECIALIZE relu :: Expr Float -> Expr Float #-}
{-# SPECIALIZE relu :: Expr Int -> Expr Int #-}
{-# INLINEABLE relu #-}

min :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
min = lift2Decorated Prelude.min "min" Nothing True 1
{-# SPECIALIZE DataFrame.Functions.min ::
    Expr Double -> Expr Double -> Expr Double
    #-}
{-# SPECIALIZE DataFrame.Functions.min ::
    Expr Float -> Expr Float -> Expr Float
    #-}
{-# SPECIALIZE DataFrame.Functions.min :: Expr Int -> Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.min #-}

max :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
max = lift2Decorated Prelude.max "max" Nothing True 1
{-# SPECIALIZE DataFrame.Functions.max ::
    Expr Double -> Expr Double -> Expr Double
    #-}
{-# SPECIALIZE DataFrame.Functions.max ::
    Expr Float -> Expr Float -> Expr Float
    #-}
{-# SPECIALIZE DataFrame.Functions.max :: Expr Int -> Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.max #-}

reduce ::
    forall a b.
    (Columnable a, Columnable b) => Expr b -> a -> (a -> b -> a) -> Expr a
reduce expr start f = Agg (FoldAgg "foldUdf" (Just start) f) expr
{-# INLINEABLE reduce #-}

toMaybe :: (Columnable a) => Expr a -> Expr (Maybe a)
toMaybe = liftDecorated Just "toMaybe" Nothing
{-# SPECIALIZE toMaybe :: Expr Double -> Expr (Maybe Double) #-}
{-# SPECIALIZE toMaybe :: Expr Float -> Expr (Maybe Float) #-}
{-# SPECIALIZE toMaybe :: Expr Int -> Expr (Maybe Int) #-}
{-# INLINEABLE toMaybe #-}

fromMaybe :: (Columnable a) => a -> Expr (Maybe a) -> Expr a
fromMaybe d = liftDecorated (Maybe.fromMaybe d) "fromMaybe" Nothing
{-# SPECIALIZE fromMaybe :: Double -> Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE fromMaybe :: Float -> Expr (Maybe Float) -> Expr Float #-}
{-# SPECIALIZE fromMaybe :: Int -> Expr (Maybe Int) -> Expr Int #-}
{-# INLINEABLE fromMaybe #-}

isJust :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isJust = liftDecorated Maybe.isJust "isJust" Nothing
{-# SPECIALIZE isJust :: Expr (Maybe Double) -> Expr Bool #-}
{-# SPECIALIZE isJust :: Expr (Maybe Int) -> Expr Bool #-}
{-# INLINEABLE isJust #-}

isNothing :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNothing = liftDecorated Maybe.isNothing "isNothing" Nothing
{-# SPECIALIZE isNothing :: Expr (Maybe Double) -> Expr Bool #-}
{-# SPECIALIZE isNothing :: Expr (Maybe Int) -> Expr Bool #-}
{-# INLINEABLE isNothing #-}

fromJust :: (Columnable a) => Expr (Maybe a) -> Expr a
fromJust = liftDecorated Maybe.fromJust "fromJust" Nothing
{-# SPECIALIZE fromJust :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE fromJust :: Expr (Maybe Int) -> Expr Int #-}
{-# INLINEABLE fromJust #-}

whenPresent ::
    forall a b.
    (Columnable a, Columnable b) => (a -> b) -> Expr (Maybe a) -> Expr (Maybe b)
whenPresent f = liftDecorated (fmap f) "whenPresent" Nothing
{-# INLINEABLE whenPresent #-}

whenBothPresent ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Expr (Maybe a) -> Expr (Maybe b) -> Expr (Maybe c)
whenBothPresent f = lift2Decorated (\l r -> f <$> l <*> r) "whenBothPresent" Nothing False 0
{-# INLINEABLE whenBothPresent #-}

recode ::
    forall a b.
    (Columnable a, Columnable b, Show (a, b)) =>
    [(a, b)] -> Expr a -> Expr (Maybe b)
recode mapping =
    Unary
        ( MkUnaryOp
            { unaryFn = (`lookup` mapping)
            , unaryName = "recode " <> T.pack (show mapping)
            , unarySymbol = Nothing
            }
        )

recodeWithCondition ::
    forall a b.
    (Columnable a, Columnable b) =>
    Expr b -> [(Expr a -> Expr Bool, b)] -> Expr a -> Expr b
recodeWithCondition fallback [] value = fallback
recodeWithCondition fallback ((cond, value) : rest) expr = ifThenElse (cond expr) (lit value) (recodeWithCondition fallback rest expr)

recodeWithDefault ::
    forall a b.
    (Columnable a, Columnable b, Show (a, b)) => b -> [(a, b)] -> Expr a -> Expr b
recodeWithDefault d mapping =
    Unary
        ( MkUnaryOp
            { unaryFn = Maybe.fromMaybe d . (`lookup` mapping)
            , unaryName =
                "recodeWithDefault " <> T.pack (show d) <> " " <> T.pack (show mapping)
            , unarySymbol = Nothing
            }
        )

firstOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
firstOrNothing = liftDecorated Maybe.listToMaybe "firstOrNothing" Nothing

lastOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
lastOrNothing = liftDecorated (Maybe.listToMaybe . reverse) "lastOrNothing" Nothing

splitOn :: T.Text -> Expr T.Text -> Expr [T.Text]
splitOn delim = liftDecorated (T.splitOn delim) "splitOn" Nothing

match :: T.Text -> Expr T.Text -> Expr (Maybe T.Text)
match regex =
    liftDecorated
        ((\r -> if T.null r then Nothing else Just r) . (=~ regex))
        ("match " <> T.pack (show regex))
        Nothing

matchAll :: T.Text -> Expr T.Text -> Expr [T.Text]
matchAll regex =
    liftDecorated
        (getAllTextMatches . (=~ regex))
        ("matchAll " <> T.pack (show regex))
        Nothing

parseDate ::
    (ParseTime t, Columnable t) => T.Text -> Expr T.Text -> Expr (Maybe t)
parseDate format =
    liftDecorated
        (parseTimeM True defaultTimeLocale (T.unpack format) . T.unpack)
        ("parseDate " <> format)
        Nothing

daysBetween :: Expr Day -> Expr Day -> Expr Int
daysBetween =
    lift2Decorated
        (\d1 d2 -> fromIntegral (diffDays d1 d2))
        "daysBetween"
        Nothing
        True
        2

bind ::
    forall a b m.
    (Columnable a, Columnable (m a), Monad m, Columnable b, Columnable (m b)) =>
    (a -> m b) -> Expr (m a) -> Expr (m b)
bind f = liftDecorated (>>= f) "bind" Nothing

-- See Section 2.4 of the Haskell Report https://www.haskell.org/definition/haskell2010.pdf
isReservedId :: T.Text -> Bool
isReservedId t = case t of
    "case" -> True
    "class" -> True
    "data" -> True
    "default" -> True
    "deriving" -> True
    "do" -> True
    "else" -> True
    "foreign" -> True
    "if" -> True
    "import" -> True
    "in" -> True
    "infix" -> True
    "infixl" -> True
    "infixr" -> True
    "instance" -> True
    "let" -> True
    "module" -> True
    "newtype" -> True
    "of" -> True
    "then" -> True
    "type" -> True
    "where" -> True
    _ -> False

isVarId :: T.Text -> Bool
isVarId t = case T.uncons t of
    -- We might want to check  c == '_' || Char.isLower c
    -- since the haskell report considers '_' a lowercase character
    -- However, to prevent an edge case where a user may have a
    -- "Name" and an "_Name_" in the same scope, wherein we'd end up
    -- with duplicate "_Name_"s, we eschew the check for '_' here.
    Just (c, _) -> Char.isLower c && Char.isAlpha c
    Nothing -> False

isHaskellIdentifier :: T.Text -> Bool
isHaskellIdentifier t = Prelude.not (isVarId t) || isReservedId t

sanitize :: T.Text -> T.Text
sanitize t
    | isValid = t
    | isHaskellIdentifier t' = "_" <> t' <> "_"
    | otherwise = t'
  where
    isValid =
        Prelude.not (isHaskellIdentifier t)
            && isVarId t
            && T.all Char.isAlphaNum t
    t' = T.map replaceInvalidCharacters . T.filter (Prelude.not . parentheses) $ t
    replaceInvalidCharacters c
        | Char.isUpper c = Char.toLower c
        | Char.isSpace c = '_'
        | Char.isPunctuation c = '_' -- '-' will also become a '_'
        | Char.isSymbol c = '_'
        | Char.isAlphaNum c = c -- Blanket condition
        | otherwise = '_' -- If we're unsure we'll default to an underscore
    parentheses c = case c of
        '(' -> True
        ')' -> True
        '{' -> True
        '}' -> True
        '[' -> True
        ']' -> True
        _ -> False

typeFromString :: [String] -> Q Type
typeFromString [] = fail "No type specified"
typeFromString [t0] = do
    let t = normalize t0
    case stripBrackets t of
        Just inner -> typeFromString [inner] <&> AppT ListT
        Nothing
            | t == "Text" || t == "Data.Text.Text" || t == "T.Text" ->
                pure (ConT ''T.Text)
            | otherwise -> do
                m <- lookupTypeName t
                case m of
                    Just name -> pure (ConT name)
                    Nothing -> fail $ "Unsupported type: " ++ t0
typeFromString [tycon, t1] = AppT <$> typeFromString [tycon] <*> typeFromString [t1]
typeFromString [tycon, t1, t2] =
    (\outer a b -> AppT (AppT outer a) b)
        <$> typeFromString [tycon]
        <*> typeFromString [t1]
        <*> typeFromString [t2]
typeFromString s = fail $ "Unsupported types: " ++ unwords s

normalize :: String -> String
normalize = dropWhile (== ' ') . reverse . dropWhile (== ' ') . reverse

stripBrackets :: String -> Maybe String
stripBrackets s =
    case s of
        ('[' : rest)
            | P.not (null rest) && last rest == ']' ->
                Just (init rest)
        _ -> Nothing

declareColumnsFromCsvFile :: String -> DecsQ
declareColumnsFromCsvFile path = do
    df <-
        liftIO
            (CSV.readSeparated (CSV.defaultReadOptions{CSV.numColumns = Just 100}) path)
    declareColumns df

declareColumnsFromParquetFile :: String -> DecsQ
declareColumnsFromParquetFile path = do
    isDir <- liftIO $ doesDirectoryExist path
    let pat = if isDir then path </> "*.parquet" else path
    matches <- liftIO $ glob pat
    files <- liftIO $ filterM (fmap Prelude.not . doesDirectoryExist) matches
    metas <- liftIO $ mapM (fmap fst . Parquet.readMetadataFromPath) files
    let nullableCols :: S.Set T.Text
        nullableCols =
            S.fromList
                [ T.pack (last colPath)
                | meta <- metas
                , rg <- rowGroups meta
                , cc <- rowGroupColumns rg
                , let cm = columnMetaData cc
                      colPath = columnPathInSchema cm
                , Prelude.not (null colPath)
                , columnNullCount (columnStatistics cm) > 0
                ]
    let df =
            foldl
                (\acc meta -> acc <> schemaToEmptyDataFrame nullableCols (schema meta))
                DataFrame.Internal.DataFrame.empty
                metas
    declareColumns df

schemaToEmptyDataFrame :: S.Set T.Text -> [SchemaElement] -> DataFrame
schemaToEmptyDataFrame nullableCols elems =
    let leafElems = filter (\e -> numChildren e == 0) elems
     in fromNamedColumns (map (schemaElemToColumn nullableCols) leafElems)

schemaElemToColumn :: S.Set T.Text -> SchemaElement -> (T.Text, Column)
schemaElemToColumn nullableCols elem =
    let name = elementName elem
        isNull = name `S.member` nullableCols
        col =
            if isNull
                then emptyNullableColumnForType (elementType elem)
                else emptyColumnForType (elementType elem)
     in (name, col)

emptyColumnForType :: TType -> Column
emptyColumnForType = \case
    BOOL -> fromList @Bool []
    BYTE -> fromList @Word8 []
    I16 -> fromList @Int16 []
    I32 -> fromList @Int32 []
    I64 -> fromList @Int64 []
    I96 -> fromList @Int64 []
    FLOAT -> fromList @Float []
    DOUBLE -> fromList @Double []
    STRING -> fromList @T.Text []
    other -> error $ "Unsupported parquet type for column: " <> show other

emptyNullableColumnForType :: TType -> Column
emptyNullableColumnForType = \case
    BOOL -> fromList @(Maybe Bool) []
    BYTE -> fromList @(Maybe Word8) []
    I16 -> fromList @(Maybe Int16) []
    I32 -> fromList @(Maybe Int32) []
    I64 -> fromList @(Maybe Int64) []
    I96 -> fromList @(Maybe Int64) []
    FLOAT -> fromList @(Maybe Float) []
    DOUBLE -> fromList @(Maybe Double) []
    STRING -> fromList @(Maybe T.Text) []
    other -> error $ "Unsupported parquet type for column: " <> show other

declareColumnsFromCsvWithOpts :: CSV.ReadOptions -> String -> DecsQ
declareColumnsFromCsvWithOpts opts path = do
    df <- liftIO (CSV.readSeparated opts path)
    declareColumns df

declareColumns :: DataFrame -> DecsQ
declareColumns = declareColumnsWithPrefix' Nothing

declareColumnsWithPrefix :: T.Text -> DataFrame -> DecsQ
declareColumnsWithPrefix prefix = declareColumnsWithPrefix' (Just prefix)

declareColumnsWithPrefix' :: Maybe T.Text -> DataFrame -> DecsQ
declareColumnsWithPrefix' prefix df =
    let
        names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs =
            zipWith
                ( \name type_ -> (name, maybe "" (sanitize . (<> "_")) prefix <> sanitize name, type_)
                )
                names
                types
     in
        fmap concat $ forM specs $ \(raw, nm, tyStr) -> do
            ty <- typeFromString (words tyStr)
            let tyDisplay = if ' ' `elem` tyStr then "(" <> T.pack tyStr <> ")" else T.pack tyStr
            trace (T.unpack (nm <> " :: Expr " <> tyDisplay)) pure ()
            let n = mkName (T.unpack nm)
            sig <- sigD n [t|Expr $(pure ty)|]
            val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
            pure [sig, val]
