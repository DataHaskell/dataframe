{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Functions where

import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression hiding (normalize)
import DataFrame.Internal.Statistics

import Control.Applicative
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.Char as Char
import Data.Function
import Data.Functor
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Maybe as Maybe
import qualified Data.Text as T
import Data.Time
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.IO.CSV as CSV
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Operators
import Debug.Trace (trace)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import Text.Regex.TDFA
import Prelude hiding (maximum, minimum)
import Prelude as P

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

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

liftDecorated ::
    (Columnable a, Columnable b) =>
    (a -> b) -> T.Text -> Maybe T.Text -> Expr a -> Expr b
liftDecorated f name rep = Unary (MkUnaryOp{unaryFn = f, unaryName = name, unarySymbol = rep})

lift2Decorated ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) ->
    T.Text ->
    Maybe T.Text ->
    Bool ->
    Int ->
    Expr c ->
    Expr b ->
    Expr a
lift2Decorated f name rep comm prec =
    Binary
        ( MkBinaryOp
            { binaryFn = f
            , binaryName = name
            , binarySymbol = rep
            , binaryCommutative = comm
            , binaryPrecedence = prec
            }
        )

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

eq :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
eq = (.==)

lt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
lt = (.<)

gt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
gt = (.>)

leq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
leq = (.<=)

geq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
geq = (.>=)

and :: Expr Bool -> Expr Bool -> Expr Bool
and = (.&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = (.||)

not :: Expr Bool -> Expr Bool
not =
    Unary
        (MkUnaryOp{unaryFn = Prelude.not, unaryName = "not", unarySymbol = Just "~"})

count :: (Columnable a) => Expr a -> Expr Int
count = Agg (FoldAgg "count" (Just 0) (\acc _ -> acc + 1))

collect :: (Columnable a) => Expr a -> Expr [a]
collect = Agg (FoldAgg "collect" (Just []) (flip (:)))

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

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum = Agg (FoldAgg "minimum" Nothing Prelude.min)

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum = Agg (FoldAgg "maximum" Nothing Prelude.max)

sum :: forall a. (Columnable a, Num a) => Expr a -> Expr a
sum = Agg (FoldAgg "sum" Nothing (+))
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.sum #-}

sumMaybe :: forall a. (Columnable a, Num a) => Expr (Maybe a) -> Expr a
sumMaybe = Agg (CollectAgg "sumMaybe" (P.sum . Maybe.catMaybes . V.toList))

mean :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
mean = Agg (CollectAgg "mean" mean')
{-# SPECIALIZE DataFrame.Functions.mean :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.mean :: Expr Int -> Expr Double #-}
{-# INLINEABLE DataFrame.Functions.mean #-}

meanMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
meanMaybe = Agg (CollectAgg "meanMaybe" (mean' . optionalToDoubleVector))

variance :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
variance = Agg (CollectAgg "variance" variance')

median :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
median = Agg (CollectAgg "median" median')

medianMaybe :: (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
medianMaybe = Agg (CollectAgg "meanMaybe" (median' . optionalToDoubleVector))

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

stddevMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
stddevMaybe = Agg (CollectAgg "stddevMaybe" (sqrt . variance' . optionalToDoubleVector))

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Expr a -> Int -> Expr a
pow = (.^^)

relu :: (Columnable a, Num a, Ord a) => Expr a -> Expr a
relu = liftDecorated (Prelude.max 0) "relu" Nothing

min :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
min = lift2Decorated Prelude.min "max" Nothing True 1

max :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
max = lift2Decorated Prelude.max "max" Nothing True 1

reduce ::
    forall a b.
    (Columnable a, Columnable b) => Expr b -> a -> (a -> b -> a) -> Expr a
reduce expr start f = Agg (FoldAgg "foldUdf" (Just start) f) expr

toMaybe :: (Columnable a) => Expr a -> Expr (Maybe a)
toMaybe = liftDecorated Just "toMaybe" Nothing

fromMaybe :: (Columnable a) => a -> Expr (Maybe a) -> Expr a
fromMaybe d = liftDecorated (Maybe.fromMaybe d) "fromMaybe" Nothing

isJust :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isJust = liftDecorated Maybe.isJust "isJust" Nothing

isNothing :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNothing = liftDecorated Maybe.isNothing "isNothing" Nothing

fromJust :: (Columnable a) => Expr (Maybe a) -> Expr a
fromJust = liftDecorated Maybe.fromJust "fromJust" Nothing

whenPresent ::
    forall a b.
    (Columnable a, Columnable b) => (a -> b) -> Expr (Maybe a) -> Expr (Maybe b)
whenPresent f = lift (fmap f)

whenBothPresent ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Expr (Maybe a) -> Expr (Maybe b) -> Expr (Maybe c)
whenBothPresent f = lift2 (\l r -> f <$> l <*> r)

recode ::
    forall a b.
    (Columnable a, Columnable b) => [(a, b)] -> Expr a -> Expr (Maybe b)
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
    (Columnable a, Columnable b) => b -> [(a, b)] -> Expr a -> Expr b
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
bind f = lift (>>= f)

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

-- TODO: We don't have to read the whole file, we can just read the schema.
declareColumnsFromParquetFile :: String -> DecsQ
declareColumnsFromParquetFile path = do
    df <- liftIO (Parquet.readParquet path)
    declareColumns df

declareColumnsFromCsvWithOpts :: CSV.ReadOptions -> String -> DecsQ
declareColumnsFromCsvWithOpts opts path = do
    df <- liftIO (CSV.readSeparated opts path)
    declareColumns df

declareColumns :: DataFrame -> DecsQ
declareColumns = declareColumnsWithPrefix Nothing

declareColumnsWithPrefix :: Maybe T.Text -> DataFrame -> DecsQ
declareColumnsWithPrefix prefix df =
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
            trace (T.unpack (nm <> " :: Expr " <> T.pack tyStr)) pure ()
            let n = mkName (T.unpack nm)
            sig <- sigD n [t|Expr $(pure ty)|]
            val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
            pure [sig, val]
