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
import DataFrame.Internal.Expression (
    Expr (..),
    NamedExpr,
    UExpr (..),
 )
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
import Debug.Trace (trace)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import Text.Regex.TDFA
import Prelude hiding (maximum, minimum)
import Prelude as P

infix 4 .==, .<, .<=, .>=, .>, ./=
infixr 3 .&&
infixr 2 .||

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

as :: (Columnable a) => Expr a -> T.Text -> NamedExpr
as expr name = (name, Wrap expr)

infixr 0 .=
(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift = UnaryOp "udf"

lift2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) -> Expr c -> Expr b -> Expr a
lift2 = BinaryOp "udf"

toDouble :: (Columnable a, Real a) => Expr a -> Expr Double
toDouble = UnaryOp "toDouble" realToFrac

div :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
div = BinaryOp "div" Prelude.div

mod :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
mod = BinaryOp "mod" Prelude.mod

(.==) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(.==) = BinaryOp "eq" (==)

(./=) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(./=) = BinaryOp "neq" (/=)

eq :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
eq = BinaryOp "eq" (==)

(.<) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.<) = BinaryOp "lt" (<)

lt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
lt = BinaryOp "lt" (<)

-- TODO: Generalize this pattern for other equality functions.
(.>) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.>) = BinaryOp "gt" (>)

gt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
gt = (.>)

(.<=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.<=) = BinaryOp "leq" (<=)

leq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
leq = (.<=)

(.>=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.>=) = BinaryOp "geq" (>=)

geq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
geq = BinaryOp "geq" (>=)

and :: Expr Bool -> Expr Bool -> Expr Bool
and = BinaryOp "and" (&&)

(.&&) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&) = BinaryOp "and" (&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = BinaryOp "or" (||)

(.||) :: Expr Bool -> Expr Bool -> Expr Bool
(.||) = BinaryOp "or" (||)

not :: Expr Bool -> Expr Bool
not = UnaryOp "not" Prelude.not

count :: (Columnable a) => Expr a -> Expr Int
count expr = AggFold expr "count" 0 (\acc _ -> acc + 1)

collect :: (Columnable a) => Expr a -> Expr [a]
collect expr = AggFold expr "collect" [] (flip (:))

mode :: (Ord a, Columnable a, Eq a) => Expr a -> Expr a
mode expr =
    AggVector
        expr
        "mode"
        ( fst
            . L.maximumBy (compare `on` snd)
            . M.toList
            . V.foldl' (\m e -> M.insertWith (+) e 1 m) M.empty
        )

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum expr = AggReduce expr "minimum" Prelude.min

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum expr = AggReduce expr "maximum" Prelude.max

sum :: forall a. (Columnable a, Num a) => Expr a -> Expr a
sum expr = AggReduce expr "sum" (+)
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.sum #-}

sumMaybe :: forall a. (Columnable a, Num a) => Expr (Maybe a) -> Expr a
sumMaybe expr = AggVector expr "sumMaybe" (P.sum . Maybe.catMaybes . V.toList)

mean :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
mean expr = AggNumericVector expr "mean" mean'
{-# SPECIALIZE DataFrame.Functions.mean :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.mean :: Expr Int -> Expr Double #-}
{-# INLINEABLE DataFrame.Functions.mean #-}

meanMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
meanMaybe expr = AggVector expr "meanMaybe" (mean' . optionalToDoubleVector)

variance :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
variance expr = AggNumericVector expr "variance" variance'

median :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
median expr = AggNumericVector expr "median" median'

medianMaybe :: (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
medianMaybe expr = AggVector expr "meanMaybe" (median' . optionalToDoubleVector)

optionalToDoubleVector :: (Real a) => V.Vector (Maybe a) -> VU.Vector Double
optionalToDoubleVector =
    VU.fromList
        . V.foldl'
            (\acc e -> if Maybe.isJust e then realToFrac (Maybe.fromMaybe 0 e) : acc else acc)
            []

percentile :: Int -> Expr Double -> Expr Double
percentile n expr =
    AggNumericVector
        expr
        (T.pack $ "percentile " ++ show n)
        (percentile' n)

stddev :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stddev expr = AggNumericVector expr "stddev" (sqrt . variance')

stddevMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
stddevMaybe expr = AggVector expr "stddevMaybe" (sqrt . variance' . optionalToDoubleVector)

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Expr a -> Int -> Expr a
pow _ 0 = Lit 1
pow (Lit n) i = Lit (n ^ i)
pow expr 1 = expr
pow expr i = BinaryOp "pow" (^) expr (lit i)

relu :: (Columnable a, Num a, Ord a) => Expr a -> Expr a
relu = UnaryOp "relu" (Prelude.max 0)

min :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
min = BinaryOp "min" Prelude.min

max :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
max = BinaryOp "max" Prelude.max

reduce ::
    forall a b.
    (Columnable a, Columnable b) => Expr b -> a -> (a -> b -> a) -> Expr a
reduce expr = AggFold expr "foldUdf"

toMaybe :: (Columnable a) => Expr a -> Expr (Maybe a)
toMaybe = UnaryOp "toMaybe" Just

fromMaybe :: (Columnable a) => a -> Expr (Maybe a) -> Expr a
fromMaybe d = UnaryOp ("fromMaybe " <> T.pack (show d)) (Maybe.fromMaybe d)

isJust :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isJust = UnaryOp "isJust" Maybe.isJust

isNothing :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNothing = UnaryOp "isNothing" Maybe.isNothing

fromJust :: (Columnable a) => Expr (Maybe a) -> Expr a
fromJust = UnaryOp "fromJust" Maybe.fromJust

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
recode mapping = UnaryOp (T.pack ("recode " ++ show mapping)) (`lookup` mapping)

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
    UnaryOp
        (T.pack ("recodeWithDefault " ++ show d ++ " " ++ show mapping))
        (Maybe.fromMaybe d . (`lookup` mapping))

firstOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
firstOrNothing = lift Maybe.listToMaybe

lastOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
lastOrNothing = lift (Maybe.listToMaybe . reverse)

splitOn :: T.Text -> Expr T.Text -> Expr [T.Text]
splitOn delim = lift (T.splitOn delim)

match :: T.Text -> Expr T.Text -> Expr (Maybe T.Text)
match regex = lift ((\r -> if T.null r then Nothing else Just r) . (=~ regex))

matchAll :: T.Text -> Expr T.Text -> Expr [T.Text]
matchAll regex = lift (getAllTextMatches . (=~ regex))

parseDate :: T.Text -> Expr T.Text -> Expr (Maybe Day)
parseDate format = lift (parseTimeM True defaultTimeLocale (T.unpack format) . T.unpack)

daysBetween :: Expr Day -> Expr Day -> Expr Int
daysBetween d1 d2 = lift fromIntegral (lift2 diffDays d1 d2)

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
declareColumns df =
    let
        names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs = zipWith (\name type_ -> (name, sanitize name, type_)) names types
     in
        fmap concat $ forM specs $ \(raw, nm, tyStr) -> do
            ty <- typeFromString (words tyStr)
            trace (T.unpack (nm <> " :: Expr " <> T.pack tyStr)) pure ()
            let n = mkName (T.unpack nm)
            sig <- sigD n [t|Expr $(pure ty)|]
            val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
            pure [sig, val]
