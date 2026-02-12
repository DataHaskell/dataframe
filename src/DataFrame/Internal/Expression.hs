{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Expression where

import Data.String
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector.Generic as VG
import DataFrame.Internal.Column
import Type.Reflection (Typeable, typeOf, typeRep)

data UnaryOp a b = MkUnaryOp
    { unaryFn :: a -> b
    , unaryName :: T.Text
    , unarySymbol :: Maybe T.Text
    }

data BinaryOp a b c = MkBinaryOp
    { binaryFn :: a -> b -> c
    , binaryName :: T.Text
    , binarySymbol :: Maybe T.Text
    , binaryCommutative :: Bool
    , binaryPrecedence :: Int
    }

data AggStrategy a b where
    CollectAgg ::
        (VG.Vector v b, Typeable v) => T.Text -> (v b -> a) -> AggStrategy a b
    FoldAgg :: T.Text -> Maybe a -> (a -> b -> a) -> AggStrategy a b

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    Lit :: (Columnable a) => a -> Expr a
    Unary ::
        (Columnable a, Columnable b) => UnaryOp b a -> Expr b -> Expr a
    Binary ::
        (Columnable c, Columnable b, Columnable a) =>
        BinaryOp c b a -> Expr c -> Expr b -> Expr a
    If :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
    Agg :: (Columnable a, Columnable b) => AggStrategy a b -> Expr b -> Expr a

data UExpr where
    UExpr :: (Columnable a) => Expr a -> UExpr

instance Show UExpr where
    show :: UExpr -> String
    show (UExpr expr) = show expr

type NamedExpr = (T.Text, UExpr)

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) =
        Binary
            ( MkBinaryOp
                { binaryFn = (+)
                , binaryName = "add"
                , binarySymbol = Just "+"
                , binaryCommutative = True
                , binaryPrecedence = 6
                }
            )

    (-) :: Expr a -> Expr a -> Expr a
    (-) =
        Binary
            ( MkBinaryOp
                { binaryFn = (-)
                , binaryName = "sub"
                , binarySymbol = Just "-"
                , binaryCommutative = False
                , binaryPrecedence = 6
                }
            )

    (*) :: Expr a -> Expr a -> Expr a
    (*) =
        Binary
            ( MkBinaryOp
                { binaryFn = (*)
                , binaryName = "mult"
                , binarySymbol = Just "*"
                , binaryCommutative = True
                , binaryPrecedence = 7
                }
            )

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate =
        Unary
            (MkUnaryOp{unaryFn = negate, unaryName = "negate", unarySymbol = Nothing})

    abs :: (Num a) => Expr a -> Expr a
    abs = Unary (MkUnaryOp{unaryFn = abs, unaryName = "abs", unarySymbol = Nothing})

    signum :: (Num a) => Expr a -> Expr a
    signum =
        Unary
            (MkUnaryOp{unaryFn = signum, unaryName = "signum", unarySymbol = Nothing})

add :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
add = (+)

sub :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
sub = (-)

mult :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
mult = (*)

instance (Fractional a, Columnable a) => Fractional (Expr a) where
    fromRational :: (Fractional a, Columnable a) => Rational -> Expr a
    fromRational = Lit . fromRational

    (/) :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
    (/) =
        Binary
            ( MkBinaryOp
                { binaryFn = (/)
                , binaryName = "divide"
                , binarySymbol = Just "/"
                , binaryCommutative = True
                , binaryPrecedence = 7
                }
            )

divide :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
divide = (/)

instance (IsString a, Columnable a) => IsString (Expr a) where
    fromString :: String -> Expr a
    fromString s = Lit (fromString s)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp = Unary (MkUnaryOp{unaryFn = exp, unaryName = "exp", unarySymbol = Nothing})
    sqrt :: (Floating a, Columnable a) => Expr a -> Expr a
    sqrt =
        Unary (MkUnaryOp{unaryFn = sqrt, unaryName = "sqrt", unarySymbol = Nothing})
    (**) :: (Floating a, Columnable a) => Expr a -> Expr a -> Expr a
    (**) =
        Binary
            ( MkBinaryOp
                { binaryFn = (**)
                , binaryName = "exponentiate"
                , binarySymbol = Just "**"
                , binaryCommutative = False
                , binaryPrecedence = 8
                }
            )
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log = Unary (MkUnaryOp{unaryFn = log, unaryName = "log", unarySymbol = Nothing})
    logBase :: (Floating a, Columnable a) => Expr a -> Expr a -> Expr a
    logBase =
        Binary
            ( MkBinaryOp
                { binaryFn = logBase
                , binaryName = "logBase"
                , binarySymbol = Nothing
                , binaryCommutative = False
                , binaryPrecedence = 1
                }
            )
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin = Unary (MkUnaryOp{unaryFn = sin, unaryName = "sin", unarySymbol = Nothing})
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos = Unary (MkUnaryOp{unaryFn = cos, unaryName = "cos", unarySymbol = Nothing})
    tan :: (Floating a, Columnable a) => Expr a -> Expr a
    tan = Unary (MkUnaryOp{unaryFn = tan, unaryName = "tan", unarySymbol = Nothing})
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin =
        Unary (MkUnaryOp{unaryFn = asin, unaryName = "asin", unarySymbol = Nothing})
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos =
        Unary (MkUnaryOp{unaryFn = acos, unaryName = "acos", unarySymbol = Nothing})
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan =
        Unary (MkUnaryOp{unaryFn = atan, unaryName = "atan", unarySymbol = Nothing})
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh =
        Unary (MkUnaryOp{unaryFn = sinh, unaryName = "sinh", unarySymbol = Nothing})
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh =
        Unary (MkUnaryOp{unaryFn = cosh, unaryName = "cosh", unarySymbol = Nothing})
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh =
        Unary
            (MkUnaryOp{unaryFn = asinh, unaryName = "asinh", unarySymbol = Nothing})
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh =
        Unary
            (MkUnaryOp{unaryFn = acosh, unaryName = "acosh", unarySymbol = Nothing})
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh =
        Unary
            (MkUnaryOp{unaryFn = atanh, unaryName = "atanh", unarySymbol = Nothing})

instance (Show a) => Show (Expr a) where
    show :: forall a. (Show a) => Expr a -> String
    show (Col name) = "(col @" ++ show (typeRep @a) ++ " " ++ show name ++ ")"
    show (Lit value) = "(lit (" ++ show value ++ "))"
    show (If cond l r) = "(ifThenElse " ++ show cond ++ " " ++ show l ++ " " ++ show r ++ ")"
    show (Unary op value) = "(" ++ T.unpack (unaryName op) ++ " " ++ show value ++ ")"
    show (Binary op a b) = "(" ++ T.unpack (binaryName op) ++ " " ++ show a ++ " " ++ show b ++ ")"
    show (Agg (CollectAgg op _) expr) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (Agg (FoldAgg op _ _) expr) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"

normalize :: (Eq a, Ord a, Show a, Typeable a) => Expr a -> Expr a
normalize expr = case expr of
    Col name -> Col name
    Lit val -> Lit val
    If cond th el -> If (normalize cond) (normalize th) (normalize el)
    Unary op e -> Unary op (normalize e)
    Binary op e1 e2
        | binaryCommutative op ->
            let n1 = normalize e1
                n2 = normalize e2
             in case testEquality (typeOf n1) (typeOf n2) of
                    Nothing -> expr
                    Just Refl ->
                        if compareExpr n1 n2 == GT
                            then Binary op n2 n1 -- Swap to canonical order
                            else Binary op n1 n2
        | otherwise -> Binary op (normalize e1) (normalize e2)
    Agg strat e -> Agg strat (normalize e)

-- Compare expressions for ordering (used in normalization)
compareExpr :: Expr a -> Expr a -> Ordering
compareExpr e1 e2 = compare (exprKey e1) (exprKey e2)
  where
    exprKey :: Expr a -> String
    exprKey (Col name) = "0:" ++ T.unpack name
    exprKey (Lit val) = "1:" ++ show val
    exprKey (If c t e) = "2:" ++ exprKey c ++ exprKey t ++ exprKey e
    exprKey (Unary op e) = "3:" ++ T.unpack (unaryName op) ++ exprKey e
    exprKey (Binary op e1 e2) = "4:" ++ T.unpack (binaryName op) ++ exprKey e1 ++ exprKey e2
    exprKey (Agg (CollectAgg name _) e) = "5:" ++ T.unpack name ++ exprKey e
    exprKey (Agg (FoldAgg name _ _) e) = "5:" ++ T.unpack name ++ exprKey e

instance (Eq a, Columnable a) => Eq (Expr a) where
    (==) l r = eqNormalized (normalize l) (normalize r)
      where
        exprEq :: (Columnable b, Columnable c) => Expr b -> Expr c -> Bool
        exprEq e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
            Just Refl -> e1 == e2
            Nothing -> False
        eqNormalized :: Expr a -> Expr a -> Bool
        eqNormalized (Col n1) (Col n2) = n1 == n2
        eqNormalized (Lit v1) (Lit v2) = v1 == v2
        eqNormalized (If c1 t1 e1) (If c2 t2 e2) =
            c1 == c2 && t1 `exprEq` t2 && e1 `exprEq` e2
        eqNormalized (Unary op1 e1) (Unary op2 e2) = unaryName op1 == unaryName op2 && e1 `exprEq` e2
        eqNormalized (Binary op1 e1a e1b) (Binary op2 e2a e2b) = binaryName op1 == binaryName op2 && e1a `exprEq` e2a && e1b `exprEq` e2b
        eqNormalized (Agg (CollectAgg n1 _) e1) (Agg (CollectAgg n2 _) e2) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (Agg (FoldAgg n1 _ _) e1) (Agg (FoldAgg n2 _ _) e2) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized _ _ = False

instance (Ord a, Columnable a) => Ord (Expr a) where
    compare :: Expr a -> Expr a -> Ordering
    compare e1 e2 = case (e1, e2) of
        (Col n1, Col n2) -> compare n1 n2
        (Lit v1, Lit v2) -> compare v1 v2
        (If c1 t1 e1', If c2 t2 e2') ->
            compare c1 c2 <> exprComp t1 t2 <> exprComp e1' e2'
        (Unary op1 e1', Unary op2 e2') -> compare (unaryName op1) (unaryName op2) <> exprComp e1' e2'
        (Binary op1 a1 b1, Binary op2 a2 b2) ->
            compare (binaryName op1) (binaryName op2) <> exprComp a1 a2 <> exprComp b1 b2
        (Agg (CollectAgg n1 _) e1', Agg (CollectAgg n2 _) e2') -> compare n1 n2 <> exprComp e1' e2'
        (Agg (FoldAgg n1 _ _) e1', Agg (FoldAgg n2 _ _) e2') -> compare n1 n2 <> exprComp e1' e2'
        -- Different constructors - compare by priority
        (Col _, _) -> LT
        (_, Col _) -> GT
        (Lit _, _) -> LT
        (_, Lit _) -> GT
        (Unary{}, _) -> LT
        (_, Unary{}) -> GT
        (Binary{}, _) -> LT
        (_, Binary{}) -> GT
        (If{}, _) -> LT
        (_, If{}) -> GT
        (Agg{}, _) -> LT

exprComp :: (Columnable b, Columnable c) => Expr b -> Expr c -> Ordering
exprComp e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
    Just Refl -> e1 `compare` e2
    Nothing -> LT

replaceExpr ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    Expr a -> Expr b -> Expr c -> Expr c
replaceExpr new old expr = case testEquality (typeRep @b) (typeRep @c) of
    Just Refl -> case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> if old == expr then new else replace'
        Nothing -> expr
    Nothing -> replace'
  where
    replace' = case expr of
        (Col _) -> expr
        (Lit _) -> expr
        (If cond l r) ->
            If (replaceExpr new old cond) (replaceExpr new old l) (replaceExpr new old r)
        (Unary op value) -> Unary op (replaceExpr new old value)
        (Binary op l r) -> Binary op (replaceExpr new old l) (replaceExpr new old r)
        (Agg op expr) -> Agg op (replaceExpr new old expr)

eSize :: Expr a -> Int
eSize (Col _) = 1
eSize (Lit _) = 1
eSize (If c l r) = 1 + eSize c + eSize l + eSize r
eSize (Unary _ e) = 1 + eSize e
eSize (Binary _ l r) = 1 + eSize l + eSize r
eSize (Agg strategy expr) = eSize expr + 1

getColumns :: Expr a -> [T.Text]
getColumns (Col cName) = [cName]
getColumns expr@(Lit _) = []
getColumns (If cond l r) = getColumns cond <> getColumns l <> getColumns r
getColumns (Unary op value) = getColumns value
getColumns (Binary op l r) = getColumns l <> getColumns r
getColumns (Agg strategy expr) = getColumns expr

prettyPrint :: Expr a -> String
prettyPrint = go 0 0
  where
    indent :: Int -> String
    indent n = replicate (n * 2) ' '

    go :: Int -> Int -> Expr a -> String
    go depth prec expr = case expr of
        Col name -> T.unpack name
        Lit value -> show value
        If cond t e ->
            let inner =
                    "if "
                        ++ go (depth + 1) 0 cond
                        ++ "\n"
                        ++ indent (depth + 1)
                        ++ "then "
                        ++ go (depth + 1) 0 t
                        ++ "\n"
                        ++ indent (depth + 1)
                        ++ "else "
                        ++ go (depth + 1) 0 e
             in if prec > 0 then "(" ++ inner ++ ")" else inner
        Unary op arg -> case unarySymbol op of
            Nothing -> T.unpack (unaryName op) ++ "(" ++ go depth 0 arg ++ ")"
            Just sym -> T.unpack sym ++ "(" ++ go depth 0 arg ++ ")"
        Binary op l r ->
            let p = binaryPrecedence op
                inner = case binarySymbol op of
                    Just name -> go depth p l ++ " " ++ T.unpack name ++ " " ++ go depth p r
                    Nothing ->
                        T.unpack (binaryName op) ++ "(" ++ go depth p l ++ ", " ++ go depth p r ++ ")"
             in if prec > p then "(" ++ inner ++ ")" else inner
        Agg (CollectAgg op _) arg -> T.unpack op ++ "(" ++ go depth 0 arg ++ ")"
        Agg (FoldAgg op _ _) arg -> T.unpack op ++ "(" ++ go depth 0 arg ++ ")"
