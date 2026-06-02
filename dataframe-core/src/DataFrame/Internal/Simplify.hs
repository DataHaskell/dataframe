{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Simplify (
    simplify,
    simplifyPredicatePair,

    -- * Path-condition entailment (for fitted-tree pruning)
    PredFact,
    factTrue,
    factFalse,
    entails,
) where

import Control.Monad (guard)
import Data.Maybe (fromMaybe)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import Type.Reflection (eqTypeRep, typeRep, (:~~:) (HRefl), pattern App)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (
    BinaryOp,
    Expr (..),
    UnaryOp (unaryName),
    eqExpr,
    normalize,
 )
import DataFrame.Operators (
    NullAnd,
    NullEq,
    NullGeq,
    NullGt,
    NullLeq,
    NullLt,
    NullNeq,
    NullOr,
    (.==.),
 )

simplify :: forall a. (Columnable a) => Expr a -> Expr a
simplify e
    | isBoolish @a = fixpoint (10 :: Int) e
    | otherwise = e
  where
    fixpoint 0 x = x
    fixpoint n x = let x' = simplifyB x in if eqExpr x x' then x else fixpoint (n - 1) x'

isBoolish :: forall a. (Columnable a) => Bool
isBoolish =
    case ( testEquality (typeRep @a) (typeRep @Bool)
         , testEquality (typeRep @a) (typeRep @(Maybe Bool))
         ) of
        (Just Refl, _) -> True
        (_, Just Refl) -> True
        _ -> False

data Conn = ConnAnd | ConnOr

connOf :: forall op c b r. (BinaryOp op) => op c b r -> Maybe Conn
connOf _
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullAnd) = Just ConnAnd
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullOr) = Just ConnOr
    | otherwise = Nothing

simplifyB :: forall a. (Columnable a) => Expr a -> Expr a
simplifyB expr = case expr of
    Binary (op :: op c b a) l r
        | Just conn <- connOf op
        , Just Refl <- testEquality (typeRep @c) (typeRep @a)
        , Just Refl <- testEquality (typeRep @b) (typeRep @a) ->
            let l' = simplifyB l; r' = simplifyB r
             in fromMaybe (Binary op l' r') (combine conn l' r')
        | otherwise -> expr
    Unary (op :: op b a) inner
        | Just Refl <- testEquality (typeRep @a) (typeRep @Bool)
        , Just Refl <- testEquality (typeRep @b) (typeRep @Bool)
        , unaryName op == "not" ->
            simplifyNot op (simplifyB inner)
        | otherwise -> expr
    If c t f ->
        let c' = simplify c
            t' = simplifyB t
            f' = simplifyB f
         in case asBoolLit c' of
                Just True -> t'
                Just False -> f'
                Nothing
                    | eqExpr t' f' -> t'
                    | Just Refl <- testEquality (typeRep @a) (typeRep @Bool)
                    , asBoolLit t' == Just True
                    , asBoolLit f' == Just False ->
                        c'
                    | otherwise -> If c' t' f'
    _ -> expr

simplifyNot :: (UnaryOp op) => op Bool Bool -> Expr Bool -> Expr Bool
simplifyNot op inner = case asBoolLit inner of
    Just b -> Lit (not b)
    Nothing -> case inner of
        Unary (op2 :: op2 b2 Bool) inner2
            | unaryName op2 == "not"
            , Just Refl <- testEquality (typeRep @b2) (typeRep @Bool) ->
                inner2
        _ -> Unary op inner

combine :: (Columnable a) => Conn -> Expr a -> Expr a -> Maybe (Expr a)
combine ConnAnd = combineAnd
combine ConnOr = combineOr

asBoolLit :: forall a. (Columnable a) => Expr a -> Maybe Bool
asBoolLit (Lit v) =
    case testEquality (typeRep @a) (typeRep @Bool) of
        Just Refl -> Just v
        Nothing -> case testEquality (typeRep @a) (typeRep @(Maybe Bool)) of
            Just Refl -> v
            Nothing -> Nothing
asBoolLit _ = Nothing

{- | Polymorphic boolean literal: @Lit b@ for @Expr Bool@, @Lit (Just b)@ for
@Expr (Maybe Bool)@.
-}
litBoolish :: forall a. (Columnable a) => Bool -> Maybe (Expr a)
litBoolish v =
    case testEquality (typeRep @a) (typeRep @Bool) of
        Just Refl -> Just (Lit v)
        Nothing -> case testEquality (typeRep @a) (typeRep @(Maybe Bool)) of
            Just Refl -> Just (Lit (Just v))
            Nothing -> Nothing

combineAnd :: (Columnable a) => Expr a -> Expr a -> Maybe (Expr a)
combineAnd l r
    | eqExpr l r = Just l
    | asBoolLit l == Just False = litBoolish False
    | asBoolLit r == Just False = litBoolish False
    | asBoolLit l == Just True = Just r
    | asBoolLit r == Just True = Just l
    | absorbs ConnOr l r = Just l
    | absorbs ConnOr r l = Just r
    | otherwise = simplifyPredicatePair True l r

combineOr :: (Columnable a) => Expr a -> Expr a -> Maybe (Expr a)
combineOr l r
    | eqExpr l r = Just l
    | asBoolLit l == Just True = litBoolish True
    | asBoolLit r == Just True = litBoolish True
    | asBoolLit l == Just False = Just r
    | asBoolLit r == Just False = Just l
    | absorbs ConnAnd l r = Just l
    | absorbs ConnAnd r l = Just r
    | otherwise = simplifyPredicatePair False l r

absorbs :: (Columnable a) => Conn -> Expr a -> Expr a -> Bool
absorbs conn x (Binary (op :: op c b a) ya yb)
    | Just c' <- connOf op
    , sameConn conn c'
    , Just Refl <- testEquality (typeRep @c) (typeRep @a)
    , Just Refl <- testEquality (typeRep @b) (typeRep @a) =
        eqExpr x ya || eqExpr x yb
absorbs _ _ _ = False

sameConn :: Conn -> Conn -> Bool
sameConn ConnAnd ConnAnd = True
sameConn ConnOr ConnOr = True
sameConn _ _ = False

data Cmp = CLt | CLeq | CGt | CGeq | CEq | CNeq deriving (Eq)

data NullK = Total | FalseOnNull | UnknownOnNull deriving (Eq)

data Atom = Atom
    { aCmp :: Cmp
    , aThr :: !Double
    , aKey :: String
    , aNull :: NullK
    , aIntegral :: Bool
    }

cmpOf :: forall op c b r. (BinaryOp op) => op c b r -> Maybe Cmp
cmpOf _
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullLt) = Just CLt
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullLeq) = Just CLeq
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullGt) = Just CGt
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullGeq) = Just CGeq
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullEq) = Just CEq
    | Just HRefl <- eqTypeRep (typeRep @op) (typeRep @NullNeq) = Just CNeq
    | otherwise = Nothing

isLower, isUpper :: Cmp -> Bool
isLower c = c == CGt || c == CGeq
isUpper c = c == CLt || c == CLeq

-- | True if @x@ is a @Maybe _@ type.
isMaybeTy :: forall x. (Columnable x) => Bool
isMaybeTy = case typeRep @x of
    App con _ -> case eqTypeRep con (typeRep @Maybe) of Just HRefl -> True; _ -> False
    _ -> False

litDouble :: forall b. (Columnable b) => Expr b -> Maybe Double
litDouble (Lit v) =
    case testEquality (typeRep @b) (typeRep @Double) of
        Just Refl -> Just v
        Nothing -> case testEquality (typeRep @b) (typeRep @Int) of
            Just Refl -> Just (fromIntegral v)
            Nothing -> case testEquality (typeRep @b) (typeRep @(Maybe Double)) of
                Just Refl -> v
                Nothing -> case testEquality (typeRep @b) (typeRep @(Maybe Int)) of
                    Just Refl -> fromIntegral <$> v
                    Nothing -> Nothing
litDouble _ = Nothing

{- | True for a column lifted from an integral type (never NaN): @toDouble (col …)@
or a column whose type is itself integral.
-}
integralColE :: forall c. (Columnable c) => Expr c -> Bool
integralColE (Unary op _) = unaryName op == "toDouble"
integralColE _ =
    or
        [ matches @Int
        , matches @(Maybe Int)
        ]
  where
    matches :: forall t. (Columnable t) => Bool
    matches = case testEquality (typeRep @c) (typeRep @t) of Just Refl -> True; _ -> False

atomOf :: forall a. (Columnable a) => Expr a -> Maybe Atom
atomOf (Unary fm (Binary (op :: op c b r) (colE :: Expr c) litE))
    | unaryName fm == "fromMaybe"
    , Just cmp <- cmpOf op
    , Just t <- litDouble litE =
        Just (Atom cmp t (show (normalize colE)) FalseOnNull (integralColE colE))
atomOf (Binary (op :: op c b a) (colE :: Expr c) litE)
    | Just cmp <- cmpOf op
    , Just t <- litDouble litE =
        let nk = if isMaybeTy @c then UnknownOnNull else Total
         in Just (Atom cmp t (show (normalize colE)) nk (integralColE colE))
atomOf _ = Nothing

simplifyPredicatePair ::
    forall a. (Columnable a) => Bool -> Expr a -> Expr a -> Maybe (Expr a)
simplifyPredicatePair isAnd a b = do
    atomA <- atomOf a
    atomB <- atomOf b
    guard (aKey atomA == aKey atomB)
    let nk = aNull atomA
        integral = aIntegral atomA
    if isAnd
        then andAtoms a atomA b atomB nk integral
        else orAtoms a atomA b atomB nk integral

-- | Contradiction folds to a literal False unless null-rows make it unknown.
litFalseGated :: (Columnable a) => NullK -> Maybe (Expr a)
litFalseGated UnknownOnNull = Nothing
litFalseGated _ = litBoolish False

{- | Tautology to literal True is sound only for total (never-null) atoms; the
exhaustive-cover form additionally needs a non-NaN (integral) column.
-}
litTrueTotal :: (Columnable a) => NullK -> Maybe (Expr a)
litTrueTotal Total = litBoolish True
litTrueTotal _ = Nothing

andAtoms ::
    (Columnable a) =>
    Expr a -> Atom -> Expr a -> Atom -> NullK -> Bool -> Maybe (Expr a)
andAtoms a atomA b atomB nk _ =
    let cA = aCmp atomA; tA = aThr atomA; cB = aCmp atomB; tB = aThr atomB
     in if
            | isLower cA, isLower cB, cA == cB -> Just (if tA >= tB then a else b)
            | isUpper cA, isUpper cB, cA == cB -> Just (if tA <= tB then a else b)
            | isLower cA, isUpper cB -> lu cA tA cB tB
            | isUpper cA, isLower cB -> lu cB tB cA tA
            | cA == CEq, cB == CEq -> if tA == tB then Just a else litFalseGated nk
            | cA == CEq, cB == CNeq -> if tA == tB then litFalseGated nk else Just a
            | cA == CNeq, cB == CEq -> if tA == tB then litFalseGated nk else Just b
            | cA == CEq -> if satisfies tA cB tB then Just a else litFalseGated nk
            | cB == CEq -> if satisfies tB cA tA then Just b else litFalseGated nk
            | cA == CNeq, cB == CNeq -> Nothing
            | cA == CNeq -> if outside tA cB tB then Just b else Nothing
            | cB == CNeq -> if outside tB cA tA then Just a else Nothing
            | otherwise -> Nothing
  where
    lu lc lo uc hi
        | lo > hi = litFalseGated nk
        | lo == hi, lc == CGeq, uc == CLeq = pointEq a lo
        | lo == hi = litFalseGated nk
        | otherwise = Nothing

orAtoms ::
    (Columnable a) =>
    Expr a -> Atom -> Expr a -> Atom -> NullK -> Bool -> Maybe (Expr a)
orAtoms a atomA b atomB nk integral =
    let cA = aCmp atomA; tA = aThr atomA; cB = aCmp atomB; tB = aThr atomB
     in if
            | isLower cA, isLower cB, cA == cB -> Just (if tA <= tB then a else b)
            | isUpper cA, isUpper cB, cA == cB -> Just (if tA >= tB then a else b)
            | isUpper cA
            , isLower cB
            , nk == Total
            , integral
            , covers cB tB cA tA ->
                litTrueTotal nk
            | isLower cA
            , isUpper cB
            , nk == Total
            , integral
            , covers cA tA cB tB ->
                litTrueTotal nk
            | cA == CNeq, cB == CNeq -> if tA == tB then Just a else litTrueTotal nk
            | cA == CEq, cB == CNeq -> if tA == tB then litTrueTotal nk else Just b
            | cA == CNeq, cB == CEq -> if tA == tB then litTrueTotal nk else Just a
            | cA == CEq, cB == CEq -> if tA == tB then Just a else Nothing
            | otherwise -> Nothing

{- | Build @col == t@ for the point-collapse rule; only strict @Expr Bool@ over a
@Double@ column (otherwise bail).
-}
pointEq :: forall a. (Columnable a) => Expr a -> Double -> Maybe (Expr a)
pointEq atom lo = case testEquality (typeRep @a) (typeRep @Bool) of
    Just Refl -> (\colE -> colE .==. Lit lo) <$> recoverColD atom
    Nothing -> Nothing

recoverColD :: Expr x -> Maybe (Expr Double)
recoverColD (Binary _ (colE :: Expr c) _) =
    case testEquality (typeRep @c) (typeRep @Double) of
        Just Refl -> Just colE
        _ -> Nothing
recoverColD (Unary _ inner) = recoverColD inner
recoverColD _ = Nothing

covers :: Cmp -> Double -> Cmp -> Double -> Bool
covers lowerCmp lo upperCmp hi =
    lo < hi || (lo == hi && (lowerCmp == CGeq || upperCmp == CLeq))

satisfies :: Double -> Cmp -> Double -> Bool
satisfies t CGt tb = t > tb
satisfies t CGeq tb = t >= tb
satisfies t CLt tb = t < tb
satisfies t CLeq tb = t <= tb
satisfies _ _ _ = False

outside :: Double -> Cmp -> Double -> Bool
outside t CGt tb = t <= tb
outside t CGeq tb = t < tb
outside t CLt tb = t >= tb
outside t CLeq tb = t > tb
outside _ _ _ = False

-- ---------------------------------------------------------------------------
-- Path-condition entailment for fitted-tree pruning.
-- ---------------------------------------------------------------------------

-- | A known same-column threshold fact accumulated along a tree path.
data PredFact = PredFact !String !Cmp !Double

-- | The fact a branch's true edge establishes (the condition holds).
factTrue :: Expr Bool -> Maybe PredFact
factTrue e = (\a -> PredFact (aKey a) (aCmp a) (aThr a)) <$> atomOf e

{- | The fact a branch's false edge establishes (the negated condition). Only
sound for non-NaN (integral) columns — a NaN row takes the false edge too,
so @¬(x>t)@ is not a clean @x<=t@ bound for floats.
-}
factFalse :: Expr Bool -> Maybe PredFact
factFalse e = do
    a <- atomOf e
    guard (aIntegral a && aNull a == Total)
    nc <- negCmp (aCmp a)
    pure (PredFact (aKey a) nc (aThr a))

negCmp :: Cmp -> Maybe Cmp
negCmp CLt = Just CGeq
negCmp CLeq = Just CGt
negCmp CGt = Just CLeq
negCmp CGeq = Just CLt
negCmp _ = Nothing

{- | @entails facts cond@: 'Just' 'True' when the path facts force @cond@ true,
'Just' 'False' when they force it false, 'Nothing' when undecided.
-}
entails :: [PredFact] -> Expr Bool -> Maybe Bool
entails facts cond = do
    a <- atomOf cond
    let decisions =
            [ d
            | PredFact fk fc ft <- facts
            , fk == aKey a
            , Just d <- [factImplies (fc, ft) (aCmp a, aThr a)]
            ]
    case decisions of
        (d : _) -> Just d
        [] -> Nothing

{- | Does the fact's solution set sit inside @cond@ ('Just' 'True'), disjoint
from it ('Just' 'False'), or neither ('Nothing')? Boundary strictness is
honoured: e.g. @x<=t@ does NOT entail @x<t@, and @x>=t ∧ x<=t@ is not empty.
-}
factImplies :: (Cmp, Double) -> (Cmp, Double) -> Maybe Bool
factImplies (fc, ft) (cc, tc)
    | isLower fc, isLower cc, subset = Just True
    | isUpper fc, isUpper cc, subset = Just True
    | isLower fc, isUpper cc, disjointAtEq = Just False
    | isUpper fc, isLower cc, disjointBelow = Just False
    | otherwise = Nothing
  where
    fIncl = fc == CGeq || fc == CLeq
    cIncl = cc == CGeq || cc == CLeq
    -- same-direction containment: strictly tighter, or equal threshold where the
    -- fact's boundary inclusivity is no stronger than the condition's.
    subset =
        (if isLower fc then ft > tc else ft < tc)
            || (ft == tc && (not fIncl || cIncl))
    -- lower fact ∩ upper cond empty: fact starts above cond's top, or they meet
    -- at a point that is not in both.
    disjointAtEq = ft > tc || (ft == tc && not (fIncl && cIncl))
    -- upper fact ∩ lower cond empty (mirror).
    disjointBelow = ft < tc || (ft == tc && not (fIncl && cIncl))
