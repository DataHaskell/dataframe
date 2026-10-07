{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Reload expressions from their two printed forms: the 'Show' form
(@(ifThenElse (leq (toDouble (col \@Int "Age")) (lit (3.5))) ...)@), which carries
column types, and the 'prettyPrint' form (@if toDouble(Age) .<=. 3.5 ...@), which
needs a 'Schema' to tell bare column names apart from operators.
-}
module DataFrame.Expression.Parse (
    ColType (..),
    Schema,
    schemaOf,
    parseShown,
    parsePretty,
    parseShownAs,
    parsePrettyAs,
) where

import Data.Char (isAlphaNum, isDigit, isSpace)
import Data.List (isPrefixOf, sortBy)
import qualified Data.Map.Strict as M
import Data.Ord (Down (..), comparing)
import qualified Data.Text as T

import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame, columnNames)
import Type.Reflection (typeRep)

import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr (..), SomeExpr (..), fromSomeExpr)
import DataFrame.Internal.Expression.Operators (
    (.&&.),
    (./=.),
    (.<.),
    (.<=.),
    (.==.),
    (.>.),
    (.>=.),
    (.||.),
 )
import DataFrame.Internal.Expression.Reflect (
    Dict (..),
    fracDict,
    integralDict,
    numDict,
    ordDict,
    realDict,
 )
import DataFrame.Operations.Core (columnAsVector)

-- | Column types the parser can build expressions over.
data ColType = TInt | TDouble | TText | TBool
    deriving (Eq, Show)

type Schema = M.Map T.Text ColType

-- | Column types of a frame, for the columns whose type the parser supports.
schemaOf :: DataFrame -> Schema
schemaOf df = M.fromList [(c, t) | c <- columnNames df, Just t <- [typeOf c]]
  where
    typeOf c
        | ok (F.col @Int c) = Just TInt
        | ok (F.col @Double c) = Just TDouble
        | ok (F.col @T.Text c) = Just TText
        | ok (F.col @Bool c) = Just TBool
        | otherwise = Nothing
    ok :: forall a. (Columnable a) => Expr a -> Bool
    ok e = either (const False) (const True) (columnAsVector e df)

-- | The untyped syntax both printers share.
data Ast
    = ACol T.Text (Maybe ColType)
    | -- | The value and whether it was written as a Double (has a point or exponent).
      ANum Double Bool
    | AStr T.Text
    | ABool Bool
    | AIf Ast Ast Ast
    | ACall T.Text [Ast]
    deriving (Show)

-- Elaboration -------------------------------------------------------------

-- | Two expressions of one type (the result of making operands agree).
data Same where
    Same :: (Columnable t) => Expr t -> Expr t -> Same

typeName :: SomeExpr -> String
typeName (SomeExpr (_ :: Expr t)) = show (typeRep @t)

-- | Use evidence for a class on an expression's type, or report which operator lacked it.
need :: String -> Maybe (Dict c) -> ((c) => Either String r) -> Either String r
need what d k = maybe (Left (what ++ ": operand type not supported")) (\Dict -> k) d

elab :: Schema -> Ast -> Either String SomeExpr
elab schema ast = case ast of
    ACol c (Just t) -> pure (colOf c t)
    ACol c Nothing ->
        maybe (Left ("unknown column " ++ show c)) (pure . colOf c) (M.lookup c schema)
    ANum v True -> pure (SomeExpr (F.lit v))
    ANum v False -> pure (SomeExpr (F.lit (round v :: Int)))
    AStr s -> pure (SomeExpr (F.lit s))
    ABool b -> pure (SomeExpr (F.lit b))
    AIf c t e -> do
        c' <- elab schema c
        cb <-
            maybe
                (Left ("if: condition has type " ++ typeName c'))
                pure
                (fromSomeExpr @Bool c')
        t' <- elab schema t
        e' <- elab schema e
        Same a b <- unify t' e'
        pure (SomeExpr (F.ifThenElse cb a b))
    ACall name args -> do
        args' <- mapM (elab schema) args
        call name args'
  where
    colOf c t = case t of
        TInt -> SomeExpr (F.col @Int c)
        TDouble -> SomeExpr (F.col @Double c)
        TText -> SomeExpr (F.col @T.Text c)
        TBool -> SomeExpr (F.col @Bool c)

-- | Make two operands the same type: a numeric literal adopts the other side's type.
unify :: SomeExpr -> SomeExpr -> Either String Same
unify a@(SomeExpr (x :: Expr p)) b@(SomeExpr (y :: Expr q))
    | Just Refl <- testEquality (typeRep @p) (typeRep @q) = pure (Same x y)
    | Just (Lit i) <- fromSomeExpr @Int a
    , Just yd <- fromSomeExpr @Double b =
        pure (Same (F.lit (fromIntegral i)) yd)
    | Just xd <- fromSomeExpr @Double a
    , Just (Lit i) <- fromSomeExpr @Int b =
        pure (Same xd (F.lit (fromIntegral i)))
    | Just (Lit d) <- fromSomeExpr @Double a
    , Just yi <- fromSomeExpr @Int b
    , whole d =
        pure (Same (F.lit (round d)) yi)
    | Just xi <- fromSomeExpr @Int a
    , Just (Lit d) <- fromSomeExpr @Double b
    , whole d =
        pure (Same xi (F.lit (round d)))
    | otherwise =
        Left ("operand types differ: " ++ typeName a ++ " vs " ++ typeName b)
  where
    whole d = fromIntegral (round d :: Int) == (d :: Double)

call :: T.Text -> [SomeExpr] -> Either String SomeExpr
call name args = case (name, args) of
    ("toDouble", [SomeExpr (e :: Expr t)]) -> need "toDouble" (realDict @t) (pure (SomeExpr (F.toDouble e)))
    ("not", [a]) ->
        maybe
            (Left "not needs a Bool operand")
            (pure . SomeExpr . F.not)
            (fromSomeExpr @Bool a)
    ("negate", [SomeExpr (e :: Expr t)]) -> need "negate" (numDict @t) (pure (SomeExpr (negate e)))
    ("abs", [SomeExpr (e :: Expr t)]) -> need "abs" (numDict @t) (pure (SomeExpr (abs e)))
    (_, [a, b]) -> do
        Same x y <- unify a b
        binary name x y
    _ ->
        Left
            ( "unsupported call "
                ++ T.unpack name
                ++ " with "
                ++ show (length args)
                ++ " arguments"
            )

binary ::
    forall t. (Columnable t) => T.Text -> Expr t -> Expr t -> Either String SomeExpr
binary name x y = case name of
    "add" -> need "add" (numDict @t) (pure (SomeExpr (x + y)))
    "sub" -> need "sub" (numDict @t) (pure (SomeExpr (x - y)))
    "mult" -> need "mult" (numDict @t) (pure (SomeExpr (x * y)))
    "divide" -> need "divide" (fracDict @t) (pure (SomeExpr (x / y)))
    "div" -> need "div" (integralDict @t) (pure (SomeExpr (F.div x y)))
    "mod" -> need "mod" (integralDict @t) (pure (SomeExpr (F.mod x y)))
    "min" -> need "min" (ordDict @t) (pure (SomeExpr (F.min x y)))
    "max" -> need "max" (ordDict @t) (pure (SomeExpr (F.max x y)))
    "eq" -> pure (SomeExpr (x .==. y))
    "neq" -> pure (SomeExpr (x ./=. y))
    "lt" -> need "lt" (ordDict @t) (pure (SomeExpr (x .<. y)))
    "gt" -> need "gt" (ordDict @t) (pure (SomeExpr (x .>. y)))
    "leq" -> need "leq" (ordDict @t) (pure (SomeExpr (x .<=. y)))
    "geq" -> need "geq" (ordDict @t) (pure (SomeExpr (x .>=. y)))
    "and" -> bool (.&&.)
    "or" -> bool (.||.)
    _ -> Left ("unsupported operator " ++ T.unpack name)
  where
    bool f = case testEquality (typeRep @t) (typeRep @Bool) of
        Just Refl -> pure (SomeExpr (f x y))
        Nothing -> Left (T.unpack name ++ " needs Bool operands")

-- The Show form -----------------------------------------------------------

data SExp = SAtom String | SList [SExp]
    deriving (Show)

sexp :: String -> Either String (SExp, String)
sexp s = case dropWhile isSpace s of
    '(' : rest -> list rest []
    '"' : _ -> case reads (dropWhile isSpace s) :: [(String, String)] of
        [(str, rest)] -> pure (SAtom (show str), rest)
        _ -> Left "bad string literal"
    rest ->
        let (tok, more) = span (\c -> not (isSpace c) && c /= '(' && c /= ')') rest
         in if null tok
                then Left ("unexpected input near " ++ take 20 rest)
                else pure (SAtom tok, more)
  where
    list str acc = case dropWhile isSpace str of
        ')' : rest -> pure (SList (reverse acc), rest)
        "" -> Left "unbalanced parentheses"
        more -> do
            (e, rest) <- sexp more
            list rest (e : acc)

fromSExp :: SExp -> Either String Ast
fromSExp e = case e of
    SList [SAtom "col", SAtom ('@' : ty), SAtom name] -> ACol <$> strLit name <*> (Just <$> colType ty)
    SList [SAtom "col", SAtom "@Maybe", SAtom _, SAtom _] -> Left "nullable columns are not supported"
    SList [SAtom "lit", inner] -> literal inner
    SList [SAtom "ifThenElse", c, t, f] -> AIf <$> fromSExp c <*> fromSExp t <*> fromSExp f
    SList (SAtom fn : args) -> ACall (T.pack fn) <$> mapM fromSExp args
    SAtom a -> Left ("bare atom " ++ a)
    SList [] -> Left "empty list"
  where
    strLit s = case reads s :: [(String, String)] of
        [(v, "")] -> pure (T.pack v)
        _ -> Left ("bad name " ++ s)
    colType ty = case ty of
        "Int" -> pure TInt
        "Double" -> pure TDouble
        "Text" -> pure TText
        "Bool" -> pure TBool
        _ -> Left ("unsupported column type " ++ ty)
    -- (lit (3.5)), (lit ("x")), (lit (True)); the inner parentheses come from show.
    literal inner = case inner of
        SList [SAtom v] -> atomLit v
        SAtom v -> atomLit v
        SList [SAtom "-", SAtom v] -> atomLit ('-' : v)
        _ -> Left ("bad literal " ++ show inner)
    atomLit v
        | v == "True" = pure (ABool True)
        | v == "False" = pure (ABool False)
        | take 1 v == "\"" = AStr <$> strLit v
        | otherwise = case reads v :: [(Double, String)] of
            [(d, "")] -> pure (ANum d (any (`elem` (".eE" :: String)) v))
            _ -> Left ("bad literal " ++ v)

-- | Parse the output of 'show' on an 'Expr'. No schema is needed: columns carry their types.
parseShown :: String -> Either String SomeExpr
parseShown s = do
    (e, rest) <- sexp s
    if all isSpace rest then pure () else Left ("trailing input: " ++ take 30 rest)
    ast <- fromSExp e
    elab M.empty ast

-- The pretty form ---------------------------------------------------------

data Tok
    = TNum Double Bool
    | TStr T.Text
    | TName T.Text
    | TFun T.Text
    | TOp String
    | TLParen
    | TRParen
    | TComma
    | TIf
    | TElse
    deriving (Eq, Show)

operators :: [String]
-- Dotted forms come from Operators; the bare forms from Functions (eq, leq, ...); same names either way.
operators =
    sortBy
        (comparing (Down . length))
        [ ".<=."
        , ".>=."
        , ".==."
        , "./=."
        , ".&&."
        , ".||."
        , ".<."
        , ".>."
        , "<="
        , ">="
        , "=="
        , "/="
        , "<"
        , ">"
        , "//"
        , "+"
        , "-"
        , "*"
        , "/"
        , "~"
        ]

functions :: [T.Text]
functions = ["toDouble", "min", "max", "mod", "abs", "negate", "not", "isJust"]

lexPretty :: Schema -> String -> Either String [Tok]
lexPretty schema = go
  where
    names = sortBy (comparing (Down . T.length)) (M.keys schema)
    go [] = pure []
    go s@(c : rest)
        | isSpace c = go rest
        | c == '(' = (TLParen :) <$> go rest
        | c == ')' = (TRParen :) <$> go rest
        | c == ',' = (TComma :) <$> go rest
        | c == '"' = case reads s :: [(String, String)] of
            [(str, more)] -> (TStr (T.pack str) :) <$> go more
            _ -> Left "bad string literal"
        | Just (n, more) <- keyword s = (n :) <$> go more
        | Just (n, more) <- longest names s = (TName n :) <$> go more
        | Just (f, more) <- longest functions s
        , take 1 (dropWhile isSpace more) == "(" =
            (TFun f :) <$> go more
        | isDigit c = number s
        -- The printer spaces binary operators (`a - b`), so a '-' touching a digit is a negative literal.
        | c == '-', (d : _) <- rest, isDigit d = number s
        | Just op <- firstPrefix operators s = (TOp op :) <$> go (drop (length op) s)
        | otherwise = Left ("cannot lex near: " ++ take 30 s)
    keyword s
        | "else" `isPrefixOf` s && boundary (drop 4 s) = Just (TElse, drop 4 s)
        | "if" `isPrefixOf` s && boundary (drop 2 s) = Just (TIf, drop 2 s)
        | otherwise = Nothing
    boundary more = null more || not (isAlphaNum (head more))
    longest cands s = case [n | n <- cands, T.unpack n `isPrefixOf` s] of
        (n : _) -> Just (n, drop (T.length n) s)
        [] -> Nothing
    firstPrefix cands s = case [op | op <- cands, op `isPrefixOf` s] of
        (op : _) -> Just op
        [] -> Nothing
    number s0 =
        let (sign, s) = case s0 of
                '-' : more0 -> ("-", more0)
                _ -> ("", s0)
            (body0, more) =
                span (\ch -> isDigit ch || ch == '.' || ch == 'e' || ch == 'E' || ch == '-') s
            body = sign ++ body0
            -- A trailing '-' belongs to the next token, not an exponent.
            (num, more') =
                if not (null body) && last body == '-'
                    then (init body, '-' : more)
                    else (body, more)
         in case reads num :: [(Double, String)] of
                [(d, "")] -> (TNum d (any (`elem` (".eE" :: String)) num) :) <$> go more'
                _ -> Left ("bad number " ++ num)

-- Precedence climbing over the printer's precedences.
type P a = [Tok] -> Either String (a, [Tok])

parseExpr :: P Ast
parseExpr = parseOr

parseOr
    , parseAnd
    , parseCmp
    , parseAdd
    , parseMul
    , parseUnary
    , parsePrimary ::
        P Ast
parseOr ts = do
    (l, rest) <- parseAnd ts
    case rest of
        TOp ".||." : more -> do
            (r, rest') <- parseOr more
            pure (ACall "or" [l, r], rest')
        _ -> pure (l, rest)
parseAnd ts = do
    (l, rest) <- parseCmp ts
    case rest of
        TOp ".&&." : more -> do
            (r, rest') <- parseAnd more
            pure (ACall "and" [l, r], rest')
        _ -> pure (l, rest)
parseCmp ts = do
    (l, rest) <- parseAdd ts
    case rest of
        TOp op : more | Just name <- lookup op cmpNames -> do
            (r, rest') <- parseAdd more
            pure (ACall name [l, r], rest')
        _ -> pure (l, rest)
  where
    cmpNames =
        [ (".<=.", "leq")
        , (".>=.", "geq")
        , (".==.", "eq")
        , ("./=.", "neq")
        , (".<.", "lt")
        , (".>.", "gt")
        , ("<=", "leq")
        , (">=", "geq")
        , ("==", "eq")
        , ("/=", "neq")
        , ("<", "lt")
        , (">", "gt")
        ]
parseAdd ts = parseMul ts >>= uncurry loop
  where
    loop l (TOp "+" : more) = parseMul more >>= \(r, rest) -> loop (ACall "add" [l, r]) rest
    loop l (TOp "-" : more) = parseMul more >>= \(r, rest) -> loop (ACall "sub" [l, r]) rest
    loop l rest = pure (l, rest)
parseMul ts = parseUnary ts >>= uncurry loop
  where
    loop l (TOp "*" : more) = parseUnary more >>= \(r, rest) -> loop (ACall "mult" [l, r]) rest
    loop l (TOp "/" : more) = parseUnary more >>= \(r, rest) -> loop (ACall "divide" [l, r]) rest
    loop l (TOp "//" : more) = parseUnary more >>= \(r, rest) -> loop (ACall "div" [l, r]) rest
    loop l rest = pure (l, rest)
parseUnary (TOp "~" : more) = do
    (e, rest) <- parseUnary more
    pure (ACall "not" [e], rest)
parseUnary (TOp "-" : more) = do
    (e, rest) <- parseUnary more
    pure (ACall "negate" [e], rest)
parseUnary ts = parsePrimary ts
parsePrimary ts = case ts of
    TNum v isD : rest -> pure (ANum v isD, rest)
    TStr s : rest -> pure (AStr s, rest)
    TName n : rest -> pure (ACol n Nothing, rest)
    TFun f : TLParen : rest -> do
        (args, rest') <- parseArgs rest []
        pure (ACall f args, rest')
    TLParen : rest -> do
        (e, rest') <- parseExpr rest
        case rest' of
            TRParen : more -> pure (e, more)
            _ -> Left "expected )"
    TIf : rest -> parseIf rest
    _ -> Left ("unexpected token " ++ show (take 3 ts))
  where
    parseArgs toks acc = do
        (e, rest) <- parseExpr toks
        case rest of
            TComma : more -> parseArgs more (e : acc)
            TRParen : more -> pure (reverse (e : acc), more)
            _ -> Left "expected , or ) in call"

-- if C T else if C2 T2 else E
parseIf :: P Ast
parseIf ts = do
    (c, rest) <- parseExpr ts
    (t, rest') <- parseExpr rest
    case rest' of
        TElse : TIf : more -> do
            (e, rest'') <- parseIf more
            pure (AIf c t e, rest'')
        TElse : more -> do
            (e, rest'') <- parseExpr more
            pure (AIf c t e, rest'')
        _ -> Left "expected else"

-- | Parse the output of 'prettyPrint', given the frame's column types.
parsePretty :: Schema -> String -> Either String SomeExpr
parsePretty schema s = do
    toks <- lexPretty schema s
    (ast, rest) <- parseExpr toks
    if null rest then pure () else Left ("trailing tokens: " ++ show (take 5 rest))
    elab schema ast

parseShownAs :: forall a. (Columnable a) => String -> Either String (Expr a)
parseShownAs s = parseShown s >>= asType

parsePrettyAs ::
    forall a. (Columnable a) => Schema -> String -> Either String (Expr a)
parsePrettyAs schema s = parsePretty schema s >>= asType

asType :: forall a. (Columnable a) => SomeExpr -> Either String (Expr a)
asType u =
    maybe
        (Left "expression has a different type from the one requested")
        pure
        (fromSomeExpr u)
