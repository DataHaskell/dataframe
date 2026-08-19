{-# LANGUAGE CPP #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Internal.Parsing where

import qualified Data.ByteString.Char8 as C
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO

import Control.Applicative (many, (<|>))
import Control.Monad (guard)
import Data.Attoparsec.Text hiding (decimal, double, signed)
import Data.Char (isDigit)
import Data.Foldable (fold)
import Data.Text.Read (decimal, signed)
import Data.Time (Day, defaultTimeLocale, parseTimeM)
import GHC.Stack (HasCallStack)
import System.IO (Handle, IOMode (..), hIsEOF, hTell, withFile)
import Prelude hiding (takeWhile)

isNullish :: T.Text -> Bool
isNullish =
    ( `S.member`
        S.fromList
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
    )

isNullishBS :: C.ByteString -> Bool
isNullishBS =
    ( `S.member`
        S.fromList
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
    )

isTrueish :: T.Text -> Bool
isTrueish t = t `elem` ["True", "true", "TRUE"]

isFalseish :: T.Text -> Bool
isFalseish t = t `elem` ["False", "false", "FALSE"]

readBool :: (HasCallStack) => T.Text -> Maybe Bool
readBool s
    | isTrueish s = Just True
    | isFalseish s = Just False
    | otherwise = Nothing

readByteStringBool :: C.ByteString -> Maybe Bool
readByteStringBool s
    | s `elem` ["True", "true", "TRUE"] = Just True
    | s `elem` ["False", "false", "FALSE"] = Just False
    | otherwise = Nothing

readByteStringDate :: String -> C.ByteString -> Maybe Day
readByteStringDate fmt = parseTimeM True defaultTimeLocale fmt . C.unpack

readInteger :: (HasCallStack) => T.Text -> Maybe Integer
readInteger s = case signed decimal (T.strip s) of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (_value, _) -> Nothing

{- | 'Data.Text.Read.decimal' at 'Int' wraps on overflow. Fields of <= 18
chars cannot overflow and keep the fast path; longer (rare) ones go
through 'Integer' with a range check, mirroring 'readByteStringInt'.
-}
readInt :: (HasCallStack) => T.Text -> Maybe Int
readInt s
    | T.length t <= 18 = case signed decimal t of
        Right (value, "") -> Just value
        _ -> Nothing
    | otherwise = case signed decimal t :: Either String (Integer, T.Text) of
        Right (value, "")
            | value >= toInteger (minBound :: Int)
            , value <= toInteger (maxBound :: Int) ->
                Just (fromInteger value)
        _ -> Nothing
  where
    t = T.strip s
{-# INLINE readInt #-}

readByteStringInt :: (HasCallStack) => C.ByteString -> Maybe Int
#if MIN_VERSION_bytestring(0,12,0)
-- bytestring >= 0.12: 'C.readInt' returns 'Nothing' on overflow.
readByteStringInt s = case C.readInt (C.strip s) of
    Just (value, "") -> Just value
    _ -> Nothing
#else
-- bytestring < 0.12: 'C.readInt' silently wraps on overflow. Fields of
-- <= 18 characters fit in an 'Int' and keep the fast path; longer ones fall
-- back to arbitrary-precision 'C.readInteger' with a range check.
readByteStringInt s
    | C.length t <= 18 = case C.readInt t of
        Just (value, "") -> Just value
        _ -> Nothing
    | otherwise = case C.readInteger t of
        Just (value, "")
            | value >= toInteger (minBound :: Int)
            , value <= toInteger (maxBound :: Int) ->
                Just (fromInteger value)
        _ -> Nothing
  where
    t = C.strip s
#endif
{-# INLINE readByteStringInt #-}

{- | Exact decimal -> 'Double': one correctly-rounded conversion, matching
strtod\/'read' on every input, subnormals and overflow included. Also
takes back the Infinity\/-Infinity tokens 'show' emits, so written CSV
round-trips.
-}
readByteStringDouble :: (HasCallStack) => C.ByteString -> Maybe Double
readByteStringDouble s
    | t == "Infinity" = Just (1 / 0)
    | t == "-Infinity" = Just (-1 / 0)
    | otherwise = parseDoubleExact t
  where
    t = C.strip s
{-# INLINE readByteStringDouble #-}

parseDoubleExact :: C.ByteString -> Maybe Double
parseDoubleExact t0 = do
    let (neg, t1) = case C.uncons t0 of
            Just ('-', r) -> (True, r)
            Just ('+', r) -> (False, r)
            _ -> (False, t0)
        (ws, t2) = C.span isDigit t1
    guard (not (C.null ws))
    (fs, t3) <- case C.uncons t2 of
        Just ('.', r) ->
            let (ds, rest) = C.span isDigit r
             in if C.null ds then Nothing else Just (ds, rest)
        _ -> Just (C.empty, t2)
    e <- case C.uncons t3 of
        Nothing -> Just 0
        Just (c, r)
            | c == 'e' || c == 'E' -> do
                let (eneg, r1) = case C.uncons r of
                        Just ('-', r') -> (True, r')
                        Just ('+', r') -> (False, r')
                        _ -> (False, r)
                guard (maybe False (isDigit . fst) (C.uncons r1))
                (ev, rest) <- C.readInteger r1
                guard (C.null rest)
                Just (if eneg then negate ev else ev)
            | otherwise -> Nothing
    let sigDigits = ws <> fs
    (sig, _) <- C.readInteger sigDigits
    let dexp = e - toInteger (C.length fs)
        nd = toInteger (C.length (C.dropWhile (== '0') sigDigits))
    Just (mkDouble neg sig nd dexp)

{- | Nearest 'Double' for @sig * 10^dexp@ in ONE rounding step. The clamps
bound the 'Rational' so an absurd exponent cannot allocate its 10^e.
-}
mkDouble :: Bool -> Integer -> Integer -> Integer -> Double
mkDouble neg sig nd dexp
    | sig == 0 = sgn 0
    | dexp > 350 = sgn (1 / 0)
    -- Even nd digits scaled this low sit under half the smallest denormal.
    | nd + dexp < -350 = sgn 0
    | otherwise =
        sgn (fromRational (fromInteger sig * 10 ^^ (fromInteger dexp :: Int)))
  where
    sgn = if neg then negate else id

readDouble :: (HasCallStack) => T.Text -> Maybe Double
readDouble = readByteStringDouble . TE.encodeUtf8
{-# INLINE readDouble #-}

readIntegerEither :: (HasCallStack) => T.Text -> Either T.Text Integer
readIntegerEither s = case signed decimal (T.strip s) of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (_value, _) -> Left s
{-# INLINE readIntegerEither #-}

-- | As 'readInt': overflow is a failed parse, not a wrapped value.
readIntEither :: (HasCallStack) => T.Text -> Either T.Text Int
readIntEither s
    | T.length t <= 18 = case signed decimal t of
        Right (value, "") -> Right value
        _ -> Left s
    | otherwise = case signed decimal t :: Either String (Integer, T.Text) of
        Right (value, "")
            | value >= toInteger (minBound :: Int)
            , value <= toInteger (maxBound :: Int) ->
                Right (fromInteger value)
        _ -> Left s
  where
    t = T.strip s
{-# INLINE readIntEither #-}

readDoubleEither :: (HasCallStack) => T.Text -> Either T.Text Double
readDoubleEither s = maybe (Left s) Right (readDouble s)
{-# INLINE readDoubleEither #-}

-- ---------------------------------------------------------------------------
-- Attoparsec CSV parser combinators (shared between Lazy.IO.CSV and others)
-- ---------------------------------------------------------------------------

parseSep :: Char -> T.Text -> [T.Text]
parseSep c s = either error id (parseOnly (record c) s)
{-# INLINE parseSep #-}

record :: Char -> Parser [T.Text]
record c =
    field c `sepBy1` char c
        <?> "record"
{-# INLINE record #-}

parseRow :: Char -> Parser [T.Text]
parseRow c = (record c <* lineEnd) <?> "record-new-line"

field :: Char -> Parser T.Text
field c =
    quotedField <|> unquotedField c
        <?> "field"
{-# INLINE field #-}

unquotedTerminators :: Char -> S.Set Char
unquotedTerminators sep = S.fromList [sep, '\n', '\r', '"']

unquotedField :: Char -> Parser T.Text
unquotedField sep =
    takeWhile (not . (`S.member` terminators)) <?> "unquoted field"
  where
    terminators = unquotedTerminators sep
{-# INLINE unquotedField #-}

quotedField :: Parser T.Text
quotedField = char '"' *> contents <* char '"' <?> "quoted field"
  where
    contents = fold <$> many (unquote <|> unescape)
      where
        unquote = takeWhile1 (notInClass "\"\\")
        unescape =
            char '\\' *> do
                T.singleton <$> do
                    char '\\' <|> char '"'
{-# INLINE quotedField #-}

lineEnd :: Parser ()
lineEnd =
    (endOfLine <|> endOfInput)
        <?> "end of line"
{-# INLINE lineEnd #-}

-- | First pass to count rows for exact allocation.
countRows :: Char -> FilePath -> IO Int
countRows c path = withFile path ReadMode $! go 0 ""
  where
    go n input h = do
        isEOF <- hIsEOF h
        if isEOF && input == mempty
            then pure n
            else
                parseWith (TIO.hGetChunk h) (parseRow c) input >>= \case
                    Fail unconsumed ctx er -> do
                        erpos <- hTell h
                        fail $
                            "Failed to parse CSV file around "
                                <> show erpos
                                <> " byte; due: "
                                <> show er
                                <> "; context: "
                                <> show ctx
                                <> " "
                                <> show unconsumed
                    Partial _ -> fail $ "Partial handler is called; n = " <> show n
                    Done (unconsumed :: T.Text) _ ->
                        go (n + 1) unconsumed h
{-# INLINE countRows #-}

-- | Infer the Haskell type name from a text sample.
inferValueType :: T.Text -> T.Text
inferValueType s = case readInt s of
    Just _ -> "Int"
    Nothing -> case readDouble s of
        Just _ -> "Double"
        Nothing -> "Other"
{-# INLINE inferValueType #-}

-- | Read a single CSV row from a handle using the given separator.
readSingleLine :: Char -> T.Text -> Handle -> IO ([T.Text], T.Text)
readSingleLine c unused handle =
    parseWith (TIO.hGetChunk handle) (parseRow c) unused >>= \case
        Fail _unconsumed ctx er -> do
            erpos <- hTell handle
            fail $
                "Failed to parse CSV file around "
                    <> show erpos
                    <> " byte; due: "
                    <> show er
                    <> "; context: "
                    <> show ctx
        Partial _ -> fail "Partial handler is called"
        Done (unconsumed :: T.Text) (row :: [T.Text]) ->
            return (row, unconsumed)
