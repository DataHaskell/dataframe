{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Internal.Parsing where

import qualified Data.ByteString.Char8 as C
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Control.Applicative (many, (<|>))
import Data.Attoparsec.Text hiding (decimal, double, signed)
import Data.ByteString.Lex.Fractional
import Data.Foldable (fold)
import Data.Text.Read (decimal, double, signed)
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

readInt :: (HasCallStack) => T.Text -> Maybe Int
readInt s = case signed decimal (T.strip s) of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (_value, _) -> Nothing
{-# INLINE readInt #-}

readByteStringInt :: (HasCallStack) => C.ByteString -> Maybe Int
readByteStringInt s = case C.readInt (C.strip s) of
    Nothing -> Nothing
    Just (value, "") -> Just value
    Just (_value, _) -> Nothing
{-# INLINE readByteStringInt #-}

readByteStringDouble :: (HasCallStack) => C.ByteString -> Maybe Double
readByteStringDouble s =
    let
        readFunc = if C.any (\c -> c == 'e' || c == 'E') s then readExponential else readDecimal
     in
        case readSigned readFunc (C.strip s) of
            Nothing -> Nothing
            Just (value, "") -> Just value
            Just (_value, _) -> Nothing
{-# INLINE readByteStringDouble #-}

readDouble :: (HasCallStack) => T.Text -> Maybe Double
readDouble s =
    case signed double s of
        Left _ -> Nothing
        Right (value, "") -> Just value
        Right (_value, _) -> Nothing
{-# INLINE readDouble #-}

readIntegerEither :: (HasCallStack) => T.Text -> Either T.Text Integer
readIntegerEither s = case signed decimal (T.strip s) of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (_value, _) -> Left s
{-# INLINE readIntegerEither #-}

readIntEither :: (HasCallStack) => T.Text -> Either T.Text Int
readIntEither s = case signed decimal (T.strip s) of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (_value, _) -> Left s
{-# INLINE readIntEither #-}

readDoubleEither :: (HasCallStack) => T.Text -> Either T.Text Double
readDoubleEither s =
    case signed double s of
        Left _ -> Left s
        Right (value, "") -> Right value
        Right (_value, _) -> Left s
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
