{-# LANGUAGE ScopedTypeVariables #-}

module Assertions where

import qualified Data.List as L

import Control.Exception
import GHC.Float (castDoubleToWord64, castFloatToWord32)
import Test.HUnit

-- Adapted from: https://github.com/BartMassey/chunk/blob/1ee4bd6545e0db6b8b5f4935d97e7606708eacc9/hunit.hs#L29
assertExpectException ::
    String ->
    String ->
    IO a ->
    Assertion
assertExpectException preface expected action = do
    r <-
        catch
            (action >> (return . Just) "no exception thrown")
            ( \(e :: SomeException) ->
                return (checkForExpectedException e)
            )
    case r of
        Nothing -> return ()
        Just msg -> assertFailure $ preface ++ ": " ++ msg
  where
    checkForExpectedException :: SomeException -> Maybe String
    checkForExpectedException e
        | expected `L.isInfixOf` show e = Nothing
        | otherwise =
            Just $
                "wrong exception detail, expected "
                    ++ expected
                    ++ ", got: "
                    ++ show e

-- | Equality that tells NaNs and signed zeros apart for floating types.
class BitEq a where
    bitEq :: a -> a -> Bool

instance BitEq Double where
    bitEq a b = castDoubleToWord64 a == castDoubleToWord64 b
instance BitEq Float where
    bitEq a b = castFloatToWord32 a == castFloatToWord32 b
instance BitEq Int where
    bitEq = (==)
instance BitEq Bool where
    bitEq = (==)
instance (BitEq a) => BitEq (Maybe a) where
    bitEq (Just a) (Just b) = bitEq a b
    bitEq Nothing Nothing = True
    bitEq _ _ = False

-- | Lists equal under 'bitEq'; on failure, reports the first differing row.
assertBitEq :: (BitEq a, Show a) => String -> [a] -> [a] -> Assertion
assertBitEq label expected actual
    | length expected /= length actual =
        assertFailure
            ( label
                ++ ": "
                ++ show (length actual)
                ++ " rows, expected "
                ++ show (length expected)
            )
    | otherwise = case [(i, e, a) | (i, e, a) <- zip3 [0 :: Int ..] expected actual, not (bitEq e a)] of
        [] -> pure ()
        (i, e, a) : _ ->
            assertFailure
                (label ++ ": row " ++ show i ++ " expected " ++ show e ++ ", got " ++ show a)
