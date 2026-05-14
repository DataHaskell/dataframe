module DataFrame.Typed.Util (
    camelToSnake,
) where

import Data.Char (isUpper, toLower)

{- | Convert a camelCase identifier to snake_case.

>>> camelToSnake "orderId"
"order_id"
>>> camelToSnake "amountUS"
"amount_u_s"
>>> camelToSnake "region"
"region"
-}
camelToSnake :: String -> String
camelToSnake [] = []
camelToSnake (c : cs) = toLower c : go cs
  where
    go [] = []
    go (x : xs)
        | isUpper x = '_' : toLower x : go xs
        | otherwise = x : go xs
