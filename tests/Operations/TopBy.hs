{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | 'topBy' returns the window a full 'sortBy' would, for every key type.
module Operations.TopBy (tests) where

import qualified Data.Text as T

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import Test.HUnit

frame :: D.DataFrame
frame =
    D.fromColumns
        [ "n" D.=: [(i * 7919) `mod` 97 | i <- [0 .. 999 :: Int]]
        , "d"
            D.=: [fromIntegral ((i * 31) `mod` 13) / 4 :: Double | i <- [0 .. 999 :: Int]]
        , "t" D.=: [T.pack (show ((i * 131) `mod` 53)) | i <- [0 .. 999 :: Int]]
        , "id" D.=: [0 .. 999 :: Int]
        ]

orders :: [(String, [D.SortOrder])]
orders =
    [ ("int asc", [D.Asc (F.int "n")])
    , ("int desc", [D.Desc (F.int "n")])
    , ("text asc", [D.Asc (F.text "t")])
    ,
        ( "double desc, text asc"
        , [D.Desc (F.double "d"), D.Asc (F.text "t")]
        )
    , ("expr", [D.Asc (F.int "n" * 3 - F.int "id")])
    ]

windows :: [(Int, Int)]
windows =
    [(0, 1), (0, 10), (5, 10), (990, 10), (995, 20), (0, 1000), (0, 2000), (0, 0)]

windowCases :: [Test]
windowCases =
    [ TestCase $
        assertEqual
            (label ++ " " ++ show (off, n))
            (D.range (off, off + n) (D.sortBy order frame))
            (D.topBy off n order frame)
    | (label, order) <- orders
    , (off, n) <- windows
    ]

tests :: [Test]
tests = map (TestLabel "topBy") windowCases
