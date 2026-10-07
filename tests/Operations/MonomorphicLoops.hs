{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | The per-type loops in 'mapColumn', 'imapColumn', 'zipWithColumns' and the
folds, checked against list references, plus how each function treats nulls.
-}
module Operations.MonomorphicLoops where

import Control.Exception (throw)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word64)
import GHC.Float (castDoubleToWord64, castFloatToWord32)

import DataFrame.Internal.Column (
    Column,
    Columnable,
    findIndices,
    foldLinearGroups,
    foldl1Column,
    foldl1DirectGroups,
    foldlColumn,
    fromList,
    fromUnboxedVector,
    headColumn,
    ifoldrColumn,
    imapColumn,
    mapColumn,
    toVector,
    zipColumns,
    zipWithColumns,
 )

import Test.HUnit

sizes :: [Int]
sizes = [37, 200003]

doublesOf :: Int -> [Double]
doublesOf n = take n (cycle [0 / 0, -(1 / 0), -1.5, -0.0, 0.0, 5.0e-324, 0.5, 2.5, 1 / 0])

intsOf :: Int -> [Int]
intsOf n = take n (cycle [minBound, -3, 0, 1, 7, maxBound])

boolsOf :: Int -> [Bool]
boolsOf n = take n (cycle [True, False, False])

col :: (Columnable a, VU.Unbox a) => [a] -> Column
col = fromUnboxedVector . VU.fromList

out :: forall a e. (Columnable a) => Either e Column -> [a]
out (Left _) = error "column op failed"
out (Right c) = V.toList (either throw id (toVector @a @V.Vector c))

-- | Element results compare by bits for floating types, by '==' otherwise.
class Same a where
    same :: [a] -> [a] -> Bool

instance Same Double where
    same a b = map castDoubleToWord64 a == map castDoubleToWord64 b
instance Same Int where
    same = (==)
instance Same Bool where
    same = (==)
instance Same Float where
    same a b = map castFloatToWord32 a == map castFloatToWord32 b

bits :: [Double] -> [Word64]
bits = map castDoubleToWord64

-- | map, imap and zip of one (element, result) pairing.
pairCases ::
    forall a c.
    (Columnable a, VU.Unbox a, Columnable c, Same c) =>
    String -> (Int -> [a]) -> (a -> c) -> (Int -> a -> c) -> (a -> a -> c) -> [Test]
pairCases label gen f fi f2 =
    [ TestList
        [ TestCase
            ( assertBool
                (label ++ " map n=" ++ show n)
                (same (out @c (mapColumn f (col xs))) (map f xs))
            )
        , TestCase
            ( assertBool
                (label ++ " imap n=" ++ show n)
                (same (out @c (imapColumn fi (col xs))) (zipWith fi [0 ..] xs))
            )
        , TestCase
            ( assertBool
                (label ++ " zip n=" ++ show n)
                (same (out @c (zipWithColumns f2 (col xs) (col ys))) (zipWith f2 xs ys))
            )
        ]
    | n <- sizes
    , let xs = gen n
          ys = reverse xs
    ]

foldCases :: [Test]
foldCases =
    concat
        [ [ TestCase
                ( assertEqual
                    ("foldl DD n=" ++ show n)
                    (bits [foldl (+) 0.25 ds])
                    (bits [fold (+) 0.25 ds])
                )
          , TestCase
                ( assertEqual
                    ("foldl DI n=" ++ show n)
                    (foldl (\k d -> k + fromEnum (d > 0)) 0 ds)
                    (fold (\k d -> k + fromEnum (d > 0)) 0 ds)
                )
          , TestCase
                ( assertEqual
                    ("foldl ID n=" ++ show n)
                    (bits [foldl (\a i -> a + fromIntegral i) 0 is])
                    (bits [fold (\a i -> a + fromIntegral i) (0 :: Double) is])
                )
          , TestCase
                (assertEqual ("foldl II n=" ++ show n) (foldl (+) 1 is) (fold (+) 1 is))
          , TestCase
                ( assertEqual
                    ("foldl1 D n=" ++ show n)
                    (bits [foldl1 (-) ds])
                    (bits [fold1 (-) ds])
                )
          , TestCase (assertEqual ("foldl1 I n=" ++ show n) (maximum is) (fold1 max is))
          , TestCase
                ( assertEqual
                    ("foldl Float n=" ++ show n)
                    (castFloatToWord32 (sum fs))
                    (castFloatToWord32 (fold (+) (0 :: Float) fs))
                )
          ]
        | n <- sizes
        , let ds = doublesOf n
              is = intsOf n
              fs = map realToFrac ds :: [Float]
        ]
  where
    fold ::
        (Columnable a, VU.Unbox a, Columnable b) => (b -> a -> b) -> b -> [a] -> b
    fold f z xs = either throw id (foldlColumn f z (col xs))
    fold1 :: (Columnable a, VU.Unbox a) => (a -> a -> a) -> [a] -> a
    fold1 f xs = either throw id (foldl1Column f (col xs))

-- | Mapping at the element type over a nullable column keeps its bitmap.
nullableCases :: [Test]
nullableCases =
    [ TestCase
        ( assertEqual
            ("nullable map n=" ++ show n)
            (map (fmap (castDoubleToWord64 . (* 2))) xs)
            ( map
                (fmap castDoubleToWord64)
                (out @(Maybe Double) (mapColumn @Double (* 2) (fromList xs)))
            )
        )
    | n <- sizes
    , let xs =
            take n (cycle [Just 1.5, Nothing, Just (-0.0), Just (0 / 0)]) :: [Maybe Double]
    ]

{- Functions at the element type over a column with nulls: f never runs on a
null row (its storage is a zero or an error thunk), null rows stay null, a
Maybe result's Nothing becomes null, and folds and searches skip null rows.
-}
nullSemantics :: [Test]
nullSemantics =
    [ eq
        "map skips null rows"
        [Just 25, Nothing, Just 50]
        (outM @Int (mapColumn @Int (100 `div`) ints3))
    , eq
        "map from boxed nullable"
        [Just 2, Nothing, Just 0]
        (outM @Int (mapColumn @T.Text T.length texts3))
    , eq
        "map to Maybe"
        [Just 4, Nothing, Nothing]
        (outM @Int (mapColumn @Int keepBig ints3))
    , eq
        "imap skips null rows"
        [Just 4, Nothing, Just 4]
        (outM @Int (imapColumn @Int (flip (+)) ints3))
    , eq
        "zip: null if either side is"
        [Just 5, Nothing, Nothing]
        (outM @Int (zipWithColumns @Int @Int (\a b -> a + b `div` b) ints3 other3))
    , eq
        "zip to Maybe"
        [Nothing, Nothing, Just 3]
        ( outM @Int
            ( zipWithColumns @Int @Int
                (\a b -> if a < b then Just (b - a) else Nothing)
                ints3
                other3'
            )
        )
    , eq "foldl skips null rows" (Just 6) (ok $ foldlColumn @Int (+) 0 ints3)
    , eq "foldl1 skips null rows" (Just 4) (ok $ foldl1Column @Int max ints3)
    , TestCase
        ( assertBool
            "foldl1 over only nulls fails"
            ( either
                (const True)
                (const False)
                (foldl1Column @Int (+) (fromList [Nothing, Nothing :: Maybe Int]))
            )
        )
    , eq
        "ifoldr skips null rows"
        (Just [0, 2])
        (ok $ ifoldrColumn @Int (\i _ r -> i : r) [] ints3)
    , eq
        "findIndices skips null rows"
        (Just (VU.fromList [0]))
        (ok $ findIndices @Int (== 4) (fromList [Just 4, Nothing, Just (1 :: Int)]))
    , eq
        "findIndices never matches a null"
        (Just VU.empty)
        (ok $ findIndices @Int (== 0) (fromList [Just 3, Nothing :: Maybe Int]))
    , eq
        "head is the first valid row"
        (Just 3)
        (ok $ headColumn @Int (fromList [Nothing, Just (3 :: Int)]))
    , eq
        "zipColumns keeps nulls"
        [Just (4, 1), Nothing, Nothing]
        (outM @(Int, Int) (Right (zipColumns ints3 other3)))
    , eq
        "foldLinearGroups skips null rows"
        [1, 2]
        ( out @Int
            ( foldLinearGroups @Int
                (+)
                0
                (fromList [Just 1, Nothing, Just 2, Nothing :: Maybe Int])
                (VU.fromList [0, 0, 1, 1])
                2
            )
        )
    , eq
        "foldl1DirectGroups: all-null group is null"
        [Nothing, Just 7]
        ( outM @Int
            ( foldl1DirectGroups @Int
                (+)
                (fromList [Nothing, Nothing, Just 3, Just (4 :: Int)])
                (VU.fromList [0 .. 3])
                (VU.fromList [0, 2, 4])
            )
        )
    , TestCase
        ( assertBool
            "toVector refuses an element read with nulls"
            (either (const True) (const False) (toVector @Int @V.Vector ints3))
        )
    , eq
        "toVector allows a bitmap with no nulls"
        (Just [1, 2])
        (ok $ V.toList <$> toVector @Int @V.Vector (fromList [Just 1, Just (2 :: Int)]))
    ]
  where
    ok :: Either e x -> Maybe x
    ok = either (const Nothing) Just
    eq :: (Eq x, Show x) => String -> x -> x -> Test
    eq label expected actual = TestCase (assertEqual label expected actual)
    ints3 = fromList [Just 4, Nothing, Just (2 :: Int)]
    other3 = fromList [Just 1, Just 0, Nothing :: Maybe Int]
    other3' = fromList [Just 1, Just 0, Just (5 :: Int)]
    texts3 = fromList [Just ("ab" :: T.Text), Nothing, Just ""]
    keepBig x = if x > 3 then Just x else Nothing

outM :: forall a e. (Columnable a) => Either e Column -> [Maybe a]
outM (Left _) = error "column op failed"
outM (Right c) = V.toList (either throw id (toVector @(Maybe a) @V.Vector c))

tests :: [Test]
tests =
    concat
        [ nullableCases
        , nullSemantics
        , pairCases @Double @Double
            "DD"
            doublesOf
            (* 1.5)
            (\i d -> d + fromIntegral i)
            (-)
        , pairCases @Double @Int
            "DI"
            doublesOf
            (fromEnum . (> 0))
            (\i d -> i + fromEnum (d < 0))
            (\a b -> fromEnum (a <= b))
        , pairCases @Double @Bool "DB" doublesOf (<= 0.5) (\i d -> even i || d > 0) (<)
        , pairCases @Int @Double
            "ID"
            intsOf
            fromIntegral
            (\i x -> fromIntegral (x + i))
            (\a b -> fromIntegral a - fromIntegral b)
        , pairCases @Int @Int "II" intsOf (* 3) (+) (-)
        , pairCases @Int @Bool "IB" intsOf odd (flip (>)) (==)
        , pairCases @Bool @Double
            "BD"
            boolsOf
            (\b -> if b then 2.5 else 0 / 0)
            (\i b -> if b then fromIntegral i else -0.0)
            (\a b -> if a && b then 1 else -1)
        , pairCases @Bool @Int
            "BI"
            boolsOf
            fromEnum
            (\i b -> if b then i else -i)
            (\a b -> fromEnum a + fromEnum b)
        , pairCases @Bool @Bool "BB" boolsOf not (\i b -> b /= even i) (/=)
        , pairCases @Float @Float
            "Float fallback"
            (map realToFrac . doublesOf)
            (* 2)
            (\i x -> x + fromIntegral i)
            (+)
        , foldCases
        ]
