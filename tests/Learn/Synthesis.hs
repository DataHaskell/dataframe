{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Feature-synthesis enumerator: it should recover small exact features
(@x²@, @a/b@) from the example rows, score them near-perfectly, and be
deterministic.
-}
module Learn.Synthesis (tests) where

import Assertions (assertExpectException)
import qualified Data.Text as T
import qualified DataFrame as D
import DataFrame.Synthesis

import Test.HUnit

quad :: D.DataFrame
quad =
    D.fromNamedColumns
        [ ("x", D.fromList xs)
        , ("y", D.fromList (map (\x -> x * x) xs))
        ]
  where
    xs = map fromIntegral [1 .. 12 :: Int] :: [Double]

ratio :: D.DataFrame
ratio =
    D.fromNamedColumns
        [ ("a", D.fromList ([2, 6, 12, 20, 30, 42] :: [Double]))
        , ("b", D.fromList ([1, 2, 3, 4, 5, 6] :: [Double]))
        , ("y", D.fromList ([2, 3, 4, 5, 6, 7] :: [Double]))
        ]

-- | Pearson r²: enumeration should find x·x and score it ~1.
recoversQuadratic :: Test
recoversQuadratic = TestCase $ do
    let sf = fit defaultSynthesisConfig (D.col @Double "y") quad
    assertBool
        ("quadratic r2 = " ++ show (sfScore sf))
        (sfScore sf > 0.999 && sfScore sf <= 1.0001)

-- | MSE: the best feature reproduces the target exactly, so -MSE ~ 0.
exactRecoveryMSE :: Test
exactRecoveryMSE = TestCase $ do
    let sf =
            fit defaultSynthesisConfig{synLoss = MeanSquaredError} (D.col @Double "y") quad
    assertBool ("exact -mse = " ++ show (sfScore sf)) (sfScore sf > -1.0e-6)

-- | Division is enumerated (with the denominator guard): a/b is recovered.
recoversRatio :: Test
recoversRatio = TestCase $ do
    let sf = fit defaultSynthesisConfig (D.col @Double "y") ratio
    assertBool
        ("ratio r2 = " ++ show (sfScore sf))
        (sfScore sf > 0.999 && sfScore sf <= 1.0001)

-- | The bank is non-trivial and its ranked features are distinct expressions.
distinctFeatures :: Test
distinctFeatures = TestCase $ do
    let sf = fit defaultSynthesisConfig (D.col @Double "y") quad
        names = [D.prettyPrint e | (e, _) <- sfFeatures sf]
    assertBool "synthesizes more than one feature" (length names > 1)
    assertBool "ranked features are distinct" (length names == length (dedup names))
  where
    dedup = foldr (\x acc -> if x `elem` acc then acc else x : acc) []

-- | Same config and data give the same best expression.
deterministic :: Test
deterministic = TestCase $ do
    let a = fit defaultSynthesisConfig (D.col @Double "y") quad
        b = fit defaultSynthesisConfig (D.col @Double "y") quad
    assertEqual
        "same best expression"
        (D.prettyPrint (sfExpr a))
        (D.prettyPrint (sfExpr b))

{- | A wide frame at the default 'synMaxSize' refuses instead of exhausting the
heap. 'synBankCap' caps what is kept, not what is generated, so the layers past
size 4 used to allocate tens of gigabytes and kill the process — which no test
can catch, because there is no process left to fail.
-}
refusesOversizedSearch :: Test
refusesOversizedSearch =
    TestCase
        ( assertExpectException
            "[Error Case]"
            "synMaxAllocBytes"
            ( print
                (D.prettyPrint (sfExpr (fit defaultSynthesisConfig (D.col @Double "y") wide)))
            )
        )

-- | The same frame is fine once the search is small enough to fit the budget.
acceptsSmallSearch :: Test
acceptsSmallSearch = TestCase $ do
    let cfg = defaultSynthesisConfig{synMaxSize = 3}
        m = fit cfg (D.col @Double "y") wide
    assertBool "a size-3 search over the wide frame returns" (sfScore m >= -1.0)

-- | 12 features over 3000 rows: the shape that killed the kernel.
wide :: D.DataFrame
wide =
    D.fromNamedColumns
        ( ("y", D.fromList (map (\i -> fromIntegral (i `mod` 7) :: Double) idx))
            : [ ( "f" <> T.pack (show c)
                , D.fromList (map (\i -> fromIntegral ((i * c) `mod` 13) :: Double) idx)
                )
              | c <- [1 .. 12 :: Int]
              ]
        )
  where
    idx = [0 .. 2999 :: Int]

tests :: [Test]
tests =
    [ refusesOversizedSearch
    , acceptsSmallSearch
    , recoversQuadratic
    , exactRecoveryMSE
    , recoversRatio
    , distinctFeatures
    , deterministic
    ]
