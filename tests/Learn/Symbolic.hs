{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Learn.Symbolic (tests) where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.PCA.Kernel
import DataFrame.SVM.RFF
import DataFrame.SymbolicRegression
import DataFrame.SymbolicRegression.Expr
import DataFrame.SymbolicRegression.Optimize (meanSquaredError)
import DataFrame.SymbolicRegression.Simplify (simplify)

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import Test.HUnit

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

interpI :: D.DataFrame -> Expr Int -> [Int]
interpI df e = case interpret @Int df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Int @VU.Vector c)
    Left err -> error (show err)

testSRRecovers :: Test
testSRRecovers = TestCase $ do
    let xs = [-3, -2, -1, 0, 1, 2, 3, 4, 5, 6] :: [Double]
        df =
            D.fromNamedColumns
                [ ("x", DI.fromList xs)
                , ("y", DI.fromList [x * x + x | x <- xs])
                ]
        m =
            fit
                defaultSRConfig
                    { srSeed = 3
                    , srGenerations = 60
                    , srPopSize = 300
                    , srUnaryOps = []
                    }
                (F.col @Double "y")
                df
        preds = interpD df (srBest m)
        truth = interpD df (F.col @Double "y")
        err = sum (zipWith (\p t -> (p - t) ^ (2 :: Int)) preds truth) / 10
    assertBool "SR recovers x*x+x to low error" (err < 1e-6)
    assertBool "Pareto front non-empty" (not (null (srPareto m)))

testSRDeterminism :: Test
testSRDeterminism = TestCase $ do
    let xs = [1 .. 8] :: [Double]
        df =
            D.fromNamedColumns
                [ ("x", DI.fromList xs)
                , ("y", DI.fromList (map (\x -> 2 * x + 1) xs))
                ]
        run =
            fit
                defaultSRConfig{srSeed = 9, srGenerations = 20}
                (F.col @Double "y")
                df
    assertEqual "SR deterministic best MSE" (srBestMSE run) (srBestMSE (rerun df))
  where
    rerun =
        fit
            defaultSRConfig{srSeed = 9, srGenerations = 20}
            (F.col @Double "y")

testSimplifyPreservesEval :: Test
testSimplifyPreservesEval = TestCase $ do
    let feats = V.singleton (VU.fromList [1, 2, 3, 4 :: Double])
        n = 4
        e = SBin SAdd (SBin SMul (SVar 0) (SConst 1)) (SConst 0)
        target = VU.fromList [1, 2, 3, 4]
    assertBool
        "simplify preserves evaluation"
        ( abs
            (meanSquaredError feats n target e - meanSquaredError feats n target (simplify e))
            < 1e-12
        )
    assertBool "simplify is size non-increasing" (srSize (simplify e) <= srSize e)
    assertEqual "simplify idempotent" (simplify e) (simplify (simplify e))

testKernelPCA :: Test
testKernelPCA = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("a", DI.fromList ([0, 0.2, -0.1, 0.1, 8, 8.1, 7.9, 8.2] :: [Double]))
                , ("b", DI.fromList ([0, -0.1, 0.2, 0.0, 5, 5.2, 4.9, 5.1] :: [Double]))
                ]
        m =
            fit
                defaultKernelPCAConfig{kpcaNComponents = 2, kpcaNLandmarks = 8, kpcaSeed = 1}
                [F.col @Double "a", F.col @Double "b"]
                df
        pc1 = interpD df (snd (head (kernelPCAExprs m)))
    assertBool "kPCA finite" (not (any isNaN pc1))
    assertBool
        "kPCA first component separates blobs"
        (signum (head pc1) /= signum (last pc1))

testRFFSVM :: Test
testRFFSVM = TestCase $ do
    let df =
            D.fromNamedColumns
                [ ("x", DI.fromList ([-3, -2, -1, -0.5, 0.5, 1, 2, 3] :: [Double]))
                , ("label", DI.fromList ([0, 0, 0, 0, 1, 1, 1, 1] :: [Int]))
                ]
        m =
            fit
                defaultRFFConfig{rffD = 80, rffGamma = 0.2, rffSeed = 2}
                (F.col @Int "label")
                df
        preds = interpI df (predict m)
    assertEqual "RFF SVM separates" [0, 0, 0, 0, 1, 1, 1, 1] preds

tests :: [Test]
tests =
    [ testSRRecovers
    , testSRDeterminism
    , testSimplifyPreservesEval
    , testKernelPCA
    , testRFFSVM
    ]
