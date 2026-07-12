{-# LANGUAGE ScopedTypeVariables #-}

module Learn.Numerics (tests) where

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Model (fit, predict)
import Test.HUnit

import DataFrame.LinearAlgebra
import DataFrame.LinearAlgebra.Eigen
import DataFrame.LinearAlgebra.Solve
import DataFrame.Random

mat :: [[Double]] -> Matrix
mat = V.fromList . map VU.fromList

approx :: Double -> Double -> Double -> Bool
approx tol a b = abs (a - b) <= tol

vApprox :: Double -> VU.Vector Double -> VU.Vector Double -> Bool
vApprox tol a b =
    VU.length a == VU.length b && VU.and (VU.zipWith (approx tol) a b)

testQR :: Test
testQR = TestCase $ do
    let a = mat [[1, 1], [1, 2], [1, 3], [1, 4]]
        b = VU.fromList [3, 5, 7, 9]
    case qrLeastSquares a b of
        Right x ->
            assertBool "QR recovers [1,2]" (vApprox 1e-9 x (VU.fromList [1, 2]))
        Left cols -> assertFailure ("unexpected rank deficiency: " ++ show cols)

testQRRankDeficient :: Test
testQRRankDeficient = TestCase $ do
    let a = mat [[1, 2], [2, 4], [3, 6]]
    case qrLeastSquares a (VU.fromList [1, 2, 3]) of
        Left _ -> pure ()
        Right _ -> assertFailure "expected rank deficiency on collinear columns"

testCholesky :: Test
testCholesky = TestCase $ do
    let a = mat [[4, 2], [2, 3]]
    case choleskySolve a (VU.fromList [2, 1]) of
        Just x ->
            assertBool "cholesky solves" (vApprox 1e-9 x (VU.fromList [0.5, 0]))
        Nothing -> assertFailure "expected PD"
    assertEqual "non-PD rejected" Nothing (cholesky (mat [[1, 2], [2, 1]]))

testJacobi :: Test
testJacobi = TestCase $ do
    let (ev, vecs) = jacobiEigenSym (mat [[2, 1], [1, 2]])
    assertBool "eigenvalues [3,1]" (vApprox 1e-9 ev (VU.fromList [3, 1]))
    let v0 = vecs V.! 0
        recon = matVec (mat [[2, 1], [1, 2]]) v0
    assertBool
        "A v = lambda v"
        (vApprox 1e-9 recon (scaleV (ev VU.! 0) v0))

testPowerIter :: Test
testPowerIter = TestCase $ do
    let (lam, _) = powerIterTop 200 (mat [[2, 1], [1, 2]])
    assertBool "dominant eigenvalue ~3" (approx 1e-6 lam 3)

testLogSumExp :: Test
testLogSumExp = TestCase $ do
    let xs = VU.fromList [1000, 1001, 1002]
        lse = logSumExp xs
    assertBool "logSumExp stable, finite" (not (isNaN lse) && not (isInfinite lse))
    assertBool "logSumExp >= max" (lse >= 1002)

testRngDeterminism :: Test
testRngDeterminism = TestCase $ do
    let (a, _) = shuffleInts 50 (mkGen 11)
        (b, _) = shuffleInts 50 (mkGen 11)
    assertEqual "same seed same shuffle" a b
    assertBool
        "is a permutation"
        (VU.toList (VU.modify (const (pure ())) a) /= [] && all (`VU.elem` a) [0 .. 49])

testRngRange :: Test
testRngRange = TestCase $ do
    let go 0 _ acc = acc
        go k g acc =
            let (x, g') = nextIntR (3, 7) g in go (k - 1 :: Int) g' (x : acc)
        xs = go 1000 (mkGen 5) []
    assertBool "ints within range" (all (\x -> x >= 3 && x <= 7) xs)

tests :: [Test]
tests =
    [ testQR
    , testQRRankDeficient
    , testCholesky
    , testJacobi
    , testPowerIter
    , testLogSumExp
    , testRngDeterminism
    , testRngRange
    ]
