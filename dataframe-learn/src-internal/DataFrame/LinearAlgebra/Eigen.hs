{-# LANGUAGE BangPatterns #-}

{- | Symmetric eigenproblems in pure Haskell: cyclic Jacobi for full
decomposition (PCA covariance, @m×m@ kernels) and power iteration for the
dominant eigenpair (FISTA step sizes). Deterministic, sign-canonicalised output.
-}
module DataFrame.LinearAlgebra.Eigen (
    jacobiEigenSym,
    powerIterTop,
) where

import Control.Monad (forM_, when)
import Control.Monad.ST (runST)
import Data.List (sortBy)
import Data.Ord (Down (..), comparing)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.LinearAlgebra (Matrix, dot, matVec, scaleV)

{- | Cyclic Jacobi eigendecomposition of a symmetric matrix. Eigenvalues are
returned in descending order paired with eigenvectors as rows, each
sign-canonicalised (largest-magnitude component positive) for unique output.
-}
jacobiEigenSym :: Matrix -> (VU.Vector Double, Matrix)
jacobiEigenSym a0
    | V.null a0 = (VU.empty, V.empty)
    | otherwise = runST $ do
        a <- VUM.new (d * d)
        forM_ [0 .. d - 1] $ \i ->
            forM_ [0 .. d - 1] $ \j ->
                VUM.write a (i * d + j) ((a0 V.! i) VU.! j)
        v <- VUM.replicate (d * d) 0
        forM_ [0 .. d - 1] $ \i -> VUM.write v (i * d + i) 1
        sweep a v 0
        afrozen <- VU.freeze a
        vmat <- VU.freeze v
        let diag = VU.generate d (\i -> afrozen VU.! (i * d + i))
            vecs =
                V.generate d $ \col ->
                    VU.generate d $ \row -> vmat VU.! (row * d + col)
            paired =
                sortBy
                    (comparing (Down . fst))
                    (zip (VU.toList diag) (V.toList vecs))
        pure
            ( VU.fromList (map fst paired)
            , V.fromList (map (canonicalSign . snd) paired)
            )
  where
    d = V.length a0
    maxSweeps = 100
    tol = 1e-12
    sweep a v s
        | s >= maxSweeps = pure ()
        | otherwise = do
            off <- offNorm a
            when (off >= tol) $ do
                forM_ [0 .. d - 2] $ \p ->
                    forM_ [p + 1 .. d - 1] $ \q -> rotate a v p q
                sweep a v (s + 1)
    offNorm a = go 0 0
      where
        go i !acc
            | i >= d = pure acc
            | otherwise = do
                r <- goRow i (i + 1) acc
                go (i + 1) r
        goRow i j !acc
            | j >= d = pure acc
            | otherwise = do
                x <- VUM.read a (i * d + j)
                goRow i (j + 1) (acc + x * x)
    rotate a v p q = do
        apq <- VUM.read a (p * d + q)
        when (abs apq > 1e-300) $ do
            app <- VUM.read a (p * d + p)
            aqq <- VUM.read a (q * d + q)
            let theta = (aqq - app) / (2 * apq)
                s' = if theta == 0 then 1 else signum theta
                t = s' / (abs theta + sqrt (theta * theta + 1))
                c = 1 / sqrt (t * t + 1)
                sn = t * c
            forM_ [0 .. d - 1] $ \i -> do
                aip <- VUM.read a (i * d + p)
                aiq <- VUM.read a (i * d + q)
                VUM.write a (i * d + p) (c * aip - sn * aiq)
                VUM.write a (i * d + q) (sn * aip + c * aiq)
            forM_ [0 .. d - 1] $ \j -> do
                apj <- VUM.read a (p * d + j)
                aqj <- VUM.read a (q * d + j)
                VUM.write a (p * d + j) (c * apj - sn * aqj)
                VUM.write a (q * d + j) (sn * apj + c * aqj)
            forM_ [0 .. d - 1] $ \i -> do
                vip <- VUM.read v (i * d + p)
                viq <- VUM.read v (i * d + q)
                VUM.write v (i * d + p) (c * vip - sn * viq)
                VUM.write v (i * d + q) (sn * vip + c * viq)

canonicalSign :: VU.Vector Double -> VU.Vector Double
canonicalSign vec =
    let idx = VU.maxIndex (VU.map abs vec)
     in if vec VU.! idx < 0 then VU.map negate vec else vec

{- | Dominant eigenvalue and eigenvector of a symmetric PSD matrix via power
iteration with a deterministic all-ones start.
-}
powerIterTop :: Int -> Matrix -> (Double, VU.Vector Double)
powerIterTop iters a
    | V.null a = (0, VU.empty)
    | otherwise = go iters (normalize (VU.replicate d 1))
  where
    d = V.length a
    normalize v =
        let nrm = sqrt (dot v v) in if nrm == 0 then v else scaleV (1 / nrm) v
    go 0 v = (dot v (matVec a v), v)
    go k v =
        let av = matVec a v
            nrm = sqrt (dot av av)
         in if nrm < 1e-300 then (0, v) else go (k - 1) (scaleV (1 / nrm) av)
