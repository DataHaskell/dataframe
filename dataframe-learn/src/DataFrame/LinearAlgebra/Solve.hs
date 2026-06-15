{-# LANGUAGE BangPatterns #-}

{- | Householder QR (for ordinary least squares) and Cholesky factorisation (for
ridge normal equations and Gaussian log-densities). Pure, deterministic, no
LAPACK; sound at the @d@ ≤ low-hundreds scales this library targets.
-}
module DataFrame.LinearAlgebra.Solve (
    qrLeastSquares,
    cholesky,
    choleskySolve,
    logDetFromChol,
    forwardSubst,
    backSubst,
) where

import Control.Monad (forM_)
import Control.Monad.ST (ST, runST)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.LinearAlgebra (Matrix)

{- | Solve @min ‖A x − b‖₂@ for an @n×d@ matrix @A@ (@n ≥ d@) via Householder QR.
@Left cols@ reports rank deficiency (near-zero @R@ diagonal) with the offending
column indices; @Right x@ is the least-squares solution.
-}
qrLeastSquares :: Matrix -> VU.Vector Double -> Either [Int] (VU.Vector Double)
qrLeastSquares a b
    | V.null a = Right VU.empty
    | n < d = Left [0 .. d - 1]
    | otherwise = runST $ do
        mat <- VUM.new (n * d)
        forM_ [0 .. n - 1] $ \i ->
            forM_ [0 .. d - 1] $ \j ->
                VUM.write mat (j * n + i) ((a V.! i) VU.! j)
        rhs <- VUM.new n
        forM_ [0 .. n - 1] $ \i -> VUM.write rhs i (b VU.! i)
        deficient <- householder mat rhs n d
        if not (null deficient)
            then pure (Left deficient)
            else do
                x <- VUM.new d
                backSubstQR mat rhs n d x
                Right <$> VU.freeze x
  where
    n = V.length a
    d = VU.length (V.head a)

householder ::
    VUM.STVector s Double -> VUM.STVector s Double -> Int -> Int -> ST s [Int]
householder mat rhs n d = go 0 []
  where
    tol = 1e-10
    go k acc
        | k >= d = pure (reverse acc)
        | otherwise = do
            normSq <- sumSq k
            let alphaMag = sqrt normSq
            akk <- VUM.read mat (k * n + k)
            let alpha = if akk > 0 then negate alphaMag else alphaMag
            if alphaMag < tol
                then go (k + 1) (k : acc)
                else do
                    VUM.write mat (k * n + k) (akk - alpha)
                    vNormSq <- sumSq k
                    if vNormSq < tol * tol
                        then go (k + 1) acc
                        else do
                            forM_ [k + 1 .. d - 1] $ \j -> reflectColumn k j vNormSq
                            reflectRhs k vNormSq
                            VUM.write mat (k * n + k) alpha
                            go (k + 1) acc
    sumSq k = foldRows k
      where
        foldRows i
            | i >= n = pure 0
            | otherwise = do
                x <- VUM.read mat (k * n + i)
                rest <- foldRows (i + 1)
                pure (x * x + rest)
    reflectColumn k j vNormSq = do
        dotv <- dotV k j k
        let beta = 2 * dotv / vNormSq
        forM_ [k .. n - 1] $ \i -> do
            vi <- VUM.read mat (k * n + i)
            aij <- VUM.read mat (j * n + i)
            VUM.write mat (j * n + i) (aij - beta * vi)
    reflectRhs k vNormSq = do
        dotv <- dotRhs k k
        let beta = 2 * dotv / vNormSq
        forM_ [k .. n - 1] $ \i -> do
            vi <- VUM.read mat (k * n + i)
            bi <- VUM.read rhs i
            VUM.write rhs i (bi - beta * vi)
    dotV k j i
        | i >= n = pure 0
        | otherwise = do
            vi <- VUM.read mat (k * n + i)
            aij <- VUM.read mat (j * n + i)
            rest <- dotV k j (i + 1)
            pure (vi * aij + rest)
    dotRhs k i
        | i >= n = pure 0
        | otherwise = do
            vi <- VUM.read mat (k * n + i)
            bi <- VUM.read rhs i
            rest <- dotRhs k (i + 1)
            pure (vi * bi + rest)

backSubstQR ::
    VUM.STVector s Double ->
    VUM.STVector s Double ->
    Int ->
    Int ->
    VUM.STVector s Double ->
    ST s ()
backSubstQR mat rhs n d x = forM_ [d - 1, d - 2 .. 0] $ \i -> do
    bi <- VUM.read rhs i
    s <- sumAbove i (i + 1) 0
    rii <- VUM.read mat (i * n + i)
    VUM.write x i ((bi - s) / rii)
  where
    sumAbove i j !acc
        | j >= d = pure acc
        | otherwise = do
            rij <- VUM.read mat (j * n + i)
            xj <- VUM.read x j
            sumAbove i (j + 1) (acc + rij * xj)

{- | Cholesky factor @L@ (lower-triangular, @A = L Lᵀ@) of a symmetric
positive-definite matrix, or 'Nothing' if a non-positive pivot is hit.
-}
cholesky :: Matrix -> Maybe Matrix
cholesky a
    | V.null a = Just V.empty
    | otherwise = runST $ do
        l <- VUM.replicate (d * d) 0
        ok <- buildL l
        if ok then Just <$> freezeLower l else pure Nothing
  where
    d = V.length a
    buildL l = go 0
      where
        go j
            | j >= d = pure True
            | otherwise = do
                s <- sumLk l j j (j - 1) 0
                let ajj = (a V.! j) VU.! j
                    diag = ajj - s
                if diag <= 0
                    then pure False
                    else do
                        let ljj = sqrt diag
                        VUM.write l (j * d + j) ljj
                        forM_ [j + 1 .. d - 1] $ \i -> do
                            sij <- sumLk l i j (j - 1) 0
                            let aij = (a V.! i) VU.! j
                            VUM.write l (i * d + j) ((aij - sij) / ljj)
                        go (j + 1)
    sumLk l i j k !acc
        | k < 0 = pure acc
        | otherwise = do
            lik <- VUM.read l (i * d + k)
            ljk <- VUM.read l (j * d + k)
            sumLk l i j (k - 1) (acc + lik * ljk)
    freezeLower l = do
        frozen <- VU.freeze l
        pure $ V.generate d $ \i -> VU.slice (i * d) d frozen

-- | Solve @L y = b@ for lower-triangular @L@.
forwardSubst :: Matrix -> VU.Vector Double -> VU.Vector Double
forwardSubst l b = runST $ do
    y <- VUM.new d
    forM_ [0 .. d - 1] $ \i -> do
        let row = l V.! i
        s <- sumKnown y row i 0 0
        VUM.write y i ((b VU.! i - s) / (row VU.! i))
    VU.freeze y
  where
    d = V.length l
    sumKnown y row i j !acc
        | j >= i = pure acc
        | otherwise = do
            yj <- VUM.read y j
            sumKnown y row i (j + 1) (acc + (row VU.! j) * yj)

-- | Solve @Lᵀ x = y@ for lower-triangular @L@.
backSubst :: Matrix -> VU.Vector Double -> VU.Vector Double
backSubst l y = runST $ do
    x <- VUM.new d
    forM_ [d - 1, d - 2 .. 0] $ \i -> do
        s <- sumKnown x i (i + 1) 0
        VUM.write x i ((y VU.! i - s) / ((l V.! i) VU.! i))
    VU.freeze x
  where
    d = V.length l
    sumKnown x i j !acc
        | j >= d = pure acc
        | otherwise = do
            xj <- VUM.read x j
            sumKnown x i (j + 1) (acc + ((l V.! j) VU.! i) * xj)

{- | Solve the SPD system @A x = b@ via Cholesky; 'Nothing' when @A@ is not
positive-definite.
-}
choleskySolve :: Matrix -> VU.Vector Double -> Maybe (VU.Vector Double)
choleskySolve a b = do
    l <- cholesky a
    pure (backSubst l (forwardSubst l b))

-- | @log det A = 2 Σ log Lᵢᵢ@ from a Cholesky factor @L@.
logDetFromChol :: Matrix -> Double
logDetFromChol l = 2 * V.sum (V.imap (\i row -> log (row VU.! i)) l)
