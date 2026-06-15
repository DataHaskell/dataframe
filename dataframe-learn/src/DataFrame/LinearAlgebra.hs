{-# LANGUAGE BangPatterns #-}

{- | Dependency-free dense linear algebra over row-major matrices, shared by the
models in @dataframe-learn@. Basic vector/matrix operations plus stability and
distance helpers; solvers live in "DataFrame.LinearAlgebra.Solve" and
eigenproblems in "DataFrame.LinearAlgebra.Eigen".
-}
module DataFrame.LinearAlgebra (
    Matrix,
    dot,
    axpy,
    scaleV,
    matVec,
    tMatVec,
    gram,
    transposeM,
    identityM,
    logSumExp,
    sqDist,
    nearestCenter,
    epsNeighbors,
) where

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

{- | Row-major dense matrix: an outer boxed vector of equal-length rows. An
@n×d@ matrix has @n@ rows of length @d@.
-}
type Matrix = V.Vector (VU.Vector Double)

-- | Inner product of two equal-length vectors.
dot :: VU.Vector Double -> VU.Vector Double -> Double
dot a b = VU.sum (VU.zipWith (*) a b)
{-# INLINE dot #-}

-- | @axpy a x y = a*x + y@.
axpy :: Double -> VU.Vector Double -> VU.Vector Double -> VU.Vector Double
axpy a = VU.zipWith (\xi yi -> a * xi + yi)
{-# INLINE axpy #-}

-- | Scalar-vector product.
scaleV :: Double -> VU.Vector Double -> VU.Vector Double
scaleV a = VU.map (* a)
{-# INLINE scaleV #-}

-- | @matVec A v@ for @A@ of shape @n×d@ and @v@ of length @d@; result length @n@.
matVec :: Matrix -> VU.Vector Double -> VU.Vector Double
matVec a v = VU.convert (V.map (`dot` v) a)

-- | @tMatVec A v = Aᵀ v@ for @A@ of shape @n×d@, @v@ of length @n@; result length @d@.
tMatVec :: Matrix -> VU.Vector Double -> VU.Vector Double
tMatVec a v
    | V.null a = VU.empty
    | otherwise = V.foldl' step (VU.replicate d 0) (V.zipWith (,) vBoxed a)
  where
    d = VU.length (V.head a)
    vBoxed = V.generate (V.length a) (v VU.!)
    step !acc (vi, row) = axpy vi row acc

-- | @gram A = Aᵀ A@, the @d×d@ symmetric matrix of column inner products.
gram :: Matrix -> Matrix
gram a
    | V.null a = V.empty
    | otherwise =
        V.generate d $ \i ->
            VU.generate d $ \j ->
                V.foldl' (\ !acc row -> acc + (row VU.! i) * (row VU.! j)) 0 a
  where
    d = VU.length (V.head a)

-- | Transpose an @n×d@ matrix to @d×n@.
transposeM :: Matrix -> Matrix
transposeM a
    | V.null a = V.empty
    | otherwise = V.generate d $ \j -> VU.generate n $ \i -> (a V.! i) VU.! j
  where
    n = V.length a
    d = VU.length (V.head a)

-- | @d×d@ identity matrix.
identityM :: Int -> Matrix
identityM d = V.generate d $ \i -> VU.generate d $ \j -> if i == j then 1 else 0

-- | Numerically stable @log Σ exp xᵢ@.
logSumExp :: VU.Vector Double -> Double
logSumExp xs
    | VU.null xs = negate (1 / 0)
    | otherwise = m + log (VU.sum (VU.map (\x -> exp (x - m)) xs))
  where
    m = VU.maximum xs

-- | Squared Euclidean distance.
sqDist :: VU.Vector Double -> VU.Vector Double -> Double
sqDist a b = VU.sum (VU.zipWith (\x y -> let z = x - y in z * z) a b)
{-# INLINE sqDist #-}

-- | Index of and squared distance to the nearest centre.
nearestCenter ::
    V.Vector (VU.Vector Double) -> VU.Vector Double -> (Int, Double)
nearestCenter centers p =
    V.ifoldl'
        ( \(!bi, !bd) i c ->
            let dd = sqDist c p in if dd < bd then (i, dd) else (bi, bd)
        )
        (-1, 1 / 0)
        centers

{- | Indices @j@ (excluding @i@) within squared radius @eps²@ of row @i@, by
brute force; @O(n d)@ per query.
-}
epsNeighbors :: Double -> Matrix -> Int -> VU.Vector Int
epsNeighbors eps rows i =
    VU.fromList
        [ j
        | j <- [0 .. n - 1]
        , j /= i
        , sqDist (rows V.! i) (rows V.! j) <= eps2
        ]
  where
    n = V.length rows
    eps2 = eps * eps
