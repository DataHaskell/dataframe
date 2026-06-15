{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

{- | k-means clustering (Lloyd's algorithm with k-means++ seeding and multiple
restarts). 'fit' trains a 'KMeansModel' (inspectable centres); 'predict' is the
arg-min cluster assignment. Per-cluster distance features are available via
'kmeansDistanceExprs' / 'kmeansTransform'.
-}
module DataFrame.KMeans (
    KMeansConfig (..),
    defaultKMeansConfig,
    KMeansModel (..),
    kmeansDistanceExprs,
    kmeansTransform,
) where

import Data.List (minimumBy)
import Data.Ord (comparing)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (Features (..), argMinExpr, extractFeatures)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), UExpr (..))
import DataFrame.LinearAlgebra (Matrix, nearestCenter, sqDist)
import DataFrame.Model (Fit (..), Predict (..))
import DataFrame.Operators ((.*.), (.+.), (.-.))
import DataFrame.Random (Gen, mkGen, nextDouble, nextIntR, splitGen)
import DataFrame.Transform (Transform (..))

data KMeansConfig = KMeansConfig
    { kmK :: !Int
    , kmNInit :: !Int
    , kmMaxIter :: !Int
    , kmTol :: !Double
    , kmSeed :: !Int
    }
    deriving (Eq, Show)

defaultKMeansConfig :: KMeansConfig
defaultKMeansConfig =
    KMeansConfig{kmK = 8, kmNInit = 10, kmMaxIter = 300, kmTol = 1.0e-4, kmSeed = 0}

-- | A fitted k-means model. 'kmCenters' are sklearn's @cluster_centers_@.
data KMeansModel = KMeansModel
    { kmCenters :: !(V.Vector (VU.Vector Double))
    , kmLabels :: !(VU.Vector Int)
    , kmInertia :: !Double
    , kmNIter :: !Int
    , kmFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

instance Fit KMeansConfig [Expr Double] KMeansModel where
    fit = fitKMeans

instance Predict KMeansModel Int where
    predict m = argMinExpr (zip [0 :: Int ..] (map snd (kmeansDistanceExprs m)))

-- | Fit k-means over the given feature columns.
fitKMeans :: KMeansConfig -> [Expr Double] -> DataFrame -> KMeansModel
fitKMeans cfg features df = best
  where
    Features names _ rows n _ = extractFeatures features df
    k = min (kmK cfg) (max 1 n)
    seeds = take (max 1 (kmNInit cfg)) (genSeeds (mkGen (kmSeed cfg)))
    runs = map (lloyd cfg k rows) seeds
    best =
        let (centers, labels, inertia, iters) =
                minimumBy (comparing (\(_, _, i, _) -> i)) runs
         in KMeansModel centers labels inertia iters (V.fromList names)

genSeeds :: Gen -> [Gen]
genSeeds g = let (g1, g2) = splitGen g in g1 : genSeeds g2

{- | One k-means run: k-means++ seeding then Lloyd iterations. Returns
@(centers, labels, inertia, nIter)@.
-}
lloyd ::
    KMeansConfig ->
    Int ->
    Matrix ->
    Gen ->
    (V.Vector (VU.Vector Double), VU.Vector Int, Double, Int)
lloyd cfg k rows g0
    | V.null rows = (V.empty, VU.empty, 0, 0)
    | otherwise = iterate' 0 initCenters
  where
    initCenters = kmeansPP k rows g0
    iterate' !iter centers =
        let labels = VU.generate (V.length rows) (\i -> fst (nearestCenter centers (rows V.! i)))
            newCenters = recompute centers labels
            shift = V.sum (V.zipWith sqDist centers newCenters)
         in if iter + 1 >= kmMaxIter cfg || shift <= kmTol cfg
                then (newCenters, labels, inertiaOf newCenters labels, iter + 1)
                else iterate' (iter + 1) newCenters
    recompute centers labels =
        V.generate k $ \c ->
            let members = [rows V.! i | i <- [0 .. V.length rows - 1], labels VU.! i == c]
             in if null members then centers V.! c else meanOf members
    inertiaOf centers labels =
        sum
            [ sqDist (rows V.! i) (centers V.! (labels VU.! i))
            | i <- [0 .. V.length rows - 1]
            ]

meanOf :: [VU.Vector Double] -> VU.Vector Double
meanOf vs =
    let d = VU.length (head vs)
        s = foldr (VU.zipWith (+)) (VU.replicate d 0) vs
     in VU.map (/ fromIntegral (length vs)) s

-- | k-means++ seeding: spread initial centres by squared-distance sampling.
kmeansPP :: Int -> Matrix -> Gen -> V.Vector (VU.Vector Double)
kmeansPP k rows g0 = V.fromList (reverse (pick [first] g1))
  where
    n = V.length rows
    (i0, g1) = nextIntR (0, n - 1) g0
    first = rows V.! i0
    pick chosen g
        | length chosen >= k = chosen
        | otherwise =
            let dists = VU.generate n (\i -> minimum [sqDist (rows V.! i) c | c <- chosen])
                (u, g') = nextDouble g
                idx = sampleCumulative dists (u * VU.sum dists)
             in pick (rows V.! idx : chosen) g'

sampleCumulative :: VU.Vector Double -> Double -> Int
sampleCumulative dists target = go 0 0
  where
    go i acc
        | i >= VU.length dists - 1 = VU.length dists - 1
        | acc + dists VU.! i >= target = i
        | otherwise = go (i + 1) (acc + dists VU.! i)

-- | Per-cluster squared-distance expressions, named @dist1@, @dist2@, …
kmeansDistanceExprs :: KMeansModel -> [(T.Text, Expr Double)]
kmeansDistanceExprs m =
    [ ("dist" <> T.pack (show (c + 1)), distExpr (kmCenters m V.! c))
    | c <- [0 .. V.length (kmCenters m) - 1]
    ]
  where
    names = V.toList (kmFeatureNames m)
    distExpr center =
        foldr (.+.) (F.lit 0) $
            [ let diff = (Col n :: Expr Double) .-. F.lit ci in diff .*. diff
            | (n, ci) <- zip names (VU.toList center)
            ]

-- | The per-cluster distance features as a composable fitted 'Transform'.
kmeansTransform :: KMeansModel -> Transform
kmeansTransform m = Transform [(n, UExpr e) | (n, e) <- kmeansDistanceExprs m]
