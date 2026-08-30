{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

{- | Principal component analysis via the symmetric Jacobi eigensolver on the
covariance of the (optionally standardized) feature columns. 'fit' trains a
'PCAModel' (components + explained variance); the projection is exposed as
'pcaExprs' / 'pcaTransform' (PCA is a transformer, so it has no 'Predict').
-}
module DataFrame.PCA (
    module DataFrame.Model,
    NComponents (..),
    PCAConfig (..),
    defaultPCAConfig,
    PCAModel (..),
    pcaExprs,
    pcaTransform,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Expression.Operators ((.*.), (.+.), (.-.))
import DataFrame.Featurize.Internal (Features (..), extractFeatures)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), UExpr (..))
import DataFrame.LinearAlgebra (gram)
import DataFrame.LinearAlgebra.Eigen (jacobiEigenSym)
import DataFrame.Model
import DataFrame.Transform (Transform (..))

-- | How many components to keep.
data NComponents = NComp !Int | VarianceCovered !Double
    deriving (Eq, Show)

data PCAConfig = PCAConfig
    { pcaNComponents :: !NComponents
    , pcaStandardize :: !Bool
    }
    deriving (Eq, Show)

defaultPCAConfig :: PCAConfig
defaultPCAConfig = PCAConfig{pcaNComponents = NComp 2, pcaStandardize = False}

{- | A fitted PCA. 'pcaComponents' are sklearn's @components_@ (row @i@ is the
@i@-th loading vector); 'pcaScale' is @Just@ the per-column std when
standardizing.
-}
data PCAModel = PCAModel
    { pcaComponents :: !(V.Vector (VU.Vector Double))
    , pcaExplainedVariance :: !(VU.Vector Double)
    , pcaExplainedVarianceRatio :: !(VU.Vector Double)
    , pcaMean :: !(VU.Vector Double)
    , pcaScale :: !(Maybe (VU.Vector Double))
    , pcaFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

instance Fit PCAConfig [Expr Double] where
    type ModelOf PCAConfig [Expr Double] = PCAModel
    fit = fitPCA

-- | Fit PCA on the given feature columns (each must be a @Col@).
fitPCA :: PCAConfig -> [Expr Double] -> DataFrame -> PCAModel
fitPCA cfg features df =
    PCAModel
        { pcaComponents = V.take k vecs
        , pcaExplainedVariance = VU.take k evar
        , pcaExplainedVarianceRatio = VU.take k ratio
        , pcaMean = means
        , pcaScale = if pcaStandardize cfg then Just scales else Nothing
        , pcaFeatureNames = V.fromList names
        }
  where
    Features names cols _ n d = extractFeatures features df
    means = VU.fromList [VU.sum c / fromIntegral (max 1 n) | c <- cols]
    scales =
        VU.fromList
            [ let mu = means VU.! j
                  v = VU.sum (VU.map (\x -> (x - mu) ^ (2 :: Int)) c) / fromIntegral (max 1 n)
                  s = sqrt v
               in if s < 1e-12 then 1 else s
            | (j, c) <- zip [0 ..] cols
            ]
    scaled =
        V.generate n $ \i ->
            VU.generate d $ \j ->
                let mu = means VU.! j
                    s = if pcaStandardize cfg then scales VU.! j else 1
                 in ((cols !! j) VU.! i - mu) / s
    denom = fromIntegral (max 1 (n - 1))
    cov = V.map (VU.map (/ denom)) (gram scaled)
    (evals, vecs) = jacobiEigenSym cov
    evar = VU.map (max 0) evals
    total = VU.sum evar
    ratio = if total == 0 then evar else VU.map (/ total) evar
    k = resolveK (pcaNComponents cfg) d ratio

-- | Per-component projection expressions, named @pc1@, @pc2@, …
pcaExprs :: PCAModel -> [(T.Text, Expr Double)]
pcaExprs m =
    [ ("pc" <> T.pack (show i), componentExpr (pcaComponents m V.! (i - 1)))
    | i <- [1 .. V.length (pcaComponents m)]
    ]
  where
    names = V.toList (pcaFeatureNames m)
    means = VU.toList (pcaMean m)
    scales = maybe (repeat 1) VU.toList (pcaScale m)
    componentExpr vec =
        foldr (.+.) (F.lit 0) $
            [ F.lit (w / s) .*. ((Col n :: Expr Double) .-. F.lit mu)
            | (w, n, mu, s) <- zip4 (VU.toList vec) names means scales
            ]

-- | The PCA projection as a composable fitted 'Transform'.
pcaTransform :: PCAModel -> Transform
pcaTransform m = Transform [(n, UExpr e) | (n, e) <- pcaExprs m]

resolveK :: NComponents -> Int -> VU.Vector Double -> Int
resolveK (NComp k) d _ = max 1 (min k d)
resolveK (VarianceCovered frac) d ratio = max 1 (min d (go 0 0 1))
  where
    go !acc !cum !i
        | i > VU.length ratio = VU.length ratio
        | cum >= frac = acc
        | otherwise = go (acc + 1) (cum + ratio VU.! (i - 1)) (i + 1)

zip4 :: [a] -> [b] -> [c] -> [d] -> [(a, b, c, d)]
zip4 (a : as) (b : bs) (c : cs) (d : ds) = (a, b, c, d) : zip4 as bs cs ds
zip4 _ _ _ _ = []
