{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

{- | Kernel PCA with an RBF kernel, solved on a set of landmark points (Nyström).
Exact kernel PCA when the landmark count covers every row, a principled
approximation otherwise. 'fit' trains the model; the projection is exposed as
'kernelPCAExprs' / 'kernelPcaTransform' (a transformer, so no 'Predict').
-}
module DataFrame.PCA.Kernel (
    KernelPCAConfig (..),
    defaultKernelPCAConfig,
    KernelPCAModel (..),
    kernelPCAExprs,
    kernelPcaTransform,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (Features (..), extractFeatures)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), UExpr (..))
import DataFrame.LinearAlgebra (sqDist)
import DataFrame.LinearAlgebra.Eigen (jacobiEigenSym)
import DataFrame.Model (Fit (..))
import DataFrame.Operators ((.*.), (.+.), (.-.))
import DataFrame.Random (mkGen, sampleIndices)
import DataFrame.Transform (Transform (..))

data KernelPCAConfig = KernelPCAConfig
    { kpcaNComponents :: !Int
    , kpcaGamma :: !(Maybe Double)
    , kpcaNLandmarks :: !Int
    , kpcaSeed :: !Int
    }
    deriving (Eq, Show)

defaultKernelPCAConfig :: KernelPCAConfig
defaultKernelPCAConfig =
    KernelPCAConfig
        { kpcaNComponents = 2
        , kpcaGamma = Nothing
        , kpcaNLandmarks = 128
        , kpcaSeed = 0
        }

{- | A fitted kernel PCA. Each component is @Σ_l βₗ·K(x, landmarkₗ) + cᵢ@ with an
RBF kernel of bandwidth 'kpcaGammaUsed'.
-}
data KernelPCAModel = KernelPCAModel
    { kpcaLandmarks :: !(V.Vector (VU.Vector Double))
    , kpcaBetas :: !(V.Vector (VU.Vector Double))
    , kpcaConsts :: !(VU.Vector Double)
    , kpcaEigenvalues :: !(VU.Vector Double)
    , kpcaGammaUsed :: !Double
    , kpcaFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

instance Fit KernelPCAConfig [Expr Double] KernelPCAModel where
    fit = fitKernelPCA

-- | Fit kernel PCA over the given feature columns.
fitKernelPCA :: KernelPCAConfig -> [Expr Double] -> DataFrame -> KernelPCAModel
fitKernelPCA cfg features df =
    KernelPCAModel
        { kpcaLandmarks = landmarks
        , kpcaBetas = betas
        , kpcaConsts = consts
        , kpcaEigenvalues = VU.take k evals
        , kpcaGammaUsed = gamma
        , kpcaFeatureNames = V.fromList names
        }
  where
    Features names _ rows n d = extractFeatures features df
    m = min (max 1 (kpcaNLandmarks cfg)) n
    (idx, _) = sampleIndices m n (mkGen (kpcaSeed cfg))
    landmarks = V.map (rows V.!) (V.convert idx)
    gamma = case kpcaGamma cfg of
        Just g -> g
        Nothing -> 1 / fromIntegral (max 1 d)
    kmat =
        V.generate m $ \i ->
            VU.generate m $ \j ->
                exp (negate gamma * sqDist (landmarks V.! i) (landmarks V.! j))
    rowMean i = VU.sum (kmat V.! i) / fromIntegral m
    totalMean = sum [rowMean i | i <- [0 .. m - 1]] / fromIntegral m
    centered =
        V.generate m $ \i ->
            VU.generate m $ \j ->
                (kmat V.! i) VU.! j - rowMean i - rowMean j + totalMean
    (evals, vecs) = jacobiEigenSym centered
    k = min (kpcaNComponents cfg) m
    alphas =
        V.generate k $ \i ->
            let lam = max 1e-12 (evals VU.! i)
             in VU.map (/ sqrt lam) (vecs V.! i)
    betas =
        V.map
            (\a -> let s = VU.sum a / fromIntegral m in VU.map (subtract s) a)
            alphas
    consts =
        VU.generate k $ \i ->
            let a = alphas V.! i
                sA = VU.sum a
             in negate (sum [a VU.! l * rowMean l | l <- [0 .. m - 1]])
                    + totalMean * sA

-- | Per-component projection expressions, named @kpc1@, @kpc2@, …
kernelPCAExprs :: KernelPCAModel -> [(T.Text, Expr Double)]
kernelPCAExprs m =
    [ ("kpc" <> T.pack (show (i + 1)), componentExpr i)
    | i <- [0 .. V.length (kpcaBetas m) - 1]
    ]
  where
    names = V.toList (kpcaFeatureNames m)
    gamma = kpcaGammaUsed m
    componentExpr i =
        foldr (.+.) (F.lit (kpcaConsts m VU.! i)) $
            [ F.lit (kpcaBetas m V.! i VU.! l) .*. kernelExpr (kpcaLandmarks m V.! l)
            | l <- [0 .. V.length (kpcaLandmarks m) - 1]
            ]
    kernelExpr landmark =
        exp (F.lit (negate gamma) .*. sqDistExpr landmark)
    sqDistExpr landmark =
        foldr (.+.) (F.lit 0) $
            [ let diff = (Col n :: Expr Double) .-. F.lit lj in diff .*. diff
            | (n, lj) <- zip names (VU.toList landmark)
            ]

-- | The kernel-PCA projection as a composable fitted 'Transform'.
kernelPcaTransform :: KernelPCAModel -> Transform
kernelPcaTransform m = Transform [(n, UExpr e) | (n, e) <- kernelPCAExprs m]
