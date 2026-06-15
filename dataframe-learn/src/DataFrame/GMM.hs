{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Gaussian mixture models fitted by EM. Full covariance by default (with a
diagonal option and an automatic fall-back when a covariance is not positive
definite), log-space responsibilities, and Cholesky-based densities for
stability. 'predict' is the hard (arg-max) component assignment; per-component
log-densities are available via 'gmmLogDensityExprs'.
-}
module DataFrame.GMM (
    CovType (..),
    GMMConfig (..),
    defaultGMMConfig,
    GMMModel (..),
    gmmLogDensityExprs,
    gmmBIC,
    gmmAIC,
) where

import Data.List (sortBy)
import qualified Data.Map.Strict as M
import Data.Ord (comparing)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (Features (..), argMaxExpr, extractFeatures)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (Matrix, logSumExp)
import DataFrame.LinearAlgebra.Solve (backSubst, cholesky, forwardSubst)
import DataFrame.Model (Fit (..), Predict (..))
import DataFrame.Operators ((.*.), (.+.), (.-.))
import DataFrame.Random (mkGen, sampleIndices)

data CovType = FullCov | DiagCov
    deriving (Eq, Show)

data GMMConfig = GMMConfig
    { gmmK :: !Int
    , gmmCovType :: !CovType
    , gmmMaxIter :: !Int
    , gmmTol :: !Double
    , gmmRegCovar :: !Double
    , gmmSeed :: !Int
    }
    deriving (Eq, Show)

defaultGMMConfig :: GMMConfig
defaultGMMConfig =
    GMMConfig
        { gmmK = 2
        , gmmCovType = FullCov
        , gmmMaxIter = 100
        , gmmTol = 1.0e-3
        , gmmRegCovar = 1.0e-6
        , gmmSeed = 0
        }

-- | A fitted mixture. 'gmmCovariances' are the per-component covariance matrices.
data GMMModel = GMMModel
    { gmmWeights :: !(VU.Vector Double)
    , gmmMeans :: !(V.Vector (VU.Vector Double))
    , gmmCovariances :: !(V.Vector Matrix)
    , gmmConverged :: !Bool
    , gmmNIter :: !Int
    , gmmLogLikelihood :: !Double
    , gmmNObs :: !Int
    , gmmFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

instance Fit GMMConfig [Expr Double] GMMModel where
    fit = fitGMM

instance Predict GMMModel Int where
    predict = gmmAssignExpr

-- | Fit a Gaussian mixture over the given feature columns.
fitGMM :: GMMConfig -> [Expr Double] -> DataFrame -> GMMModel
fitGMM cfg features df = canonical finalModel
  where
    Features names _ rows n d = extractFeatures features df
    k = min (gmmK cfg) (max 1 n)
    reg = gmmRegCovar cfg
    (initIdx, _) = sampleIndices k n (mkGen (gmmSeed cfg))
    means0 = V.map (rows V.!) (V.convert initIdx)
    varDiag =
        VU.generate d $ \j ->
            let mu = sum [(rows V.! i) VU.! j | i <- [0 .. n - 1]] / fromIntegral (max 1 n)
             in ( sum [((rows V.! i) VU.! j - mu) ^ (2 :: Int) | i <- [0 .. n - 1]]
                    / fromIntegral (max 1 n)
                )
                    + reg
    cov0 = diagMatrix varDiag
    covs0 = V.replicate k cov0
    weights0 = VU.replicate k (1 / fromIntegral k)
    finalModel = em 0 weights0 means0 covs0 (-(1 / 0)) False
    em !iter weights means covs prevLL converged
        | iter >= gmmMaxIter cfg || converged =
            GMMModel weights means covs converged iter prevLL n (V.fromList names)
        | otherwise =
            let (logResp, ll) = eStep cfg rows weights means covs
                (weights', means', covs') = mStep cfg rows logResp d reg
                done = abs (ll - prevLL) < gmmTol cfg
             in em (iter + 1) weights' means' covs' ll done

-- | Per-component log-density expressions (log weight + Gaussian log pdf).
gmmLogDensityExprs :: GMMModel -> M.Map Int (Expr Double)
gmmLogDensityExprs m =
    M.fromList
        [ ( c
          , logDensityExpr
                (gmmWeights m VU.! c)
                (gmmMeans m V.! c)
                (gmmCovariances m V.! c)
                names
          )
        | c <- [0 .. V.length (gmmMeans m) - 1]
        ]
  where
    names = V.toList (gmmFeatureNames m)

-- | The hard-assignment expression: arg-max of component log-densities.
gmmAssignExpr :: GMMModel -> Expr Int
gmmAssignExpr m =
    argMaxExpr (M.toList (gmmLogDensityExprs m))

-- | Bayesian information criterion (lower is better).
gmmBIC :: GMMModel -> Double
gmmBIC m =
    negate (2 * gmmLogLikelihood m)
        + fromIntegral (nParams m) * log (fromIntegral (max 1 (gmmNObs m)))

-- | Akaike information criterion (lower is better).
gmmAIC :: GMMModel -> Double
gmmAIC m = negate (2 * gmmLogLikelihood m) + 2 * fromIntegral (nParams m)

nParams :: GMMModel -> Int
nParams m =
    let k = VU.length (gmmWeights m)
        d = if V.null (gmmMeans m) then 0 else VU.length (V.head (gmmMeans m))
     in (k - 1) + k * d + k * (d * (d + 1) `div` 2)

eStep ::
    GMMConfig ->
    Matrix ->
    VU.Vector Double ->
    V.Vector (VU.Vector Double) ->
    V.Vector Matrix ->
    (V.Vector (VU.Vector Double), Double)
eStep _ rows weights means covs = (logResp, totalLL)
  where
    k = VU.length weights
    comps =
        V.generate k $ \c ->
            ( log (max 1e-300 (weights VU.! c))
            , means V.! c
            , gaussianLogPdf (covs V.! c) (means V.! c)
            )
    perRow x =
        let lps = VU.generate k (\c -> let (lw, _, f) = comps V.! c in lw + f x)
            lse = logSumExp lps
         in (VU.map (subtract lse) lps, lse)
    results = V.map perRow rows
    logResp = V.map fst results
    totalLL = V.sum (V.map snd results)

mStep ::
    GMMConfig ->
    Matrix ->
    V.Vector (VU.Vector Double) ->
    Int ->
    Double ->
    (VU.Vector Double, V.Vector (VU.Vector Double), V.Vector Matrix)
mStep cfg rows logResp d reg = (weights, means, covs)
  where
    n = V.length rows
    k = if V.null logResp then 0 else VU.length (V.head logResp)
    resp = V.map (VU.map exp) logResp
    nk = VU.generate k (\c -> sum [resp V.! i VU.! c | i <- [0 .. n - 1]])
    weights = VU.map (/ fromIntegral (max 1 n)) nk
    means =
        V.generate k $ \c ->
            let s =
                    foldr
                        (VU.zipWith (+))
                        (VU.replicate d 0)
                        [VU.map (* (resp V.! i VU.! c)) (rows V.! i) | i <- [0 .. n - 1]]
             in VU.map (/ max 1e-12 (nk VU.! c)) s
    covs =
        V.generate k $ \c ->
            let mu = means V.! c
                acc = foldr addOuter (zeroMatrix d) [0 .. n - 1]
                addOuter i m =
                    let diff = VU.zipWith (-) (rows V.! i) mu
                        w = resp V.! i VU.! c
                     in addScaledOuter w diff m
                scaled = scaleMatrix (1 / max 1e-12 (nk VU.! c)) acc
                regd = addDiagScalar reg scaled
             in case gmmCovType cfg of
                    FullCov -> regd
                    DiagCov -> diagOnly regd

gaussianLogPdf :: Matrix -> VU.Vector Double -> VU.Vector Double -> Double
gaussianLogPdf cov mu =
    case cholesky cov of
        Just l ->
            let logdet = 2 * sum [log ((l V.! i) VU.! i) | i <- [0 .. d - 1]]
             in \x ->
                    let diff = VU.zipWith (-) x mu
                        z = forwardSubst l diff
                        quad = VU.sum (VU.map (^ (2 :: Int)) z)
                     in negate 0.5 * (fromIntegral d * log (2 * pi) + logdet + quad)
        Nothing ->
            let var = VU.generate d (\i -> max 1e-12 ((cov V.! i) VU.! i))
             in \x ->
                    negate 0.5
                        * VU.sum
                            ( VU.generate d $ \j ->
                                let diff = x VU.! j - mu VU.! j
                                 in diff * diff / var VU.! j + log (2 * pi * var VU.! j)
                            )
  where
    d = VU.length mu

logDensityExpr ::
    Double -> VU.Vector Double -> Matrix -> [T.Text] -> Expr Double
logDensityExpr weight mu cov names =
    case precisionAndLogdet cov of
        Just (prec, logdet) ->
            let constTerm =
                    log (max 1e-300 weight)
                        - 0.5 * (fromIntegral d * log (2 * pi) + logdet)
                quad =
                    foldr (.+.) (F.lit 0) $
                        [ F.lit (-(0.5 * prec V.! a VU.! b)) .*. (centered a .*. centered b)
                        | a <- [0 .. d - 1]
                        , b <- [0 .. d - 1]
                        ]
             in F.lit constTerm .+. quad
        Nothing -> F.lit (log (max 1e-300 weight))
  where
    d = VU.length mu
    centered j = (Col (names !! j) :: Expr Double) .-. F.lit (mu VU.! j)

precisionAndLogdet :: Matrix -> Maybe (Matrix, Double)
precisionAndLogdet cov = do
    l <- cholesky cov
    let d = V.length cov
        logdet = 2 * sum [log ((l V.! i) VU.! i) | i <- [0 .. d - 1]]
        cols = [forwardThenBack l (unitVec d i) | i <- [0 .. d - 1]]
        prec =
            V.fromList
                [VU.fromList [cols !! j VU.! i | j <- [0 .. d - 1]] | i <- [0 .. d - 1]]
    pure (prec, logdet)

forwardThenBack :: Matrix -> VU.Vector Double -> VU.Vector Double
forwardThenBack l b = backSubst l (forwardSubst l b)

canonical :: GMMModel -> GMMModel
canonical m =
    let order =
            map snd $
                sortBy
                    (comparing fst)
                    [ (firstCoord (gmmMeans m V.! c), c)
                    | c <- [0 .. V.length (gmmMeans m) - 1]
                    ]
        firstCoord v = if VU.null v then 0 else VU.head v
     in m
            { gmmWeights = VU.fromList [gmmWeights m VU.! c | c <- order]
            , gmmMeans = V.fromList [gmmMeans m V.! c | c <- order]
            , gmmCovariances = V.fromList [gmmCovariances m V.! c | c <- order]
            }

diagMatrix :: VU.Vector Double -> Matrix
diagMatrix v =
    let d = VU.length v
     in V.generate d (\i -> VU.generate d (\j -> if i == j then v VU.! i else 0))

diagOnly :: Matrix -> Matrix
diagOnly m =
    let d = V.length m
     in V.generate
            d
            (\i -> VU.generate d (\j -> if i == j then (m V.! i) VU.! j else 0))

zeroMatrix :: Int -> Matrix
zeroMatrix d = V.replicate d (VU.replicate d 0)

scaleMatrix :: Double -> Matrix -> Matrix
scaleMatrix s = V.map (VU.map (* s))

addDiagScalar :: Double -> Matrix -> Matrix
addDiagScalar s = V.imap (\i row -> row VU.// [(i, row VU.! i + s)])

addScaledOuter :: Double -> VU.Vector Double -> Matrix -> Matrix
addScaledOuter w diff m =
    let d = VU.length diff
     in V.generate d $ \i ->
            VU.generate d $ \j ->
                (m V.! i) VU.! j + w * (diff VU.! i) * (diff VU.! j)

unitVec :: Int -> Int -> VU.Vector Double
unitVec d i = VU.generate d (\j -> if i == j then 1 else 0)
