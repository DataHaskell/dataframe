{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Proximal-gradient (FISTA) solver for L1/L2-regularized generalized linear
models. 'fitL1Logistic' is the binary logistic split solver used by
'DataFrame.DecisionTree'; 'fitProx' generalizes it to any 'SmoothLoss'
(squared loss for lasso/elastic-net, squared hinge for LinearSVC). Features are
standardized internally and weights de-standardized, so the model applies to
raw column values.
-}
module DataFrame.LinearSolver (
    -- * Model
    LinearModel (..),

    -- * Configuration
    SolverConfig (..),
    defaultSolverConfig,

    -- * Solvers
    fitL1Logistic,
    fitProx,

    -- * Expr conversion
    modelToExpr,

    -- * Internals (exposed for testing)
    standardize,
    columnStats,
    softThreshold,
    sigmoid,
    dotProduct,
) where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (Matrix, gram, scaleV)
import DataFrame.LinearAlgebra.Eigen (powerIterTop)
import DataFrame.LinearSolver.Loss (
    SmoothLoss (..),
    logisticLoss,
    sigmoid,
 )
import DataFrame.Operators ((.*.), (.+.), (.>.))

import Control.Monad.ST (ST, runST)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

{- | A fitted linear classifier: predicts the positive class when
@sum (weights .* features) + intercept > 0@. Weights of exactly @0@ mark
features dropped by the L1 penalty (filtered out by 'modelToExpr').
-}
data LinearModel = LinearModel
    { lmWeights :: !(VU.Vector Double)
    , lmIntercept :: !Double
    , lmFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

-- | Hyper-parameters for the FISTA solver.
data SolverConfig = SolverConfig
    { scL1Lambda :: !Double
    -- ^ Strength of the L1 penalty on weights (intercept is not regularized).
    , scL2Lambda :: !Double
    {- ^ Strength of the L2 penalty @(λ₂/2)·|w|²@ (Elastic Net; Zou & Hastie
    2005). Combined with @scL1Lambda@ this gives the standard elastic-net
    objective @λ₁·|w|₁ + (λ₂/2)·|w|²@. At @scL2Lambda = 0@ the solver
    reduces to pure L1 (the original behaviour). The Friedman/Hastie/
    Tibshirani 2010 glmnet proximal step under step size @1/L@ is
    @softThreshold(z, λ₁/L) / (1 + λ₂/L)@ with @L = (d+1)/4 + λ₂@.
    -}
    , scMaxIter :: !Int
    -- ^ Maximum number of FISTA iterations.
    , scTol :: !Double
    -- ^ Convergence tolerance on the weight delta (L-inf norm).
    , scSampleWeights :: !(Maybe (VU.Vector Double))
    {- ^ Optional per-row sample weights, length @n@. @Nothing@ is uniform
    weight 1 (legacy behaviour, A1-A18 path). The 1/N gradient
    normalisation is preserved by convention: weights should have mean
    1 (i.e. @Σ w_i = N@) so the existing Lipschitz bound stays valid.
    See 'fitLinearCandidate' in 'DataFrame.DecisionTree' for the
    class-balanced construction @w_i = N / (2 · N_class(label_i))@.
    -}
    }
    deriving (Eq, Show)

defaultSolverConfig :: SolverConfig
defaultSolverConfig =
    SolverConfig
        { scL1Lambda = 0.005
        , scL2Lambda = 0.005
        , scMaxIter = 200
        , scTol = 1.0e-4
        , scSampleWeights = Nothing
        }

{- | Fit L1-regularized binary logistic regression by FISTA. Rows are feature
vectors of equal length; labels are in @{\-1,+1}@. Features are standardized
internally and weights de-standardized, so the model applies to raw values.
-}
fitL1Logistic ::
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
{-# INLINEABLE fitL1Logistic #-}
fitL1Logistic = runFista logisticLoss logisticLipschitz
  where
    logisticLipschitz _ keepN = fromIntegral (keepN + 1) / 4

{- | Fit any 'SmoothLoss' with the elastic-net proximal-gradient engine. The
Lipschitz constant uses the spectral norm of the standardized Gram matrix
(power iteration), which is tight for squared and squared-hinge losses where
the logistic trace bound would be far too loose.
-}
fitProx ::
    SmoothLoss ->
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
fitProx loss = runFista loss specNormLipschitz
  where
    specNormLipschitz xKept _ =
        let n = V.length xKept
            gramN = V.map (scaleV (1 / fromIntegral n)) (gram xKept)
            (specNorm, _) = powerIterTop 50 gramN
         in slCurvBound loss * (specNorm + 1)

{- | Shared FISTA scaffolding: standardize, drop near-constant columns, run the
inner loop, de-standardize. @lipschitzOf@ receives the standardized kept-feature
matrix and the number of kept columns and returns the smooth-part Lipschitz
bound; the L2 penalty contribution @λ₂@ is added here.
-}
runFista ::
    SmoothLoss ->
    (Matrix -> Int -> Double) ->
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
runFista loss lipschitzOf cfg rows labels featureNames
    | n == 0 || d == 0 = zeroModel
    | otherwise =
        let (!means, !stds, !variances) = columnStats rows
            !keep = keptIndices variances
         in if VU.null keep
                then zeroModel
                else
                    let !meansKept = gatherBy keep means
                        !stdsKept = gatherBy keep stds
                        !xKept = V.map (standardizeRowKept keep means stds) rows
                        !lipschitz =
                            lipschitzOf xKept (VU.length keep) + scL2Lambda cfg
                        (!wStdKept, !bStd) =
                            fistaLoop
                                loss
                                (scL1Lambda cfg)
                                (scL2Lambda cfg)
                                lipschitz
                                (scMaxIter cfg)
                                (scTol cfg)
                                (scSampleWeights cfg)
                                xKept
                                labels
                                (VU.replicate (VU.length keep) 0)
                                0
                        !wRawKept = VU.zipWith (/) wStdKept stdsKept
                        !bRaw = bStd - VU.sum (VU.zipWith (*) wRawKept meansKept)
                     in LinearModel (expandWeights d keep wRawKept) bRaw featureNames
  where
    !n = V.length rows
    !d = V.length featureNames
    zeroModel = LinearModel (VU.replicate d 0) 0 featureNames

{- | Indices of columns whose variance clears the near-constant threshold.
Columns below it are dropped before fitting; their weight ends up @0@.
-}
keptIndices :: VU.Vector Double -> VU.Vector Int
keptIndices variances =
    VU.fromList
        [ j
        | j <- [0 .. VU.length variances - 1]
        , VU.unsafeIndex variances j >= 1.0e-12
        ]

{- | Gather the entries of @v@ at @idxs@, preserving order. unsafeIndex is
safe: every index in @idxs@ is in range by construction.
-}
gatherBy :: VU.Vector Int -> VU.Vector Double -> VU.Vector Double
gatherBy idxs v = VU.map (VU.unsafeIndex v) idxs

{- | Standardize one row to the kept columns only (subtract column mean, divide
by column std). unsafeIndex is safe: rows share the column layout.
-}
standardizeRowKept ::
    VU.Vector Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double
standardizeRowKept keep means stds row = VU.map standardizeAt keep
  where
    standardizeAt j =
        (VU.unsafeIndex row j - VU.unsafeIndex means j) / VU.unsafeIndex stds j

{- | Scatter kept-column weights back into a full-width vector, with @0@ for
the dropped (near-constant) columns.
-}
expandWeights :: Int -> VU.Vector Int -> VU.Vector Double -> VU.Vector Double
expandWeights d keep wKept = VU.create $ do
    mv <- VUM.replicate d 0
    VU.iforM_ keep $ \k j -> VUM.unsafeWrite mv j (VU.unsafeIndex wKept k)
    pure mv

{- | Convert a fitted model to an 'Expr Bool' over its feature columns,
dropping zero-weight features. With no non-zero weights it returns the
constant @Lit (intercept > 0)@.
-}
modelToExpr :: LinearModel -> Expr Bool
modelToExpr m =
    case nonZero of
        [] -> F.lit (b > 0)
        (w0, n0) : rest -> score rest (term w0 n0) .>. F.lit (0 :: Double)
  where
    b = lmIntercept m
    nonZero =
        [ (w, n)
        | (w, n) <- zip (VU.toList (lmWeights m)) (V.toList (lmFeatureNames m))
        , w /= 0
        ]
    term w n = F.lit w .*. (Col n :: Expr Double)
    score rest first = foldl (\acc (w, n) -> acc .+. term w n) first rest .+. F.lit b

{- | Per-column @(means, stds, variances)@ of a feature matrix. Cheaper than
'standardize' when only the statistics are needed. unsafeIndex within is
safe: all rows share width @d@.
-}
columnStats ::
    V.Vector (VU.Vector Double) ->
    (VU.Vector Double, VU.Vector Double, VU.Vector Double)
columnStats x
    | V.null x = (VU.empty, VU.empty, VU.empty)
    | otherwise =
        let !d = VU.length (V.unsafeHead x)
            !invN = 1 / fromIntegral (V.length x)
            !means = columnMeans d invN x
            !variances = columnVariances d invN means x
            !stds = VU.map (\v -> if v < 1e-12 then 1 else sqrt v) variances
         in (means, stds, variances)

-- | Mean of each of the @d@ columns; @invN@ is @1 / nRows@.
columnMeans :: Int -> Double -> V.Vector (VU.Vector Double) -> VU.Vector Double
columnMeans d invN x = runST $ do
    acc <- VUM.replicate d 0
    V.forM_ x $ \row ->
        VU.iforM_ row $ \j v -> VUM.unsafeModify acc (+ v) j
    scaleInPlace invN acc
    VU.unsafeFreeze acc

-- | Variance of each of the @d@ columns about the supplied @means@.
columnVariances ::
    Int ->
    Double ->
    VU.Vector Double ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double
columnVariances d invN means x = runST $ do
    acc <- VUM.replicate d 0
    V.forM_ x $ \row ->
        VU.iforM_ row $ \j v ->
            let !c = v - VU.unsafeIndex means j in VUM.unsafeModify acc (+ c * c) j
    scaleInPlace invN acc
    VU.unsafeFreeze acc

-- | Multiply every element of a mutable vector by @factor@ in place.
scaleInPlace :: Double -> VUM.MVector s Double -> ST s ()
scaleInPlace factor mv = go 0
  where
    go !j
        | j >= VUM.length mv = pure ()
        | otherwise = VUM.unsafeModify mv (* factor) j >> go (j + 1)

{- | Standardize each column to zero mean and unit variance, also returning
@(means, stds, variances)@. Near-constant columns get std @1@; callers use
the raw variances to detect and drop them (see 'fitL1Logistic').
-}
standardize ::
    V.Vector (VU.Vector Double) ->
    ( V.Vector (VU.Vector Double)
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    )
standardize x
    | V.null x = (x, VU.empty, VU.empty, VU.empty)
    | otherwise =
        let (!means, !stds, !variances) = columnStats x
            !d = VU.length (V.unsafeHead x)
            standardizeRow row =
                VU.generate d $ \j ->
                    (VU.unsafeIndex row j - VU.unsafeIndex means j) / VU.unsafeIndex stds j
         in (V.map standardizeRow x, means, stds, variances)

{- | Proximal operator for the L1 norm: shrink @v@ toward zero by @lambda@,
clamping at zero.
-}
softThreshold :: Double -> Double -> Double
softThreshold lambda v
    | v > lambda = v - lambda
    | v < -lambda = v + lambda
    | otherwise = 0

{- | Dot product of two unboxed vectors. Caller must ensure equal length;
lengths are not checked.
-}
dotProduct :: VU.Vector Double -> VU.Vector Double -> Double
dotProduct u v = VU.sum (VU.zipWith (*) u v)

{- | Gradient of the average loss at @(w, b)@. Returns @(gradW, gradB)@.

Sample-weighted variant: when @sampleWeights@ is @Just ws@ the per-row
contribution is multiplied by @ws[i]@. With weights of mean 1
(i.e. @Σ w_i = N@; the class-balanced convention used by
'fitLinearCandidate'), the @1/N@ normalisation is preserved exactly.
-}
lossGradient ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
lossGradient loss sampleWeights features labels w b = (gradW, gradB)
  where
    !invN = 1 / fromIntegral (V.length features)
    !coeffs = rowCoeffs loss sampleWeights features labels w b invN
    !gradW = accumulateGradW (VU.length w) features coeffs
    !gradB = VU.sum coeffs

{- | Per-row loss coefficient @c_i = ℓ'(y_i, z_i) / N@ at margin
@z_i = w·x_i + b@, optionally scaled by @ws[i]@.

unsafeIndex is safe: @i@ ranges over @[0,n-1]@ and @labels@ /
@sampleWeights@ both have length @n@ by construction.
-}
rowCoeffs ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    Double ->
    VU.Vector Double
rowCoeffs loss sampleWeights features labels w b invN =
    VU.generate (V.length features) $ \i ->
        let !yi = VU.unsafeIndex labels i
            !row = V.unsafeIndex features i
            !z = dotProduct w row + b
            !base = slGradZ loss yi z * invN
         in case sampleWeights of
                Nothing -> base
                Just ws -> base * VU.unsafeIndex ws i

{- | Accumulate the weight gradient in one pass over every (row, feature)
pair, scattering into a length-@d@ mutable vector.
-}
accumulateGradW ::
    Int -> V.Vector (VU.Vector Double) -> VU.Vector Double -> VU.Vector Double
accumulateGradW d features coeffs = runST $ do
    mv <- VUM.replicate d 0
    V.iforM_ features $ \i row ->
        let !c = VU.unsafeIndex coeffs i
         in VU.iforM_ row $ \j v -> VUM.unsafeModify mv (+ c * v) j
    VU.unsafeFreeze mv

{- | Inner FISTA loop over standardized features. Returns the final @(w, b)@;
the caller is responsible for de-standardization.

@lambda1@ and @lambda2@ are the L1 / L2 penalty strengths; @lp@ is the
Lipschitz constant of the smooth part @(d+1)/4 + λ₂@. The Elastic-Net
proximal step is applied per FHT 2010 glmnet §2.6:
@prox(z) = softThreshold(z, λ₁/lp) / (1 + λ₂/lp)@.
-}
fistaLoop ::
    SmoothLoss ->
    Double ->
    Double ->
    Double ->
    Int ->
    Double ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaLoop loss lambda1 lambda2 lp maxIter tol sampleWeights features labels w0 b0 =
    go 0 w0 b0 w0 b0 1.0
  where
    !shrink = lambda1 / lp
    !ridgeDenom = 1 + lambda2 / lp
    !stepInv = 1 / lp
    proxStep = fistaProxStep loss sampleWeights features labels shrink ridgeDenom stepInv
    go !iter !xWPrev !xBPrev !yW !yB !t
        | iter >= maxIter = (xWPrev, xBPrev)
        | iter > 0 && delta < tol = (xW, xB)
        | otherwise = go (iter + 1) xW xB yWNew yBNew tNew
      where
        (!xW, !xB) = proxStep yW yB
        !delta = if VU.null xW then 0 else deltaInf xWPrev xW
        (!yWNew, !yBNew, !tNew) = fistaMomentum t xWPrev xBPrev xW xB

{- | One fused FISTA prox step: gradient step plus the Elastic-Net
proximal operator (soft-threshold then ridge shrinkage), without
materializing the intermediate trial weights.

The Elastic-Net prox of @g(w) = λ₁·|w|₁ + (λ₂/2)·|w|²@ at step @1/lp@ is
@softThreshold(z, λ₁/lp) / (1 + λ₂/lp)@ (FHT 2010 glmnet §2.6; Beck &
Teboulle 2009 §4). The intercept is unregularised (no L1 or L2 applied).
-}
fistaProxStep ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    Double ->
    Double ->
    Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaProxStep loss sampleWeights features labels shrink ridgeDenom stepInv yW yB =
    let (gW, gB) = lossGradient loss sampleWeights features labels yW yB
        !wNew =
            VU.zipWith
                (\yi gi -> softThreshold shrink (yi - gi * stepInv) / ridgeDenom)
                yW
                gW
        !bNew = yB - gB * stepInv
     in (wNew, bNew)

{- | Nesterov momentum extrapolation: new look-ahead point @(yW, yB)@ and the
updated step size @t@.
-}
fistaMomentum ::
    Double ->
    VU.Vector Double ->
    Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double, Double)
fistaMomentum t xWPrev xBPrev xW xB =
    let !tNew = (1 + sqrt (1 + 4 * t * t)) / 2
        !mom = (t - 1) / tNew
        !yW = VU.zipWith (\new old -> new + mom * (new - old)) xW xWPrev
        !yB = xB + mom * (xB - xBPrev)
     in (yW, yB, tNew)

{- | L-inf norm of the weight delta. unsafeIndex is safe: both vectors share
the same length by construction.
-}
{-# INLINE deltaInf #-}
deltaInf :: VU.Vector Double -> VU.Vector Double -> Double
deltaInf xWPrev = VU.ifoldl' (\acc i x -> max acc (abs (x - VU.unsafeIndex xWPrev i))) 0
