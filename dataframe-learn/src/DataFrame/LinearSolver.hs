{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | L1-regularized logistic regression used as the per-node split solver in
'DataFrame.DecisionTree'. Produces a sparse oblique hyperplane that can be
compiled to an 'Expr Bool' over numeric columns.
-}
module DataFrame.LinearSolver (
    -- * Model
    LinearModel (..),

    -- * Configuration
    SolverConfig (..),
    defaultSolverConfig,

    -- * Solver
    fitL1Logistic,

    -- * Expr conversion
    modelToExpr,

    -- * Internals (exposed for testing)
    standardize,
    softThreshold,
    sigmoid,
    dotProduct,
) where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr (..))
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
    , scMaxIter :: !Int
    -- ^ Maximum number of FISTA iterations.
    , scTol :: !Double
    -- ^ Convergence tolerance on the weight delta (L-inf norm).
    }
    deriving (Eq, Show)

defaultSolverConfig :: SolverConfig
defaultSolverConfig =
    SolverConfig
        { scL1Lambda = 0.01
        , scMaxIter = 200
        , scTol = 1.0e-4
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
fitL1Logistic cfg rows labels featureNames
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
                        !lipschitz = fromIntegral (VU.length keep + 1) / 4
                        (!wStdKept, !bStd) =
                            fistaLoop
                                (scL1Lambda cfg)
                                lipschitz
                                (scMaxIter cfg)
                                (scTol cfg)
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

-- | Numerically stable logistic sigmoid.
sigmoid :: Double -> Double
sigmoid z
    | z >= 0 = 1 / (1 + exp (-z))
    | otherwise = let ez = exp z in ez / (1 + ez)

{- | Dot product of two unboxed vectors. Caller must ensure equal length;
lengths are not checked.
-}
dotProduct :: VU.Vector Double -> VU.Vector Double -> Double
dotProduct u v = VU.sum (VU.zipWith (*) u v)

{- | Gradient of the average binary logistic loss at @(w, b)@ for labels in
@{\-1,+1}@. Returns @(gradW, gradB)@.
-}
logisticGradient ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
logisticGradient features labels w b = (gradW, gradB)
  where
    !invN = 1 / fromIntegral (V.length features)
    !coeffs = rowCoeffs features labels w b invN
    !gradW = accumulateGradW (VU.length w) features coeffs
    !gradB = VU.sum coeffs

{- | Per-row loss coefficient @c_i = -y_i * sigmoid(-y_i * margin_i) / N@.
unsafeIndex is safe: @i@ ranges over @[0,n-1]@ and @labels@ has length n.
-}
rowCoeffs ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    Double ->
    VU.Vector Double
rowCoeffs features labels w b invN =
    VU.generate (V.length features) $ \i ->
        let !yi = VU.unsafeIndex labels i
            !row = V.unsafeIndex features i
            !margin = yi * (dotProduct w row + b)
         in -(yi * sigmoid (-margin) * invN)

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
-}
fistaLoop ::
    Double ->
    Double ->
    Int ->
    Double ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaLoop lambda lp maxIter tol features labels w0 b0 = go 0 w0 b0 w0 b0 1.0
  where
    proxStep = fistaProxStep features labels (lambda / lp) (1 / lp)
    go !iter !xWPrev !xBPrev !yW !yB !t
        | iter >= maxIter = (xWPrev, xBPrev)
        | iter > 0 && delta < tol = (xW, xB)
        | otherwise = go (iter + 1) xW xB yWNew yBNew tNew
      where
        (!xW, !xB) = proxStep yW yB
        !delta = if VU.null xW then 0 else deltaInf xWPrev xW
        (!yWNew, !yBNew, !tNew) = fistaMomentum t xWPrev xBPrev xW xB

{- | One fused FISTA prox step: gradient step plus soft-threshold, without
materializing the intermediate trial weights.
-}
fistaProxStep ::
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    Double ->
    Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaProxStep features labels shrink stepInv yW yB =
    let (gW, gB) = logisticGradient features labels yW yB
        !wNew = VU.zipWith (\yi gi -> softThreshold shrink (yi - gi * stepInv)) yW gW
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
