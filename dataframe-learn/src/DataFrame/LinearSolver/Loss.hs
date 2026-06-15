{-# LANGUAGE OverloadedStrings #-}

{- | Smooth losses for the proximal-gradient engine. Each carries its
derivative @∂ℓ/∂z@ at @z = w·x + b@ and a global bound on the curvature
@∂²ℓ/∂z²@ (used for the FISTA step size).
-}
module DataFrame.LinearSolver.Loss (
    SmoothLoss (..),
    sigmoid,
    logisticLoss,
    squaredLoss,
    sqHingeLoss,
) where

import qualified Data.Text as T

{- | A convex, @C¹@ per-sample loss @ℓ(y, z)@. 'slGradZ' is @∂ℓ/∂z@;
'slCurvBound' bounds @∂²ℓ/∂z²@ over all @(y, z)@.
-}
data SmoothLoss = SmoothLoss
    { slName :: !T.Text
    , slGradZ :: Double -> Double -> Double
    , slCurvBound :: !Double
    }

-- | Numerically stable logistic sigmoid.
sigmoid :: Double -> Double
sigmoid z
    | z >= 0 = 1 / (1 + exp (-z))
    | otherwise = let ez = exp z in ez / (1 + ez)

-- | Binary logistic loss for labels in @{\-1,+1}@: @ℓ = log(1 + exp(-y z))@.
logisticLoss :: SmoothLoss
logisticLoss =
    SmoothLoss "logistic" (\y z -> negate (y * sigmoid (negate (y * z)))) 0.25

-- | Squared error for regression: @ℓ = ½ (z - y)²@.
squaredLoss :: SmoothLoss
squaredLoss = SmoothLoss "squared" (flip (-)) 1.0

-- | Squared hinge for classification (LinearSVC default), labels @{\-1,+1}@.
sqHingeLoss :: SmoothLoss
sqHingeLoss =
    SmoothLoss
        "squared_hinge"
        (\y z -> let m = 1 - y * z in if m > 0 then negate (2 * y * m) else 0)
        2.0
