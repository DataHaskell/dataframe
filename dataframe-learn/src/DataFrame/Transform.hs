{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Fitted column transforms as a composable monoid. A 'Transform' is a list of
named output expressions; @s <> t@ means \"apply @s@, then @t@\", fusing @t@'s
references to @s@'s outputs by simultaneous substitution. 'applyTransform' runs
one against a frame; 'compileThrough' folds a transform into a model's
prediction expression so the result is a single expression over the raw inputs.

Every right-hand side must be row-wise (no aggregation/window), and within one
transform each expression reads the original frame.
-}
module DataFrame.Transform (
    Transform (..),
    applyTransform,
    compileThrough,
    ScalerModel (..),
    standardScaler,
    scalerTransform,
) where

import Control.Exception (throw)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Expression.Operators ((.-.), (./.))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (
    Expr (..),
    NamedExpr,
    UExpr (..),
    substituteColumns,
 )
import DataFrame.Operations.Core (columnAsDoubleVector)
import DataFrame.Operations.Transformations (deriveMany)

-- | A fitted transform: named output columns derived from the input frame.
newtype Transform = Transform {transformOutputs :: [NamedExpr]}

instance Semigroup Transform where
    Transform s <> Transform t =
        Transform (s ++ map (subst (M.fromList s)) t)
      where
        subst :: M.Map T.Text UExpr -> NamedExpr -> NamedExpr
        subst m (nm, UExpr e) = (nm, UExpr (substituteColumns m e))

instance Monoid Transform where
    mempty = Transform []

-- | Apply a transform to a frame (deriving its outputs in order).
applyTransform :: Transform -> DataFrame -> DataFrame
applyTransform (Transform os) = deriveMany os

{- | Fold a preprocessing transform into a model's prediction expression,
yielding one expression over the transform's input columns.
-}
compileThrough :: (Columnable a) => Transform -> Expr a -> Expr a
compileThrough (Transform os) = substituteColumns (M.fromList os)

-- | A fitted standardizer: per-column means and standard deviations.
data ScalerModel = ScalerModel
    { smColumns :: !(V.Vector T.Text)
    , smMeans :: !(VU.Vector Double)
    , smStds :: !(VU.Vector Double)
    }
    deriving (Eq, Show)

-- | Fit a standard scaler over the named columns.
standardScaler :: [T.Text] -> DataFrame -> ScalerModel
standardScaler names df =
    ScalerModel (V.fromList names) (VU.fromList means) (VU.fromList stds)
  where
    cols = map column names
    column n = case columnAsDoubleVector (F.col @Double n) df of
        Right v -> v
        Left e -> throw e
    means = [VU.sum c / fromIntegral (max 1 (VU.length c)) | c <- cols]
    stds =
        [ let mu = mean
              v =
                VU.sum (VU.map (\x -> (x - mu) ^ (2 :: Int)) c)
                    / fromIntegral (max 1 (VU.length c))
              s = sqrt v
           in if s < 1e-12 then 1 else s
        | (mean, c) <- zip means cols
        ]

-- | The scaler as a 'Transform': @(col - μ) / σ@ per column.
scalerTransform :: ScalerModel -> Transform
scalerTransform m =
    Transform
        [ (n, UExpr ((F.col @Double n .-. F.lit mu) ./. F.lit sigma))
        | (n, mu, sigma) <-
            zip3
                (V.toList (smColumns m))
                (VU.toList (smMeans m))
                (VU.toList (smStds m))
        ]
