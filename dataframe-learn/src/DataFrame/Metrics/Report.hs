{- | Bundled, pretty-printing evaluation summaries: a labelled confusion matrix
and scikit-learn-style regression / classification reports. The @*Expr@ variants
take a model's prediction expression and a truth column directly, so a full
report is a one-liner after fitting.
-}
module DataFrame.Metrics.Report (
    ConfusionMatrix (..),
    confusionMatrix,
    confusionMatrixExpr,
    RegressionReport (..),
    regressionReport,
    regressionReportExpr,
    ClassStats (..),
    ClassificationReport (..),
    classificationReport,
    classificationReportExpr,
) where

import Data.List (nub, sort, sortBy)
import Data.Ord (Down (..), comparing)
import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Metrics (
    Average (..),
    accuracy,
    classCounts,
    columnOf,
    f1,
    f1Of,
    mae,
    mse,
    precOf,
    r2,
    recOf,
    rmse,
 )

-- | A labelled confusion matrix: class order plus row-major @actual×predicted@.
data ConfusionMatrix = ConfusionMatrix
    { cmClasses :: ![Double]
    , cmCounts :: ![[Int]]
    }
    deriving (Eq)

-- | Confusion matrix over the class set of @truth ∪ preds@.
confusionMatrix :: VU.Vector Double -> VU.Vector Double -> ConfusionMatrix
confusionMatrix preds truth = ConfusionMatrix classes counts
  where
    classes = sort (nub (VU.toList truth ++ VU.toList preds))
    counts =
        [ [ VU.length (VU.filter id (VU.zipWith (\p t -> t == a && p == c) preds truth))
          | c <- classes
          ]
        | a <- classes
        ]

-- | Confusion matrix from a prediction expression and a truth column.
confusionMatrixExpr ::
    Expr Double -> Expr Double -> DataFrame -> ConfusionMatrix
confusionMatrixExpr predExpr truthExpr df =
    confusionMatrix (columnOf df predExpr) (columnOf df truthExpr)

instance Show ConfusionMatrix where
    show (ConfusionMatrix classes counts) =
        unlines (header : rows)
      where
        lbls = map show classes
        w = maximum (8 : map length lbls) + 2
        cell s = replicate (max 1 (w - length s)) ' ' ++ s
        header = cell "a\\p" ++ concatMap cell lbls
        rows = [cell a ++ concatMap (cell . show) row | (a, row) <- zip lbls counts]

-- | Regression metrics bundle.
data RegressionReport = RegressionReport
    { rrMSE :: !Double
    , rrRMSE :: !Double
    , rrMAE :: !Double
    , rrR2 :: !Double
    }
    deriving (Eq)

instance Show RegressionReport where
    show r =
        unlines
            [ "Regression report"
            , "  mse  = " ++ show (rrMSE r)
            , "  rmse = " ++ show (rrRMSE r)
            , "  mae  = " ++ show (rrMAE r)
            , "  r2   = " ++ show (rrR2 r)
            ]

-- | Regression report from prediction/truth vectors.
regressionReport :: VU.Vector Double -> VU.Vector Double -> RegressionReport
regressionReport preds truth =
    RegressionReport
        (mse preds truth)
        (rmse preds truth)
        (mae preds truth)
        (r2 preds truth)

-- | Regression report from a prediction expression and a truth column.
regressionReportExpr ::
    Expr Double -> Expr Double -> DataFrame -> RegressionReport
regressionReportExpr predExpr truthExpr df =
    regressionReport (columnOf df predExpr) (columnOf df truthExpr)

-- | Per-class precision/recall/F1/support.
data ClassStats = ClassStats
    { csPrecision :: !Double
    , csRecall :: !Double
    , csF1 :: !Double
    , csSupport :: !Int
    }
    deriving (Eq, Show)

{- | A scikit-learn-style classification report: per-class stats plus accuracy
and macro/weighted F1.
-}
data ClassificationReport = ClassificationReport
    { crPerClass :: ![(Double, ClassStats)]
    , crAccuracy :: !Double
    , crMacroF1 :: !Double
    , crWeightedF1 :: !Double
    }
    deriving (Eq)

instance Show ClassificationReport where
    show r =
        unlines $
            (pad "class" ++ pad "precision" ++ pad "recall" ++ pad "f1" ++ pad "support")
                : [ pad (show c)
                        ++ pad (num (csPrecision s))
                        ++ pad (num (csRecall s))
                        ++ pad (num (csF1 s))
                        ++ pad (show (csSupport s))
                  | (c, s) <- crPerClass r
                  ]
                ++ [ ""
                   , "accuracy    = " ++ num (crAccuracy r)
                   , "macro f1    = " ++ num (crMacroF1 r)
                   , "weighted f1 = " ++ num (crWeightedF1 r)
                   ]
      where
        pad s = let w = 12 in s ++ replicate (max 1 (w - length s)) ' '
        num x = show (fromIntegral (round (x * 1000) :: Int) / 1000 :: Double)

-- | Classification report from prediction/truth vectors.
classificationReport ::
    VU.Vector Double -> VU.Vector Double -> ClassificationReport
classificationReport preds truth =
    ClassificationReport
        perClass
        (accuracy preds truth)
        (f1 Macro preds truth)
        (f1 Weighted preds truth)
  where
    perClass =
        sortBy (comparing (Down . csSupport . snd)) $
            [ (c, ClassStats (precOf s) (recOf s) (f1Of s) (supOf s))
            | (c, s) <- classCounts preds truth
            ]
    supOf (_, _, _, sup) = sup

-- | Classification report from a prediction expression and a truth column.
classificationReportExpr ::
    Expr Double -> Expr Double -> DataFrame -> ClassificationReport
classificationReportExpr predExpr truthExpr df =
    classificationReport (columnOf df predExpr) (columnOf df truthExpr)
