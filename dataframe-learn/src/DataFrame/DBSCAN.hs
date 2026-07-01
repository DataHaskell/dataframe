{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

{- | Density-based clustering (DBSCAN). Brute-force @O(n²)@ region queries, no
spatial index — suitable for the in-memory scales this library targets. DBSCAN
is transductive: it has a 'Fit' instance but deliberately no 'Predict' instance
(there is no honest single prediction expression). 'dbscanSurrogateExpr' fits an
interpretable decision-tree surrogate on the cluster labels instead.
-}
module DataFrame.DBSCAN (
    module DataFrame.Model,
    DBSCANConfig (..),
    defaultDBSCANConfig,
    DBSCANModel (..),
    dbscanSurrogateExpr,
) where

import Control.Monad.ST (runST)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import DataFrame.DecisionTree.Fit (fitDecisionTree)
import DataFrame.DecisionTree.Types (TreeConfig)
import DataFrame.Featurize.Internal (
    Features (..),
    columnExprName,
    extractFeatures,
    materializeColumn,
 )
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame, fromNamedColumns)
import DataFrame.Internal.Expression (Expr)
import DataFrame.LinearAlgebra (epsNeighbors)
import DataFrame.Model

data DBSCANConfig = DBSCANConfig
    { dbEps :: !Double
    , dbMinSamples :: !Int
    }
    deriving (Eq, Show)

defaultDBSCANConfig :: DBSCANConfig
defaultDBSCANConfig = DBSCANConfig{dbEps = 0.5, dbMinSamples = 5}

{- | A fitted DBSCAN labelling. 'dbLabels' uses @-1@ for noise (sklearn's
@labels_@); 'dbCoreSampleIndices' are the core points.
-}
data DBSCANModel = DBSCANModel
    { dbLabels :: !(VU.Vector Int)
    , dbCoreSampleIndices :: !(VU.Vector Int)
    , dbNClusters :: !Int
    }
    deriving (Eq, Show)

instance Fit DBSCANConfig [Expr Double] where
    type ModelOf DBSCANConfig [Expr Double] = DBSCANModel
    fit = fitDBSCAN

-- | Cluster the feature columns with DBSCAN.
fitDBSCAN :: DBSCANConfig -> [Expr Double] -> DataFrame -> DBSCANModel
fitDBSCAN cfg features df =
    DBSCANModel labels coreIdx nClusters
  where
    Features _ _ rows n _ = extractFeatures features df
    nbrs = V.generate n (epsNeighbors (dbEps cfg) rows)
    isCore i = VU.length (nbrs V.! i) + 1 >= dbMinSamples cfg
    coreIdx = VU.fromList [i | i <- [0 .. n - 1], isCore i]
    labels = clusterLabels n nbrs isCore
    nClusters = if VU.null labels then 0 else 1 + maximum (-1 : VU.toList labels)

clusterLabels ::
    Int -> V.Vector (VU.Vector Int) -> (Int -> Bool) -> VU.Vector Int
clusterLabels n nbrs isCore = runST $ do
    lab <- VUM.replicate n (-2)
    let seedLoop c i
            | i >= n = pure ()
            | otherwise = do
                li <- VUM.read lab i
                if li /= -2
                    then seedLoop c (i + 1)
                    else
                        if not (isCore i)
                            then VUM.write lab i (-1) >> seedLoop c (i + 1)
                            else do
                                VUM.write lab i c
                                expand lab c (VU.toList (nbrs V.! i))
                                seedLoop (c + 1) (i + 1)
        expand _ _ [] = pure ()
        expand lab c (q : qs) = do
            lq <- VUM.read lab q
            if lq == -1
                then VUM.write lab q c >> expand lab c qs
                else
                    if lq /= -2
                        then expand lab c qs
                        else do
                            VUM.write lab q c
                            let extra = if isCore q then VU.toList (nbrs V.! q) else []
                            expand lab c (extra ++ qs)
    seedLoop 0 0
    VU.freeze lab

{- | Fit a decision-tree surrogate on the DBSCAN labels so new rows can be
assigned an (approximate) cluster. Noise (@-1@) is its own class.
-}
dbscanSurrogateExpr ::
    TreeConfig -> [Expr Double] -> DBSCANModel -> DataFrame -> Expr Int
dbscanSurrogateExpr cfg features model df =
    fitDecisionTree cfg (F.col @Int clusterCol) augmented
  where
    clusterCol = "__cluster__"
    cols = map (\e -> (columnExprName e, materializeColumn df e)) features
    augmented =
        fromNamedColumns $
            [(n, DI.fromList (VU.toList v)) | (n, v) <- cols]
                ++ [(clusterCol, DI.fromList (VU.toList (dbLabels model)))]
