{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE UndecidableInstances #-}

{- | sklearn-style standalone tree estimators returning inspectable records
(depth, leaf count, per-feature split usage). 'fit' trains a classifier (from a
'TreeConfig') or a regressor (from a 'RegTreeConfig'); 'predict' is the compiled
tree expression, and the record exposes the raw 'Tree' too. The bare
'DataFrame.DecisionTree.Fit.fitDecisionTree' remains for callers that only want
the classifier @Expr@.
-}
module DataFrame.DecisionTree.Model (
    DecisionTreeClassifier (..),
    DecisionTreeRegressor (..),
) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T

import qualified Data.Vector as V

import DataFrame.DecisionTree.Cart (cartFeatures)
import DataFrame.DecisionTree.Fit (fitDecisionTree, treeToExpr)
import DataFrame.DecisionTree.Regression (RegTreeConfig, fitRegTreeOn)
import DataFrame.DecisionTree.Types (Tree (..), TreeConfig)
import DataFrame.Featurize.Internal (targetDoubles)
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (..), getColumns)
import DataFrame.Model (Fit (..), Predict (..))

-- | A fitted classification tree with structural diagnostics.
data DecisionTreeClassifier a = DecisionTreeClassifier
    { dtcExpr :: !(Expr a)
    , dtcDepth :: !Int
    , dtcNLeaves :: !Int
    , dtcFeatureUsage :: !(M.Map T.Text Int)
    }
    deriving (Show)

-- | A fitted regression tree with structural diagnostics.
data DecisionTreeRegressor = DecisionTreeRegressor
    { dtrTree :: !(Tree Double)
    , dtrExpr :: !(Expr Double)
    , dtrDepth :: !Int
    , dtrNLeaves :: !Int
    , dtrFeatureUsage :: !(M.Map T.Text Int)
    }
    deriving (Show)

instance (Columnable a, Ord a) => Fit TreeConfig (Expr a) (DecisionTreeClassifier a) where
    fit cfg target df =
        DecisionTreeClassifier
            e
            (exprDepth e)
            (exprLeaves e)
            (usageCounts (exprUsage e))
      where
        e = fitDecisionTree cfg target df

instance Predict (DecisionTreeClassifier a) a where
    predict = dtcExpr

instance Fit RegTreeConfig (Expr Double) DecisionTreeRegressor where
    fit cfg target df =
        DecisionTreeRegressor
            t
            e
            (exprDepth e)
            (exprLeaves e)
            (usageCounts (exprUsage e))
      where
        t = case target of
            Col name ->
                fitRegTreeOn
                    cfg
                    (V.fromList (cartFeatures name df))
                    (targetDoubles target df)
                    Nothing
            _ ->
                error
                    ("fit @DecisionTreeRegressor: target must be a column, got " ++ show target)
        e = treeToExpr t

instance Predict DecisionTreeRegressor Double where
    predict = dtrExpr

usageCounts :: [T.Text] -> M.Map T.Text Int
usageCounts = foldr (\c -> M.insertWith (+) c 1) M.empty

exprUsage :: Expr a -> [T.Text]
exprUsage (If c t e) = getColumns c ++ exprUsage t ++ exprUsage e
exprUsage _ = []

exprLeaves :: Expr a -> Int
exprLeaves (If _ t e) = exprLeaves t + exprLeaves e
exprLeaves _ = 1

exprDepth :: Expr a -> Int
exprDepth (If _ t e) = 1 + max (exprDepth t) (exprDepth e)
exprDepth _ = 0
