{- | Interpretable decision trees on DataFrames (CART refined by TAO). The
curated public surface: classifier/regressor configs, fitted-model records,
and their 'Fit'\/'Predict' instances.
-}
module DataFrame.DecisionTree (
    -- * Estimators

    {- | Fitted-model records with their @Fit@\/@Predict@ instances and the
    estimator classes (via "DataFrame.Model").
    -}
    module DataFrame.DecisionTree.Model,

    -- * Classifier configuration
    TreeConfig (..),
    defaultTreeConfig,
    SynthConfig (..),
    defaultSynthConfig,
    ColumnOrdering (..),
    orderable,
    defaultColumnOrdering,
    withOrdFrom,

    -- * Regressor configuration
    RegTreeConfig (..),
    defaultRegTreeConfig,

    -- * Fitted tree structure
    Tree (..),
    treeDepth,

    -- * Solver configuration (fills @TreeConfig.linearSolverConfig@)
    SolverConfig (..),
    defaultSolverConfig,
) where

import DataFrame.DecisionTree.Model
import DataFrame.DecisionTree.Regression (
    RegTreeConfig (..),
    defaultRegTreeConfig,
 )
import DataFrame.DecisionTree.Types (
    ColumnOrdering (..),
    SynthConfig (..),
    Tree (..),
    TreeConfig (..),
    defaultColumnOrdering,
    defaultSynthConfig,
    defaultTreeConfig,
    orderable,
    treeDepth,
    withOrdFrom,
 )
import DataFrame.LinearSolver (SolverConfig (..), defaultSolverConfig)
