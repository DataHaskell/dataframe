{- | Linear models for the dataframe ecosystem: regression (OLS, ridge, lasso,
elastic net) and one-vs-rest logistic classification. Re-exports the focused
submodules.
-}
module DataFrame.LinearModel (
    module DataFrame.LinearModel.Regression,
    module DataFrame.LinearModel.Logistic,
    -- Re-exported from the internal solver so the public configs' and records'
    -- @SolverConfig@\/@LinearModel@ fields stay usable.
    SolverConfig (..),
    defaultSolverConfig,
    LinearModel (..),
) where

import DataFrame.LinearModel.Logistic
import DataFrame.LinearModel.Regression
import DataFrame.LinearSolver (
    LinearModel (..),
    SolverConfig (..),
    defaultSolverConfig,
 )
