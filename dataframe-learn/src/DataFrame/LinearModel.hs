{- | Linear models for the dataframe ecosystem: regression (OLS, ridge, lasso,
elastic net) and one-vs-rest logistic classification. Re-exports the focused
submodules.
-}
module DataFrame.LinearModel (
    module DataFrame.LinearModel.Regression,
    module DataFrame.LinearModel.Logistic,
) where

import DataFrame.LinearModel.Logistic
import DataFrame.LinearModel.Regression
