{- | The one import an application needs for the @dataframe-learn@ estimators:
the @fit@\/@predict@ verbs, every model config and fitted-model record, and all
their 'Fit'\/'Predict' instances.

@
import DataFrame.Learn
...
fit defaultRegTreeConfig target df
@
-}
module DataFrame.Learn (
    module DataFrame.Model,
    module DataFrame.LinearModel,
    module DataFrame.SVM,
    module DataFrame.SVM.RFF,
    module DataFrame.PCA,
    module DataFrame.PCA.Kernel,
    module DataFrame.KMeans,
    module DataFrame.GMM,
    module DataFrame.DBSCAN,
    module DataFrame.Boosting,
    module DataFrame.SymbolicRegression,
    module DataFrame.Synthesis,
    module DataFrame.Segmented,
    module DataFrame.DecisionTree,
    module DataFrame.Metrics,
    module DataFrame.Metrics.Report,
    module DataFrame.ModelSelection,
    module DataFrame.Transform,
    module DataFrame.Transform.Serialize,
) where

import DataFrame.Boosting
import DataFrame.DBSCAN
import DataFrame.DecisionTree
import DataFrame.GMM
import DataFrame.KMeans
import DataFrame.LinearModel
import DataFrame.Metrics
import DataFrame.Metrics.Report
import DataFrame.Model
import DataFrame.ModelSelection
import DataFrame.PCA
import DataFrame.PCA.Kernel
import DataFrame.SVM
import DataFrame.SVM.RFF
import DataFrame.Segmented
import DataFrame.SymbolicRegression
import DataFrame.Synthesis
import DataFrame.Transform
import DataFrame.Transform.Serialize
