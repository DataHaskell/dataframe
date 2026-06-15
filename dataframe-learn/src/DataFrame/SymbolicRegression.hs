{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Symbolic regression by genetic programming (modelled on the
@symbolic-regression@ library, ported dependency-light: no e-graphs, no NLOPT).
'predict' is the best discovered @Expr Double@; the search also returns the
accuracy-vs-complexity Pareto front. Deterministic given the seed.
-}
module DataFrame.SymbolicRegression (
    UnOp (..),
    SRConfig (..),
    defaultSRConfig,
    SRModel (..),
) where

import Control.Exception (throw)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (featureNames, targetDoubles)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Model (Fit (..), Predict (..))
import DataFrame.Operations.Core (columnAsDoubleVector)
import DataFrame.Random (mkGen)
import DataFrame.SymbolicRegression.Expr (
    UnOp (..),
    allUnOps,
    toDataFrameExpr,
 )
import DataFrame.SymbolicRegression.GP (GPParams (..), runGP)
import DataFrame.SymbolicRegression.Simplify (simplify)

data SRConfig = SRConfig
    { srSeed :: !Int
    , srPopSize :: !Int
    , srGenerations :: !Int
    , srMaxSize :: !Int
    , srTournament :: !Int
    , srCrossoverP :: !Double
    , srMutationP :: !Double
    , srOptimizeP :: !Double
    , srParsimony :: !Double
    , srUnaryOps :: ![UnOp]
    }
    deriving (Eq, Show)

defaultSRConfig :: SRConfig
defaultSRConfig =
    SRConfig
        { srSeed = 42
        , srPopSize = 200
        , srGenerations = 40
        , srMaxSize = 25
        , srTournament = 5
        , srCrossoverP = 0.9
        , srMutationP = 0.3
        , srOptimizeP = 0.15
        , srParsimony = 1.0e-3
        , srUnaryOps = allUnOps
        }

{- | A fitted symbolic regressor. 'srBest' is the lowest-error expression;
'srPareto' is the @(complexity, mse, expr)@ frontier.
-}
data SRModel = SRModel
    { srBest :: !(Expr Double)
    , srBestMSE :: !Double
    , srPareto :: ![(Int, Double, Expr Double)]
    , srGenerationsRun :: !Int
    }

instance Fit SRConfig (Expr Double) SRModel where
    fit = fitSymbolicRegression

instance Predict SRModel Double where
    predict = srBest

-- | Search for an expression predicting @target@ from the other columns.
fitSymbolicRegression :: SRConfig -> Expr Double -> DataFrame -> SRModel
fitSymbolicRegression cfg target df =
    SRModel
        { srBest = translate best
        , srBestMSE = bestMse
        , srPareto = [(sz, mse, translate e) | (sz, mse, e) <- front]
        , srGenerationsRun = gens
        }
  where
    names = featureNames target df
    nameVec = V.fromList names
    cols = V.fromList (map (materialize df . Col) names)
    target' = targetDoubles target df
    n = VU.length target'
    params =
        GPParams
            { gpFeats = cols
            , gpN = n
            , gpTarget = target'
            , gpNVars = length names
            , gpUnOps = srUnaryOps cfg
            , gpPopSize = srPopSize cfg
            , gpGenerations = srGenerations cfg
            , gpMaxSize = srMaxSize cfg
            , gpTournament = srTournament cfg
            , gpCrossoverP = srCrossoverP cfg
            , gpMutationP = srMutationP cfg
            , gpOptimizeP = srOptimizeP cfg
            , gpParsimony = srParsimony cfg
            }
    (best, front, gens) = runGP params (mkGen (srSeed cfg))
    bestMse = case [m | (_, m, e) <- front, e == best] of
        (m : _) -> m
        [] -> 1 / 0
    translate = toDataFrameExpr nameVec . simplify

materialize :: DataFrame -> Expr Double -> VU.Vector Double
materialize df e = case columnAsDoubleVector e df of
    Right v -> v
    Left err -> throw err
