{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Shared internal helpers used across the model fitters: turning a 'DataFrame'
plus a target/feature 'Expr' into the numeric matrices the algorithms consume,
and the common expression builders (affine score, arg-max/arg-min over named
scores) that every linear model and classifier would otherwise re-implement.
-}
module DataFrame.Featurize.Internal (
    -- * Supervised extraction
    featureNames,
    numericMatrix,
    targetDoubles,
    targetValues,

    -- * Unsupervised extraction
    Features (..),
    extractFeatures,
    columnExprName,
    materializeColumn,

    -- * Expression builders
    affineExpr,
    argMaxExpr,
    argMinExpr,
) where

import Control.Exception (throw)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame, columnNames)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (Matrix, transposeM)
import DataFrame.Operations.Core (columnAsDoubleVector, columnAsVector)
import DataFrame.Operators ((.&&.), (.*.), (.+.), (.<=.), (.>=.))

-- | Every column name except the supervised target's.
featureNames :: Expr a -> DataFrame -> [T.Text]
featureNames (Col target) df = filter (/= target) (columnNames df)
featureNames _ df = columnNames df

{- | The named columns as a row-major @n×d@ matrix of doubles (non-numeric
columns are coerced through 'columnAsDoubleVector'), paired with the names.
-}
numericMatrix :: [T.Text] -> DataFrame -> (V.Vector T.Text, Matrix)
numericMatrix names df = (V.fromList names, transposeM colMajor)
  where
    colMajor = V.fromList (map column names)
    column name = case columnAsDoubleVector (F.col @Double name) df of
        Right v -> v
        Left e -> throw (asFeatureError name e)

asFeatureError :: T.Text -> DataFrameException -> DataFrameException
asFeatureError name (TypeMismatchException ctx) =
    TypeMismatchException
        ctx
            { errorColumnName = Just (T.unpack name)
            , callingFunctionName =
                Just
                    "model fit (feature columns must be numeric Double; drop or encode non-numeric columns)"
            }
asFeatureError _ e = e

-- | The target column as a vector of doubles.
targetDoubles :: Expr Double -> DataFrame -> VU.Vector Double
targetDoubles expr df = case columnAsDoubleVector expr df of
    Right v -> v
    Left e -> throw e

-- | The target column as a vector of its own type (for classifiers).
targetValues :: (Columnable a) => Expr a -> DataFrame -> V.Vector a
targetValues expr df = case columnAsVector expr df of
    Right v -> v
    Left e -> throw e

{- | The extracted feature columns of an unsupervised fit, in the shapes the
algorithms need: names, column-major vectors, the row-major matrix, and the
@(n, d)@ dimensions.
-}
data Features = Features
    { ftNames :: ![T.Text]
    , ftCols :: ![VU.Vector Double]
    , ftRows :: !Matrix
    , ftN :: !Int
    , ftD :: !Int
    }

-- | Extract the given feature columns once, in every shape the fitters use.
extractFeatures :: [Expr Double] -> DataFrame -> Features
extractFeatures features df = Features names cols rows n d
  where
    names = map columnExprName features
    cols = map (materializeColumn df) features
    n = case cols of
        (x : _) -> VU.length x
        _ -> 0
    d = length cols
    rows = V.generate n (\i -> VU.generate d (\j -> (cols !! j) VU.! i))

-- | The column name behind a @Col@ feature expression.
columnExprName :: Expr Double -> T.Text
columnExprName (Col n) = n
columnExprName e = error ("expected a column expression, got " ++ show e)

-- | Interpret a @Col@ (or numeric) expression to a @Double@ vector.
materializeColumn :: DataFrame -> Expr Double -> VU.Vector Double
materializeColumn df e = case columnAsDoubleVector e df of
    Right v -> v
    Left err -> throw err

{- | An affine score @b + Σ wⱼ·colⱼ@ over named columns, dropping zero weights
(the shared core of linear/logistic/SVM margins).
-}
affineExpr :: Double -> [(Double, T.Text)] -> Expr Double
affineExpr b terms =
    foldr
        (.+.)
        (F.lit b)
        [F.lit w .*. (Col n :: Expr Double) | (w, n) <- terms, w /= 0]

{- | The class whose score is greatest, as a nested-@If@ expression; ties go to
the earlier class.
-}
argMaxExpr :: (Columnable a) => [(a, Expr Double)] -> Expr a
argMaxExpr = argExtreme (.>=.)

-- | The class whose score is smallest (e.g. nearest cluster by distance).
argMinExpr :: (Columnable a) => [(a, Expr Double)] -> Expr a
argMinExpr = argExtreme (.<=.)

argExtreme ::
    (Columnable a) =>
    (Expr Double -> Expr Double -> Expr Bool) -> [(a, Expr Double)] -> Expr a
argExtreme _ [] = error "argExtreme: no classes"
argExtreme _ [(c, _)] = Lit c
argExtreme cmp ((c, sc) : rest) =
    If
        (foldr ((\o acc -> cmp sc o .&&. acc) . snd) (F.lit True) rest)
        (Lit c)
        (argExtreme cmp rest)
