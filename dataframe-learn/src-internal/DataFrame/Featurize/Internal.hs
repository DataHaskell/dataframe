{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Shared internal helpers used across the model fitters: turning a 'DataFrame'
plus a target/feature 'Expr' into the numeric matrices the algorithms consume,
plus common expression builders (affine score, arg-max/arg-min over scores).
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
import Data.Maybe (isJust)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (TypeRep, typeRep)

import Data.Either (fromRight)
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable, columnBitmap, columnTypeString)
import qualified DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame (DataFrame, columnNames, getColumn)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (Matrix, transposeM)
import DataFrame.Operations.Core (
    columnAsDoubleVector,
    columnAsUnboxedVector,
    columnAsVector,
 )
import DataFrame.Operators ((.&&.), (.*.), (.+.), (.<=.), (.>=.))

-- | Every column name except the supervised target's.
featureNames :: Expr a -> DataFrame -> [T.Text]
featureNames (Col target) df = filter (/= target) (columnNames df)
featureNames _ df = columnNames df

{- | The named columns as a row-major @n×d@ matrix, paired with the names. Every
column must already be stored as non-null 'Double'; a nullable ('Maybe Double')
or non-'Double' column is a fit-time error rather than a silent coercion.
-}
numericMatrix :: [T.Text] -> DataFrame -> (V.Vector T.Text, Matrix)
numericMatrix names df = (V.fromList names, transposeM colMajor)
  where
    colMajor = V.fromList (map (`requireDoubleColumn` df) names)

{- | Read a feature\/target column strictly as a non-null 'Double' vector. A
nullable ('Maybe Double') column, or a column of any other type, is a fit-time
error naming the column and pointing at the fix.
-}
requireDoubleColumn :: T.Text -> DataFrame -> VU.Vector Double
requireDoubleColumn name df = case getColumn name df of
    Nothing ->
        throw
            ( TypeMismatchException
                ( MkTypeErrorContext
                    (Right (typeRep @Double))
                    (Left "missing" :: Either String (TypeRep Double))
                    (Just (T.unpack name))
                    Nothing
                )
            )
    Just c ->
        if isJust (columnBitmap c)
            then throw (nullableInputError name (columnTypeString c))
            else
                if D.hasElemType @Double c
                    then fromRight undefined (columnAsUnboxedVector (F.col @Double name) df)
                    else
                        throw
                            ( asDoubleInputError
                                name
                                ( TypeMismatchException
                                    ( MkTypeErrorContext
                                        (Right (typeRep @Double))
                                        (Left (columnTypeString c) :: Either String (TypeRep Double))
                                        (Just (T.unpack name))
                                        Nothing
                                    )
                                )
                            )

-- | Actionable error for a non-'Double' input column (wraps the type mismatch).
asDoubleInputError :: T.Text -> DataFrameException -> DataFrameException
asDoubleInputError name (TypeMismatchException ctx) =
    TypeMismatchException
        ctx
            { errorColumnName = Just (T.unpack name)
            , callingFunctionName =
                Just
                    "model fit (input columns must be Double; convert with toDouble or build the column as Double)"
            }
asDoubleInputError _ e = e

-- | Actionable error for a nullable ('Maybe Double') input column.
nullableInputError :: T.Text -> String -> DataFrameException
nullableInputError name found =
    TypeMismatchException
        ( MkTypeErrorContext
            { userType = Right (typeRep @Double)
            , expectedType = Left found
            , errorColumnName = Just (T.unpack name)
            , callingFunctionName =
                Just
                    "model fit (input columns must be non-null Double; drop or impute missing values before fitting)"
            } ::
            TypeErrorContext Double ()
        )

{- | The target column as a non-null 'Double' vector. A nullable or non-'Double'
target is a fit-time error, not a coercion.
-}
targetDoubles :: Expr Double -> DataFrame -> VU.Vector Double
targetDoubles (Col name) df = requireDoubleColumn name df
targetDoubles expr df = case columnAsUnboxedVector expr df of
    Right v -> v
    Left e -> throw (asDoubleInputError (T.pack "<target>") e)

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
columnExprName e =
    throw (NonColumnReferenceException ("columnExprName: " <> T.pack (show e)))

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
argExtreme _ [] = throw (EmptyDataSetException "argExtreme")
argExtreme _ [(c, _)] = Lit c
argExtreme cmp ((c, sc) : rest) =
    If
        (foldr ((\o acc -> cmp sc o .&&. acc) . snd) (F.lit True) rest)
        (Lit c)
        (argExtreme cmp rest)
