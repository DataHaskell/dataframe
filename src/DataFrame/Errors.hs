{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DataFrame.Errors where

import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU

import Control.Exception
import Data.Array
import qualified Data.List as L
import Data.Typeable (Typeable)
import DataFrame.Display.Terminal.Colours
import Type.Reflection (TypeRep)

data TypeErrorContext a b = MkTypeErrorContext
    { userType :: Either String (TypeRep a)
    , expectedType :: Either String (TypeRep b)
    , errorColumnName :: Maybe String
    , callingFunctionName :: Maybe String
    }

data DataFrameException where
    TypeMismatchException ::
        forall a b.
        (Typeable a, Typeable b) =>
        TypeErrorContext a b ->
        DataFrameException
    AggregatedAndNonAggregatedException :: T.Text -> T.Text -> DataFrameException
    ColumnsNotFoundException :: [T.Text] -> T.Text -> [T.Text] -> DataFrameException
    EmptyDataSetException :: T.Text -> DataFrameException
    InternalException :: T.Text -> DataFrameException
    NonColumnReferenceException :: T.Text -> DataFrameException
    UnaggregatedException :: T.Text -> DataFrameException
    WrongQuantileNumberException :: Int -> DataFrameException
    WrongQuantileIndexException :: VU.Vector Int -> Int -> DataFrameException
    deriving (Exception)

instance Show DataFrameException where
    show :: DataFrameException -> String
    show (TypeMismatchException context) =
        let
            errorString =
                typeMismatchError
                    (either id show (userType context))
                    (either id show (expectedType context))
         in
            addCallPointInfo
                (errorColumnName context)
                (callingFunctionName context)
                errorString
    show (ColumnsNotFoundException columnNames callPoint availableColumns) = columnsNotFound columnNames callPoint availableColumns
    show (EmptyDataSetException callPoint) = emptyDataSetError callPoint
    show (WrongQuantileNumberException q) = wrongQuantileNumberError q
    show (WrongQuantileIndexException qs q) = wrongQuantileIndexError qs q
    show (InternalException msg) = "Internal error: " ++ T.unpack msg
    show (NonColumnReferenceException msg) = "Expression must be a column reference in: " ++ T.unpack msg
    show (UnaggregatedException expr) = "Expression is not fully aggregated: " ++ T.unpack expr
    show (AggregatedAndNonAggregatedException expr1 expr2) =
        "Cannot combine aggregated and non-aggregated expressions: \n"
            ++ T.unpack expr1
            ++ "\n"
            ++ T.unpack expr2

columnNotFound :: T.Text -> T.Text -> [T.Text] -> String
columnNotFound missingColumn = columnsNotFound [missingColumn]

columnsNotFound :: [T.Text] -> T.Text -> [T.Text] -> String
columnsNotFound missingColumns callPoint availableColumns =
    red "\n\n[ERROR] "
        ++ missingColumnsLabel missingColumns
        ++ ": "
        ++ T.unpack (T.intercalate ", " missingColumns)
        ++ " for operation "
        ++ T.unpack callPoint
        ++ formatSuggestions missingColumns availableColumns
        ++ "\n\n"
  where
    missingColumnsLabel [_] = "Column not found"
    missingColumnsLabel _ = "Columns not found"

    formatSuggestions [missingColumn] columns =
        case guessColumnName missingColumn columns of
            "" -> ""
            guessed ->
                "\n\tDid you mean "
                    ++ T.unpack guessed
                    ++ "?"
    formatSuggestions names columns =
        case traverse (`suggestColumnName` columns) names of
            Just guessedColumns
                | not (null guessedColumns) ->
                    "\n\tDid you mean "
                        ++ formatColumnSuggestions guessedColumns
                        ++ "?"
            _ -> ""

    suggestColumnName missingColumn columns = case guessColumnName missingColumn columns of
        "" -> Nothing
        guessed -> Just guessed

    formatColumnSuggestions guessedColumns =
        "["
            ++ L.intercalate ", " (map (show . T.unpack) guessedColumns)
            ++ "]"

typeMismatchError :: String -> String -> String
typeMismatchError givenType expType =
    red $
        red "\n\n[Error]: Type Mismatch"
            ++ "\n\tWhile running your code I tried to "
            ++ "get a column of type: "
            ++ red (show givenType)
            ++ " but the column in the dataframe was actually of type: "
            ++ green (show expType)

emptyDataSetError :: T.Text -> String
emptyDataSetError callPoint =
    red "\n\n[ERROR] "
        ++ T.unpack callPoint
        ++ " cannot be called on empty data sets"

wrongQuantileNumberError :: Int -> String
wrongQuantileNumberError q =
    red "\n\n[ERROR] "
        ++ "Quantile number q should satisfy "
        ++ "q >= 2, but here q is "
        ++ show q

wrongQuantileIndexError :: VU.Vector Int -> Int -> String
wrongQuantileIndexError qs q =
    red "\n\n[ERROR] "
        ++ "For quantile number q, "
        ++ "each quantile index i "
        ++ "should satisfy 0 <= i <= q, "
        ++ "but here q is "
        ++ show q
        ++ " and indexes are "
        ++ show qs

addCallPointInfo :: Maybe String -> Maybe String -> String -> String
addCallPointInfo (Just name) (Just cp) err =
    err
        ++ ( "\n\tThis happened when calling function "
                ++ brightGreen cp
                ++ " on "
                ++ brightGreen name
           )
addCallPointInfo Nothing (Just cp) err =
    err
        ++ ( "\n\tThis happened when calling function "
                ++ brightGreen cp
           )
addCallPointInfo (Just name) Nothing err =
    err
        ++ ( "\n\tOn "
                ++ name
                ++ "\n\n"
           )
addCallPointInfo Nothing Nothing err = err

guessColumnName :: T.Text -> [T.Text] -> T.Text
guessColumnName userInput columns = case map (\k -> (editDistance userInput k, k)) columns of
    [] -> ""
    res -> (snd . minimum) res

editDistance :: T.Text -> T.Text -> Int
editDistance xs ys = table ! (m, n)
  where
    (m, n) = (T.length xs, T.length ys)
    x = array (1, m) (zip [1 ..] (T.unpack xs))
    y = array (1, n) (zip [1 ..] (T.unpack ys))

    table :: Array (Int, Int) Int
    table = array bnds [(ij, dist ij) | ij <- range bnds]
    bnds = ((0, 0), (m, n))

    dist (0, j) = j
    dist (i, 0) = i
    dist (i, j) =
        minimum
            [ table ! (i - 1, j) + 1
            , table ! (i, j - 1) + 1
            , if x ! i == y ! j then table ! (i - 1, j - 1) else 1 + table ! (i - 1, j - 1)
            ]
