{-# LANGUAGE InstanceSigs #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module DataFrame.Operations.Merge where

import qualified Data.List as L
import qualified Data.Text as T
import qualified DataFrame.Internal.Column as D
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Core as D

import Data.Maybe

{- | Vertically merge two dataframes using shared columns.
Columns that exist in only one dataframe are padded with Nothing.
-}
instance Semigroup D.DataFrame where
    (<>) :: D.DataFrame -> D.DataFrame -> D.DataFrame
    (<>) a b =
        let
            addColumns a' b' df name
                | snd (D.dimensions a') == 0 && snd (D.dimensions b') == 0 = df
                | snd (D.dimensions a') == 0 = fromMaybe df $ do
                    col <- D.getColumn name b'
                    pure $ D.insertColumn name col df
                | snd (D.dimensions b') == 0 = fromMaybe df $ do
                    col <- D.getColumn name a'
                    pure $ D.insertColumn name col df
                | otherwise =
                    let
                        numRowsA = fst $ D.dimensions a'
                        numRowsB = fst $ D.dimensions b'
                        sumRows = numRowsA + numRowsB

                        optA = D.getColumn name a'
                        optB = D.getColumn name b'
                     in
                        case optB of
                            Nothing -> case optA of
                                Nothing ->
                                    -- N.B. this case should never happen, because we're dealing with columns coming from
                                    -- union of column names of both dataframes. Nothing + Nothing would mean column
                                    -- wasn't in either dataframe, which shouldn't happen
                                    D.insertColumn name (D.fromList ([] :: [T.Text])) df
                                Just a'' ->
                                    D.insertColumn name (D.expandColumn sumRows a'') df
                            Just b'' -> case optA of
                                Nothing ->
                                    D.insertColumn name (D.leftExpandColumn sumRows b'') df
                                Just a'' ->
                                    let concatedColumns = D.concatColumnsEither a'' b''
                                     in D.insertColumn name concatedColumns df
            result = L.foldl' (addColumns a b) D.empty (D.columnNames a `L.union` D.columnNames b)
         in
            result
                { D.derivingExpressions = D.derivingExpressions a <> D.derivingExpressions b
                }

instance Monoid D.DataFrame where
    mempty = D.empty

-- | Add two dataframes side by side/horizontally.
(|||) :: D.DataFrame -> D.DataFrame -> D.DataFrame
(|||) a b =
    let result =
            D.fold
                (\name acc -> D.insertColumn name (D.unsafeGetColumn name b) acc)
                (D.columnNames b)
                a
     in result
            { D.derivingExpressions =
                D.derivingExpressions result <> D.derivingExpressions b
            }
