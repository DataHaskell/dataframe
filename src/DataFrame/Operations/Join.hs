{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Join where

import Control.Applicative (asum)
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..))
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D
import DataFrame.Operations.Aggregation as D
import DataFrame.Operations.Core as D
import Type.Reflection

-- | Equivalent to SQL join types.
data JoinType
    = INNER
    | LEFT
    | RIGHT
    | FULL_OUTER

    validateJoinColumns :: [T.Text] -> DataFrame -> DataFrame -> ()
validateJoinColumns cs df1 df2 =
    let
        df1Cols = D.columnNames df1
        df2Cols = D.columnNames df2

        missingInDf1 = filter (`notElem` df1Cols) cs
        missingInDf2 = filter (`notElem` df2Cols) cs

        missing = missingInDf1 ++ missingInDf2
     in
        if not (null missing)
            then error $ "Column not found: " <> show missing
            else ()

{- | Join two dataframes using SQL join semantics.

Only inner join is implemented for now.
-}
join ::
    JoinType ->
    [T.Text] ->
    DataFrame -> -- Right hand side
    DataFrame -> -- Left hand side
    DataFrame
join INNER xs right = innerJoin xs right
join LEFT xs right = leftJoin xs right
join RIGHT xs right = rightJoin xs right
join FULL_OUTER xs right = fullOuterJoin xs right

{- | Performs an inner join on two dataframes using the specified key columns.
Returns only rows where the key values exist in both dataframes.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.innerJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1
 K2   | A2  | B2

@
-}
innerJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoin cs right left =
    let
        _ = validateJoinColumns cs right left
        -- Prepare Keys for the Right DataFrame
        rightIndicesToGroup =
            [c | (k, c) <- M.toList (D.columnIndices right), k `elem` cs]

        rightRowRepresentations :: VU.Vector Int
        rightRowRepresentations = D.computeRowHashes rightIndicesToGroup right

        -- Build the Hash Map: Int -> Vector of Indices
        -- We use ifoldr to efficiently insert (index, key) without intermediate allocations.
        rightKeyMap :: HM.HashMap Int (VU.Vector Int)
        rightKeyMap =
            let accumulator =
                    VU.ifoldr
                        (\i key acc -> HM.insertWith (++) key [i] acc)
                        HM.empty
                        rightRowRepresentations
             in HM.map (VU.fromList . reverse) accumulator

        -- Prepare Keys for Left DataFrame
        leftIndicesToGroup =
            [c | (k, c) <- M.toList (D.columnIndices left), k `elem` cs]

        leftRowRepresentations :: VU.Vector Int
        leftRowRepresentations = D.computeRowHashes leftIndicesToGroup left

        -- Perform the Join
        (leftIndexChunks, rightIndexChunks) =
            VU.ifoldr
                ( \lIdx key (lAcc, rAcc) ->
                    case HM.lookup key rightKeyMap of
                        Nothing -> (lAcc, rAcc)
                        Just rIndices ->
                            let len = VU.length rIndices
                                -- Replicate the Left Index to match the number of Right matches
                                lChunk = VU.replicate len lIdx
                             in (lChunk : lAcc, rIndices : rAcc)
                )
                ([], [])
                leftRowRepresentations

        -- Flatten chunks
        expandedLeftIndicies = VU.concat leftIndexChunks
        expandedRightIndicies = VU.concat rightIndexChunks

        resultLen = VU.length expandedLeftIndicies

        -- Construct Result DataFrames
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesStable expandedLeftIndicies) (D.columns left)
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions left))
                }

        expandedRight =
            right
                { columns = VB.map (D.atIndicesStable expandedRightIndicies) (D.columns right)
                , dataframeDimensions = (resultLen, snd (D.dataframeDimensions right))
                }

        leftColumns = D.columnNames left
        rightColumns = D.columnNames right

        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            expandedLeft

{- | Performs a left join on two dataframes using the specified key columns.
Returns all rows from the left dataframe, with matching rows from the right dataframe.
Non-matching rows will have Nothing/null values for columns from the right dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.leftJoin ["key"] df other

------------------------
 key  |  A  |     B
------|-----|----------
 Text | Text| Maybe Text
------|-----|----------
 K0   | A0  | Just "B0"
 K1   | A1  | Just "B1"
 K2   | A2  | Just "B2"
 K3   | A3  | Nothing

@
-}
leftJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoin cs right left =
    let
        _ = validateJoinColumns cs right left
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = D.computeRowHashes leftIndicesToGroup left
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = D.computeRowHashes rightIndicesToGroup right
        rightKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed rightRowRepresentations)
        rightKeyCountsAndIndicesVec = M.map VU.fromList rightKeyCountsAndIndices
        leftRowCount = fst (D.dimensions left)
        pairs =
            [ (i, maybeRight)
            | i <- [0 .. leftRowCount - 1]
            , maybeRight <-
                case M.lookup (leftRowRepresentations VU.! i) rightKeyCountsAndIndicesVec of
                    Nothing -> [Nothing]
                    Just rVec -> map Just (VU.toList rVec)
            ]
        expandedLeftIndicies = VU.fromList (map fst pairs)
        expandedRightIndicies = VB.fromList (map snd pairs)
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesStable expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VU.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        expandedRight =
            right
                { columns = VB.map (D.atIndicesWithNulls expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VB.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            initDf

{- | Performs a right join on two dataframes using the specified key columns.
Returns all rows from the right dataframe, with matching rows from the left dataframe.
Non-matching rows will have Nothing/null values for columns from the left dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1"]), ("B", D.fromList ["B0", "B1"])]
ghci> D.rightJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1

@
-}
rightJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
rightJoin cs left right = leftJoin cs right left

fullOuterJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
fullOuterJoin cs right left =
    let
        _ = validateJoinColumns cs right left
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = D.computeRowHashes leftIndicesToGroup left
        leftKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed leftRowRepresentations)
        leftKeyCountsAndIndicesVec = M.map VU.fromList leftKeyCountsAndIndices
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = D.computeRowHashes rightIndicesToGroup right
        rightKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed rightRowRepresentations)
        rightKeyCountsAndIndicesVec = M.map VU.fromList rightKeyCountsAndIndices
        matchedPairs =
            concatMap
                ( \(lVec, rVec) ->
                    [ (Just lIdx, Just rIdx)
                    | lIdx <- VU.toList lVec
                    , rIdx <- VU.toList rVec
                    ]
                )
                ( M.elems
                    (M.intersectionWith (,) leftKeyCountsAndIndicesVec rightKeyCountsAndIndicesVec)
                )
        leftOnlyPairs =
            concatMap
                (map (\lIdx -> (Just lIdx, Nothing)) . VU.toList)
                (M.elems (leftKeyCountsAndIndicesVec `M.difference` rightKeyCountsAndIndicesVec))
        rightOnlyPairs =
            concatMap
                (map (\rIdx -> (Nothing, Just rIdx)) . VU.toList)
                (M.elems (rightKeyCountsAndIndicesVec `M.difference` leftKeyCountsAndIndicesVec))
        pairs = matchedPairs ++ leftOnlyPairs ++ rightOnlyPairs
        expandedLeftIndicies = VB.fromList (map fst pairs)
        expandedRightIndicies = VB.fromList (map snd pairs)
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesWithNulls expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VB.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        expandedRight =
            right
                { columns = VB.map (D.atIndicesWithNulls expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VB.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then case (D.unsafeGetColumn name expandedRight, D.unsafeGetColumn name expandedLeft) of
                        ( OptionalColumn (left :: VB.Vector (Maybe a))
                            , OptionalColumn (right :: VB.Vector (Maybe b))
                            ) -> case testEquality (typeRep @a) (typeRep @b) of
                                Nothing -> error "Cannot join columns of different types"
                                Just Refl ->
                                    D.insert
                                        name
                                        (VB.map (fromMaybe undefined) (VB.zipWith (\l r -> asum [l, r]) left right))
                                        df
                        _ -> error "Join should have optional keys."
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            ) -- ???
            rightColumns
            initDf
