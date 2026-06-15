{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- |
Internal shared helpers used by both the terminal and web plot backends.
Not part of the public API.
-}
module DataFrame.Display.Internal.Common (
    -- * Aggregation
    Agg (..),
    aggLabel,
    aggregateByGroup,

    -- * Column extraction
    extractStringColumn,
    extractNumericColumn,
    columnToStrings,
    columnToDoubles,

    -- * Type guards
    isNumericColumn,
    isNumericColumnCheck,

    -- * Categorical helpers
    getCategoricalCounts,

    -- * Top-N rollup
    groupWithOther,
    groupWithOtherForPie,
) where

import qualified Data.Bifunctor
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)
import GHC.Stack (HasCallStack)
import Type.Reflection (TypeRep, typeRep)

import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    isNumeric,
    materializePacked,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), getColumn)
import DataFrame.Internal.Types

-- | Aggregation strategy for grouped/categorical plots.
data Agg
    = -- | Count rows per group; ignores the value column.
      Count
    | -- | Sum of value column per group.
      Sum
    | -- | Arithmetic mean of value column per group.
      Mean
    | -- | Median of value column per group.
      Median
    | -- | Minimum of value column per group.
      Min
    | -- | Maximum of value column per group.
      Max
    deriving (Eq, Show)

-- | Short label for an aggregation, used in auto-generated chart titles.
aggLabel :: Agg -> T.Text
aggLabel Count = "count"
aggLabel Sum = "sum"
aggLabel Mean = "mean"
aggLabel Median = "median"
aggLabel Min = "min"
aggLabel Max = "max"

{- | Apply an aggregation across rows grouped by the given category column.

For 'Count', the value column is ignored. For all other aggregations, the
value column is extracted as numeric and folded per group. Group order is
the order in which categories first appear.
-}
aggregateByGroup ::
    (HasCallStack) =>
    Agg ->
    -- | Grouping column (categorical).
    T.Text ->
    -- | Value column; required for everything except 'Count'.
    Maybe T.Text ->
    DataFrame ->
    [(T.Text, Double)]
aggregateByGroup Count groupCol _ df =
    let groups = extractStringColumn groupCol df
        m = L.foldl' (\acc g -> M.insertWith (+) g 1 acc) M.empty groups
        seen = L.nub groups
     in [(g, M.findWithDefault 0 g m) | g <- seen]
aggregateByGroup agg groupCol mValueCol df = case mValueCol of
    Nothing ->
        error $
            "Aggregation "
                ++ show agg
                ++ " requires a value column; only Count works without one."
    Just valueCol ->
        let groups = extractStringColumn groupCol df
            values = extractNumericColumn valueCol df
            seen = L.nub groups
            pairs = zip groups values
            byGroup =
                L.foldl'
                    (\acc (g, v) -> M.insertWith (flip (++)) g [v] acc)
                    M.empty
                    pairs
            reduce = numericReducer agg
         in [(g, reduce (M.findWithDefault [] g byGroup)) | g <- seen]

{- | The numeric reducers, broken out so the overlapping-Count case in
  'aggregateByGroup' can be dispatched at the head pattern without
  producing a redundant inner match.
-}
numericReducer :: Agg -> [Double] -> Double
numericReducer Sum = sum
numericReducer Mean = \vs -> if null vs then 0 else sum vs / fromIntegral (length vs)
numericReducer Median = medianD
numericReducer Min = minimum
numericReducer Max = maximum
numericReducer Count = fromIntegral . length

medianD :: [Double] -> Double
medianD [] = 0
medianD xs =
    let sorted = L.sort xs
        n = length sorted
        mid = n `div` 2
     in if even n
            then (sorted !! (mid - 1) + sorted !! mid) / 2
            else sorted !! mid

isNumericColumn :: DataFrame -> T.Text -> Bool
isNumericColumn df colName = maybe False isNumeric (getColumn colName df)

isNumericColumnCheck :: T.Text -> DataFrame -> Bool
isNumericColumnCheck colName df = isNumericColumn df colName

extractStringColumn :: (HasCallStack) => T.Text -> DataFrame -> [T.Text]
extractStringColumn colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx -> columnToStrings (columns df V.! idx)

extractNumericColumn :: (HasCallStack) => T.Text -> DataFrame -> [Double]
extractNumericColumn colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx -> columnToDoubles (columns df V.! idx)

-- | Render a column's values as strings (identity for @Text@, @show@ otherwise).
columnToStrings :: (HasCallStack) => Column -> [T.Text]
columnToStrings col = case col of
    BoxedColumn _ (vec :: V.Vector a) ->
        case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> V.toList vec
            Nothing -> V.toList $ V.map (T.pack . show) vec
    UnboxedColumn _ vec ->
        V.toList $ VG.map (T.pack . show) (VG.convert vec)
    PackedText _ _ -> columnToStrings (materializePacked col)

-- | Coerce a numeric column to @[Double]@; errors if the element type is not numeric.
columnToDoubles :: (HasCallStack) => Column -> [Double]
columnToDoubles col = case col of
    BoxedColumn _ vec -> vectorToDoubles vec
    UnboxedColumn _ vec -> unboxedVectorToDoubles vec
    PackedText _ _ -> columnToDoubles (materializePacked col)

vectorToDoubles :: forall a. (Columnable a, Show a) => V.Vector a -> [Double]
vectorToDoubles vec =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> V.toList vec
        Nothing -> case sIntegral @a of
            STrue -> V.toList $ V.map fromIntegral vec
            SFalse -> case sFloating @a of
                STrue -> V.toList $ V.map realToFrac vec
                SFalse ->
                    error $ "Column is not numeric (type: " ++ show (typeRep @a) ++ ")"

unboxedVectorToDoubles ::
    forall a. (Columnable a, VU.Unbox a, Show a) => VU.Vector a -> [Double]
unboxedVectorToDoubles vec =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> VU.toList vec
        Nothing -> case sIntegral @a of
            STrue -> VU.toList $ VU.map fromIntegral vec
            SFalse -> case sFloating @a of
                STrue -> VU.toList $ VU.map realToFrac vec
                SFalse ->
                    error $ "Column is not numeric (type: " ++ show (typeRep @a) ++ ")"

getCategoricalCounts ::
    (HasCallStack) => T.Text -> DataFrame -> Maybe [(T.Text, Double)]
getCategoricalCounts colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn _ (vec :: V.Vector a) ->
                        Just (countBoxed (typeRep @a) vec)
                    UnboxedColumn _ (vec :: VU.Vector a) ->
                        Just (countUnboxed (typeRep @a) vec)
                    PackedText _ _ -> case materializePacked col of
                        BoxedColumn _ (vec :: V.Vector a) -> Just (countBoxed (typeRep @a) vec)
                        _ -> Nothing
  where
    countBoxed ::
        forall a. (Show a) => TypeRep a -> V.Vector a -> [(T.Text, Double)]
    countBoxed tr vec
        | Just Refl <- testEquality tr (typeRep @T.Text) = toPairsText $ countValues vec
        | Just Refl <- testEquality tr (typeRep @String) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Integer) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Int) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Double) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Float) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Bool) = toPairs $ countValues vec
        | Just Refl <- testEquality tr (typeRep @Char) = toPairs $ countValues vec
        | otherwise = countByShow $ V.toList vec

    countUnboxed ::
        forall a. (Show a, VU.Unbox a) => TypeRep a -> VU.Vector a -> [(T.Text, Double)]
    countUnboxed tr vec
        | Just Refl <- testEquality tr (typeRep @Int) = toPairs $ countValuesUnboxed vec
        | Just Refl <- testEquality tr (typeRep @Double) =
            toPairs $ countValuesUnboxed vec
        | Just Refl <- testEquality tr (typeRep @Float) =
            toPairs $ countValuesUnboxed vec
        | Just Refl <- testEquality tr (typeRep @Bool) =
            toPairs $ countValuesUnboxed vec
        | Just Refl <- testEquality tr (typeRep @Char) =
            toPairs $ countValuesUnboxed vec
        | Just Refl <- testEquality tr (typeRep @Word8) =
            toPairs $ countValuesUnboxed vec
        | otherwise = countByShow $ VU.toList vec

    toPairs :: (Show a) => [(a, Int)] -> [(T.Text, Double)]
    toPairs = map (\(k, v) -> (T.pack (show k), fromIntegral v))

    toPairsText :: [(T.Text, Int)] -> [(T.Text, Double)]
    toPairsText = map (Data.Bifunctor.second fromIntegral)

    countValues :: (Ord a) => V.Vector a -> [(a, Int)]
    countValues vec = M.toList $ V.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

    countValuesUnboxed :: (Ord a, VU.Unbox a) => VU.Vector a -> [(a, Int)]
    countValuesUnboxed vec = M.toList $ VU.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

    countByShow :: (Show a) => [a] -> [(T.Text, Double)]
    countByShow xs =
        map (Data.Bifunctor.bimap T.pack fromIntegral) $
            M.toList $
                L.foldl' (\acc x -> M.insertWith (+) (show x) (1 :: Int) acc) M.empty xs

{- | Keep the top-N entries by value; fold the rest into a single "Other"
bucket. Used to limit bar/pie counts to a readable number.
-}
groupWithOther :: Int -> [(T.Text, Double)] -> [(T.Text, Double)]
groupWithOther n items =
    let sorted = L.sortOn (negate . snd) items
        (topN, rest) = splitAt n sorted
        otherSum = sum (map snd rest)
     in if null rest || otherSum == 0
            then topN
            else topN ++ [("Other (" <> T.pack (show (length rest)) <> " items)", otherSum)]

-- | Like 'groupWithOther' but annotates the "Other" bucket with its share %.
groupWithOtherForPie :: Int -> [(T.Text, Double)] -> [(T.Text, Double)]
groupWithOtherForPie n items =
    let total = sum (map snd items)
        sorted = L.sortOn (negate . snd) items
        (topN, rest) = splitAt n sorted
        otherSum = sum (map snd rest)
        otherPct = if total == 0 then 0 else round (100 * otherSum / total) :: Int
     in if null rest || otherSum == 0
            then topN
            else
                topN
                    ++ [
                           ( "Other ("
                                <> T.pack (show (length rest))
                                <> " items, "
                                <> T.pack (show otherPct)
                                <> "%)"
                           , otherSum
                           )
                       ]
