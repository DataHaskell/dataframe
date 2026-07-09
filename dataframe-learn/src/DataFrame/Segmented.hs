{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

{- | Segment a frame by its categorical (Text) columns and fit a separate base
model per value-combination, instead of rejecting the categoricals. A row is
routed to its segment's model at predict time via a nested-@If@ expression keyed
on @column == value@; combinations unseen in training, rows with a missing
categorical key, and segments too small to fit fall through to a global model fit
on all rows.

'Segmented' wraps any base estimator whose prediction is an @Expr a@ (linear,
logistic, symbolic regression). 'fit' returns a 'SegmentedModel'; 'predict' is the
compiled routing expression over the raw columns.

Optional partial pooling (@segPool = λ@, linear base only) shrinks each segment's
coefficients toward a random-effects reference — the @n_g@-weighted mean of the
per-segment fits — so small segments borrow strength from the rest of the data.
@λ = 0@ is independent per-segment fitting; @λ > 0@ on a non-linear base is a
fit-time error.
-}
module DataFrame.Segmented (
    module DataFrame.Model,
    Segmented (..),
    segmented,
    segmentOn,
    pooled,
    Segment (..),
    SegmentedModel (..),
    SegmentFit (..),
) where

import Data.List (foldl', (\\))
import qualified Data.Map.Strict as M
import Data.Maybe (isJust)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (featureNames, numericMatrix, targetDoubles)
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    bitmapTestBit,
    columnBitmap,
    hasElemType,
 )
import DataFrame.Internal.DataFrame (DataFrame, unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Types (SBool (..), sIntegral)
import DataFrame.LinearAlgebra (Matrix, gram, matVec, tMatVec)
import DataFrame.LinearAlgebra.Solve (choleskySolve, qrLeastSquares)
import DataFrame.LinearModel.Logistic (LogisticConfig)
import DataFrame.LinearModel.Regression (
    LinearConfig (..),
    LinearRegressor (..),
 )
import DataFrame.Model
import DataFrame.Operations.Core (nRows)
import DataFrame.Operations.Subset (columnToTextVec, exclude, rowsAtIndices)
import DataFrame.Operators ((.&&.), (.==.))
import DataFrame.SymbolicRegression (SRConfig)

{- | A base estimator @cfg@ wrapped to fit one model per categorical
value-combination. @segOn@ chooses the columns to segment on ('Nothing' =
auto-detect Text columns under @segMaxCard@); @segMinRows@ (floored at
@features + 1@) is the smallest segment that gets its own model; @segPool@ is the
pooling strength @λ@ in standardized units.
-}
data Segmented cfg = Segmented
    { segBase :: !cfg
    , segOn :: !(Maybe [T.Text])
    , segMaxCard :: !Int
    , segMinRows :: !Int
    , segPool :: !Double
    }
    deriving (Eq, Show)

-- | Wrap a base config with the defaults: auto-detect, cap 32, min 30 rows, no pooling.
segmented :: cfg -> Segmented cfg
segmented base = Segmented base Nothing 32 30 0

-- | Segment only on the named columns (each must be Text), overriding auto-detect.
segmentOn :: Segmented cfg -> [T.Text] -> Segmented cfg
segmentOn s cols = s{segOn = Just cols}

-- | Set the pooling strength @λ@ (shrink segments toward the reference).
pooled :: Segmented cfg -> Double -> Segmented cfg
pooled s lam = s{segPool = lam}

-- | One fitted segment: its categorical key, row count, and base model.
data Segment model = Segment
    { segKey :: ![T.Text]
    , segN :: !Int
    , segModel :: !model
    }
    deriving (Show)

{- | A fitted segmented model: the columns segmented on, the per-segment models
(ascending key order), the observed combinations that fell back (key + row
count), the global fallback model, and the compiled routing expression.
-}
data SegmentedModel a model = SegmentedModel
    { smCatCols :: ![T.Text]
    , smSegments :: ![Segment model]
    , smFellBack :: ![([T.Text], Int)]
    , smFallback :: !model
    , smExpr :: !(Expr a)
    }
    deriving (Show)

{- | How a base estimator fits its per-segment models under pooling strength @λ@.
The default fits each segment independently and rejects @λ > 0@; the linear
instance overrides it with closed-form shrinkage toward a random-effects
reference. Every base model used with 'Segmented' needs an instance (a one-line
empty instance suffices to inherit the independent default).
-}
class (Fit cfg (Expr a)) => SegmentFit cfg a where
    -- | Fit the qualifying segments (each a numeric-only frame, in order).
    fitSegments :: cfg -> Double -> Expr a -> [DataFrame] -> [ModelOf cfg (Expr a)]
    fitSegments cfg lam target dfs
        | lam == 0 = map (fit cfg target) dfs
        | otherwise =
            error
                "Segmented: pooling (lambda > 0) is not supported for this base model; use lambda = 0 or a linear base."

-- | Logistic segments support independent fitting (lambda = 0) only, for now.
instance (Columnable a, Ord a) => SegmentFit LogisticConfig a

-- | Symbolic-regression segments support independent fitting (lambda = 0) only.
instance SegmentFit SRConfig Double

instance
    ( Fit cfg (Expr a)
    , SegmentFit cfg a
    , Predict (ModelOf cfg (Expr a))
    , Prediction (ModelOf cfg (Expr a)) ~ Expr a
    , Columnable a
    ) =>
    Fit (Segmented cfg) (Expr a)
    where
    type ModelOf (Segmented cfg) (Expr a) = SegmentedModel a (ModelOf cfg (Expr a))
    type FrameReq (Segmented cfg) (Expr a) = 'AnyFrame
    fit = fitSegmented

instance Predict (SegmentedModel a model) where
    type Prediction (SegmentedModel a model) = Expr a
    predict = smExpr

fitSegmented ::
    forall cfg a.
    ( Fit cfg (Expr a)
    , SegmentFit cfg a
    , Predict (ModelOf cfg (Expr a))
    , Prediction (ModelOf cfg (Expr a)) ~ Expr a
    , Columnable a
    ) =>
    Segmented cfg ->
    Expr a ->
    DataFrame ->
    SegmentedModel a (ModelOf cfg (Expr a))
fitSegmented (Segmented base mcols maxCard minRows lam) target df =
    seq (guardNumeric df textCols target) result
  where
    mTarget = case target of
        Col n -> Just n
        _ -> Nothing
    feats = featureNames target df
    -- Every Text feature is dropped from the numeric design; the chosen subset
    -- (catCols) additionally drives the partitioning.
    textCols = [c | c <- feats, isTextCol df c]
    catCols = resolveCatCols df mTarget textCols mcols maxCard
    numericFrame = exclude textCols
    globalM = fit base target (numericFrame df)
    result
        | null catCols =
            SegmentedModel [] [] [] globalM (predict globalM)
        | otherwise =
            let d = length (feats \\ textCols)
                floor' = max minRows (d + 1)
                grouped = groupByKey df catCols
                (qualifying, undersized) =
                    span' (\(_, ixs) -> VU.length ixs >= floor') grouped
                qualFrames =
                    [numericFrame (rowsAtIndices ixs df) | (_, ixs) <- qualifying]
                segModels = fitSegments base lam target qualFrames
                segments =
                    zipWith
                        (\(k, ixs) m -> Segment k (VU.length ixs) m)
                        qualifying
                        segModels
                fellBack = [(k, VU.length ixs) | (k, ixs) <- undersized]
                expr = buildExpr catCols segments globalM
             in SegmentedModel catCols segments fellBack globalM expr

-- | 'span' over a predicate that need not hold contiguously (a filter partition).
span' :: (b -> Bool) -> [b] -> ([b], [b])
span' p xs = (filter p xs, filter (not . p) xs)

{- | Compile the routing: a right-folded @If@ ladder of @key == value@ conjuncts,
ending in the fallback model's prediction. Keys are disjoint, so order is
immaterial.
-}
buildExpr ::
    (Columnable a, Predict model, Prediction model ~ Expr a) =>
    [T.Text] ->
    [Segment model] ->
    model ->
    Expr a
buildExpr catCols segs fallback =
    foldr
        (\(Segment key _ m) acc -> If (keyCond catCols key) (predict m) acc)
        (predict fallback)
        segs

-- | @col1 == v1 && col2 == v2 && ...@ for a segment's key.
keyCond :: [T.Text] -> [T.Text] -> Expr Bool
keyCond catCols vals =
    foldr1 (.&&.) [(Col c :: Expr T.Text) .==. Lit v | (c, v) <- zip catCols vals]

{- | The Text feature columns to segment on, chosen from the frame's Text features
@textCols@. An explicit list is validated to be all-Text; auto-detect keeps Text
columns with at most @maxCard@ distinct values.
-}
resolveCatCols ::
    DataFrame -> Maybe T.Text -> [T.Text] -> Maybe [T.Text] -> Int -> [T.Text]
resolveCatCols df mTarget textCols mcols maxCard = case mcols of
    Just cols ->
        let bad = filter (\c -> not (isTextCol df c) || Just c == mTarget) cols
         in if null bad
                then cols
                else
                    error
                        ( "Segmented: segmentOn columns must be Text features (not the target); invalid: "
                            ++ show bad
                        )
    Nothing -> [c | c <- textCols, distinctCount df c <= maxCard]

isTextCol :: DataFrame -> T.Text -> Bool
isTextCol df c = hasElemType @T.Text (unsafeGetColumn c df)

distinctCount :: DataFrame -> T.Text -> Int
distinctCount df c =
    Set.size (Set.fromList (V.toList (columnToTextVec (unsafeGetColumn c df))))

{- | Reject feature columns that are neither Text (dropped/segmented) nor non-null
'Double', naming each with its fix — clearer than the base fitter's raw type
mismatch, and steering the user to model their types well.
-}
guardNumeric :: DataFrame -> [T.Text] -> Expr a -> ()
guardNumeric df textCols target =
    case problems of
        [] -> ()
        ps ->
            error
                ( "Segmented: unusable feature column(s):\n"
                    ++ unlines (map fmt ps)
                )
  where
    problems =
        [ (c, r)
        | c <- featureNames target df \\ textCols
        , Just r <- [reason (unsafeGetColumn c df)]
        ]
    fmt (c, r) = "  " ++ T.unpack c ++ ": " ++ r
    reason col
        | isJust (columnBitmap col) =
            Just
                "has missing values — drop them (filterJust / filterAllJust) or model missingness explicitly; imputing risks train/inference skew"
        | isIntegralCol col =
            Just "is an integer column — cast to Double with F.toDouble"
        | not (hasElemType @Double col) =
            Just
                "is not Double — convert to Double (numeric) or segment on it (categorical)"
        | otherwise = Nothing
    isIntegralCol col = case col of
        UnboxedColumn _ (_ :: VU.Vector b) -> case sIntegral @b of
            STrue -> True
            _ -> False
        _ -> False

{- | Group row indices by their composite categorical key, dropping rows whose key
has a null in any segmented column (served by the fallback). Ascending key order.
-}
groupByKey :: DataFrame -> [T.Text] -> [([T.Text], VU.Vector Int)]
groupByKey df catCols =
    map (\(k, is) -> (k, VU.fromList (reverse is))) (M.toAscList grouped)
  where
    n = nRows df
    cols = map (`unsafeGetColumn` df) catCols
    textVecs = map columnToTextVec cols
    bitmaps = map columnBitmap cols
    validRow i = all (maybe True (`bitmapTestBit` i)) bitmaps
    keyOf i = [tv V.! i | tv <- textVecs]
    grouped =
        foldl'
            (\m i -> if validRow i then M.insertWith (++) (keyOf i) [i] m else m)
            M.empty
            [0 .. n - 1]

{- | Linear segments with exact closed-form pooling. @λ = 0@ is independent OLS
(identical to the base fit); @λ > 0@ standardizes the design, shrinks each
segment's coefficients toward the @n_g@-weighted mean of the per-segment fits, and
maps back to raw coefficient space.
-}
instance SegmentFit LinearConfig Double where
    fitSegments cfg lam target dfs
        | lam == 0 = map (fit cfg target) dfs
        | null dfs = []
        | otherwise = shrinkLinear cfg lam target dfs

shrinkLinear ::
    LinearConfig -> Double -> Expr Double -> [DataFrame] -> [LinearRegressor]
shrinkLinear cfg lam target dfs = map toReg dsegs
  where
    names = featureNames target (head dfs)
    d = length names
    mats = [snd (numericMatrix names dframe) | dframe <- dfs]
    ys = [targetDoubles target dframe | dframe <- dfs]
    ns = map V.length mats
    -- Standardization stats over the pooled qualifying rows.
    pooledRows = V.concat mats
    nP = V.length pooledRows
    means =
        VU.generate d $ \j ->
            sum [(pooledRows V.! i) VU.! j | i <- [0 .. nP - 1]] / fromIntegral nP
    sds =
        VU.generate d $ \j ->
            let m = means VU.! j
                var =
                    sum [sq ((pooledRows V.! i) VU.! j - m) | i <- [0 .. nP - 1]]
                        / fromIntegral nP
             in sqrt var
    stdValue j x = let s = sds VU.! j in if s == 0 then 0 else (x - means VU.! j) / s
    augStd row = VU.cons 1 (VU.imap stdValue row)
    zMats = [V.map augStd m | m <- mats]
    -- First pass: independent standardized OLS per segment.
    olsAll = zipWith fitOLS zMats ys
    fitOLS z y = either (const Nothing) Just (qrLeastSquares z y)
    good = [(n, sol) | (n, Just sol) <- zip ns olsAll]
    totW = fromIntegral (sum (map fst good)) :: Double
    dref
        | null good = VU.replicate (d + 1) 0
        | otherwise =
            VU.generate (d + 1) $ \k ->
                sum [fromIntegral n * (sol VU.! k) | (n, sol) <- good] / totW
    -- Second pass: ridge of the residual toward the reference, penalizing all
    -- parameters; delta_seg = dref + eta.
    dsegs = zipWith solveSeg zMats ys
    solveSeg z y =
        let r = VU.zipWith (-) y (matVec z dref)
            a = addDiagonal lam (gram z)
            rhs = tMatVec z r
         in case choleskySolve a rhs of
                Just eta -> VU.zipWith (+) dref eta
                Nothing -> dref
    toReg dseg =
        let bStd = dseg VU.! 0
            wStd = VU.drop 1 dseg
            rawCoef = VU.imap (\j w -> let s = sds VU.! j in if s == 0 then 0 else w / s) wStd
            adj =
                sum
                    [ let s = sds VU.! j
                       in if s == 0 then 0 else (wStd VU.! j) * (means VU.! j) / s
                    | j <- [0 .. d - 1]
                    ]
         in LinearRegressor rawCoef (bStd - adj) (V.fromList names) (lcPenalty cfg)

sq :: Double -> Double
sq x = x * x

-- | Add @lam@ to the diagonal of a square matrix.
addDiagonal :: Double -> Matrix -> Matrix
addDiagonal lam = V.imap (\i row -> row VU.// [(i, (row VU.! i) + lam)])
