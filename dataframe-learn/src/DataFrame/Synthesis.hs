{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Feature synthesis by bottom-up enumerative search with observational
equivalence — the canonical enumerative method from Solar-Lezama's
/Introduction to Program Synthesis/, hardened for a numeric, examples-only
setting.

Given a frame and a numeric target column, it searches for a small, interpretable
arithmetic expression over the other columns whose values track the target. The
specification is purely the example rows; there is no SMT solver and no logical
spec. Deterministic and pure.

The engine:

  * enumerates programs by increasing AST size (so the first representative of any
    behaviour is the smallest — interpretability for free);
  * evaluates each candidate /incrementally/ by combining the cached result
    vectors of its subprograms (one vector op), never re-interpreting the whole
    tree;
  * keeps exactly one program per /observational-equivalence/ class — candidates
    producing the same column (up to a float tolerance) are interchangeable, so
    duplicates are dropped rather than re-explored;
  * breaks commutative symmetry (never both @a+b@ and @b+a@) and uses protected
    operators (@sqrt|x|@, @log(|x|+1)@) plus a denominator guard so domain errors
    never arise;
  * caps each size layer by fit score when it grows large (a cost-guided
    tractability bound over /distinct/ behaviours, not a lossy beam over raw
    syntax).

'fit' returns the best 'SynthesizedFeature'; 'predict' is its expression.
'synthesizeFeatures' returns the whole ranked, deduplicated feature bank — useful
as automated feature engineering feeding a downstream model.

Deferred (documented next steps, not yet implemented): skeleton enumeration with
closed-form least-squares coefficient fitting, hard-row counterexample sampling
for very large frames, and piecewise (condition-abduction) features.
-}
module DataFrame.Synthesis (
    module DataFrame.Model,
    LossFunction (..),
    SynthesisConfig (..),
    defaultSynthesisConfig,
    SynthesizedFeature (..),
    synthesizeFeatures,
) where

import Data.Bits (xor)
import Data.Either (fromRight)
import Data.List (sortBy)
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.Ord (Down (..), comparing)
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word64)
import GHC.Float (castDoubleToWord64)

import DataFrame.Featurize.Internal (featureNames)
import qualified DataFrame.Functions as F
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Statistics (
    meanSquaredError,
    mutualInformationBinned,
    percentile',
    variance',
 )
import DataFrame.Model
import DataFrame.Operations.Core (columnAsDoubleVector)

-- | How a candidate's output column is scored against the target (higher is better).
data LossFunction
    = -- | Pearson @r²@: scale-invariant, the default for derived features.
      PearsonCorrelation
    | -- | Binned mutual information: captures nonlinear association.
      MutualInformation
    | -- | Negative mean squared error: for reproducing a target exactly.
      MeanSquaredError
    deriving (Eq, Show)

-- | Search hyperparameters.
data SynthesisConfig = SynthesisConfig
    { synMaxSize :: !Int
    -- ^ Largest AST (node count) to enumerate.
    , synBankCap :: !Int
    -- ^ Max observationally-distinct programs kept per size layer.
    , synLoss :: !LossFunction
    , synTopK :: !Int
    -- ^ How many ranked features to return in the bank.
    }
    deriving (Eq, Show)

defaultSynthesisConfig :: SynthesisConfig
defaultSynthesisConfig =
    SynthesisConfig
        { synMaxSize = 6
        , synBankCap = 500
        , synLoss = PearsonCorrelation
        , synTopK = 16
        }

{- | A synthesized feature. 'sfExpr' is the best-scoring expression and 'sfFeatures'
is the ranked, observationally-distinct bank (expression and its score).
-}
data SynthesizedFeature = SynthesizedFeature
    { sfExpr :: !(Expr Double)
    , sfScore :: !Double
    , sfFeatures :: ![(Expr Double, Double)]
    }

instance Fit SynthesisConfig (Expr Double) SynthesizedFeature where
    fit = synthesizeFeatures

instance Predict SynthesizedFeature Double where
    predict = sfExpr

-- | A candidate's evaluated column over the example rows.
type Output = VU.Vector Double

data Prog = Prog
    { progExpr :: !(Expr Double)
    , progSize :: !Int
    , progOut :: !Output
    }

-- | Search for expressions over the non-target columns that track @target@.
synthesizeFeatures ::
    SynthesisConfig -> Expr Double -> DataFrame -> SynthesizedFeature
synthesizeFeatures cfg target df
    | null leaves || VU.null tgt = SynthesizedFeature (Lit 0) (negate (1 / 0)) []
    | otherwise = SynthesizedFeature best bestScore ranked
  where
    feats = featureNames target df
    tgt = fromRight VU.empty (columnAsDoubleVector target df)
    n = VU.length tgt
    leaves = mkLeaves df feats n
    bank = grow cfg tgt leaves
    scored =
        [ (progExpr p, progSize p, s)
        | p <- bank
        , Just s <- [scoreOf (synLoss cfg) tgt (progOut p)]
        ]
    sorted = sortBy (comparing (\(_, sz, s) -> (Down s, sz))) scored
    ranked = [(e, s) | (e, _, s) <- take (synTopK cfg) sorted]
    (best, bestScore) = case ranked of
        ((e, s) : _) -> (e, s)
        [] -> (Lit 0, negate (1 / 0))

-- | Size-1 programs: numeric feature columns and a pool of constants, OE-deduped.
mkLeaves :: DataFrame -> [T.Text] -> Int -> [Prog]
mkLeaves df feats n = fst (dedupProgs M.empty candidates)
  where
    candidates =
        [ Prog (Col name) 1 o
        | name <- feats
        , Right o <- [columnAsDoubleVector (Col name :: Expr Double) df]
        ]
            ++ [Prog (Lit v) 1 (VU.replicate n v) | v <- constantPool df feats]

{- | Domain-informed constants: per-column quartiles, variance, and std, plus a few
small integers. (Duplicates collapse under observational equivalence.)
-}
constantPool :: DataFrame -> [T.Text] -> [Double]
constantPool df feats =
    [0, 1, 2, -1]
        ++ [ roundSig 2 v
           | name <- feats
           , Right c <- [columnAsDoubleVector (Col name :: Expr Double) df]
           , v <-
                [percentile' p c | p <- [1, 25, 75, 99]] ++ [variance' c, sqrt (variance' c)]
           ]

-- | Grow the bank one size layer at a time, keeping one program per OE class.
grow :: SynthesisConfig -> Output -> [Prog] -> [Prog]
grow cfg tgt leaves = go 2 leaves (foldr (seenInsert . progOut) M.empty leaves)
  where
    go size bank seen
        | size > synMaxSize cfg = bank
        | otherwise =
            let (kept, seen') = absorb cfg tgt seen (layer size bank)
             in go (size + 1) (bank ++ kept) seen'

-- | All candidate programs of exactly @size@ nodes, built from smaller ones.
layer :: Int -> [Prog] -> [Prog]
layer size bank = unaries ++ pows ++ comms ++ subs ++ divs
  where
    atSize s = filter ((== s) . progSize) bank
    args1 = atSize (size - 1)
    unaries =
        [ Prog (mk e) size (VU.map f o)
        | (mk, f) <- unaryProds
        , Prog e _ o <- args1
        ]
    pows =
        [ Prog (F.pow e k) size (VU.map (^ k) o)
        | Prog e _ o <- args1
        , k <- [2 .. 6 :: Int]
        ]
    comms =
        [ Prog (mk ea eb) size (VU.zipWith f oa ob)
        | (mk, f) <- commutativeProds
        , (Prog ea _ oa, Prog eb _ ob) <- unorderedPairs size bank
        ]
    subs =
        [ Prog (ea - eb) size (VU.zipWith (-) oa ob)
        | (Prog ea _ oa, Prog eb _ ob) <- orderedPairs size bank
        ]
    divs =
        [ Prog (ea / eb) size (VU.zipWith (/) oa ob)
        | (Prog ea _ oa, Prog eb _ ob) <- orderedPairs size bank
        , VU.all ((> 1e-9) . abs) ob
        ]

-- | Protected unary operators: total on all reals (no NaN/domain errors).
unaryProds :: [(Expr Double -> Expr Double, Double -> Double)]
unaryProds =
    [ (sqrt . abs, sqrt . abs)
    , (abs, abs)
    , (\e -> log (abs e + 1), \x -> log (abs x + 1))
    , (exp, exp)
    , (sin, sin)
    , (cos, cos)
    , (F.relu, max 0)
    , (signum, signum)
    ]

-- | Commutative binary operators (enumerated over unordered operand pairs).
commutativeProds ::
    [(Expr Double -> Expr Double -> Expr Double, Double -> Double -> Double)]
commutativeProds =
    [ ((+), (+))
    , ((*), (*))
    , (F.min, min)
    , (F.max, max)
    ]

-- | Ordered operand pairs whose sizes sum to @size-1@ (for non-commutative ops).
orderedPairs :: Int -> [Prog] -> [(Prog, Prog)]
orderedPairs size bank =
    [ (a, b)
    | sa <- [1 .. size - 2]
    , let sb = size - 1 - sa
    , sb >= 1
    , a <- atSize sa
    , b <- atSize sb
    ]
  where
    atSize s = filter ((== s) . progSize) bank

-- | Unordered operand pairs (for commutative ops): each pair once.
unorderedPairs :: Int -> [Prog] -> [(Prog, Prog)]
unorderedPairs size bank =
    [ (a, b)
    | sa <- [1 .. size - 2]
    , let sb = size - 1 - sa
    , sb >= 1
    , sa <= sb
    , (i, a) <- zip [0 :: Int ..] (atSize sa)
    , (j, b) <- zip [0 :: Int ..] (atSize sb)
    , sa < sb || i <= j
    ]
  where
    atSize s = filter ((== s) . progSize) bank

{- | Keep the valid, observationally-novel candidates of a layer, then cap by fit
score (cost-guided). Returns the kept programs and the updated OE-class set.
-}
absorb ::
    SynthesisConfig -> Output -> Seen -> [Prog] -> ([Prog], Seen)
absorb cfg tgt seen0 cands = (capLayer cfg tgt fresh, seen')
  where
    (fresh, seen') = dedupProgs seen0 cands

-- | When a layer has more distinct programs than the cap, keep the best-scoring.
capLayer :: SynthesisConfig -> Output -> [Prog] -> [Prog]
capLayer cfg tgt progs
    | length progs <= synBankCap cfg = progs
    | otherwise = take (synBankCap cfg) (sortBy (comparing (Down . rank)) progs)
  where
    rank p = fromMaybe (negate (1 / 0)) (scoreOf (synLoss cfg) tgt (progOut p))

-- | Fit score of an output against the target (higher is better), or @Nothing@.
scoreOf :: LossFunction -> Output -> Output -> Maybe Double
scoreOf lf tgt out
    | VU.length out /= VU.length tgt = Nothing
    | otherwise = finite $ case lf of
        PearsonCorrelation -> pearsonR2 tgt out
        MutualInformation -> mutualInformationBinned bins tgt out
        MeanSquaredError -> negate <$> meanSquaredError tgt out
  where
    bins = max 10 (ceiling (sqrt (fromIntegral (VU.length tgt) :: Double)))
    -- Belt-and-suspenders: drop any non-finite score so it cannot win the ranking.
    finite (Just s) | isNaN s || isInfinite s = Nothing
    finite ms = ms

{- | Pearson @r²@ via the numerically stable centered two-pass formula. Returns
'Nothing' when the feature (or target) is constant, and is bounded by
Cauchy–Schwarz to @[0,1]@ — unlike the one-pass @n·Σxy − Σx·Σy@ form, which
cancels catastrophically for low-variance features and can report @r² > 1@.
-}
pearsonR2 :: Output -> Output -> Maybe Double
pearsonR2 ys xs
    | n < 2 = Nothing
    | sxx <= 0 || syy <= 0 = Nothing
    | otherwise = Just (min 1 (sxy * sxy / (sxx * syy)))
  where
    n = VU.length xs
    nf = fromIntegral n
    mx = VU.sum xs / nf
    my = VU.sum ys / nf
    sxy = VU.sum (VU.zipWith (\x y -> (x - mx) * (y - my)) xs ys)
    sxx = VU.sum (VU.map (\x -> (x - mx) * (x - mx)) xs)
    syy = VU.sum (VU.map (\y -> (y - my) * (y - my)) ys)

-- | An output is usable iff it is non-empty and free of NaN/±Inf.
valid :: Output -> Bool
valid o = not (VU.null o) && VU.all (\x -> not (isNaN x || isInfinite x)) o

{- | Quantize an output to nine significant digits, so float noise (@x*2@ vs
@x+x@) collapses while genuinely distinct features stay apart.
-}
quantize :: Output -> Output
quantize = VU.map (\x -> if x == 0 then 0 else signum x * roundSig 9 (abs x))

{- | The observational-equivalence class set: a map from a 64-bit FNV-1a
fingerprint of the quantized output to the (usually one) quantized outputs with
that fingerprint. Bucketing on the fingerprint keeps membership cheap, and
verifying exact equality within the bucket makes a hash collision harmless — two
genuinely different columns that happen to collide are kept apart, not merged.
-}
type Seen = M.Map Int [Output]

-- | FNV-1a fingerprint of an already-quantized output's bit patterns.
fpOf :: Output -> Int
fpOf = fromIntegral . VU.foldl' step (1469598103934665603 :: Word64)
  where
    step !h x = (h `xor` castDoubleToWord64 x) * 1099511628211

-- | Record an output's observational-equivalence class.
seenInsert :: Output -> Seen -> Seen
seenInsert o = M.insertWith (++) (fpOf q) [q]
  where
    q = quantize o

-- | Round a positive double to @n@ significant digits.
roundSig :: Int -> Double -> Double
roundSig n x
    | x == 0 = 0
    | otherwise =
        let magnitude = floor (logBase 10 (abs x)) :: Int
            scale = 10 ** fromIntegral (n - 1 - magnitude)
         in fromIntegral (round (x * scale) :: Integer) / scale

{- | Keep the first valid program of each observational-equivalence class,
preserving order; returns the kept programs and the grown class set.
-}
dedupProgs :: Seen -> [Prog] -> ([Prog], Seen)
dedupProgs = go []
  where
    go acc s [] = (reverse acc, s)
    go acc s (p : ps)
        | not (valid o) = go acc s ps
        | member = go acc s ps
        | otherwise = go (p : acc) (M.insertWith (++) fp [q] s) ps
      where
        o = progOut p
        q = quantize o
        fp = fpOf q
        member = maybe False (q `elem`) (M.lookup fp s)
