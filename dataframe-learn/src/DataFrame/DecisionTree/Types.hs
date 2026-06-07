{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Shared types, configuration and ordering machinery for the decision-tree
-- learner. Imported by every other @DataFrame.DecisionTree.*@ module.
module DataFrame.DecisionTree.Types (
    Tree (..),
    treeDepth,
    TreeConfig (..),
    SynthConfig (..),
    defaultTreeConfig,
    defaultSynthConfig,
    ColumnOrdering (..),
    orderable,
    defaultColumnOrdering,
    withOrdFrom,
    CarePoint (..),
    Direction (..),
    ttrace,
) where

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (..))
import qualified DataFrame.LinearSolver as LS

import Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.Map.Strict as M
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import Data.Word (Word16, Word32, Word64, Word8)
import qualified Debug.Trace as Trace
import System.Environment (lookupEnv)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (SomeTypeRep (..), typeRep)

-- | A fitted tree: a leaf value, or an internal node testing a boolean
-- expression with @True@ routing left.
data Tree a
    = Leaf !a
    | Branch !(Expr Bool) !(Tree a) !(Tree a)
    deriving (Show)

treeDepth :: Tree a -> Int
treeDepth (Leaf _) = 0
treeDepth (Branch _ l r) = 1 + max (treeDepth l) (treeDepth r)

-- | A row the parent node must route to a specific child for the subtrees to
-- classify it correctly (the TAO objective is the count of misroutes).
data CarePoint = CarePoint
    { cpIndex :: !Int
    , cpCorrectDir :: !Direction
    }
    deriving (Eq, Show)

data Direction = GoLeft | GoRight
    deriving (Eq, Show)

data TreeConfig = TreeConfig
    { maxTreeDepth :: Int
    , minSamplesSplit :: Int
    , minLeafSize :: Int
    , percentiles :: [Int]
    , expressionPairs :: Int
    , synthConfig :: SynthConfig
    , taoIterations :: Int
    , taoConvergenceTol :: Double
    , columnOrdering :: ColumnOrdering
    , useLinearSolver :: Bool
    , linearSolverConfig :: LS.SolverConfig
    , minCarePointsForLinear :: Int
    , pureReplacementLinear :: Bool
    }

data SynthConfig = SynthConfig
    { maxExprDepth :: Int
    , boolExpansion :: Int
    , disallowedCombinations :: [(T.Text, T.Text)]
    , complexityPenalty :: Double
    , enableStringOps :: Bool
    , enableCrossCols :: Bool
    , enableArithOps :: Bool
    , maxCategoricalSubsetCardinality :: Int
    , perColumnQuota :: Maybe Int
    }
    deriving (Eq, Show)

defaultSynthConfig :: SynthConfig
defaultSynthConfig =
    SynthConfig
        { maxExprDepth = 2
        , boolExpansion = 2
        , disallowedCombinations = []
        , complexityPenalty = 0.05
        , enableStringOps = True
        , enableCrossCols = True
        , enableArithOps = True
        , maxCategoricalSubsetCardinality = 4
        , perColumnQuota = Just 3
        }

defaultTreeConfig :: TreeConfig
defaultTreeConfig =
    TreeConfig
        { maxTreeDepth = 4
        , minSamplesSplit = 5
        , minLeafSize = 1
        , percentiles = [0, 10 .. 100]
        , expressionPairs = 10
        , synthConfig = defaultSynthConfig
        , taoIterations = 10
        , taoConvergenceTol = 1e-6
        , columnOrdering = defaultColumnOrdering
        , useLinearSolver = True
        , linearSolverConfig = LS.defaultSolverConfig
        , minCarePointsForLinear = 10
        , pureReplacementLinear = False
        }

-- | Which column types support ordering for splits. Register a type with
-- 'orderable' and combine with @<>@.
newtype ColumnOrdering = ColumnOrdering (M.Map SomeTypeRep OrdDict)

instance Semigroup ColumnOrdering where
    ColumnOrdering a <> ColumnOrdering b = ColumnOrdering (a <> b)

instance Monoid ColumnOrdering where
    mempty = ColumnOrdering M.empty

-- | Register a type as orderable for decision-tree splits.
orderable :: forall a -> (Columnable a, Ord a) => ColumnOrdering
orderable a = ColumnOrdering (M.singleton (SomeTypeRep (typeRep @a)) (OrdDict (Proxy @a)))

-- | All standard numeric, text, and primitive types.
defaultColumnOrdering :: ColumnOrdering
defaultColumnOrdering = mconcat (numericOrderings ++ otherOrderings)

numericOrderings :: [ColumnOrdering]
numericOrderings =
    [ orderable Int
    , orderable Int8
    , orderable Int16
    , orderable Int32
    , orderable Int64
    , orderable Word
    , orderable Word8
    , orderable Word16
    , orderable Word32
    , orderable Word64
    , orderable Integer
    , orderable Double
    , orderable Float
    ]

otherOrderings :: [ColumnOrdering]
otherOrderings =
    [orderable Bool, orderable Char, orderable T.Text, orderable String]

-- | Existential @Ord@ dictionary keyed by type representation.
data OrdDict where
    OrdDict :: (Columnable a, Ord a) => Proxy a -> OrdDict

-- | Run @k@ with the @Ord a@ instance recovered from the ordering registry,
-- or 'Nothing' when @a@ is not registered.
withOrdFrom ::
    forall a r. (Columnable a) => ColumnOrdering -> ((Ord a) => r) -> Maybe r
withOrdFrom (ColumnOrdering m) k = case M.lookup (SomeTypeRep (typeRep @a)) m of
    Just (OrdDict (_ :: Proxy b)) -> case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Just k
        Nothing -> Nothing
    Nothing -> Nothing

{-# NOINLINE taoTraceEnabled #-}
taoTraceEnabled :: Bool
taoTraceEnabled = unsafePerformIO (fmap (== Just "1") (lookupEnv "TAO_TRACE"))

-- | Emit a trace line when @TAO_TRACE=1@; a no-op otherwise.
ttrace :: String -> a -> a
ttrace msg x
    | taoTraceEnabled = Trace.trace ("[TAO] " ++ msg) x
    | otherwise = x
