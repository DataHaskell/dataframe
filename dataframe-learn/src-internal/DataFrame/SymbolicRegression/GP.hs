{- | A compact generational genetic-programming search over 'SRExpr': ramped
initialization, tournament selection, subtree crossover/mutation, elitism, and a
complexity-keyed Pareto archive. Deterministic given the seed.
-}
module DataFrame.SymbolicRegression.GP (
    GPParams (..),
    runGP,
) where

import Data.List (foldl', minimumBy, sortBy)
import qualified Data.Map.Strict as M
import Data.Ord (comparing)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Random (Gen, nextDouble, nextIntR)
import DataFrame.SymbolicRegression.Expr
import DataFrame.SymbolicRegression.Optimize (
    meanSquaredError,
    optimizeConstants,
 )
import DataFrame.SymbolicRegression.Simplify (simplify)

-- | GP hyper-parameters resolved from the public config.
data GPParams = GPParams
    { gpFeats :: !(V.Vector (VU.Vector Double))
    , gpN :: !Int
    , gpTarget :: !(VU.Vector Double)
    , gpNVars :: !Int
    , gpUnOps :: ![UnOp]
    , gpPopSize :: !Int
    , gpGenerations :: !Int
    , gpMaxSize :: !Int
    , gpTournament :: !Int
    , gpCrossoverP :: !Double
    , gpMutationP :: !Double
    , gpOptimizeP :: !Double
    , gpParsimony :: !Double
    }

type Scored = (SRExpr, Double)

-- | Run the search; returns @(best, pareto front, generations run)@.
runGP :: GPParams -> Gen -> (SRExpr, [(Int, Double, SRExpr)], Int)
runGP p g0 =
    let (pop0, g1) = initPop p g0
        scored0 = map (scoreOf p) pop0
        arch0 = foldl' (archiveInsert p) M.empty scored0
        (_, finalArch, gN, _) =
            iterate' 0 scored0 arch0 g1
        best = bestOfArchive finalArch
        front =
            [ (sz, mse, e)
            | (sz, (mse, e)) <- M.toList finalArch
            ]
     in (snd3 best, sortBy (comparing fst3) front, gN)
  where
    iterate' gen pop arch g
        | gen >= gpGenerations p = (pop, arch, gen, g)
        | otherwise =
            let (pop', g') = nextGen p pop g
                arch' = foldl' (archiveInsert p) arch pop'
             in iterate' (gen + 1) pop' arch' g'
    fst3 (a, _, _) = a
    snd3 (_, b, _) = b
    bestOfArchive arch =
        case M.toList arch of
            [] -> (0 :: Int, SConst 0, 1 / 0)
            xs ->
                let (sz, (mse, e)) = minimumBy (comparing (fst . snd)) xs
                 in (sz, e, mse)

scoreOf :: GPParams -> SRExpr -> Scored
scoreOf p e = (e, meanSquaredError (gpFeats p) (gpN p) (gpTarget p) e)

fitness :: GPParams -> Scored -> Double
fitness p (e, mse) = mse + gpParsimony p * fromIntegral (srSize e)

archiveInsert ::
    GPParams -> M.Map Int (Double, SRExpr) -> Scored -> M.Map Int (Double, SRExpr)
archiveInsert _ arch (e, mse)
    | isNaN mse || isInfinite mse = arch
    | otherwise =
        let key = srSize (simplify e)
         in M.insertWith better key (mse, e) arch
  where
    better newv@(m1, _) oldv@(m2, _) = if m1 < m2 then newv else oldv

initPop :: GPParams -> Gen -> ([SRExpr], Gen)
initPop p = go (gpPopSize p) []
  where
    go 0 acc g = (acc, g)
    go k acc g =
        let (depth, g1) = nextIntR (1, 4) g
            (e, g2) = randomExpr p depth g1
         in go (k - 1) (e : acc) g2

randomExpr :: GPParams -> Int -> Gen -> (SRExpr, Gen)
randomExpr p depth g
    | depth <= 1 = randomLeaf p g
    | otherwise =
        let (r, g1) = nextDouble g
         in if r < 0.3
                then randomLeaf p g1
                else
                    let (isUn, g2) = nextDouble g1
                     in if isUn < 0.3 && not (null (gpUnOps p))
                            then
                                let (oi, g3) = nextIntR (0, length (gpUnOps p) - 1) g2
                                    (e, g4) = randomExpr p (depth - 1) g3
                                 in (SUn (gpUnOps p !! oi) e, g4)
                            else
                                let (oi, g3) = nextIntR (0, length allBinOps - 1) g2
                                    (a, g4) = randomExpr p (depth - 1) g3
                                    (b, g5) = randomExpr p (depth - 1) g4
                                 in (SBin (allBinOps !! oi) a b, g5)

randomLeaf :: GPParams -> Gen -> (SRExpr, Gen)
randomLeaf p g =
    let (r, g1) = nextDouble g
     in if r < 0.6 && gpNVars p > 0
            then let (j, g2) = nextIntR (0, gpNVars p - 1) g1 in (SVar j, g2)
            else let (c, g2) = nextDouble g1 in (SConst (c * 4 - 2), g2)

nextGen :: GPParams -> [Scored] -> Gen -> ([Scored], Gen)
nextGen p pop g0 =
    let elite = minimumBy (comparing (fitness p)) pop
        (rest, g1) = go (gpPopSize p - 1) [] g0
     in (elite : rest, g1)
  where
    go 0 acc g = (acc, g)
    go k acc g =
        let (child, g') = breed p pop g
            scored = optimizeMaybe p child g'
         in go (k - 1) (fst scored : acc) (snd scored)

optimizeMaybe :: GPParams -> SRExpr -> Gen -> (Scored, Gen)
optimizeMaybe p e g =
    let (r, g1) = nextDouble g
        e' =
            if r < gpOptimizeP p
                then optimizeConstants (gpFeats p) (gpN p) (gpTarget p) 15 e
                else e
     in (scoreOf p e', g1)

breed :: GPParams -> [Scored] -> Gen -> (SRExpr, Gen)
breed p pop g0 =
    let (pa, g1) = tournament p pop g0
        (doX, g2) = nextDouble g1
        (child, g3) =
            if doX < gpCrossoverP p
                then
                    let (pb, g2') = tournament p pop g2
                        (c, g3') = crossover pa pb g2'
                     in (c, g3')
                else (pa, g2)
        (doM, g4) = nextDouble g3
        (child', g5) =
            if doM < gpMutationP p then mutate p child g4 else (child, g4)
        capped = if srSize child' > gpMaxSize p then pa else child'
     in (simplify capped, g5)

tournament :: GPParams -> [Scored] -> Gen -> (SRExpr, Gen)
tournament p pop g0 =
    let (picks, g1) = pickN (gpTournament p) g0
        chosen = map (pop !!) picks
     in (fst (minimumBy (comparing (fitness p)) chosen), g1)
  where
    n = length pop
    pickN 0 g = ([], g)
    pickN k g =
        let (i, g') = nextIntR (0, n - 1) g
            (is, g'') = pickN (k - 1) g'
         in (i : is, g'')

crossover :: SRExpr -> SRExpr -> Gen -> (SRExpr, Gen)
crossover a b g0 =
    let (ia, g1) = nextIntR (0, srSize a - 1) g0
        (ib, g2) = nextIntR (0, srSize b - 1) g1
        sub = subtreeAt ib b
     in (replaceAt ia a sub, g2)

mutate :: GPParams -> SRExpr -> Gen -> (SRExpr, Gen)
mutate p e g0 =
    let (i, g1) = nextIntR (0, srSize e - 1) g0
        (depth, g2) = nextIntR (1, 3) g1
        (newSub, g3) = randomExpr p depth g2
     in (replaceAt i e newSub, g3)

subtreeAt :: Int -> SRExpr -> SRExpr
subtreeAt 0 e = e
subtreeAt i (SUn _ e) = subtreeAt (i - 1) e
subtreeAt i (SBin _ a b) =
    let sa = srSize a
     in if i <= sa then subtreeAt (i - 1) a else subtreeAt (i - 1 - sa) b
subtreeAt _ e = e

replaceAt :: Int -> SRExpr -> SRExpr -> SRExpr
replaceAt 0 _ new = new
replaceAt i (SUn op e) new = SUn op (replaceAt (i - 1) e new)
replaceAt i (SBin op a b) new =
    let sa = srSize a
     in if i <= sa
            then SBin op (replaceAt (i - 1) a new) b
            else SBin op a (replaceAt (i - 1 - sa) b new)
replaceAt _ e _ = e
