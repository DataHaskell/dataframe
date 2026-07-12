{-# LANGUAGE CPP #-}

{- | Deterministic, platform-independent random sampling for the stochastic
fitters. Built on @random@'s SplitMix 'StdGen'; the distributions here are our
own so a seeded fit is bit-reproducible across platforms.
-}
module DataFrame.Random (
    Gen,
    mkGen,
    splitGen,
    nextWord64,
    nextDouble,
    nextIntR,
    gaussianPair,
    gaussianVector,
    shuffleInts,
    sampleIndices,
) where

import Control.Monad (forM_)
import Control.Monad.ST (runST)
import Data.Bits (shiftR)
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import System.Random (StdGen, genWord64, mkStdGen)
import qualified System.Random as R

-- | The pure splittable generator. A fit is a function of @(seed, data)@.
type Gen = StdGen

-- | Seed a generator from an 'Int'.
mkGen :: Int -> Gen
mkGen = mkStdGen

-- | Split into two independent generators.
splitGen :: Gen -> (Gen, Gen)
#if MIN_VERSION_random(1,3,0)
splitGen = R.splitGen
#else
splitGen = R.split
#endif

-- | Raw 64-bit draw.
nextWord64 :: Gen -> (Word64, Gen)
nextWord64 = genWord64

-- | Uniform 'Double' in @[0, 1)@ from the top 53 bits (exact mantissa).
nextDouble :: Gen -> (Double, Gen)
nextDouble g =
    let (w, g') = genWord64 g
        d = fromIntegral (w `shiftR` 11) * (1 / 9007199254740992)
     in (d, g')

{- | Uniform 'Int' in the inclusive range @[lo, hi]@ by rejection sampling
(unbiased). Returns @lo@ when @hi <= lo@.
-}
nextIntR :: (Int, Int) -> Gen -> (Int, Gen)
nextIntR (lo, hi) g
    | hi <= lo = (lo, g)
    | otherwise = loop g
  where
    range = fromIntegral (hi - lo + 1) :: Word64
    threshold = negate range `mod` range
    loop gg =
        let (w, gg') = genWord64 gg
         in if w >= threshold
                then (lo + fromIntegral (w `mod` range), gg')
                else loop gg'

{- | A pair of independent standard normals via Box-Muller, consuming exactly two
uniforms so stream offsets stay data-independent.
-}
gaussianPair :: Gen -> ((Double, Double), Gen)
gaussianPair g =
    let (u1, g1) = nextDouble g
        (u2, g2) = nextDouble g1
        u1' = if u1 <= 0 then 2.220446049250313e-16 else u1
        r = sqrt (-(2 * log u1'))
        a = 2 * pi * u2
     in ((r * cos a, r * sin a), g2)

-- | A length-@n@ vector of standard normals.
gaussianVector :: Int -> Gen -> (VU.Vector Double, Gen)
gaussianVector n g0 = go n g0 []
  where
    go k g acc
        | k <= 0 = (VU.fromList (take n (reverse acc)), g)
        | otherwise =
            let ((z0, z1), g') = gaussianPair g
             in go (k - 2) g' (z1 : z0 : acc)

{- | A uniformly random permutation of @[0 .. n-1]@ (Fisher-Yates), threading the
generator purely.
-}
shuffleInts :: Int -> Gen -> (VU.Vector Int, Gen)
shuffleInts n g0
    | n <= 1 = (VU.enumFromN 0 (max 0 n), g0)
    | otherwise =
        let (swaps, g1) = genSwaps (n - 1) g0 []
            v = runST $ do
                m <- VU.thaw (VU.enumFromN 0 n)
                forM_ swaps $ uncurry (VUM.swap m)
                VU.freeze m
         in (v, g1)
  where
    genSwaps i g acc
        | i < 1 = (reverse acc, g)
        | otherwise =
            let (j, g') = nextIntR (0, i) g
             in genSwaps (i - 1) g' ((i, j) : acc)

{- | @sampleIndices k n@ draws @k@ distinct indices from @[0 .. n-1]@ (the first
@k@ of a full shuffle); returns all @n@ when @k >= n@.
-}
sampleIndices :: Int -> Int -> Gen -> (VU.Vector Int, Gen)
sampleIndices k n g =
    let (perm, g') = shuffleInts n g
     in (VU.take (min k n) perm, g')
