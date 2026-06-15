{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | The "predict is the model's denotation" claim, discharged as a test rather
than left as a slogan: the @Expr@ that 'predict' compiles must evaluate to the
same numbers as the fitted record's own parameters, computed independently here
in plain Haskell. If @affineExpr@ / @argMinExpr@ ever drift from the record they
are built from, these fail.
-}
module Learn.Denotation (tests) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Interpreter (interpret)

import DataFrame.KMeans
import DataFrame.LinearModel
import DataFrame.Model (fit, predict)

import Test.HUnit

interpD :: D.DataFrame -> Expr Double -> [Double]
interpD df e = case interpret @Double df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Double @VU.Vector c)
    Left err -> error (show err)

interpI :: D.DataFrame -> Expr Int -> [Int]
interpI df e = case interpret @Int df e of
    Right (TColumn c) -> either (const []) VU.toList (toVector @Int @VU.Vector c)
    Left err -> error (show err)

df :: D.DataFrame
df =
    D.fromNamedColumns
        [ ("x1", DI.fromList xs1)
        , ("x2", DI.fromList xs2)
        , ("y", DI.fromList [3 + 2 * a - 0.5 * b | (a, b) <- zip xs1 xs2])
        ]
  where
    xs1 = [1, 2, 3, 4, 5, 6, 7, 8] :: [Double]
    xs2 = [2, 1, 4, 3, 6, 5, 8, 7] :: [Double]

col :: D.DataFrame -> T.Text -> [Double]
col d n = interpD d (F.col @Double n)

-- | @interpret (predict m)@ must equal @intercept + Σ coefⱼ·featureⱼ@ from the record.
linearDenotation :: Test
linearDenotation = TestCase $ do
    let m = fit defaultLinearConfig (F.col @Double "y") df
        feats = V.toList (regFeatureNames m)
        coefs = VU.toList (regCoef m)
        cols = map (col df) feats
        native =
            [ regIntercept m + sum (zipWith (*) coefs row)
            | row <- transposeL cols
            ]
        symbolic = interpD df (predict m)
    assertBool
        "linear: predict Expr matches the record's affine prediction"
        (and (zipWith (\a b -> abs (a - b) < 1e-9) native symbolic))

-- | @interpret (predict km)@ must equal the nearest-centroid label from @kmCenters@.
kmeansDenotation :: Test
kmeansDenotation = TestCase $ do
    let feats = ["x1", "x2"]
        km = fit defaultKMeansConfig{kmK = 3, kmSeed = 1} (map (F.col @Double) feats) df
        centers = map VU.toList (V.toList (kmCenters km))
        rows = transposeL (map (col df) feats)
        native = [nearest centers row | row <- rows]
        symbolic = interpI df (predict km)
    assertEqual
        "kmeans: predict Expr matches nearest-centroid label"
        native
        symbolic
  where
    nearest centers row =
        snd (minimum [(sqDist c row, i) | (i, c) <- zip [0 ..] centers])
    sqDist c row = sum [(a - b) ^ (2 :: Int) | (a, b) <- zip c row]

transposeL :: [[a]] -> [[a]]
transposeL [] = []
transposeL xss
    | any null xss = []
    | otherwise = map head xss : transposeL (map tail xss)

tests :: [Test]
tests = [linearDenotation, kmeansDenotation]
