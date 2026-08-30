{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | Regression tests for the PackedText migration: a string column stored as a
'DI.PackedText' (the form the CSV readers emit) must flow through the
decision-tree feature/target extraction and the full-outer-join key coalescing
without crashing, and must behave IDENTICALLY to the same data held as a boxed
@Text@ column. If a non-exhaustive @Column@ match regresses, the packed cases
either crash (test error) or diverge from the boxed baseline (assertion fails).
-}
module PackedTextMigration (tests) where

import Control.Monad (zipWithM_)
import qualified Data.ByteString as B
import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Text.Encoding (encodeUtf8)
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)

import DataFrame.DecisionTree (defaultTreeConfig)
import DataFrame.DecisionTree.Model ()
import DataFrame.Model (fit, predict)
import DataFrame.Operations.Join (fullOuterJoin)

import Test.HUnit

{- | Build a 'DI.PackedText' column directly from raw UTF-8 bytes + offsets,
exactly as the CSV readers do (mirrors @Internal.PackedText.packedFromTexts@).
-}
packedFromTexts :: [T.Text] -> DI.Column
packedFromTexts ts =
    let bytess = map (B.unpack . encodeUtf8) ts
        flat = concat bytess
        offs = scanl (+) 0 (map length bytess)
     in DI.PackedText
            Nothing
            (mkPackedContiguous (arrayFromBytes flat) (VU.fromList offs))

arrayFromBytes :: [Word8] -> A.Array
arrayFromBytes ws = A.run $ do
    m <- A.new (length ws)
    zipWithM_ (A.unsafeWrite m) [0 ..] ws
    pure m

-- | The two column encodings of the same text data.
packed, boxed :: [T.Text] -> DI.Column
packed = packedFromTexts
boxed = DI.fromList

{- | A decision tree using a TEXT FEATURE (exercises Cart.featuresOfColumn and the
categorical condition builders). The packed-feature tree must equal the
boxed-feature tree, proving the feature is used identically — not dropped or
crashed on.
-}
treeOnTextFeature :: Test
treeOnTextFeature = TestCase $ do
    let df mk =
            D.fromNamedColumns
                [ ("color", mk ["red", "red", "blue", "blue", "green", "green"])
                , ("y", DI.fromList ([0, 0, 1, 1, 2, 2] :: [Double]))
                ]
        packedTree = fit defaultTreeConfig (D.col @Double "y") (df packed)
        boxedTree = fit defaultTreeConfig (D.col @Double "y") (df boxed)
    assertEqual
        "tree on packed-text feature == tree on boxed-text feature"
        (D.prettyPrint (predict boxedTree))
        (D.prettyPrint (predict packedTree))

{- | A classifier whose TARGET is a packed-text column (exercises
Cart.cartTargetLabels). Packed target must give the same tree as a boxed one.
-}
classifierOnTextTarget :: Test
classifierOnTextTarget = TestCase $ do
    let df mk =
            D.fromNamedColumns
                [ ("x", DI.fromList ([1, 2, 3, 4, 5, 6] :: [Double]))
                , ("label", mk ["a", "a", "a", "b", "b", "b"])
                ]
        packedClf = fit defaultTreeConfig (D.col @T.Text "label") (df packed)
        boxedClf = fit defaultTreeConfig (D.col @T.Text "label") (df boxed)
    assertEqual
        "classifier with packed-text target == boxed-text target"
        (D.prettyPrint (predict boxedClf))
        (D.prettyPrint (predict packedClf))

{- | A full outer join on a packed-text KEY column (exercises the
coalesceKeyColumn path that previously errored on PackedText). Result must
match the boxed-key join and recover all four distinct keys.
-}
fullOuterJoinOnTextKey :: Test
fullOuterJoinOnTextKey = TestCase $ do
    let l mk =
            D.fromNamedColumns
                [("k", mk ["a", "b", "c"]), ("lv", DI.fromList ([1, 2, 3] :: [Double]))]
        r mk =
            D.fromNamedColumns
                [("k", mk ["b", "c", "d"]), ("rv", DI.fromList ([20, 30, 40] :: [Double]))]
        packedJoin = fullOuterJoin ["k"] (l packed) (r packed)
        boxedJoin = fullOuterJoin ["k"] (l boxed) (r boxed)
    assertEqual
        "full outer join on packed key recovers all 4 keys"
        4
        (fst (D.dimensions packedJoin))
    assertEqual
        "packed-key join row count == boxed-key join row count"
        (fst (D.dimensions boxedJoin))
        (fst (D.dimensions packedJoin))

tests :: [Test]
tests = [treeOnTextFeature, classifierOnTextTarget, fullOuterJoinOnTextKey]
