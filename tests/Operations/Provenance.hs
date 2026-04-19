{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Provenance where

import qualified Data.Map as M
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI
import DataFrame.Operations.Merge ((|||))

import Test.HUnit

-- Base frame with no derived columns.
base :: D.DataFrame
base =
    D.fromNamedColumns
        [ ("x", DI.fromList [1 .. 5 :: Int])
        , ("y", DI.fromList [2 .. 6 :: Int])
        ]

-- A frame with one derived column "z".
withZ :: D.DataFrame
withZ = D.derive "z" (F.col @Int "x" + F.col "y") base

-- ── insertColumn ──────────────────────────────────────────────────────────────

-- Inserting a new column must not wipe provenance of existing derived columns.
insertPreservesProvenance :: Test
insertPreservesProvenance =
    TestCase
        ( assertBool
            "insertColumn should preserve existing derivingExpressions"
            ( M.member
                "z"
                (DI.derivingExpressions (D.insertColumn "w" (DI.fromList [0 :: Int]) withZ))
            )
        )

-- Overwriting a derived column removes only *that* expression, not others.
insertOverwriteDropsOwnExpr :: Test
insertOverwriteDropsOwnExpr =
    let
        df2 = D.derive "w" (F.col @Int "x") withZ
        df3 = D.insertColumn "z" (DI.fromList [99 :: Int]) df2
     in
        TestCase $ do
            assertBool
                "overwritten column z should be removed from derivingExpressions"
                (not $ M.member "z" (DI.derivingExpressions df3))
            assertBool
                "sibling column w expression should be preserved"
                (M.member "w" (DI.derivingExpressions df3))

-- ── derive ────────────────────────────────────────────────────────────────────

-- derive adds the expression to derivingExpressions.
deriveTracksExpression :: Test
deriveTracksExpression =
    TestCase
        ( assertBool
            "derive should record z in derivingExpressions"
            (M.member "z" (DI.derivingExpressions withZ))
        )

-- Multiple derives accumulate.
deriveManyTracksAll :: Test
deriveManyTracksAll =
    let df = D.derive "w" (F.col @Int "x") withZ
     in TestCase
            ( assertEqual
                "two derive calls should leave two expressions"
                2
                (M.size (DI.derivingExpressions df))
            )

-- Re-deriving a column replaces its expression and keeps the count stable.
deriveOverwriteReplacesExpression :: Test
deriveOverwriteReplacesExpression =
    let df = D.derive "z" (F.col @Int "y") withZ -- overwrite z
     in TestCase
            ( assertEqual
                "re-deriving z should not duplicate the entry"
                1
                (M.size (DI.derivingExpressions df))
            )

-- ── deriveWithExpr ────────────────────────────────────────────────────────────

-- deriveWithExpr should also track the expression.
deriveWithExprTracksExpression :: Test
deriveWithExprTracksExpression =
    let (_, df) = D.deriveWithExpr @Int "z" (F.col @Int "x" + F.col "y") base
     in TestCase
            ( assertBool
                "deriveWithExpr should record z in derivingExpressions"
                (M.member "z" (DI.derivingExpressions df))
            )

-- ── showDerivedExpressions ────────────────────────────────────────────────────

-- A frame with no derived columns returns identity provenance for each column.
showDerivedEmpty :: Test
showDerivedEmpty =
    TestCase
        ( assertEqual
            "raw-column frame should have identity provenance for each column"
            ["x", "y"]
            (map fst (D.showDerivedExpressions base))
        )

-- Frame with one derived column has an entry for the derived column.
showDerivedContainsName :: Test
showDerivedContainsName =
    TestCase
        ( assertBool
            "showDerivedExpressions should include the derived column name"
            ("z" `elem` map fst (D.showDerivedExpressions withZ))
        )

-- ── Semigroup (<>) provenance propagation ─────────────────────────────────────

-- Vertical merge preserves expressions from both sides.
semiGroupPreservesLeft :: Test
semiGroupPreservesLeft =
    let merged = withZ <> base
     in TestCase
            ( assertBool
                "<> should preserve derivingExpressions from the left frame"
                (M.member "z" (DI.derivingExpressions merged))
            )

semiGroupPreservesBoth :: Test
semiGroupPreservesBoth =
    let dfW = D.derive "w" (F.col @Int "y") base
        merged = withZ <> dfW
     in TestCase
            ( assertEqual
                "<> should union derivingExpressions from both frames"
                2
                (M.size (DI.derivingExpressions merged))
            )

-- Left frame wins when both sides have an expression for the same column.
semiGroupLeftBias :: Test
semiGroupLeftBias =
    let dfLeft = D.derive "z" (F.col @Int "x") base
        dfRight = D.derive "z" (F.col @Int "y") base
        merged = dfLeft <> dfRight
     in TestCase
            ( assertEqual
                "<> should retain exactly one entry for a shared column name"
                1
                (M.size (DI.derivingExpressions merged))
            )

-- Merging with an empty frame must not lose provenance.
semiGroupWithEmpty :: Test
semiGroupWithEmpty =
    TestCase
        ( assertBool
            "withZ <> empty should keep z's expression"
            (M.member "z" (DI.derivingExpressions (withZ <> D.empty)))
        )

emptyWithSemiGroup :: Test
emptyWithSemiGroup =
    TestCase
        ( assertBool
            "empty <> withZ should keep z's expression"
            (M.member "z" (DI.derivingExpressions (D.empty <> withZ)))
        )

-- ── Horizontal merge (|||) provenance propagation ────────────────────────────

horizontalMergePreservesLeft :: Test
horizontalMergePreservesLeft =
    let dfW = D.derive "w" (F.col @Int "y") base
        extra = D.fromNamedColumns [("q", DI.fromList [0 :: Int, 0, 0, 0, 0])]
        merged = dfW ||| extra
     in TestCase
            ( assertBool
                "||| should preserve derivingExpressions from the left frame"
                (M.member "w" (DI.derivingExpressions merged))
            )

horizontalMergePreservesRight :: Test
horizontalMergePreservesRight =
    let extra = D.fromNamedColumns [("q", DI.fromList [0 :: Int, 0, 0, 0, 0])]
        dfW =
            D.derive
                "w"
                (F.col @Int "y")
                (D.fromNamedColumns [("y", DI.fromList [2 .. 6 :: Int])])
        merged = extra ||| dfW
     in TestCase
            ( assertBool
                "||| should bring in derivingExpressions from the right frame"
                (M.member "w" (DI.derivingExpressions merged))
            )

horizontalMergePreservesBoth :: Test
horizontalMergePreservesBoth =
    let dfZ = withZ
        dfW = D.fromNamedColumns [("q", DI.fromList [0 :: Int, 0, 0, 0, 0])]
        -- give dfW a derived column on a separate base
        dfWD = D.derive "w" (F.col @Int "q") dfW
        merged = dfZ ||| dfWD
     in TestCase
            ( assertEqual
                "||| should union derivingExpressions"
                2
                (M.size (DI.derivingExpressions merged))
            )

tests :: [Test]
tests =
    [ TestLabel "insertPreservesProvenance" insertPreservesProvenance
    , TestLabel "insertOverwriteDropsOwnExpr" insertOverwriteDropsOwnExpr
    , TestLabel "deriveTracksExpression" deriveTracksExpression
    , TestLabel "deriveManyTracksAll" deriveManyTracksAll
    , TestLabel "deriveOverwriteReplacesExpression" deriveOverwriteReplacesExpression
    , TestLabel "deriveWithExprTracksExpression" deriveWithExprTracksExpression
    , TestLabel "showDerivedEmpty" showDerivedEmpty
    , TestLabel "showDerivedContainsName" showDerivedContainsName
    , TestLabel "semiGroupPreservesLeft" semiGroupPreservesLeft
    , TestLabel "semiGroupPreservesBoth" semiGroupPreservesBoth
    , TestLabel "semiGroupLeftBias" semiGroupLeftBias
    , TestLabel "semiGroupWithEmpty" semiGroupWithEmpty
    , TestLabel "emptyWithSemiGroup" emptyWithSemiGroup
    , TestLabel "horizontalMergePreservesLeft" horizontalMergePreservesLeft
    , TestLabel "horizontalMergePreservesRight" horizontalMergePreservesRight
    , TestLabel "horizontalMergePreservesBoth" horizontalMergePreservesBoth
    ]
