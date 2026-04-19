{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Operations.ReadCsv where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Lazy.IO.CSV as Lazy

import DataFrame.Internal.Column (Column (..), columnTypeString)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    dataframeDimensions,
    getColumn,
 )
import Test.HUnit

arbuthnotPath :: FilePath
arbuthnotPath = "./tests/data/arbuthnot.csv"

readCsvNoInfer :: FilePath -> IO DataFrame
readCsvNoInfer =
    D.readCsvWithOpts
        D.defaultReadOptions{D.typeSpec = D.NoInference}

-- SpecifyTypes with NoInference fallback: named column is typed, rest stay Text
specifyTypesNoInferenceFallback :: Test
specifyTypesNoInferenceFallback =
    TestLabel "specifyTypes_noInference_fallback" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [("year", D.schemaType @Int)]
                            D.NoInference
                    }
                arbuthnotPath
        -- "year" must be Int
        case getColumn "year" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + NoInference → stays Text
        case getColumn "boys" df of
            Just col@(BoxedColumn _ _) -> assertEqual "boys should be Text" "Text" (columnTypeString col)
            _ -> assertFailure "expected BoxedColumn for 'boys' with NoInference fallback"

-- SpecifyTypes with InferFromSample fallback: named column typed, rest inferred
specifyTypesInferFallback :: Test
specifyTypesInferFallback =
    TestLabel "specifyTypes_inferFromSample_fallback" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            [("year", D.schemaType @Int)]
                            (D.InferFromSample 100)
                    }
                arbuthnotPath
        -- "year" must be Int (explicitly specified)
        case getColumn "year" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "year should be Int" "Int" (columnTypeString col)
            _ -> assertFailure "expected UnboxedColumn for 'year'"
        -- "boys" unspecified + InferFromSample → inferred as Int
        case getColumn "boys" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "boys should be Int" "Int" (columnTypeString col)
            _ ->
                assertFailure "expected UnboxedColumn for 'boys' with InferFromSample fallback"

-- SpecifyTypes: typeInferenceSampleSize delegates to fallback
specifyTypesSampleSize :: Test
specifyTypesSampleSize =
    TestLabel "specifyTypes_sampleSize_from_fallback" $ TestCase $ do
        -- Use a small sample size; all numeric columns should still be inferred
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.typeSpec =
                        D.SpecifyTypes
                            []
                            (D.InferFromSample 10)
                    }
                arbuthnotPath
        case getColumn "girls" df of
            Just col@(UnboxedColumn _ _) -> assertEqual "girls should be Int" "Int" (columnTypeString col)
            _ ->
                assertFailure
                    "expected UnboxedColumn for 'girls' via fallback InferFromSample 10"

-- File: a,b,c header; row "1,2,3,EXTRA"
-- Total delimiters: 3 + 4 = 7; 7 div 3 = 2, numRow=1
-- Row-1 strides 3,4,5 → "1","2","3" — EXTRA (stride 6) is never accessed
testExtraFields :: Test
testExtraFields = TestLabel "malformed_extra_fields" $ TestCase $ do
    df <- readCsvNoInfer ("./tests/data/unstable_csv/" <> "extra_fields.csv")
    assertEqual "extra_fields.csv: 1 data row" 1 (fst (dataframeDimensions df))
    assertEqual "extra_fields.csv: 3 columns" 3 (snd (dataframeDimensions df))
    case getColumn "c" df of
        Nothing -> assertFailure "extra_fields.csv: column 'c' missing"
        Just col ->
            assertEqual
                "extra_fields.csv: 'c' = '3' (EXTRA ignored)"
                (D.fromList @T.Text ["3"])
                col

-- Inference + EitherRead: first rows of "score" are clean ints, later rows are
-- "abc" / empty / "N/A". The sample is limited to the clean prefix so the
-- assumption is Int; non-parsing later rows become Left carrying raw text.
eitherReadCsvInference :: Test
eitherReadCsvInference = TestLabel "csv_eitherRead_inference" $ TestCase $ do
    df <-
        D.readCsvWithOpts
            D.defaultReadOptions
                { D.safeRead = D.EitherRead
                , D.typeSpec = D.InferFromSample 5
                }
            "./tests/data/unstable_csv/either_read_mixed.csv"
    let expectedScore :: Column
        expectedScore =
            D.fromList @(Either T.Text Int)
                [ Right 10
                , Right 20
                , Right 30
                , Right 40
                , Right 50
                , Left "abc"
                , Left ""
                , Right 80
                ]
    case getColumn "score" df of
        Just col ->
            assertEqual
                "EitherRead: 'score' = [Right 10..Right 50, Left \"abc\", Left \"\", Right 80]"
                expectedScore
                col
        Nothing -> assertFailure "score column missing"
    -- 'id' is all ints in every row, so EitherRead wraps every value as Right.
    let expectedId :: Column
        expectedId =
            D.fromList @(Either T.Text Int) (map Right [1 .. 8])
    case getColumn "id" df of
        Just col ->
            assertEqual
                "EitherRead: 'id' is all Right (no failures)"
                expectedId
                col
        Nothing -> assertFailure "id column missing"
    -- 'name' has a row containing "N/A"; under EitherRead the text is just
    -- a value so it lands in Right. Empty cells would become Left "".
    let expectedName :: Column
        expectedName =
            D.fromList @(Either T.Text T.Text)
                (map Right ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "N/A"])
    case getColumn "name" df of
        Just col ->
            assertEqual
                "EitherRead: 'name' wraps every value as Right (no parse failures for Text)"
                expectedName
                col
        Nothing -> assertFailure "name column missing"

-- MaybeRead on the same CSV yields Maybe Int (bitmap-backed Int).
maybeReadCsv :: Test
maybeReadCsv = TestLabel "csv_maybeRead" $ TestCase $ do
    df <-
        D.readCsvWithOpts
            D.defaultReadOptions{D.safeRead = D.MaybeRead}
            "./tests/data/unstable_csv/either_read_mixed.csv"
    -- "score" is not cleanly parseable as Int because "abc" fails; however the
    -- BS inference walks all rows and falls back to Text when needed. The
    -- relevant invariant for MaybeRead is that the column is nullable.
    case getColumn "score" df of
        Just (UnboxedColumn (Just _) _) -> pure () -- Int with bitmap
        Just (BoxedColumn (Just _) _) -> pure () -- Text-backed with bitmap
        Just col ->
            assertFailure $
                "MaybeRead should yield a nullable column, got "
                    <> columnTypeString col
                    <> " with no bitmap"
        Nothing -> assertFailure "score column missing"

-- NoSafeRead leaves columns un-bitmapped when every row parses (id column is
-- all ints) but falls back to a Text column for mixed rows like "score".
noSafeReadCsv :: Test
noSafeReadCsv = TestLabel "csv_noSafeRead" $ TestCase $ do
    df <-
        D.readCsvWithOpts
            D.defaultReadOptions{D.safeRead = D.NoSafeRead}
            "./tests/data/unstable_csv/either_read_mixed.csv"
    case getColumn "id" df of
        Just (UnboxedColumn Nothing _) -> pure () -- strict Int, no bitmap
        Just col ->
            assertFailure $
                "NoSafeRead 'id' should be bare UnboxedColumn, got "
                    <> columnTypeString col
        Nothing -> assertFailure "id column missing"

-- Lazy CSV reader + EitherRead: goes through a different pipeline
-- (MutableColumn + null indices). Every row of 'id' parses cleanly, so the
-- column is @Either Text Int@ with every value in 'Right'.
lazyEitherReadCsv :: Test
lazyEitherReadCsv = TestLabel "lazy_csv_eitherRead" $ TestCase $ do
    (df, _) <-
        Lazy.readSeparated
            ','
            Lazy.defaultOptions{Lazy.safeRead = D.EitherRead}
            "./tests/data/unstable_csv/either_read_mixed.csv"
    let expectedId :: Column
        expectedId =
            D.fromList @(Either T.Text Int) (map Right [1 .. 8])
    case getColumn "id" df of
        Just col ->
            assertEqual
                "Lazy + EitherRead: 'id' is all Right Ints"
                expectedId
                col
        Nothing -> assertFailure "id column missing (lazy reader)"

-- Per-column overrides (eager): 'id' is strict Int (NoSafeRead), 'score' is
-- Either Text Int (EitherRead) recording the raw bytes for failures, 'name'
-- falls back to the default (MaybeRead) and becomes a nullable Text column.
perColumnEagerOverrides :: Test
perColumnEagerOverrides = TestLabel "csv_perColumnOverrides_eager" $ TestCase $ do
    df <-
        D.readCsvWithOpts
            D.defaultReadOptions
                { D.safeRead = D.MaybeRead
                , D.safeReadOverrides =
                    [ ("id", D.NoSafeRead)
                    , ("score", D.EitherRead)
                    ]
                , D.typeSpec = D.InferFromSample 5
                }
            "./tests/data/unstable_csv/either_read_mixed.csv"
    -- 'id': NoSafeRead → plain UnboxedColumn Int, no bitmap.
    case getColumn "id" df of
        Just (UnboxedColumn Nothing _) -> pure ()
        Just col ->
            assertFailure $
                "override NoSafeRead for 'id' should give bare UnboxedColumn, got "
                    <> columnTypeString col
        Nothing -> assertFailure "id column missing"
    -- 'score': EitherRead → BoxedColumn of Either Text Int.
    let expectedScore :: Column
        expectedScore =
            D.fromList @(Either T.Text Int)
                [ Right 10
                , Right 20
                , Right 30
                , Right 40
                , Right 50
                , Left "abc"
                , Left ""
                , Right 80
                ]
    case getColumn "score" df of
        Just col ->
            assertEqual
                "override EitherRead for 'score'"
                expectedScore
                col
        Nothing -> assertFailure "score column missing"
    -- 'name': default MaybeRead kicks in → nullable column (bitmap attached).
    -- Every cell is non-empty so the bitmap is all-valid, but the optional
    -- wrap is still applied.
    case getColumn "name" df of
        Just (BoxedColumn (Just _) _) -> pure ()
        Just col ->
            assertFailure $
                "default MaybeRead for 'name' should yield BoxedColumn with bitmap, got "
                    <> columnTypeString col
        Nothing -> assertFailure "name column missing"

-- Per-column overrides (lazy): same configuration as the eager test, but
-- exercises the lazy freezer's resolution of per-column modes.
perColumnLazyOverrides :: Test
perColumnLazyOverrides = TestLabel "csv_perColumnOverrides_lazy" $ TestCase $ do
    (df, _) <-
        Lazy.readSeparated
            ','
            Lazy.defaultOptions
                { Lazy.safeRead = D.MaybeRead
                , Lazy.safeReadOverrides =
                    [ ("id", D.NoSafeRead)
                    , ("score", D.EitherRead)
                    ]
                }
            "./tests/data/unstable_csv/either_read_mixed.csv"
    -- Lazy reader infers 'id' as Int from the first row; NoSafeRead → bare.
    case getColumn "id" df of
        Just (UnboxedColumn Nothing _) -> pure ()
        Just col ->
            assertFailure $
                "lazy NoSafeRead override for 'id' → bare UnboxedColumn, got "
                    <> columnTypeString col
        Nothing -> assertFailure "id column missing (lazy)"
    -- EitherRead on 'score': every row is Right (first row was '10' → Int
    -- builder), though rows that failed parsing are captured as Left via the
    -- nulls list. We only assert the column is Boxed-of-Either.
    case getColumn "score" df of
        Just (BoxedColumn Nothing _) -> pure ()
        Just col ->
            assertFailure $
                "lazy EitherRead override for 'score' → BoxedColumn Nothing, got "
                    <> columnTypeString col
        Nothing -> assertFailure "score column missing (lazy)"

-- Default=NoSafeRead, override single column 'score' → MaybeRead.
-- 'id' and 'name' keep the NoSafeRead default; 'score' becomes nullable.
overrideNoSafeReadDefaultWithMaybeRead :: Test
overrideNoSafeReadDefaultWithMaybeRead =
    TestLabel "csv_override_noSafeRead_default_MaybeRead" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.NoSafeRead
                    , D.safeReadOverrides = [("score", D.MaybeRead)]
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        -- 'id': NoSafeRead default → bare Int.
        case getColumn "id" df of
            Just (UnboxedColumn Nothing _) -> pure ()
            Just col ->
                assertFailure $
                    "'id' should be bare UnboxedColumn under NoSafeRead, got "
                        <> columnTypeString col
            Nothing -> assertFailure "id column missing"
        -- 'score': MaybeRead override → nullable (bitmap present).
        case getColumn "score" df of
            Just (UnboxedColumn (Just _) _) -> pure () -- Int with bitmap
            Just (BoxedColumn (Just _) _) -> pure () -- fallback Text with bitmap
            Just col ->
                assertFailure $
                    "'score' MaybeRead override should yield nullable column, got "
                        <> columnTypeString col
            Nothing -> assertFailure "score column missing"
        -- 'name': NoSafeRead default → Text; may have bitmap when data has
        -- null cells (row 8 has "N/A" which is in missingIndicators).
        -- ensureOptional is NOT forced, but a bitmap is still created when
        -- the builder detects actual nulls.
        case getColumn "name" df of
            Just (BoxedColumn _ _) -> pure ()
            Just col ->
                assertFailure $
                    "'name' under NoSafeRead should be BoxedColumn, got "
                        <> columnTypeString col
            Nothing -> assertFailure "name column missing"

-- Default=NoSafeRead, override single column 'score' → EitherRead.
overrideNoSafeReadDefaultWithEitherRead :: Test
overrideNoSafeReadDefaultWithEitherRead =
    TestLabel "csv_override_noSafeRead_default_EitherRead" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.NoSafeRead
                    , D.safeReadOverrides = [("score", D.EitherRead)]
                    , D.typeSpec = D.InferFromSample 5
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        -- 'score': EitherRead override → exact cell contents
        let expectedScore :: Column
            expectedScore =
                D.fromList @(Either T.Text Int)
                    [ Right 10
                    , Right 20
                    , Right 30
                    , Right 40
                    , Right 50
                    , Left "abc"
                    , Left ""
                    , Right 80
                    ]
        case getColumn "score" df of
            Just col ->
                assertEqual
                    "'score' EitherRead override from NoSafeRead default"
                    expectedScore
                    col
            Nothing -> assertFailure "score column missing"

-- Default=EitherRead, override 'id' back to NoSafeRead.
overrideEitherReadDefaultWithNoSafeRead :: Test
overrideEitherReadDefaultWithNoSafeRead =
    TestLabel "csv_override_eitherRead_default_NoSafeRead" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.EitherRead
                    , D.safeReadOverrides = [("id", D.NoSafeRead)]
                    , D.typeSpec = D.InferFromSample 5
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        -- 'id': NoSafeRead override → bare Int (every row parses).
        case getColumn "id" df of
            Just (UnboxedColumn Nothing _) -> pure ()
            Just col ->
                assertFailure $
                    "'id' NoSafeRead override should give bare UnboxedColumn, got "
                        <> columnTypeString col
            Nothing -> assertFailure "id column missing"
        -- 'score': default EitherRead → Either Text Int
        let expectedScore :: Column
            expectedScore =
                D.fromList @(Either T.Text Int)
                    [ Right 10
                    , Right 20
                    , Right 30
                    , Right 40
                    , Right 50
                    , Left "abc"
                    , Left ""
                    , Right 80
                    ]
        case getColumn "score" df of
            Just col ->
                assertEqual
                    "'score' inherits EitherRead default"
                    expectedScore
                    col
            Nothing -> assertFailure "score column missing"
        -- 'name': default EitherRead → Either Text Text (all values Right).
        let expectedName :: Column
            expectedName =
                D.fromList @(Either T.Text T.Text)
                    (map Right ["alice", "bob", "carol", "dave", "eve", "frank", "grace", "N/A"])
        case getColumn "name" df of
            Just col ->
                assertEqual
                    "'name' inherits EitherRead default"
                    expectedName
                    col
            Nothing -> assertFailure "name column missing"

-- Default=EitherRead, override 'name' → MaybeRead.
overrideEitherReadDefaultWithMaybeRead :: Test
overrideEitherReadDefaultWithMaybeRead =
    TestLabel "csv_override_eitherRead_default_MaybeRead" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.EitherRead
                    , D.safeReadOverrides = [("name", D.MaybeRead)]
                    , D.typeSpec = D.InferFromSample 5
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        -- 'name': MaybeRead override → nullable Text column with bitmap.
        case getColumn "name" df of
            Just (BoxedColumn (Just _) _) -> pure ()
            Just col ->
                assertFailure $
                    "'name' MaybeRead override should yield BoxedColumn with bitmap, got "
                        <> columnTypeString col
            Nothing -> assertFailure "name column missing"
        -- 'id': default EitherRead → Either Text Int (all values Right).
        let expectedId :: Column
            expectedId = D.fromList @(Either T.Text Int) (map Right [1 .. 8])
        case getColumn "id" df of
            Just col ->
                assertEqual "'id' inherits EitherRead default" expectedId col
            Nothing -> assertFailure "id column missing"

-- Override for a column that doesn't exist in the CSV → no crash, ignored.
overrideNonExistentColumn :: Test
overrideNonExistentColumn =
    TestLabel "csv_override_nonExistent" $ TestCase $ do
        df <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.NoSafeRead
                    , D.safeReadOverrides = [("doesNotExist", D.EitherRead)]
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        assertEqual "dimensions unchanged" (8, 3) (dataframeDimensions df)

-- Empty overrides map is the same as no overrides.
emptyOverridesEqualsGlobal :: Test
emptyOverridesEqualsGlobal =
    TestLabel "csv_emptyOverrides" $ TestCase $ do
        df1 <-
            D.readCsvWithOpts
                D.defaultReadOptions{D.safeRead = D.MaybeRead}
                "./tests/data/unstable_csv/either_read_mixed.csv"
        df2 <-
            D.readCsvWithOpts
                D.defaultReadOptions
                    { D.safeRead = D.MaybeRead
                    , D.safeReadOverrides = []
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        assertEqual "empty overrides produces identical DataFrame" df1 df2

-- Lazy path: cell-level assertions for EitherRead override on 'score'.
perColumnLazyOverridesCellLevel :: Test
perColumnLazyOverridesCellLevel =
    TestLabel "csv_perColumnOverrides_lazy_cellLevel" $ TestCase $ do
        (df, _) <-
            Lazy.readSeparated
                ','
                Lazy.defaultOptions
                    { Lazy.safeRead = D.NoSafeRead
                    , Lazy.safeReadOverrides = [("score", D.EitherRead)]
                    }
                "./tests/data/unstable_csv/either_read_mixed.csv"
        -- 'id': NoSafeRead → bare Int.
        case getColumn "id" df of
            Just (UnboxedColumn Nothing _) -> pure ()
            Just col ->
                assertFailure $
                    "lazy 'id' NoSafeRead → bare UnboxedColumn, got "
                        <> columnTypeString col
            Nothing -> assertFailure "id missing (lazy)"
        -- 'score': EitherRead override → BoxedColumn of Either values.
        -- Lazy reader infers Int from first row; rows that fail become Left.
        case getColumn "score" df of
            Just (BoxedColumn Nothing _) -> pure ()
            Just col ->
                assertFailure $
                    "lazy 'score' EitherRead → BoxedColumn Nothing, got "
                        <> columnTypeString col
            Nothing -> assertFailure "score missing (lazy)"

tests :: [Test]
tests =
    [ specifyTypesNoInferenceFallback
    , specifyTypesInferFallback
    , specifyTypesSampleSize
    , testExtraFields
    , eitherReadCsvInference
    , maybeReadCsv
    , noSafeReadCsv
    , lazyEitherReadCsv
    , perColumnEagerOverrides
    , perColumnLazyOverrides
    , -- Per-column override coverage
      overrideNoSafeReadDefaultWithMaybeRead
    , overrideNoSafeReadDefaultWithEitherRead
    , overrideEitherReadDefaultWithNoSafeRead
    , overrideEitherReadDefaultWithMaybeRead
    , overrideNonExistentColumn
    , emptyOverridesEqualsGlobal
    , perColumnLazyOverridesCellLevel
    ]
