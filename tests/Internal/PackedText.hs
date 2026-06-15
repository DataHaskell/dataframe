{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Correctness oracle for the packed-text column variant. Every assertion
builds a 'PackedText' column from raw bytes + offsets and the same data as a
'BoxedColumn Text', then checks display, extraction, equality, ordering,
hashing, groupBy and join all agree. This pins behavioral parity before any
reader emits packed columns.
-}
module Internal.PackedText (tests) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU

import Control.Monad (zipWithM_)
import qualified Data.ByteString as B
import Data.Text.Encoding (encodeUtf8)
import Data.Word (Word8)
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (unsafeGetColumn)
import DataFrame.Internal.PackedText (mkPackedContiguous)
import qualified DataFrame.Operations.Aggregation as Agg
import Test.HUnit

-- Sample string corpus, including empty, ASCII, and multibyte UTF-8 fields.
sampleRows :: [T.Text]
sampleRows =
    [ "apple"
    , "banana"
    , ""
    , "apple"
    , "Zebra"
    , "café"
    , "naïve"
    , "日本語"
    , "banana"
    , "apple"
    ]

-- Build an A.Array from a flat byte list.
arrayFromBytes :: [Word8] -> A.Array
arrayFromBytes ws = A.run $ do
    m <- A.new (length ws)
    zipWithM_ (A.unsafeWrite m) [0 ..] ws
    pure m

-- Build a PackedText column directly from raw UTF-8 bytes + offsets.
packedFromTexts :: [T.Text] -> DI.Column
packedFromTexts ts =
    let bytess = map (B.unpack . encodeUtf8) ts
        flat = concat bytess
        offs = scanl (+) 0 (map length bytess)
        arr = arrayFromBytes flat
     in DI.PackedText Nothing (mkPackedContiguous arr (VU.fromList offs))

boxedFromTexts :: [T.Text] -> DI.Column
boxedFromTexts = DI.fromList

packedCol, boxedCol :: DI.Column
packedCol = packedFromTexts sampleRows
boxedCol = boxedFromTexts sampleRows

-- Single-column dataframes for groupBy / hashing.
packedDf, boxedDf :: D.DataFrame
packedDf = D.fromNamedColumns [("k", packedCol)]
boxedDf = D.fromNamedColumns [("k", boxedCol)]

displayParity :: Test
displayParity =
    TestCase $
        assertEqual "show packed == show boxed" (show boxedCol) (show packedCol)

columnEqParity :: Test
columnEqParity = TestCase $ do
    assertBool "packed == boxed" (packedCol == boxedCol)
    assertBool "boxed == packed" (boxedCol == packedCol)
    assertBool "packed == packed" (packedCol == packedFromTexts sampleRows)

extractionParity :: Test
extractionParity =
    TestCase $
        assertEqual
            "toList @Text packed == boxed"
            (DI.toList @T.Text boxedCol)
            (DI.toList @T.Text packedCol)

orderingParity :: Test
orderingParity = TestCase $ do
    let sortedPacked = D.sortBy [D.Asc (F.col @T.Text "k")] packedDf
        sortedBoxed = D.sortBy [D.Asc (F.col @T.Text "k")] boxedDf
    assertBool "asc sort parity" (sortedPacked == sortedBoxed)
    let descPacked = D.sortBy [D.Desc (F.col @T.Text "k")] packedDf
        descBoxed = D.sortBy [D.Desc (F.col @T.Text "k")] boxedDf
    assertBool "desc sort parity" (descPacked == descBoxed)

hashingParity :: Test
hashingParity =
    TestCase $
        assertEqual
            "row hashes equal"
            (Agg.computeRowHashes [0] boxedDf)
            (Agg.computeRowHashes [0] packedDf)

groupByParity :: Test
groupByParity =
    TestCase $
        assertEqual
            "groupBy + distinct parity"
            (D.distinct boxedDf)
            (D.distinct packedDf)

joinParity :: Test
joinParity = TestCase $ do
    let lhsP = D.fromNamedColumns [("k", packedCol)]
        lhsB = D.fromNamedColumns [("k", boxedCol)]
        rhs = D.fromNamedColumns [("k", boxedFromTexts ["apple", "banana", "日本語"])]
        joinedP = D.innerJoin ["k"] lhsP rhs
        joinedB = D.innerJoin ["k"] lhsB rhs
    assertBool "inner join parity" (joinedP == joinedB)

-- A gather (atIndicesStable) on a packed column stays packed and equals the
-- boxed gather. Indices repeat, reorder, and drop rows.
gatherPreservesPacked :: Test
gatherPreservesPacked = TestCase $ do
    let ixs = VU.fromList [9, 0, 0, 5, 7, 2, 3 :: Int]
        gp = DI.atIndicesStable ixs packedCol
        gb = DI.atIndicesStable ixs boxedCol
    assertBool "gathered packed stays PackedText" (DI.isPackedText gp)
    assertBool "gathered packed == gathered boxed" (gp == gb)
    assertEqual
        "gathered packed toList == boxed"
        (DI.toList @T.Text gb)
        (DI.toList @T.Text gp)

-- A sort on a packed column stays packed and equals the boxed sort.
sortPreservesPacked :: Test
sortPreservesPacked = TestCase $ do
    let sortedP = D.sortBy [D.Asc (F.col @T.Text "k")] packedDf
        sortedB = D.sortBy [D.Asc (F.col @T.Text "k")] boxedDf
    assertBool
        "sorted packed column stays PackedText"
        (DI.isPackedText (unsafeGetColumn "k" sortedP))
    assertBool "sorted packed == sorted boxed" (sortedP == sortedB)

-- A left join (sentinel gather, -1 for missing right rows) on a packed right
-- column stays packed, builds the correct null bitmap, and equals boxed.
leftJoinSentinelPreservesPacked :: Test
leftJoinSentinelPreservesPacked = TestCase $ do
    let lhsP = D.fromNamedColumns [("k", packedCol)]
        lhsB = D.fromNamedColumns [("k", boxedCol)]
        rhsP =
            D.fromNamedColumns
                [ ("k", packedFromTexts ["apple", "banana", "日本語"])
                , ("v", packedFromTexts ["RA", "RB", "RC"])
                ]
        rhsB =
            D.fromNamedColumns
                [ ("k", boxedFromTexts ["apple", "banana", "日本語"])
                , ("v", boxedFromTexts ["RA", "RB", "RC"])
                ]
        joinedP = D.leftJoin ["k"] lhsP rhsP
        joinedB = D.leftJoin ["k"] lhsB rhsB
    assertBool
        "left-joined packed right column stays PackedText"
        (DI.isPackedText (unsafeGetColumn "v" joinedP))
    assertBool "left join packed == boxed" (joinedP == joinedB)

tests :: [Test]
tests =
    [ TestLabel "PackedText display parity" displayParity
    , TestLabel "PackedText equality parity" columnEqParity
    , TestLabel "PackedText extraction parity" extractionParity
    , TestLabel "PackedText ordering parity" orderingParity
    , TestLabel "PackedText hashing parity" hashingParity
    , TestLabel "PackedText groupBy parity" groupByParity
    , TestLabel "PackedText join parity" joinParity
    , TestLabel "PackedText gather preserves packed" gatherPreservesPacked
    , TestLabel "PackedText sort preserves packed" sortPreservesPacked
    , TestLabel
        "PackedText left-join sentinel preserves packed"
        leftJoinSentinelPreservesPacked
    ]
