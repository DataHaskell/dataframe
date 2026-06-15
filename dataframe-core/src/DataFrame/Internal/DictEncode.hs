{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Dictionary-encode a text (or factor) group key to dense @Int@ codes
(research #4 / #5).

A text group key is hashed and bucketed exactly as the grouping hash table does,
but instead of carrying the full grouping output it only assigns each row a dense
first-appearance code @0 .. card-1@ (a NULL row gets its own reserved code) and
reports the cardinality. The intent was to feed those codes to the int fast path
(direct-indexed low-card, or a packed composite key) so a text group key reaches
it.

VERDICT (this round): routing through the codes profiled SLOWER than the existing
hash group-by on EVERY db-benchmark group-by question, so
'DataFrame.Internal.Grouping' does not take the dict path (see 'tryDictGroup'
there). The dict-build is its own hash pass over every row, and the hash
group-by already fuses hashing and grouping into one pass; substituting int codes
adds the encode pass without removing the dominant grouping work. Single low-card
(Q1 id1), single high-card (Q3/Q7 id3) and the composites (Q2 id1:id2, Q10 six
keys) were each measured and all lost.

This module remains a correct, unit-tested building block: it produces the codes
and the cardinality only; the routing decision lives in
'DataFrame.Internal.Grouping'.
-}
module DataFrame.Internal.DictEncode (
    dictEncodeColumn,
    dictEncodeColumnUpTo,
    dictMaxCardinality,
) where

import Control.Monad.ST (runST)
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Bitmap, Column (..), bitmapTestBit)
import DataFrame.Internal.Hash (fnvOffset, mixBytes, mixText, nullSalt)
import DataFrame.Internal.HashTable (htInsert, newHashTable)
import DataFrame.Internal.PackedText (
    PackedTextData,
    packedLength,
    packedSlice,
    sliceEqBytes,
 )

{- | Largest distinct-value count we will dictionary-encode. Above this the codes
no longer index a reasonable direct accumulator and the dict-build pass is pure
overhead, so the caller keeps the plain hash group-by. Matches the direct-group
histogram budget.
-}
dictMaxCardinality :: Int
dictMaxCardinality = 1048576

{- | Dictionary-encode a text-like column to dense first-appearance @Int@ codes.

Returns @Just (codes, cardinality)@ where @codes!i@ is the dense id of row @i@'s
value (a NULL row, when the column is nullable, is assigned its own reserved code
distinct from every present value) and @cardinality@ is the number of distinct
codes used. Returns 'Nothing' for any non-text column or when the cardinality
exceeds 'dictMaxCardinality' (so the caller falls back to the hash group-by).

Only 'PackedText' and boxed 'Data.Text.Text' columns are encoded; everything else
is 'Nothing'.
-}
dictEncodeColumn :: Column -> Maybe (VU.Vector Int, Int)
dictEncodeColumn = dictEncodeColumnUpTo dictMaxCardinality

{- | Dictionary-encode like 'dictEncodeColumn' but bail to 'Nothing' as soon as
the distinct count would exceed @maxCard@. The early bail lets a low-cardinality
PROBE (the single-key direct path) avoid a full high-cardinality pass when the
column turns out to be high-card.
-}
dictEncodeColumnUpTo :: Int -> Column -> Maybe (VU.Vector Int, Int)
dictEncodeColumnUpTo maxCard (PackedText bm p) = encodePacked maxCard bm p
dictEncodeColumnUpTo maxCard (BoxedColumn bm (v :: V.Vector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> encodeBoxedText maxCard bm v
        Nothing -> Nothing
dictEncodeColumnUpTo _ _ = Nothing

{- | Encode a packed-text column. Hashes each row's raw UTF-8 bytes (the same
'mixBytes' the grouping hash uses) and re-verifies byte equality on collisions,
assigning dense codes in first-appearance order. A null row hashes 'nullSalt' and
re-verifies as equal-to-null only.
-}
encodePacked ::
    Int -> Maybe Bitmap -> PackedTextData -> Maybe (VU.Vector Int, Int)
encodePacked maxCard bm p =
    let !n = packedLength p
        valid i = case bm of
            Just b -> bitmapTestBit b i
            Nothing -> True
        hashAt i =
            if valid i
                then let (arr, o, l) = packedSlice p i in mixBytes fnvOffset arr o l
                else nullSalt
        eqAt a b =
            case (valid a, valid b) of
                (True, True) ->
                    let (arrA, oA, lA) = packedSlice p a
                        (arrB, oB, lB) = packedSlice p b
                     in sliceEqBytes arrA oA lA arrB oB lB
                (False, False) -> True
                _ -> False
     in buildCodes maxCard n hashAt eqAt

{- | Encode a boxed 'Data.Text.Text' column, mirroring 'encodePacked' but over
boxed values (used when a user-built Text column is grouped).
-}
encodeBoxedText ::
    Int -> Maybe Bitmap -> V.Vector T.Text -> Maybe (VU.Vector Int, Int)
encodeBoxedText maxCard bm v =
    let !n = V.length v
        valid i = case bm of
            Just b -> bitmapTestBit b i
            Nothing -> True
        hashAt i =
            if valid i then mixText fnvOffset (V.unsafeIndex v i) else nullSalt
        eqAt a b =
            case (valid a, valid b) of
                (True, True) -> V.unsafeIndex v a == V.unsafeIndex v b
                (False, False) -> True
                _ -> False
     in buildCodes maxCard n hashAt eqAt

{- | The shared code-assignment loop. Buckets every row through an
open-addressing table on its precomputed hash, re-verifying the real value with
@eqAt@ on a hash hit, assigning dense first-appearance codes. Bails to 'Nothing'
the moment the distinct count would exceed 'dictMaxCardinality'.
-}
buildCodes ::
    Int -> Int -> (Int -> Int) -> (Int -> Int -> Bool) -> Maybe (VU.Vector Int, Int)
buildCodes maxCard n hashAt eqAt
    | n == 0 = Just (VU.empty, 0)
    | otherwise = runST $ do
        ht <- newHashTable (min n (maxCard + 1))
        codes <- VUM.new n
        let go !i !next
                | i >= n = pure (Just next)
                | next > maxCard = pure Nothing
                | otherwise = do
                    let !h = hashAt i
                    (code, isNew) <- htInsert ht eqAt next i h
                    VUM.unsafeWrite codes i code
                    go (i + 1) (if isNew then next + 1 else next)
        mres <- go 0 0
        case mres of
            Nothing -> pure Nothing
            Just card -> do
                frozen <- VU.unsafeFreeze codes
                pure (Just (frozen, card))
