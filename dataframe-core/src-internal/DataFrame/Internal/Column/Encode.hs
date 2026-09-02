{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Dictionary-encode a text (or factor) group key to dense @Int@ codes: each row
gets a first-appearance code @0..card-1@ (NULL reserved) plus the cardinality.

TODO: mchavinda - revise if this module is still necessary.
-}
module DataFrame.Internal.Column.Encode (
    dictEncodeColumn,
    dictEncodeColumnUpTo,
    dictCompactColumn,
    dictMaxCardinality,
) where

import Control.Monad (when)
import Control.Monad.ST (runST)
import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Type.Reflection (typeRep)

import DataFrame.Internal.Algorithms.Hash (
    fnvOffset,
    mixBytes,
    mixText,
    nullSalt,
 )
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.Column.Bitmap (Bitmap, bitmapTestBit)
import DataFrame.Internal.Data.HashTable (htInsert, newHashTable)
import DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    mkOffsets,
    mkSel,
    packedLength,
    packedSlice,
    sliceEqBytes,
 )

{- | Largest distinct-value count we will dictionary-encode. Above this the codes
no longer index a reasonable direct accumulator and the encode pass is pure
overhead, so the caller keeps the plain hash group-by.
-}
dictMaxCardinality :: Int
dictMaxCardinality = 1048576

{- | Dictionary-encode a text-like column to dense first-appearance @Int@ codes,
returning @Just (codes, cardinality)@ (a NULL row gets its own reserved code).
'Nothing' for non-text columns or cardinality above 'dictMaxCardinality'.
-}
dictEncodeColumn :: Column -> Maybe (VU.Vector Int, Int)
dictEncodeColumn = dictEncodeColumnUpTo dictMaxCardinality

{- | Dictionary-encode like 'dictEncodeColumn' but bail to 'Nothing' as soon as
the distinct count would exceed @maxCard@, letting a low-cardinality probe avoid
a full high-cardinality pass.
-}
dictEncodeColumnUpTo :: Int -> Column -> Maybe (VU.Vector Int, Int)
dictEncodeColumnUpTo maxCard (PackedText bm p) = encodePacked maxCard bm p
dictEncodeColumnUpTo maxCard (BoxedColumn bm (v :: V.Vector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> encodeBoxedText maxCard bm v
        Nothing -> Nothing
dictEncodeColumnUpTo _ _ = Nothing

{- | Encode a packed-text column: hash each row's raw UTF-8 bytes (the grouping
'mixBytes'), re-verify byte equality on collisions, assign dense codes in
first-appearance order. A null row hashes 'nullSalt'.
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

{- | The shared code-assignment loop: bucket every row through an open-addressing
table on its precomputed hash, re-verify with @eqAt@ on a hit, assign dense
first-appearance codes. Bails to 'Nothing' once the distinct count exceeds @maxCard@.
-}
buildCodes ::
    Int -> Int -> (Int -> Int) -> (Int -> Int -> Bool) -> Maybe (VU.Vector Int, Int)
buildCodes maxCard n hashAt eqAt
    | n == 0 = Just (VU.empty, 0)
    | otherwise = runST $ do
        ht <- newHashTable (min n (maxCard + 1))
        codes <- VUM.new n
        let go !i !next
                | next > maxCard = pure Nothing
                | i >= n = pure (Just next)
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

dictCompactColumn :: Column -> Column
dictCompactColumn col@(PackedText bm p) =
    case encodePacked dictMaxCardinality bm p of
        Just (codes, card)
            | 2 * card <= packedLength p ->
                PackedText bm (dictPacked p codes card)
        _ -> col
dictCompactColumn col = col

dictPacked :: PackedTextData -> VU.Vector Int -> Int -> PackedTextData
dictPacked p codes card = runST $ do
    let n = VU.length codes
    reps <- VUM.replicate card (-1)
    let findReps !i !remaining
            | remaining <= 0 || i >= n = pure ()
            | otherwise = do
                let c = VU.unsafeIndex codes i
                cur <- VUM.unsafeRead reps c
                if cur < 0
                    then VUM.unsafeWrite reps c i >> findReps (i + 1) (remaining - 1)
                    else findReps (i + 1) remaining
    findReps 0 card
    repsV <- VU.unsafeFreeze reps
    let lens = VU.map (\r -> let (_, _, l) = packedSlice p r in l) repsV
        offs = VU.scanl' (+) 0 lens
        total = VU.last offs
    marr <- A.new (max 1 total)
    let copyRep !c =
            when (c < card) $ do
                let r = VU.unsafeIndex repsV c
                    (arr, o, l) = packedSlice p r
                A.copyI l marr (VU.unsafeIndex offs c) arr o
                copyRep (c + 1)
    copyRep 0
    arr <- A.unsafeFreeze marr
    pure
        ( PackedTextData
            { ptBytes = arr
            , ptOffsets = mkOffsets offs
            , ptSel = Just (mkSel card codes)
            , ptCanonicalSel = True
            }
        )
