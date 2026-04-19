module DataFrame.IO.Unstable.Parquet.Levels (
    -- Level readers
    readLevelsV1V,
    readLevelsV2V,
    -- Stitch functions
    stitchNullableV,
    stitchListV,
    stitchList2V,
    stitchList3V,
) where

import Control.Monad.ST (runST)
import qualified Data.ByteString as BS
import Data.Int (Int32)
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word32)
import DataFrame.IO.Parquet.Encoding (bitWidthForMaxLevel)
import DataFrame.IO.Unstable.Parquet.Encoding (decodeRLEBitPackedHybridV)
import DataFrame.Internal.Binary (littleEndianWord32)

-- ---------------------------------------------------------------------------
-- Level readers
-- ---------------------------------------------------------------------------

readLevelsV1V ::
    -- | Total number of values in the page
    Int ->
    -- | maxDefinitionLevel
    Int ->
    -- | maxRepetitionLevel
    Int ->
    BS.ByteString ->
    (VU.Vector Int, VU.Vector Int, Int, BS.ByteString)
readLevelsV1V n maxDef maxRep bs =
    let bwRep = bitWidthForMaxLevel maxRep
        bwDef = bitWidthForMaxLevel maxDef
        (repVec, afterRep) = decodeLevelBlock bwRep n bs
        (defVec, afterDef) = decodeLevelBlock bwDef n afterRep
        nPresent = VU.foldl' (\acc d -> acc + fromEnum (d == maxDef)) 0 defVec
     in (defVec, repVec, nPresent, afterDef)
  where
    decodeLevelBlock 0 n' buf = (VU.replicate n' 0, buf)
    decodeLevelBlock bw n' buf =
        let blockLen = fromIntegral (littleEndianWord32 (BS.take 4 buf)) :: Int
            blockData = BS.take blockLen (BS.drop 4 buf)
            after = BS.drop (4 + blockLen) buf
            (raw, _) = decodeRLEBitPackedHybridV bw n' blockData
         in (VU.map (fromIntegral :: Word32 -> Int) raw, after)

readLevelsV2V ::
    -- | Total number of values
    Int ->
    -- | maxDefinitionLevel
    Int ->
    -- | maxRepetitionLevel
    Int ->
    -- | Repetition-level byte length (from page header)
    Int32 ->
    -- | Definition-level byte length (from page header)
    Int32 ->
    BS.ByteString ->
    (VU.Vector Int, VU.Vector Int, Int, BS.ByteString)
readLevelsV2V n maxDef maxRep repLen defLen bs =
    let (repBytes, afterRepBytes) = BS.splitAt (fromIntegral repLen) bs
        (defBytes, afterDefBytes) = BS.splitAt (fromIntegral defLen) afterRepBytes
        bwRep = bitWidthForMaxLevel maxRep
        bwDef = bitWidthForMaxLevel maxDef
        repVec
            | bwRep == 0 = VU.replicate n 0
            | otherwise =
                let (raw, _) = decodeRLEBitPackedHybridV bwRep n repBytes
                 in VU.map (fromIntegral :: Word32 -> Int) raw
        defVec
            | bwDef == 0 = VU.replicate n 0
            | otherwise =
                let (raw, _) = decodeRLEBitPackedHybridV bwDef n defBytes
                 in VU.map (fromIntegral :: Word32 -> Int) raw
        nPresent = VU.foldl' (\acc d -> acc + fromEnum (d == maxDef)) 0 defVec
     in (defVec, repVec, nPresent, afterDefBytes)

{- | Build a full-length vector of @Maybe a@ from definition levels and a
compact present-values vector.

For each index @i@:

  * @defVec VU.! i == maxDef@  →  @Just (values VB.! j)@, advancing @j@
  * @defVec VU.! i <  maxDef@  →  @Nothing@

The length of the result equals @VU.length defVec@.
-}
stitchNullableV ::
    Int ->
    VU.Vector Int ->
    VB.Vector a ->
    VB.Vector (Maybe a)
stitchNullableV maxDef defVec values = runST $ do
    let n = VU.length defVec
    mv <- VBM.replicate n Nothing
    let go i j
            | i >= n = pure ()
            | VU.unsafeIndex defVec i == maxDef = do
                VBM.unsafeWrite mv i (Just (VB.unsafeIndex values j))
                go (i + 1) (j + 1)
            | otherwise = go (i + 1) j
    go 0 0
    VB.unsafeFreeze mv

{- | Stitch a singly-nested list column (@maxRep == 1@) from vector-format
definition and repetition levels plus a compact present-values vector.
Returns one @Maybe [Maybe a]@ per top-level row.
-}
stitchListV ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe a]]
stitchListV maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 (pairWithValsV maxDef repVec defVec values))
  where
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow grp = Just [v | (_, _, v) <- grp]

{- | Stitch a doubly-nested list column (@maxRep == 2@).
@defT1@ is the def threshold at which the depth-1 element is present.
-}
stitchList2V ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe [Maybe a]]]
stitchList2V defT1 maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 triplets)
  where
    triplets = pairWithValsV maxDef repVec defVec values
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow row = Just (map toOuter (splitAtRepBound 1 row))
    toOuter [] = Nothing
    toOuter ((_, d, _) : _) | d < defT1 = Nothing
    toOuter outer = Just (map toLeaf (splitAtRepBound 2 outer))
    toLeaf [] = Nothing
    toLeaf ((_, _, v) : _) = v

{- | Stitch a triply-nested list column (@maxRep == 3@).
@defT1@ and @defT2@ are the def thresholds for depth-1 and depth-2
elements respectively.
-}
stitchList3V ::
    Int ->
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe [Maybe [Maybe a]]]]
stitchList3V defT1 defT2 maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 triplets)
  where
    triplets = pairWithValsV maxDef repVec defVec values
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow row = Just (map toOuter (splitAtRepBound 1 row))
    toOuter [] = Nothing
    toOuter ((_, d, _) : _) | d < defT1 = Nothing
    toOuter outer = Just (map toMiddle (splitAtRepBound 2 outer))
    toMiddle [] = Nothing
    toMiddle ((_, d, _) : _) | d < defT2 = Nothing
    toMiddle middle = Just (map toLeaf (splitAtRepBound 3 middle))
    toLeaf [] = Nothing
    toLeaf ((_, _, v) : _) = v

-- ---------------------------------------------------------------------------
-- Internal helpers
-- ---------------------------------------------------------------------------

{- | Zip rep and def level vectors with a present-values vector, tagging each
position as @Just value@ (when @def == maxDef@) or @Nothing@.
Returns a flat list of @(rep, def, Maybe a)@ triplets for row-splitting.
-}
pairWithValsV ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [(Int, Int, Maybe a)]
pairWithValsV maxDef repVec defVec values = go 0 0
  where
    n = VU.length defVec
    go i j
        | i >= n = []
        | otherwise =
            let r = VU.unsafeIndex repVec i
                d = VU.unsafeIndex defVec i
             in if d == maxDef
                    then (r, d, Just (VB.unsafeIndex values j)) : go (i + 1) (j + 1)
                    else (r, d, Nothing) : go (i + 1) j

{- | Group a flat triplet list into rows.
A new group begins whenever @rep <= bound@.
-}
splitAtRepBound :: Int -> [(Int, Int, Maybe a)] -> [[(Int, Int, Maybe a)]]
splitAtRepBound _ [] = []
splitAtRepBound bound (t : ts) =
    let (rest, remaining) = span (\(r, _, _) -> r > bound) ts
     in (t : rest) : splitAtRepBound bound remaining
