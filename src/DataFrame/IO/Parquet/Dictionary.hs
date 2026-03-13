{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Dictionary where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import Data.IORef
import Data.Int
import Data.Maybe
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import DataFrame.IO.Parquet.Encoding
import DataFrame.IO.Parquet.Levels
import DataFrame.IO.Parquet.Time
import DataFrame.IO.Parquet.Types
import DataFrame.Internal.Binary (
    littleEndianInt32,
    littleEndianWord32,
    littleEndianWord64,
 )
import qualified DataFrame.Internal.Column as DI
import GHC.Float

dictCardinality :: DictVals -> Int
dictCardinality (DBool ds) = V.length ds
dictCardinality (DInt32 ds) = V.length ds
dictCardinality (DInt64 ds) = V.length ds
dictCardinality (DInt96 ds) = V.length ds
dictCardinality (DFloat ds) = V.length ds
dictCardinality (DDouble ds) = V.length ds
dictCardinality (DText ds) = V.length ds

readDictVals :: ParquetType -> BS.ByteString -> Maybe Int32 -> DictVals
readDictVals PBOOLEAN bs (Just count) = DBool (V.fromList (take (fromIntegral count) $ readPageBool bs))
readDictVals PINT32 bs _ = DInt32 (V.fromList (readPageInt32 bs))
readDictVals PINT64 bs _ = DInt64 (V.fromList (readPageInt64 bs))
readDictVals PINT96 bs _ = DInt96 (V.fromList (readPageInt96Times bs))
readDictVals PFLOAT bs _ = DFloat (V.fromList (readPageFloat bs))
readDictVals PDOUBLE bs _ = DDouble (V.fromList (readPageWord64 bs))
readDictVals PBYTE_ARRAY bs _ = DText (V.fromList (readPageBytes bs))
readDictVals PFIXED_LEN_BYTE_ARRAY bs (Just len) = DText (V.fromList (readPageFixedBytes bs (fromIntegral len)))
readDictVals t _ _ = error $ "Unsupported dictionary type: " ++ show t

readPageInt32 :: BS.ByteString -> [Int32]
readPageInt32 xs
    | BS.null xs = []
    | otherwise = littleEndianInt32 (BS.take 4 xs) : readPageInt32 (BS.drop 4 xs)

readPageWord64 :: BS.ByteString -> [Double]
readPageWord64 xs
    | BS.null xs = []
    | otherwise =
        castWord64ToDouble (littleEndianWord64 (BS.take 8 xs))
            : readPageWord64 (BS.drop 8 xs)

readPageBytes :: BS.ByteString -> [T.Text]
readPageBytes xs
    | BS.null xs = []
    | otherwise =
        let lenBytes = fromIntegral (littleEndianInt32 $ BS.take 4 xs)
            totalBytesRead = lenBytes + 4
         in decodeUtf8Lenient (BS.take lenBytes (BS.drop 4 xs))
                : readPageBytes (BS.drop totalBytesRead xs)

readPageBool :: BS.ByteString -> [Bool]
readPageBool bs =
    concatMap (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7]) (BS.unpack bs)

readPageInt64 :: BS.ByteString -> [Int64]
readPageInt64 xs
    | BS.null xs = []
    | otherwise =
        fromIntegral (littleEndianWord64 (BS.take 8 xs)) : readPageInt64 (BS.drop 8 xs)

readPageFloat :: BS.ByteString -> [Float]
readPageFloat xs
    | BS.null xs = []
    | otherwise =
        castWord32ToFloat (littleEndianWord32 (BS.take 4 xs))
            : readPageFloat (BS.drop 4 xs)

readNInt96Times :: Int -> BS.ByteString -> ([UTCTime], BS.ByteString)
readNInt96Times 0 bs = ([], bs)
readNInt96Times k bs =
    let timestamp96 = BS.take 12 bs
        utcTime = int96ToUTCTime timestamp96
        bs' = BS.drop 12 bs
        (times, rest) = readNInt96Times (k - 1) bs'
     in (utcTime : times, rest)

readPageInt96Times :: BS.ByteString -> [UTCTime]
readPageInt96Times bs
    | BS.null bs = []
    | otherwise =
        let (times, _) = readNInt96Times (BS.length bs `div` 12) bs
         in times

readPageFixedBytes :: BS.ByteString -> Int -> [T.Text]
readPageFixedBytes xs len
    | BS.null xs = []
    | otherwise =
        decodeUtf8Lenient (BS.take len xs) : readPageFixedBytes (BS.drop len xs) len

{- | Dispatch to the right multi-level list stitching function.
For maxRep=1 uses stitchList; for 2/3 uses stitchList2/3 with computed thresholds.
Threshold formula: defT_r = maxDef - 2*(maxRep - r).
-}
stitchForRepBool :: Int -> Int -> [Int] -> [Int] -> [Bool] -> DI.Column
stitchForRepBool maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepInt32 :: Int -> Int -> [Int] -> [Int] -> [Int32] -> DI.Column
stitchForRepInt32 maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepInt64 :: Int -> Int -> [Int] -> [Int] -> [Int64] -> DI.Column
stitchForRepInt64 maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepUTCTime :: Int -> Int -> [Int] -> [Int] -> [UTCTime] -> DI.Column
stitchForRepUTCTime maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepFloat :: Int -> Int -> [Int] -> [Int] -> [Float] -> DI.Column
stitchForRepFloat maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepDouble :: Int -> Int -> [Int] -> [Int] -> [Double] -> DI.Column
stitchForRepDouble maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

stitchForRepText :: Int -> Int -> [Int] -> [Int] -> [T.Text] -> DI.Column
stitchForRepText maxRep maxDef rep def vals = case maxRep of
    2 -> DI.fromList (stitchList2 (maxDef - 2) maxDef rep def vals)
    3 -> DI.fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef rep def vals)
    _ -> DI.fromList (stitchList maxDef rep def vals)

{- | Build a Column from a dictionary + index vector + def levels in a single
mutable-vector pass, avoiding the intermediate [a] and [Maybe a] lists.
For maxRep > 0 (list columns) the caller must use the rep-stitching path instead.
-}
applyDictToColumn ::
    (DI.Columnable a, DI.Columnable (Maybe a)) =>
    V.Vector a ->
    VU.Vector Int ->
    Int -> -- maxDef
    [Int] -> -- defLvls
    IO DI.Column
applyDictToColumn dict idxs maxDef defLvls
    | maxDef == 0 = do
        -- All rows are required; no nullability to check.
        let n = VU.length idxs
        pure $ DI.fromVector (V.generate n (\i -> dict V.! (idxs VU.! i)))
    | otherwise = do
        let n = length defLvls
        mv <- VM.new n
        hasNullRef <- newIORef False
        let go _ _ [] = pure ()
            go !i !j (d : ds)
                | d == maxDef = do
                    VM.write mv i (Just (dict V.! (idxs VU.! j)))
                    go (i + 1) (j + 1) ds
                | otherwise = do
                    writeIORef hasNullRef True
                    VM.write mv i Nothing
                    go (i + 1) j ds
        go 0 0 defLvls
        vec <- V.freeze mv
        hasNull <- readIORef hasNullRef
        pure $
            if hasNull
                then DI.fromVector vec -- VB.Vector (Maybe a) → OptionalColumn
                else DI.fromVector (V.map fromJust vec) -- VB.Vector a → BoxedColumn/UnboxedColumn

decodeDictV1 ::
    Maybe DictVals ->
    Int ->
    Int ->
    [Int] ->
    [Int] ->
    Int ->
    BS.ByteString ->
    IO DI.Column
decodeDictV1 dictValsM maxDef maxRep repLvls defLvls nPresent bytes =
    case dictValsM of
        Nothing -> error "Dictionary-encoded page but dictionary is missing"
        Just dictVals ->
            let (idxs, _rest) = decodeDictIndicesV1 nPresent (dictCardinality dictVals) bytes
             in do
                    when (VU.length idxs /= nPresent) $
                        error $
                            "dict index count mismatch: got "
                                ++ show (VU.length idxs)
                                ++ ", expected "
                                ++ show nPresent
                    if maxRep > 0
                        then do
                            case dictVals of
                                DBool ds ->
                                    pure $
                                        stitchForRepBool maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                                DInt32 ds ->
                                    pure $
                                        stitchForRepInt32 maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                                DInt64 ds ->
                                    pure $
                                        stitchForRepInt64 maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                                DInt96 ds ->
                                    pure $
                                        stitchForRepUTCTime
                                            maxRep
                                            maxDef
                                            repLvls
                                            defLvls
                                            (map (ds V.!) (VU.toList idxs))
                                DFloat ds ->
                                    pure $
                                        stitchForRepFloat maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                                DDouble ds ->
                                    pure $
                                        stitchForRepDouble maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                                DText ds ->
                                    pure $
                                        stitchForRepText maxRep maxDef repLvls defLvls (map (ds V.!) (VU.toList idxs))
                        else case dictVals of
                            -- Fast path: unboxable types, no nulls — one allocation via VU.map
                            DInt32 ds | maxDef == 0 -> pure $ DI.fromUnboxedVector (VU.map (ds V.!) idxs)
                            DInt64 ds | maxDef == 0 -> pure $ DI.fromUnboxedVector (VU.map (ds V.!) idxs)
                            DFloat ds | maxDef == 0 -> pure $ DI.fromUnboxedVector (VU.map (ds V.!) idxs)
                            DDouble ds | maxDef == 0 -> pure $ DI.fromUnboxedVector (VU.map (ds V.!) idxs)
                            DBool ds -> applyDictToColumn ds idxs maxDef defLvls
                            DInt32 ds -> applyDictToColumn ds idxs maxDef defLvls
                            DInt64 ds -> applyDictToColumn ds idxs maxDef defLvls
                            DInt96 ds -> applyDictToColumn ds idxs maxDef defLvls
                            DFloat ds -> applyDictToColumn ds idxs maxDef defLvls
                            DDouble ds -> applyDictToColumn ds idxs maxDef defLvls
                            DText ds -> applyDictToColumn ds idxs maxDef defLvls

toMaybeInt32 :: Int -> [Int] -> [Int32] -> DI.Column
toMaybeInt32 maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeDouble :: Int -> [Int] -> [Double] -> DI.Column
toMaybeDouble maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeText :: Int -> [Int] -> [T.Text] -> DI.Column
toMaybeText maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe "") filled)
            else DI.fromList filled

toMaybeBool :: Int -> [Int] -> [Bool] -> DI.Column
toMaybeBool maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe False) filled)
            else DI.fromList filled

toMaybeInt64 :: Int -> [Int] -> [Int64] -> DI.Column
toMaybeInt64 maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeFloat :: Int -> [Int] -> [Float] -> DI.Column
toMaybeFloat maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0.0) filled)
            else DI.fromList filled

toMaybeUTCTime :: Int -> [Int] -> [UTCTime] -> DI.Column
toMaybeUTCTime maxDef def times =
    let filled = stitchNullable maxDef def times
        defaultTime = UTCTime (fromGregorian 1970 1 1) (secondsToDiffTime 0)
     in if all isJust filled
            then DI.fromList (map (fromMaybe defaultTime) filled)
            else DI.fromList filled
