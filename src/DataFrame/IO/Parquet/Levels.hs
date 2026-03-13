module DataFrame.IO.Parquet.Levels where

import qualified Data.ByteString as BS
import Data.Int
import Data.List
import qualified Data.Text as T

import DataFrame.IO.Parquet.Encoding
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types
import DataFrame.Internal.Binary (littleEndianWord32)

readLevelsV1 ::
    Int -> Int -> Int -> BS.ByteString -> ([Int], [Int], BS.ByteString)
readLevelsV1 n maxDef maxRep bs =
    let bwDef = bitWidthForMaxLevel maxDef
        bwRep = bitWidthForMaxLevel maxRep

        (repLvls, afterRep) =
            if bwRep == 0
                then (replicate n 0, bs)
                else
                    let repLength = littleEndianWord32 (BS.take 4 bs)
                        repData = BS.take (fromIntegral repLength) (BS.drop 4 bs)
                        afterRepData = BS.drop (4 + fromIntegral repLength) bs
                        (repVals, _) = decodeRLEBitPackedHybrid bwRep n repData
                     in (map fromIntegral repVals, afterRepData)

        (defLvls, afterDef) =
            if bwDef == 0
                then (replicate n 0, afterRep)
                else
                    let defLength = littleEndianWord32 (BS.take 4 afterRep)
                        defData = BS.take (fromIntegral defLength) (BS.drop 4 afterRep)
                        afterDefData = BS.drop (4 + fromIntegral defLength) afterRep
                        (defVals, _) = decodeRLEBitPackedHybrid bwDef n defData
                     in (map fromIntegral defVals, afterDefData)
     in (defLvls, repLvls, afterDef)

readLevelsV2 ::
    Int ->
    Int ->
    Int ->
    Int32 ->
    Int32 ->
    BS.ByteString ->
    ([Int], [Int], BS.ByteString)
readLevelsV2 n maxDef maxRep defLen repLen bs =
    let (repBytes, afterRepBytes) = BS.splitAt (fromIntegral repLen) bs
        (defBytes, afterDefBytes) = BS.splitAt (fromIntegral defLen) afterRepBytes
        bwDef = bitWidthForMaxLevel maxDef
        bwRep = bitWidthForMaxLevel maxRep
        (repLvlsRaw, _) =
            if bwRep == 0
                then (replicate n 0, repBytes)
                else decodeRLEBitPackedHybrid bwRep n repBytes
        (defLvlsRaw, _) =
            if bwDef == 0
                then (replicate n 0, defBytes)
                else decodeRLEBitPackedHybrid bwDef n defBytes
     in (map fromIntegral defLvlsRaw, map fromIntegral repLvlsRaw, afterDefBytes)

stitchNullable :: Int -> [Int] -> [a] -> [Maybe a]
stitchNullable maxDef = go
  where
    go [] _ = []
    go (d : ds) vs
        | d == maxDef = case vs of
            (v : vs') -> Just v : go ds vs'
            [] -> error "value stream exhausted"
        | otherwise = Nothing : go ds vs

data SNode = SNode
    { sName :: String
    , sRep :: RepetitionType
    , sChildren :: [SNode]
    }
    deriving (Show, Eq)

parseOne :: [SchemaElement] -> (SNode, [SchemaElement])
parseOne [] = error "parseOne: empty schema list"
parseOne (se : rest) =
    let childCount = fromIntegral (numChildren se)
        (kids, rest') = parseMany childCount rest
     in ( SNode
            { sName = T.unpack (elementName se)
            , sRep = repetitionType se
            , sChildren = kids
            }
        , rest'
        )

parseMany :: Int -> [SchemaElement] -> ([SNode], [SchemaElement])
parseMany 0 xs = ([], xs)
parseMany n xs =
    let (node, xs') = parseOne xs
        (nodes, xs'') = parseMany (n - 1) xs'
     in (node : nodes, xs'')

parseAll :: [SchemaElement] -> [SNode]
parseAll [] = []
parseAll xs = let (n, xs') = parseOne xs in n : parseAll xs'

-- | Tag leaf values as Just/Nothing according to maxDef.
pairWithVals :: Int -> [(Int, Int)] -> [a] -> [(Int, Int, Maybe a)]
pairWithVals _ [] _ = []
pairWithVals maxDef ((r, d) : rds) vs
    | d == maxDef = case vs of
        (v : vs') -> (r, d, Just v) : pairWithVals maxDef rds vs'
        [] -> error "pairWithVals: value stream exhausted"
    | otherwise = (r, d, Nothing) : pairWithVals maxDef rds vs

-- | Split triplets into groups; a new group begins whenever rep <= bound.
splitAtRepBound :: Int -> [(Int, Int, Maybe a)] -> [[(Int, Int, Maybe a)]]
splitAtRepBound _ [] = []
splitAtRepBound bound (t : ts) =
    let (rest, remaining) = span (\(r, _, _) -> r > bound) ts
     in (t : rest) : splitAtRepBound bound remaining

{- | Reconstruct a list column from Dremel encoding levels.
rep=0 starts a new top-level row; def=0 means the entire list slot is null.
Returns one Maybe [Maybe a] per row.
-}
stitchList :: Int -> [Int] -> [Int] -> [a] -> [Maybe [Maybe a]]
stitchList maxDef repLvls defLvls vals =
    let triplets = pairWithVals maxDef (zip repLvls defLvls) vals
        rows = splitAtRepBound 0 triplets
     in map toRow rows
  where
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow grp = Just [v | (_, _, v) <- grp]

{- | Reconstruct a 2-level nested list (maxRep=2) from Dremel triplets.
defT1: def threshold at which the depth-1 element is present (not null).
maxDef: def threshold at which the leaf is present.
-}
stitchList2 :: Int -> Int -> [Int] -> [Int] -> [a] -> [Maybe [Maybe [Maybe a]]]
stitchList2 defT1 maxDef repLvls defLvls vals =
    let triplets = pairWithVals maxDef (zip repLvls defLvls) vals
     in map toRow (splitAtRepBound 0 triplets)
  where
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow row = Just (map toOuter (splitAtRepBound 1 row))
    toOuter [] = Nothing
    toOuter ((_, d, _) : _) | d < defT1 = Nothing
    toOuter outer = Just (map toLeaf (splitAtRepBound 2 outer))
    toLeaf [] = Nothing
    toLeaf ((_, _, v) : _) = v

{- | Reconstruct a 3-level nested list (maxRep=3) from Dremel triplets.
defT1, defT2: def thresholds at which depth-1 and depth-2 elements are present.
maxDef: def threshold at which the leaf is present.
-}
stitchList3 ::
    Int -> Int -> Int -> [Int] -> [Int] -> [a] -> [Maybe [Maybe [Maybe [Maybe a]]]]
stitchList3 defT1 defT2 maxDef repLvls defLvls vals =
    let triplets = pairWithVals maxDef (zip repLvls defLvls) vals
     in map toRow (splitAtRepBound 0 triplets)
  where
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

levelsForPath :: [SchemaElement] -> [String] -> (Int, Int)
levelsForPath schemaTail = go 0 0 (parseAll schemaTail)
  where
    go defC repC _ [] = (defC, repC)
    go defC repC nodes (p : ps) =
        case find (\n -> sName n == p) nodes of
            Nothing -> (defC, repC)
            Just n ->
                let defC' = defC + (if sRep n == OPTIONAL || sRep n == REPEATED then 1 else 0)
                    repC' = repC + (if sRep n == REPEATED then 1 else 0)
                 in go defC' repC' (sChildren n) ps
