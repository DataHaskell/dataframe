module DataFrame.IO.Parquet.Levels where

import qualified Data.ByteString as BS
import Data.Int
import Data.List
import qualified Data.Text as T
import Data.Word

import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Encoding
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types

readLevelsV1 ::
    Int -> Int -> Int -> BS.ByteString -> ([Int], [Int], BS.ByteString)
readLevelsV1 n maxDef maxRep bs =
    let bwDef = bitWidthForMaxLevel maxDef
        bwRep = bitWidthForMaxLevel maxRep

        (repLvlsU32, afterRep) =
            if bwRep == 0
                then (replicate n 0, bs)
                else
                    let repLength = littleEndianWord32 (BS.take 4 bs)
                        repData = BS.take (fromIntegral repLength) (BS.drop 4 bs)
                        afterRepData = BS.drop (4 + fromIntegral repLength) bs
                        (repVals, _) = decodeRLEBitPackedHybrid bwRep n repData
                     in (repVals, afterRepData)

        (defLvlsU32, afterDef) =
            if bwDef == 0
                then (replicate n 0, afterRep)
                else
                    let defLength = littleEndianWord32 (BS.take 4 afterRep)
                        defData = BS.take (fromIntegral defLength) (BS.drop 4 afterRep)
                        afterDefData = BS.drop (4 + fromIntegral defLength) afterRep
                        (defVals, _) = decodeRLEBitPackedHybrid bwDef n defData
                     in (defVals, afterDefData)
     in (map fromIntegral defLvlsU32, map fromIntegral repLvlsU32, afterDef)

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
        (repLvlsU32, _) =
            if bwRep == 0
                then (replicate n 0, repBytes)
                else decodeRLEBitPackedHybrid bwRep n repBytes
        (defLvlsU32, _) =
            if bwDef == 0
                then (replicate n 0, defBytes)
                else decodeRLEBitPackedHybrid bwDef n defBytes
     in (map fromIntegral defLvlsU32, map fromIntegral repLvlsU32, afterDefBytes)

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

levelsForPath :: [SchemaElement] -> [String] -> (Int, Int)
levelsForPath schemaTail = go 0 0 (parseAll schemaTail)
  where
    go defC repC _ [] = (defC, repC)
    go defC repC nodes (p : ps) =
        case find (\n -> sName n == p) nodes of
            Nothing -> (defC, repC)
            Just n ->
                let defC' = defC + (if sRep n == OPTIONAL then 1 else 0)
                    repC' = repC + (if sRep n == REPEATED then 1 else 0)
                 in go defC' repC' (sChildren n) ps
