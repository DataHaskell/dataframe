{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

{- | QuickCheck parity properties for "DataFrame.Internal.Parsing.Fast".

Every fast slice parser must agree with the existing reference parser
(`readByteStringInt` / `readByteStringDouble` / `readByteStringBool` /
`readByteStringDate` / `isNullishBS`) on arbitrary byte fields, with
Doubles compared bit-for-bit ('castDoubleToWord64').
-}
module Properties.FastParsing (tests) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C
import qualified Data.Text.Encoding as TE

import Data.Word (Word64, Word8)
import GHC.Float (castDoubleToWord64)
import Test.QuickCheck

import DataFrame.Internal.Parsing (
    isNullish,
    isNullishBS,
    readByteStringBool,
    readByteStringDate,
    readByteStringDouble,
    readByteStringInt,
 )
import DataFrame.Internal.Parsing.Fast

-- | Bytes 'Data.ByteString.Char8.strip' removes (probed over all 256).
stripBytes :: [Word8]
stripBytes = [0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0xA0]

genPad :: Gen BS.ByteString
genPad =
    frequency
        [ (4, pure BS.empty)
        , (3, BS.pack <$> resize 3 (listOf (elements stripBytes)))
        ]

genDigits :: Int -> Int -> Gen BS.ByteString
genDigits lo hi = do
    n <- chooseInt (lo, hi)
    C.pack <$> vectorOf n (elements "0123456789")

-- | Digit runs that probe the 18/19/20-digit mantissa window edges.
genDigitRun :: Gen BS.ByteString
genDigitRun =
    frequency
        [ (6, genDigits 1 17)
        , (3, genDigits 18 21)
        , (1, genDigits 22 40)
        , (2, BS.append <$> genDigits 1 6 <*> pure "")
        ,
            ( 1
            , (BS.append . BS.pack . flip replicate 0x30 <$> chooseInt (1, 25))
                <*> genDigits 0 19
            )
        ]

genNumeric :: Gen BS.ByteString
genNumeric = do
    sign <- elements ["", "-", "+"]
    whole <- frequency [(8, genDigitRun), (1, pure "")]
    frac <-
        frequency
            [ (3, pure "")
            , (4, BS.append "." <$> genDigitRun)
            , (1, pure ".")
            ]
    ex <-
        frequency
            [ (4, pure "")
            , (3, genExponent)
            ]
    pre <- genPad
    post <- genPad
    pure (BS.concat [pre, sign, whole, frac, ex, post])
  where
    genExponent = do
        e <- elements ["e", "E"]
        s <- elements ["", "-", "+"]
        ds <-
            frequency
                [ (6, genDigits 1 3)
                , (2, genDigits 4 25)
                , (1, pure "")
                ]
        pure (BS.concat [e, s, ds])

genToken :: Gen BS.ByteString
genToken = do
    tok <-
        elements @BS.ByteString
            [ "Nothing"
            , "NULL"
            , ""
            , " "
            , "nan"
            , "null"
            , "N/A"
            , "NaN"
            , "NAN"
            , "NA"
            , "True"
            , "true"
            , "TRUE"
            , "False"
            , "false"
            , "FALSE"
            , "Infinity"
            , "-Infinity"
            , "na"
            , "Null"
            , "N/a"
            , "TRUe"
            , "FALSe"
            ]
    frequency [(4, pure tok), (1, flip BS.append tok <$> genPad)]

genDateLike :: Gen BS.ByteString
genDateLike = do
    y <- chooseInt (0, 12000)
    m <- chooseInt (0, 15)
    d <- chooseInt (0, 35)
    sep <- elements ["-", "/", "-"]
    let pad2 n = (if n < 10 then "0" else "") ++ show n
    body <-
        elements
            [ show y ++ "-" ++ pad2 m ++ "-" ++ pad2 d
            , show y ++ sep ++ show m ++ sep ++ show d
            , pad2 y ++ "-" ++ pad2 m ++ "-" ++ pad2 d
            ]
    pre <- genPad
    post <- genPad
    pure (BS.concat [pre, C.pack body, post])

genJunk :: Gen BS.ByteString
genJunk =
    frequency
        [ (3, C.pack <$> resize 12 (listOf (elements "0123456789.eE+- aZ_,")))
        , (1, BS.pack <$> resize 12 (listOf arbitrary))
        ]

-- | An arbitrary CSV field, weighted towards adversarial numerics.
newtype Field = Field {unField :: BS.ByteString}
    deriving (Eq, Show)

instance Arbitrary Field where
    arbitrary =
        Field
            <$> frequency
                [ (6, genNumeric)
                , (2, genToken)
                , (1, genDateLike)
                , (2, genJunk)
                ]
    shrink (Field b) = map (Field . BS.pack) (shrink (BS.unpack b))

bitsOf :: Maybe Double -> Maybe Word64
bitsOf = fmap castDoubleToWord64

-- | Embed a field in junk and parse it through the slice interface.
slicedOn ::
    (BS.ByteString -> Int -> Int -> a) ->
    BS.ByteString ->
    BS.ByteString ->
    BS.ByteString ->
    a
slicedOn p pre f post =
    p (BS.concat [pre, f, post]) (BS.length pre) (BS.length pre + BS.length f)

prop_intParity :: Field -> Property
prop_intParity (Field f) = parseIntField f === readByteStringInt f

prop_intShowRoundTrip :: Int -> Property
prop_intShowRoundTrip n = parseIntField (C.pack (show n)) === Just n

prop_intSlice :: Field -> Field -> Field -> Property
prop_intSlice (Field pre) (Field f) (Field post) =
    slicedOn parseIntFieldSlice pre f post === readByteStringInt f

prop_doubleParity :: Field -> Property
prop_doubleParity (Field f) =
    bitsOf (parseDoubleField f) === bitsOf (readByteStringDouble f)

-- | 'show' output must parse back to the exact same double (NaN is nullish).
prop_doubleShowRoundTrip :: Double -> Property
prop_doubleShowRoundTrip d =
    not (isNaN d) ==>
        let f = C.pack (show d)
         in bitsOf (parseDoubleField f) === bitsOf (Just d)

prop_doubleSlice :: Field -> Field -> Field -> Property
prop_doubleSlice (Field pre) (Field f) (Field post) =
    bitsOf (slicedOn parseDoubleFieldSlice pre f post)
        === bitsOf (readByteStringDouble f)

prop_boolParity :: Field -> Property
prop_boolParity (Field f) = parseBoolField f === readByteStringBool f

prop_boolSlice :: Field -> Field -> Field -> Property
prop_boolSlice (Field pre) (Field f) (Field post) =
    slicedOn parseBoolFieldSlice pre f post === readByteStringBool f

prop_missingParity :: Field -> Property
prop_missingParity (Field f) = isMissingField f === isNullishBS f

prop_missingTextParity :: Field -> Property
prop_missingTextParity (Field f) =
    isMissingField f === isNullish (TE.decodeUtf8Lenient f)

prop_missingSlice :: Field -> Field -> Field -> Property
prop_missingSlice (Field pre) (Field f) (Field post) =
    slicedOn isMissingFieldSlice pre f post === isNullishBS f

prop_missingIn :: Field -> Property
prop_missingIn (Field f) =
    isMissingFieldIn ["NA", "nope", ""] f
        === (f `elem` ["NA", "nope", ""])

prop_dateParity :: Field -> Property
prop_dateParity (Field f) =
    parseDateField f === readByteStringDate "%Y-%m-%d" f

prop_dateSlice :: Field -> Field -> Field -> Property
prop_dateSlice (Field pre) (Field f) (Field post) =
    slicedOn parseDateFieldSlice pre f post
        === readByteStringDate "%Y-%m-%d" f

tests :: [(String, Property)]
tests =
    [ ("intParity", property prop_intParity)
    , ("intShowRoundTrip", property prop_intShowRoundTrip)
    , ("intSlice", property prop_intSlice)
    , ("doubleParity", property prop_doubleParity)
    , ("doubleShowRoundTrip", property prop_doubleShowRoundTrip)
    , ("doubleSlice", property prop_doubleSlice)
    , ("boolParity", property prop_boolParity)
    , ("boolSlice", property prop_boolSlice)
    , ("missingParity", property prop_missingParity)
    , ("missingTextParity", property prop_missingTextParity)
    , ("missingSlice", property prop_missingSlice)
    , ("missingIn", property prop_missingIn)
    , ("dateParity", property prop_dateParity)
    , ("dateSlice", property prop_dateSlice)
    ]
