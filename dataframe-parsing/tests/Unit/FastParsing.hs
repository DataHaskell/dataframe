{-# LANGUAGE OverloadedStrings #-}

{- | Pinned adversarial cases for "DataFrame.Internal.Parsing.Fast".

Each case asserts parity with the reference parsers plus, where the
parser-semantics contract (benchmarks/csv/results/code-audit.md section 2)
pins a concrete value, the value itself.
-}
module Unit.FastParsing (tests) where

import qualified Data.ByteString as BS

import Data.Word (Word64)
import GHC.Float (castDoubleToWord64)
import Test.HUnit

import DataFrame.Internal.Parsing (
    isNullishBS,
    readByteStringBool,
    readByteStringDate,
    readByteStringDouble,
    readByteStringInt,
 )
import DataFrame.Internal.Parsing.Fast

bitsOf :: Maybe Double -> Maybe Word64
bitsOf = fmap castDoubleToWord64

intCases :: [BS.ByteString]
intCases =
    [ ""
    , "-"
    , "+"
    , "0"
    , "-0"
    , "042"
    , " \t42\n "
    , "- 42"
    , "+42"
    , "1 2"
    , "9223372036854775807"
    , "9223372036854775808"
    , "-9223372036854775808"
    , "-9223372036854775809"
    , "00000000000000000000042"
    , "18446744073709551616"
    , "99999999999999999999999999"
    , "12e3"
    , "0x10"
    , BS.pack [0xA0, 0x34, 0x32, 0xA0]
    ]

doubleCases :: [BS.ByteString]
doubleCases =
    [ ".5"
    , "+.5"
    , "5."
    , "5.e3"
    , "1e3.5"
    , "1_000"
    , "1e+3"
    , "1E-3"
    , " 1.5 "
    , "1e999"
    , "-1e999"
    , "0e999"
    , "-0e999"
    , "1e-999"
    , "0.1"
    , "-0"
    , "-0.0"
    , "2.2250738585072011e-308"
    , "1.7976931348623157e308"
    , "4.9406564584124654e-324"
    , "2.225073858507201e-308"
    , "0.30000000000000004"
    , "9007199254740993"
    , "9007199254740993.9007199254740993"
    , "12345678901234567890"
    , "12345678901234567890.12345678901234567890"
    , "123456789012345678901234567890e-25"
    , "1e00000000000000005"
    , "1e-00000000000000005"
    , "00000000000000000000001.5"
    , "1.00000000000000000000005"
    , "0.000000000000000000000000000005"
    , "1e308"
    , "1e309"
    , "1e-308"
    , "1e-309"
    , "1e1023"
    , "1e1024"
    , "1e1025"
    , "1e-1024"
    , "1e-1025"
    , "Infinity"
    , "NaN"
    , "1e"
    , "1e+"
    , "1e-"
    , "1.5e "
    , "1.2.3"
    , "--5"
    , "1e18446744073709551617"
    ]

pinnedDoubles :: [(BS.ByteString, Maybe Double)]
pinnedDoubles =
    [ ("1e+3", Just 1000)
    , (" 1.5 ", Just 1.5)
    , ("1e999", Just (1 / 0))
    , ("1e-999", Just 0)
    , (".5", Nothing)
    , ("5.", Nothing)
    , ("1e3.5", Nothing)
    , ("1_000", Nothing)
    , ("Infinity", Nothing)
    , ("NaN", Nothing)
    , -- The reference parser is NOT correctly rounded here; bit-exact
      -- parity means we must reproduce its 2.2250738585072e-308.
      ("2.2250738585072011e-308", Just 2.2250738585072e-308)
    ]

boolCases :: [BS.ByteString]
boolCases =
    [ "True"
    , "true"
    , "TRUE"
    , "False"
    , "false"
    , "FALSE"
    , " True"
    , "True "
    , "TRUe"
    , "tRue"
    , "FALSe"
    , ""
    , "T"
    , "Truee"
    ]

missingCases :: [BS.ByteString]
missingCases =
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
    , "na"
    , "Na"
    , "Null"
    , "N/a"
    , "  "
    , "NULL "
    , " NA"
    , "Nothing!"
    , "nothing"
    , "n"
    , "N"
    ]

dateCases :: [BS.ByteString]
dateCases =
    [ "2024-02-29"
    , "2023-02-29"
    , "2023-02-30"
    , "2023-13-01"
    , "2023-00-10"
    , "2023-01-00"
    , "2023-1-2"
    , " 2024-02-29 "
    , "0001-01-01"
    , "0000-01-01"
    , "10000-01-01"
    , "2023/01/02"
    , "2023-01-023"
    , ""
    ]

parityCase ::
    (Eq a, Show a) =>
    String -> (BS.ByteString -> a) -> (BS.ByteString -> a) -> BS.ByteString -> Test
parityCase name fast ref input =
    TestLabel (name ++ ": " ++ show input) . TestCase $
        assertEqual (name ++ " parity on " ++ show input) (ref input) (fast input)

pinnedCase :: BS.ByteString -> Maybe Double -> Test
pinnedCase input expected =
    TestLabel ("pinned double: " ++ show input) . TestCase $
        assertEqual
            ("pinned value for " ++ show input)
            (bitsOf expected)
            (bitsOf (parseDoubleField input))

tests :: [Test]
tests =
    map (parityCase "int" parseIntField readByteStringInt) intCases
        ++ map
            (parityCase "double" (bitsOf . parseDoubleField) (bitsOf . readByteStringDouble))
            doubleCases
        ++ map (uncurry pinnedCase) pinnedDoubles
        ++ map (parityCase "bool" parseBoolField readByteStringBool) boolCases
        ++ map (parityCase "missing" isMissingField isNullishBS) missingCases
        ++ map (parityCase "date" parseDateField (readByteStringDate "%Y-%m-%d")) dateCases
        ++ [ TestLabel "int maxBound" . TestCase $
                assertEqual
                    "maxBound"
                    (Just (maxBound :: Int))
                    (parseIntField "9223372036854775807")
           , TestLabel "int overflow" . TestCase $
                assertEqual "maxBound+1" Nothing (parseIntField "9223372036854775808")
           , TestLabel "int minBound" . TestCase $
                assertEqual
                    "minBound"
                    (Just (minBound :: Int))
                    (parseIntField "-9223372036854775808")
           , TestLabel "negative zero" . TestCase $
                assertEqual "-0.0 bits" (bitsOf (Just (-0.0))) (bitsOf (parseDoubleField "-0"))
           ]
