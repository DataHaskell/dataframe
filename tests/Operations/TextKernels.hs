{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | contains and length on packed text agree with the Text functions per row.
module Operations.TextKernels (tests) where

import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Text.Encoding as TE
import Data.Text.Internal (Text (Text))
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Data.PackedText (mkPackedContiguous)
import Test.HUnit

-- | Packed UTF-8 column of the given texts.
packed :: [T.Text] -> DI.Column
packed ts =
    let bytes = BS.concat (map TE.encodeUtf8 ts)
        offs = scanl (+) 0 (map (BS.length . TE.encodeUtf8) ts)
        Text arr _ _ = TE.decodeUtf8 bytes
     in DI.PackedText Nothing (mkPackedContiguous (arr :: A.Array) (VU.fromList offs))

rows :: [T.Text]
rows =
    [ "http://www.google.com/search?q=1"
    , ""
    , "goo"
    , "gle"
    , "Google Docs"
    , "ищу google здесь"
    , "ééé.google.é"
    , "a"
    , "g"
    , "gooogle"
    ]

frame :: D.DataFrame
frame = D.fromColumns [("t", packed rows)]

containsCase :: T.Text -> Test
containsCase needle =
    TestCase $
        assertEqual
            ("contains " ++ show needle)
            (map (T.isInfixOf needle) rows)
            ( D.columnAsList
                (F.bool "m")
                (D.derive "m" (F.contains needle (F.col "t")) frame)
            )

lengthCase :: Test
lengthCase =
    TestCase $
        assertEqual
            "length"
            (map T.length rows)
            (D.columnAsList (F.int "l") (D.derive "l" (F.length (F.col "t")) frame))

tests :: [Test]
tests =
    map (TestLabel "text kernels") $
        lengthCase
            : map
                containsCase
                [ "google"
                , "Google"
                , ".google."
                , ""
                , "g"
                , "gle"
                , "goo"
                , "здесь"
                , "é.g"
                , "x"
                , "gooogle!"
                ]
