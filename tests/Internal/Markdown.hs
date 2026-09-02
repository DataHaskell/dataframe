{-# LANGUAGE OverloadedStrings #-}

{- | Markdown rendering must escape pipe-table metacharacters in cell values,
or an operator name like @<|>@ splits the row into the wrong columns.
-}
module Internal.Markdown (tests) where

import qualified Data.Text as T

import DataFrame.Display.Terminal.PrettyPrint (escapeMarkdownCell)
import DataFrame.Internal.Column (ensureOptional, fromList)
import qualified DataFrame.Internal.DataFrame as D
import Test.HUnit

-- Count the column-delimiter pipes in a rendered row: a bare '|' delimits a
-- cell; an escaped "\|" does not.
unescapedPipes :: T.Text -> Int
unescapedPipes = go . T.unpack
  where
    go ('\\' : '|' : rest) = go rest
    go ('|' : rest) = 1 + go rest
    go (_ : rest) = go rest
    go [] = 0

tests :: [Test]
tests =
    [ TestLabel "escapeMarkdownCell escapes a bare pipe" $
        TestCase (escapeMarkdownCell "f<|>g" @?= "f<\\|>g")
    , TestLabel "escapeMarkdownCell turns a newline into <br>" $
        TestCase (escapeMarkdownCell "a\nb" @?= "a<br>b")
    , TestLabel "toMarkdown keeps an operator name in one cell" $
        TestCase $ do
            let df =
                    D.fromNamedColumns
                        [ ("name", fromList ["plain" :: T.Text, "f<|>g"])
                        , ("mb", fromList [1.0 :: Double, 2.0])
                        ]
                md = D.toMarkdown df
                tableRows = filter (T.isPrefixOf "|") (T.lines md)
                delims = map unescapedPipes tableRows
            assertBool "pipe in a value left unescaped" ("f<\\|>g" `T.isInfixOf` md)
            assertBool
                ("rows misaligned, delimiter counts: " ++ show delims)
                (case delims of [] -> False; (d : ds) -> all (== d) ds)
    , TestLabel "nullable boxed Text renders without quotes" $
        TestCase $ do
            let df =
                    D.fromNamedColumns
                        [("name", ensureOptional (fromList ["Ada" :: T.Text]))]
                md = D.toMarkdown df
            assertBool
                "nullable Text value is missing"
                ("Ada" `T.isInfixOf` md)
            assertBool "nullable Text contains quotes" (not ("\"Ada\"" `T.isInfixOf` md))
    ]
