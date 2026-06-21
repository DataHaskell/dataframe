{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Display.Terminal.PrettyPrint where

import qualified Data.Text as T
import qualified Data.Vector as V

{- | Output format for 'showTable'. 'Plain' renders a terminal-style table with
ASCII borders; 'Markdown' renders a GitHub-flavoured pipe table suitable for
notebooks.
-}
data RenderFormat = Plain | Markdown
    deriving (Show, Eq)

-- Utility functions to show a DataFrame as a Markdown-ish table.

-- Adapted from: https://stackoverflow.com/questions/5929377/format-list-output-in-haskell
-- a type for fill functions
type Filler = Int -> T.Text -> T.Text

-- a type for describing table columns
data ColDesc t = ColDesc
    { colTitleFill :: Filler
    , colTitle :: T.Text
    , colValueFill :: Filler
    }

-- functions that fill a string (s) to a given width (n) by adding pad
-- character (c) to align left, right, or center
fillLeft :: Char -> Int -> T.Text -> T.Text
fillLeft c n s = s <> T.replicate (n - T.length s) (T.singleton c)

fillRight :: Char -> Int -> T.Text -> T.Text
fillRight c n s = T.replicate (n - T.length s) (T.singleton c) <> s

fillCenter :: Char -> Int -> T.Text -> T.Text
fillCenter c n s =
    T.replicate l (T.singleton c) <> s <> T.replicate r (T.singleton c)
  where
    x = n - T.length s
    l = x `div` 2
    r = x - l

-- functions that fill with spaces
left :: Int -> T.Text -> T.Text
left = fillLeft ' '

right :: Int -> T.Text -> T.Text
right = fillRight ' '

center :: Int -> T.Text -> T.Text
center = fillCenter ' '

{- | Render a table from column-major data. @columns@ has one 'V.Vector' per
column; widths are computed in one pass per column (no row-major transpose),
and row lines are built by indexing each column at row @i@.
-}
showTable ::
    RenderFormat ->
    [T.Text] ->
    [T.Text] ->
    [V.Vector T.Text] ->
    T.Text
showTable fmt header types columns =
    let isMarkdown = fmt == Markdown
        -- In a GitHub pipe table a literal '|' ends the cell and a newline ends
        -- the row, so a value like the operator name <|> would split the row into
        -- the wrong columns. Escape both for the Markdown format only.
        esc = if isMarkdown then escapeMarkdownCell else id
        hdr = map esc header
        tys = map esc types
        cols = map (V.map esc) columns
        consolidatedHeader =
            if isMarkdown
                then zipWith (\h t -> h <> "<br>" <> t) hdr tys
                else hdr
        cs = map (\h -> ColDesc center h left) consolidatedHeader
        nRows = case cols of
            (c : _) -> V.length c
            [] -> 0
        columnMaxWidth col
            | V.null col = 0
            | otherwise = V.foldl' (\acc x -> max acc (T.length x)) 0 col
        widths =
            zipWith3
                (\h t col -> T.length h `max` T.length t `max` columnMaxWidth col)
                consolidatedHeader
                tys
                cols
        dashesOf w = T.replicate w "-"
        border = T.intercalate "---" (map dashesOf widths)
        separator = T.intercalate "-|-" (map dashesOf widths)
        fillCells fill cells =
            T.intercalate " | " (zipWith3 fill cs widths cells)
        rowCells i = map (V.! i) cols
        rowLines = [fillCells colValueFill (rowCells i) | i <- [0 .. nRows - 1]]
        wrapMd t = T.concat ["| ", t, " |"]
        outputLines =
            if isMarkdown
                then
                    wrapMd (fillCells colTitleFill consolidatedHeader)
                        : wrapMd separator
                        : map wrapMd rowLines
                else
                    border
                        : fillCells colTitleFill consolidatedHeader
                        : separator
                        : fillCells colTitleFill tys
                        : separator
                        : rowLines
     in T.unlines outputLines

{- | Escape a value for a GitHub-flavoured Markdown table cell: a bare @|@ would
end the cell and a newline would end the row, so both are neutralised (the pipe
backslash-escaped, the newline turned into a @\<br\>@).
-}
escapeMarkdownCell :: T.Text -> T.Text
escapeMarkdownCell = T.replace "\n" "<br>" . T.replace "|" "\\|"
