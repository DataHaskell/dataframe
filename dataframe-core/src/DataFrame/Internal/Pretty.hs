{- | A minimal Wadler/Leijen-style document combinator and width-aware renderer.

This is a small self-contained port of the "prettiest printer" algorithm
(Wadler, "A prettier printer"; Leijen, @wl-pprint@). A 'Doc' describes a
layout abstractly; 'render' chooses where soft breaks ('line') turn into
newlines so the result fits a target width. 'Group' marks a region that
should be laid out flat when it fits, broken otherwise.
-}
module DataFrame.Internal.Pretty (
    Doc,
    text,
    line,
    hardline,
    nest,
    group,
    (<+>),
    hcat,
    punctuate,
    parens,
    parensWhenBroken,
    defaultWidth,
    render,
) where

data Doc
    = Empty
    | Text String
    | Line
    | Cat Doc Doc
    | Nest Int Doc
    | Group Doc
    | Hard
    | Alt Doc Doc

instance Semigroup Doc where
    (<>) = Cat

instance Monoid Doc where
    mempty = Empty

-- | A literal chunk of text. Must not contain newlines (use 'line'/'hardline').
text :: String -> Doc
text = Text

{- | A soft break: a single space when its enclosing 'group' fits the width,
otherwise a newline + current indentation.
-}
line :: Doc
line = Line

-- | A hard break that never flattens; any enclosing 'group' is forced to break.
hardline :: Doc
hardline = Hard

-- | Add @k@ spaces to the indentation applied at line breaks inside @d@.
nest :: Int -> Doc -> Doc
nest = Nest

-- | Lay the document out flat if it fits the remaining width, broken otherwise.
group :: Doc -> Doc
group = Group

-- | Concatenate two documents separated by a single space.
(<+>) :: Doc -> Doc -> Doc
x <+> y = x <> Text " " <> y

infixr 6 <+>

hcat :: [Doc] -> Doc
hcat = mconcat

-- | Append @sep@ after every element but the last.
punctuate :: Doc -> [Doc] -> [Doc]
punctuate _ [] = []
punctuate _ [d] = [d]
punctuate sep (d : ds) = (d <> sep) : punctuate sep ds

parens :: Doc -> Doc
parens d = Text "(" <> d <> Text ")"

{- | Render @d@ bare when it fits flat on the current line; wrap it in
parentheses when it must break across multiple lines. Used to keep operator
grouping unambiguous once a sub-expression wraps, without adding parenthesis
noise to expressions that stay on one line.
-}
parensWhenBroken :: Doc -> Doc
parensWhenBroken d = Group (Alt d (parens d))

defaultWidth :: Int
defaultWidth = 80

data Mode = Flat | Break

-- | Render a document, breaking soft lines so output fits @width@ columns.
render :: Int -> Doc -> String
render width doc = layout 0 [(0, Break, doc)]
  where
    layout :: Int -> [(Int, Mode, Doc)] -> String
    layout _ [] = ""
    layout col ((i, m, d) : rest) = case d of
        Empty -> layout col rest
        Text s -> s ++ layout (col + length s) rest
        Cat x y -> layout col ((i, m, x) : (i, m, y) : rest)
        Nest j x -> layout col ((i + j, m, x) : rest)
        Line -> case m of
            Flat -> ' ' : layout (col + 1) rest
            Break -> '\n' : replicate i ' ' ++ layout i rest
        Hard -> '\n' : replicate i ' ' ++ layout i rest
        Group x ->
            if fits (width - col) ((i, Flat, x) : rest)
                then layout col ((i, Flat, x) : rest)
                else layout col ((i, Break, x) : rest)
        Alt flat broken -> case m of
            Flat -> layout col ((i, Flat, flat) : rest)
            Break -> layout col ((i, Break, broken) : rest)

    fits :: Int -> [(Int, Mode, Doc)] -> Bool
    fits w _ | w < 0 = False
    fits _ [] = True
    fits w ((i, m, d) : rest) = case d of
        Empty -> fits w rest
        Text s -> fits (w - length s) rest
        Cat x y -> fits w ((i, m, x) : (i, m, y) : rest)
        Nest j x -> fits w ((i + j, m, x) : rest)
        Line -> case m of
            Flat -> fits (w - 1) rest
            Break -> True
        -- A forced break inside the group being measured (Flat) means it cannot
        -- lay out on one line; in the surrounding context (Break) it just ends
        -- the current line, so the prefix so far fits.
        Hard -> case m of
            Flat -> False
            Break -> True
        Group x -> fits w ((i, Flat, x) : rest)
        Alt flat broken -> case m of
            Flat -> fits w ((i, Flat, flat) : rest)
            Break -> fits w ((i, Break, broken) : rest)
