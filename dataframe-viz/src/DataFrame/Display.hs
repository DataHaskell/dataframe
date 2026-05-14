{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use newtype instead of data" #-}
module DataFrame.Display where

import qualified Data.Text.IO as T
import DataFrame.Display.Terminal.PrettyPrint (RenderFormat (..))
import qualified DataFrame.Internal.DataFrame as D

data DisplayOptions = DisplayOptions
    { {-- | Controls truncation on all three axes (rows, columns, cell width).
      --
      --   * 'Nothing' renders every row and column at full width.
      --   * 'Just cfg' caps each axis whose limit in @cfg@ is positive.
      --}
      displayTruncate :: Maybe D.TruncateConfig
    }

{- | Defaults aimed at terminal use: 10 rows, plus the standard column and cell
caps from 'D.defaultTruncateConfig'.
-}
defaultDisplayOptions :: DisplayOptions
defaultDisplayOptions =
    DisplayOptions
        { displayTruncate = Just D.defaultTruncateConfig{D.maxRows = 10}
        }

-- | Render a 'DataFrame' to stdout according to 'DisplayOptions'.
display :: DisplayOptions -> D.DataFrame -> IO ()
display opts = T.putStrLn . D.asTextWith Plain (displayTruncate opts)
