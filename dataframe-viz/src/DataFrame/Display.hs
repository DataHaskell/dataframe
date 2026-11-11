{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use newtype instead of data" #-}
module DataFrame.Display where

import qualified Data.Text.IO as T
import DataFrame.Display.Terminal.PrettyPrint (RenderFormat (..))
import qualified DataFrame.Internal.DataFrame as D

data DisplayOptions = DisplayOptions
    { displayTruncate :: Maybe D.TruncateConfig
    -- ^ 'Nothing' shows everything; 'Just cfg' caps rows, columns, widths.
    }

-- | Terminal defaults: 10 rows plus the standard column and cell caps.
defaultDisplayOptions :: DisplayOptions
defaultDisplayOptions =
    DisplayOptions
        { displayTruncate = Just D.defaultTruncateConfig{D.maxRows = 10}
        }

-- | Render a 'DataFrame' to stdout according to 'DisplayOptions'.
display :: DisplayOptions -> D.DataFrame -> IO ()
display opts = T.putStrLn . D.asTextWith Plain (displayTruncate opts)

displayRows :: Int -> D.DataFrame -> IO ()
displayRows n = T.putStrLn . D.asTextWith Plain (Just D.defaultTruncateConfig{D.maxRows = n})
