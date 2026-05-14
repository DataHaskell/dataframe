module DataFrame.Display.Terminal.Colours where

-- terminal color functions
red :: String -> String
red s = "\ESC[31m" ++ s ++ "\ESC[0m"

green :: String -> String
green s = "\ESC[32m" ++ s ++ "\ESC[0m"

brightGreen :: String -> String
brightGreen s = "\ESC[92m" ++ s ++ "\ESC[0m"

brightBlue :: String -> String
brightBlue s = "\ESC[94m" ++ s ++ "\ESC[0m"
