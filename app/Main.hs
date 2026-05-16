{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Exception (bracket)
import Control.Monad
import Data.Time (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Directory (
    XdgDirectory (..),
    createDirectoryIfMissing,
    doesFileExist,
    getCurrentDirectory,
    getModificationTime,
    getXdgDirectory,
 )
import System.Exit (ExitCode (..))
import System.FilePath ((</>))
import System.Process

#ifndef mingw32_HOST_OS
import System.Posix.Signals (Handler (..), installHandler, sigINT)
#endif

main :: IO ()
main = do
    cacheDir <- getXdgDirectory XdgCache "dataframe_repl"
    currDir <- getCurrentDirectory
    print currDir
    createDirectoryIfMissing True cacheDir

    let ghciScript = cacheDir </> "dataframe.ghci"
        projectFile = cacheDir </> "cabal.project"
        cabalFile = cacheDir </> "df-repl.cabal"
        mainFile = cacheDir </> "Main.hs"

    -- Ensure the directory is a valid Cabal project with a local target to repl into.
    ensureBoilerplate projectFile cabalFile mainFile

    -- Keep your ghci script updated (weekly).
    shouldDownload <- needsUpdate ghciScript
    when shouldDownload $ do
        putStrLn "\ESC[92mDownloading latest version of dataframe config...\ESC[0m"
        output <-
            readProcess
                "curl"
                [ "--output"
                , ghciScript
                , "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/dataframe.ghci"
                ]
                ""
        putStrLn output

        putStrLn =<< readProcess "cabal" ["update"] ""

    let command = "cabal"
        args =
            [ "repl"
            , "exe:df-repl"
            , "--project-file=" ++ projectFile
            , "-O2"
            , "--repl-option=-ghci-script=" ++ ghciScript
            ]

        baseCp =
            (proc command args)
                { cwd = Just currDir
                , std_in = Inherit
                , std_out = Inherit
                , std_err = Inherit
                }

#ifdef mingw32_HOST_OS
        cp = baseCp { delegate_ctlc = True }
#else
        cp = baseCp
#endif

#ifndef mingw32_HOST_OS
    -- Unix: ignore Ctrl-C in the wrapper so the child handles it.
    bracket
        (installHandler sigINT Ignore Nothing)
        (\old -> installHandler sigINT old Nothing)
        (\_ -> runChild cp)
#else
    -- Windows: delegate Ctrl-C handling to the child.
    runChild cp
#endif

ensureBoilerplate :: FilePath -> FilePath -> FilePath -> IO ()
ensureBoilerplate projectFile cabalFile mainFile = do
    -- cabal.project
    writeIfMissing
        projectFile
        $ unlines
            [ "packages: ."
            ]

    writeIfMissing
        cabalFile
        $ unlines
            [ "cabal-version:      3.0"
            , "name:               df-repl"
            , "version:            0.1.0.0"
            , "build-type:         Simple"
            , ""
            , "executable df-repl"
            , "  main-is:          Main.hs"
            , "  hs-source-dirs:   ."
            , "  default-language: Haskell2010"
            , "  build-depends:    base, dataframe, text, time, random, dataframe-core, dataframe-operations, dataframe-csv, dataframe-parquet, dataframe-json, dataframe-th, dataframe-csv-th, dataframe-parquet-th, dataframe-viz, dataframe-lazy, dataframe-parsing"
            ]

    writeIfMissing
        mainFile
        $ unlines
            [ "module Main where"
            , ""
            , "main :: IO ()"
            , "main = putStrLn \"df-repl\""
            ]

writeIfMissing :: FilePath -> String -> IO ()
writeIfMissing fp contents = do
    exists <- doesFileExist fp
    unless exists $ writeFile fp contents

runChild :: CreateProcess -> IO ()
runChild cp = do
    (_, _, _, ph) <- createProcess cp
    ec <- waitForProcess ph
    case ec of
        ExitSuccess -> pure ()
        ExitFailure n -> fail ("cabal repl failed with exit code " <> show n)

oneWeek :: NominalDiffTime
oneWeek = 7 * 24 * 60 * 60

needsUpdate :: FilePath -> IO Bool
needsUpdate filepath = do
    exists <- doesFileExist filepath
    if not exists
        then return True
        else do
            modTime <- getModificationTime filepath
            currentTime <- getCurrentTime
            let age = diffUTCTime currentTime modTime
            return (age > oneWeek)
