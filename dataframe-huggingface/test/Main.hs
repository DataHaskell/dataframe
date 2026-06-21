{-# LANGUAGE OverloadedStrings #-}

-- Pure unit tests for the HuggingFace URI helpers. The network paths
-- (resolve/download) need live HTTP and are not exercised here.
module Main (main) where

import Control.Monad (unless)
import DataFrame.IO.HuggingFace
import System.Exit (exitFailure)

check :: String -> Bool -> IO Bool
check name ok = do
    unless ok $ putStrLn ("FAIL: " ++ name)
    pure ok

main :: IO ()
main = do
    let glob = "hf://datasets/owner/ds/data/train-*.parquet"
        noGlob = "hf://datasets/owner/ds/data/train.parquet"
        bare = "hf://datasets/owner/ds"
    results <-
        sequence
            [ check "isHFUri hf" (isHFUri glob)
            , check "isHFUri local" (not (isHFUri "/tmp/local.parquet"))
            , check "parseHFUri ref" $
                case parseHFUri glob of
                    Right r ->
                        hfOwner r == "owner"
                            && hfDataset r == "ds"
                            && hfGlob r == "data/train-*.parquet"
                    Left _ -> False
            , check "parseHFUri bare is Left" $
                case parseHFUri bare of
                    Left _ -> True
                    Right _ -> False
            , check "hasGlob wildcard" (hasGlob "data/train-*.parquet")
            , check "hasGlob none" (not (hasGlob "data/train.parquet"))
            , check "directHFUrl" $
                case parseHFUri noGlob of
                    Right r ->
                        directHFUrl r
                            == "https://huggingface.co/datasets/owner/ds/resolve/main/data/train.parquet"
                    Left _ -> False
            , check "hfUrlRepoPath" $
                hfUrlRepoPath
                    ( HFParquetFile
                        "https://huggingface.co/datasets/o/d/resolve/main/data/train-0.parquet"
                        ""
                        ""
                        ""
                    )
                    == "data/train-0.parquet"
            ]
    unless (and results) exitFailure
    putStrLn "dataframe-huggingface: all pure-helper tests passed"
