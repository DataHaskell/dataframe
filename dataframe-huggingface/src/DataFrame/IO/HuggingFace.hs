{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- | Read Parquet datasets from HuggingFace URIs
(@hf:\/\/datasets\/{owner}\/{dataset}\/{glob}@) into 'DataFrame's. Globs are
resolved via the datasets-server API, files downloaded to a temp dir, then read
with the local "DataFrame.IO.Parquet".
-}
module DataFrame.IO.HuggingFace (
    -- * Eager readers
    readParquet,
    readParquetWithOpts,
    readParquetFiles,
    readParquetFilesWithOpts,

    -- * Lazy / streaming reader
    scanParquet,

    -- * Read options (re-exported from "DataFrame.IO.Parquet")
    ParquetReadOptions (..),
    defaultParquetReadOptions,

    -- * URI helpers
    HFRef (..),
    HFParquetFile (..),
    isHFUri,
    parseHFUri,
    hasGlob,
    directHFUrl,
    hfUrlRepoPath,

    -- * Resolution and download
    getHFToken,
    resolveHFUrls,
    downloadHFFiles,
    fetchHFParquetFiles,
) where

import Control.Exception (try)
import Control.Monad (forM, when)
import Data.Aeson (FromJSON (..), eitherDecodeStrict, withObject, (.:))
import qualified Data.ByteString as BS
import qualified Data.List as L
import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import DataFrame.Core (DataFrame)
import DataFrame.IO.Parquet (
    ParquetReadOptions (..),
    defaultParquetReadOptions,
 )
import qualified DataFrame.IO.Parquet as Parquet
import qualified DataFrame.Lazy as Lazy
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as DS
import DataFrame.Schema (Schema)
import Network.HTTP.Simple (
    getResponseBody,
    getResponseStatusCode,
    httpBS,
    parseRequest,
    setRequestHeader,
 )
import System.Directory (getHomeDirectory, getTemporaryDirectory)
import System.Environment (lookupEnv)
import System.FilePath ((</>))
import System.FilePath.Glob (compile, match)
import System.IO.Temp (createTempDirectory, getCanonicalTemporaryDirectory)

-- Eager readers -----------------------------------------------------------

{- | Read a Parquet dataset from a @hf://@ URI (or a local path) into a
'DataFrame'.

==== __Example__
@
ghci> readParquet "hf://datasets/owner/dataset/data/train-*.parquet"
@
-}
readParquet :: FilePath -> IO DataFrame
readParquet = readParquetWithOpts defaultParquetReadOptions

-- | Read a Parquet dataset from a @hf://@ URI (or local path) with options.
readParquetWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetWithOpts opts path
    | isHFUri path = readDownloaded opts =<< fetchHFParquetFiles path
    | otherwise = Parquet.readParquetWithOpts opts path

{- | Read multiple Parquet files from a @hf://@ URI (or a local directory/glob).

For multi-file reads, @rowRange@ is applied once after concatenation (global
range semantics).
-}
readParquetFiles :: FilePath -> IO DataFrame
readParquetFiles = readParquetFilesWithOpts defaultParquetReadOptions

-- | Read multiple Parquet files from a @hf://@ URI (or local dir/glob) with options.
readParquetFilesWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetFilesWithOpts opts path
    | isHFUri path = readDownloaded opts =<< fetchHFParquetFiles path
    | otherwise = Parquet.readParquetFilesWithOpts opts path

{- | Read a list of downloaded local files, concatenate them, and apply the
global row range once over the result.
-}
readDownloaded :: ParquetReadOptions -> [FilePath] -> IO DataFrame
readDownloaded opts files = do
    let optsNoRange = opts{rowRange = Nothing}
    dfs <- mapM (Parquet.readParquetWithOpts optsNoRange) files
    pure (applyRowRange opts (mconcat dfs))

applyRowRange :: ParquetReadOptions -> DataFrame -> DataFrame
applyRowRange opts df =
    maybe df (`DS.range` df) (rowRange opts)

-- Lazy / streaming reader -------------------------------------------------

{- | Build a lazy 'Lazy.LazyDataFrame' over a @hf://@ dataset (or local path).
HuggingFace files are downloaded to a temp dir up front, then scanned lazily
from disk — constant-memory query, but the whole dataset is fetched first.
-}
scanParquet :: Schema -> T.Text -> IO Lazy.LazyDataFrame
scanParquet schema uri
    | isHFUri (T.unpack uri) = do
        dir <- downloadToDir (T.unpack uri)
        pure (Lazy.scanParquet schema (T.pack dir))
    | otherwise = pure (Lazy.scanParquet schema uri)

-- | Resolve a @hf://@ URI and download all matching files into a fresh temp dir.
downloadToDir :: FilePath -> IO FilePath
downloadToDir uri = do
    (mToken, files) <- resolveFiles uri
    tmp <- getCanonicalTemporaryDirectory
    dir <- createTempDirectory tmp "hf-parquet"
    _ <- downloadHFFilesTo dir mToken files
    pure dir

-- HuggingFace resolution --------------------------------------------------

data HFRef = HFRef
    { hfOwner :: T.Text
    , hfDataset :: T.Text
    , hfGlob :: T.Text
    }

data HFParquetFile = HFParquetFile
    { hfpUrl :: T.Text
    , hfpConfig :: T.Text
    , hfpSplit :: T.Text
    , hfpFilename :: T.Text
    }
    deriving (Show)

instance FromJSON HFParquetFile where
    parseJSON = withObject "HFParquetFile" $ \o ->
        HFParquetFile
            <$> o .: "url"
            <*> o .: "config"
            <*> o .: "split"
            <*> o .: "filename"

newtype HFParquetResponse = HFParquetResponse {hfParquetFiles :: [HFParquetFile]}

instance FromJSON HFParquetResponse where
    parseJSON = withObject "HFParquetResponse" $ \o ->
        HFParquetResponse <$> o .: "parquet_files"

isHFUri :: FilePath -> Bool
isHFUri = L.isPrefixOf "hf://"

parseHFUri :: FilePath -> Either String HFRef
parseHFUri path =
    let stripped = drop (length ("hf://datasets/" :: String)) path
     in case T.splitOn "/" (T.pack stripped) of
            (owner : dataset : rest)
                | not (null rest) ->
                    Right $ HFRef owner dataset (T.intercalate "/" rest)
            _ ->
                Left $ "Invalid hf:// URI (expected hf://datasets/owner/dataset/glob): " ++ path

getHFToken :: IO (Maybe BS.ByteString)
getHFToken = do
    envToken <- lookupEnv "HF_TOKEN"
    case envToken of
        Just t -> pure (Just (encodeUtf8 (T.pack t)))
        Nothing -> do
            home <- getHomeDirectory
            let tokenPath = home </> ".cache" </> "huggingface" </> "token"
            result <- try (BS.readFile tokenPath) :: IO (Either IOError BS.ByteString)
            case result of
                Right bs -> pure (Just (BS.takeWhile (/= 10) bs))
                Left _ -> pure Nothing

{- | Extract the repo-relative path from a HuggingFace download URL.
URL format: https://huggingface.co/datasets/{owner}/{dataset}/resolve/{ref}/{path}
Returns the {path} portion (e.g. "data/train-00000-of-00001.parquet").
-}
hfUrlRepoPath :: HFParquetFile -> String
hfUrlRepoPath f =
    case T.breakOn "/resolve/" (hfpUrl f) of
        (_, rest)
            | not (T.null rest) ->
                T.unpack $ T.drop 1 $ T.dropWhile (/= '/') $ T.drop (T.length "/resolve/") rest
        _ ->
            T.unpack (hfpConfig f) </> T.unpack (hfpSplit f) </> T.unpack (hfpFilename f)

matchesGlob :: T.Text -> HFParquetFile -> Bool
matchesGlob g f = match (compile (T.unpack g)) (hfUrlRepoPath f)

resolveHFUrls :: Maybe BS.ByteString -> HFRef -> IO [HFParquetFile]
resolveHFUrls mToken ref = do
    let dataset = hfOwner ref <> "/" <> hfDataset ref
    let apiUrl = "https://datasets-server.huggingface.co/parquet?dataset=" ++ T.unpack dataset
    req0 <- parseRequest apiUrl
    let req = case mToken of
            Nothing -> req0
            Just tok -> setRequestHeader "Authorization" ["Bearer " <> tok] req0
    resp <- httpBS req
    let status = getResponseStatusCode resp
    when (status /= 200) $
        ioError $
            userError $
                "HuggingFace API returned status "
                    ++ show status
                    ++ " for dataset "
                    ++ T.unpack dataset
    case eitherDecodeStrict (getResponseBody resp) of
        Left err -> ioError $ userError $ "Failed to parse HF API response: " ++ err
        Right hfResp -> pure $ filter (matchesGlob (hfGlob ref)) (hfParquetFiles hfResp)

-- | Download files into the system temp directory, returning the local paths.
downloadHFFiles :: Maybe BS.ByteString -> [HFParquetFile] -> IO [FilePath]
downloadHFFiles mToken files = do
    tmpDir <- getTemporaryDirectory
    downloadHFFilesTo tmpDir mToken files

-- | Download files into @destDir@, returning the local paths.
downloadHFFilesTo ::
    FilePath -> Maybe BS.ByteString -> [HFParquetFile] -> IO [FilePath]
downloadHFFilesTo destDir mToken files =
    forM files $ \f -> do
        let fname = case (hfpConfig f, hfpSplit f) of
                (c, s) | T.null c && T.null s -> T.unpack (hfpFilename f)
                (c, s) -> T.unpack c <> "_" <> T.unpack s <> "_" <> T.unpack (hfpFilename f)
        let destPath = destDir </> fname
        req0 <- parseRequest (T.unpack (hfpUrl f))
        let req = case mToken of
                Nothing -> req0
                Just tok -> setRequestHeader "Authorization" ["Bearer " <> tok] req0
        resp <- httpBS req
        let status = getResponseStatusCode resp
        when (status /= 200) $
            ioError $
                userError $
                    "Failed to download " ++ T.unpack (hfpUrl f) ++ " (HTTP " ++ show status ++ ")"
        BS.writeFile destPath (getResponseBody resp)
        pure destPath

-- | True when the path contains glob wildcard characters.
hasGlob :: T.Text -> Bool
hasGlob = T.any (\c -> c == '*' || c == '?' || c == '[')

{- | Build the direct HF repo download URL for a path with no wildcards.
Format: https://huggingface.co/datasets/{owner}/{dataset}/resolve/main/{path}
-}
directHFUrl :: HFRef -> T.Text
directHFUrl ref =
    "https://huggingface.co/datasets/"
        <> hfOwner ref
        <> "/"
        <> hfDataset ref
        <> "/resolve/main/"
        <> hfGlob ref

{- | Resolve a @hf://@ URI to the token and the list of files to fetch, without
downloading.
-}
resolveFiles :: FilePath -> IO (Maybe BS.ByteString, [HFParquetFile])
resolveFiles uri = do
    ref <- case parseHFUri uri of
        Left err -> ioError (userError err)
        Right r -> pure r
    mToken <- getHFToken
    files <-
        if hasGlob (hfGlob ref)
            then do
                hfFiles <- resolveHFUrls mToken ref
                when (null hfFiles) $
                    ioError $
                        userError $
                            "No parquet files found for " ++ uri
                pure hfFiles
            else do
                let url = directHFUrl ref
                let filename = last $ T.splitOn "/" (hfGlob ref)
                pure [HFParquetFile url "" "" filename]
    pure (mToken, files)

-- | Resolve and download all matching files for a @hf://@ URI to temp paths.
fetchHFParquetFiles :: FilePath -> IO [FilePath]
fetchHFParquetFiles uri = do
    (mToken, files) <- resolveFiles uri
    downloadHFFiles mToken files
