{- | The shared read pipeline behind every "DataFrame.IO.CSV.Fast" entry
point: scan for delimiters, classify rows, then build every column with
the typed extraction passes — chunk-parallel when the input is large and
capabilities allow ('autoChunkCount'), unless the caller pins a chunk
count. The result is fully forced before it is returned.
-}
module DataFrame.IO.CSV.Fast.Core (
    readSeparatedCore,
) where

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as Text
import qualified Data.Vector as Vector
import qualified Data.Vector.Storable as VS

import Control.Exception (throwIO)
import Control.Monad (when)
import Data.Word (Word8)
import Debug.Trace (traceMarkerIO)

import DataFrame.IO.CSV (
    HeaderSpec (..),
    RaggedRowPolicy (..),
    ReadOptions (..),
    UnclosedQuotePolicy (..),
    schemaTypeMap,
 )
import DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    byteStringView,
    checkNoRaggedRows,
    classifyRowEnds,
    collectDataRows,
    fieldsInRow,
    getDelimiterIndicesPolicy,
 )
import DataFrame.IO.CSV.Fast.IndexPar (
    ParIndex (..),
    getDelimiterIndicesPar,
 )
import DataFrame.IO.CSV.Fast.Parallel (autoChunkCount, buildAllColumns)
import DataFrame.IO.CSV.Fast.Passes (ColumnEnv (..))
import DataFrame.IO.CSV.Fast.Slice (FieldCtx (..), extractField, stripBom)
import DataFrame.Internal.DataFrame (DataFrame (..), forceDataFrame)
import DataFrame.Operations.Typing (effectiveSafeRead, parseWithTypes)

{- | @readSeparatedCore chunkSpec separator opts buffer@; @chunkSpec@
pins the chunk count ('Nothing' = capability\/size-gated automatic).
-}
readSeparatedCore ::
    Maybe Int ->
    Word8 ->
    ReadOptions ->
    VS.Vector Word8 ->
    IO DataFrame
readSeparatedCore chunkSpec separator opts fileRaw = do
    let (file, _bomLen) = stripBom fileRaw
        contentLen = VS.length file
    nChunks <- maybe (autoChunkCount contentLen) pure chunkSpec
    -- Parallel reads scan in parallel too; sequential reads (or builds
    -- without SIMD) keep the original scan + row classification.
    mPar <-
        if nChunks > 1
            then getDelimiterIndicesPar nChunks separator contentLen file
            else pure Nothing
    (indices, rowEnds) <- case mPar of
        Just par -> do
            when
                ( piEndsInQuote par
                    && fastCsvOnUnclosedQuote opts == RaiseOnUnclosedQuote
                )
                (throwIO CsvUnclosedQuote)
            pure (piDelims par, piRowEnds par)
        Nothing -> do
            is <-
                getDelimiterIndicesPolicy
                    (fastCsvOnUnclosedQuote opts)
                    separator
                    contentLen
                    file
            pure (is, classifyRowEnds file contentLen is)
    -- Eventlog phase markers (visible under +RTS -l; no-ops otherwise).
    traceMarkerIO "fastcsv:scan-done"
    let totalRows = VS.length rowEnds
    if totalRows == 0
        then return emptyDataFrame
        else do
            let ctx =
                    FieldCtx
                        { fcFile = file
                        , fcBS = byteStringView contentLen file
                        , fcDelims = indices
                        , fcRowEnds = rowEnds
                        , fcContentLen = contentLen
                        , fcTrim = fastCsvTrimUnquoted opts
                        }
                headerNumCol = fieldsInRow rowEnds 0
                (columnNames, dataStartRow, numCol) = case headerSpec opts of
                    NoHeader ->
                        ( Vector.fromList $
                            map (Text.pack . show) [0 .. headerNumCol - 1]
                        , 0
                        , headerNumCol
                        )
                    UseFirstRow ->
                        ( Vector.fromList $
                            map (extractField ctx 0) [0 .. headerNumCol - 1]
                        , 1
                        , headerNumCol
                        )
                    ProvideNames ns ->
                        (Vector.fromList ns, 0, length ns)
            if numCol == 0
                then return emptyDataFrame
                else do
                    let dataRows =
                            collectDataRows
                                file
                                indices
                                rowEnds
                                contentLen
                                dataStartRow
                                totalRows
                        numRow = VS.length dataRows
                    when (fastCsvOnRaggedRow opts == RaiseOnRagged) $
                        checkNoRaggedRows rowEnds dataRows numCol
                    traceMarkerIO "fastcsv:rows-done"
                    VS.unsafeWith file $ \filePtr -> do
                        let env =
                                ColumnEnv
                                    { ceCtx = ctx
                                    , ceRows = dataRows
                                    , ceNumRow = numRow
                                    , cePtr = filePtr
                                    , ceTextHint =
                                        max 64 (contentLen `div` numCol)
                                    , ceOpts = opts
                                    }
                        results <- buildAllColumns nChunks env columnNames numCol
                        traceMarkerIO "fastcsv:build-done"
                        let cols = Vector.fromListN numCol (map fst results)
                            legacySchema =
                                S.fromList
                                    [ columnNames Vector.! c
                                    | (c, (_, True)) <- zip [0 ..] results
                                    ]
                            colIndices =
                                M.fromList $
                                    zip (Vector.toList columnNames) [0 ..]
                            df =
                                DataFrame
                                    cols
                                    colIndices
                                    (numRow, numCol)
                                    M.empty
                            resolveMode =
                                effectiveSafeRead
                                    (safeRead opts)
                                    (safeReadOverrides opts)
                            df' =
                                if S.null legacySchema
                                    then df
                                    else
                                        parseWithTypes
                                            resolveMode
                                            ( M.restrictKeys
                                                (schemaTypeMap (typeSpec opts))
                                                legacySchema
                                            )
                                            df
                        -- The parallel merge already forced every column
                        -- payload, so only the column thunks themselves
                        -- need forcing; the deep walk would re-chase 6M+
                        -- pointers per boxed column for nothing.
                        if nChunks > 1 && S.null legacySchema
                            then do
                                Vector.foldl' (\() c -> c `seq` ()) () cols `seq`
                                    return df
                            else return $! forceDataFrame df'

{- | An empty 'DataFrame' — returned when the input has no delimiters
(empty file or single line with no separator and no newline). Guards
against a divide-by-zero in the row-stride math when 'numCol == 0'.
-}
emptyDataFrame :: DataFrame
emptyDataFrame = DataFrame Vector.empty M.empty (0, 0) M.empty
