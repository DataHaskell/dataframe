{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Driver for the default CSV reader (Round-2 WS-D): a single strict
scan feeding per-column sinks, with cassava-parity semantics pinned by
@IO.CsvGolden@. Ragged rows pad trailing columns with null and drop
extra fields (audit D6). The result is fully forced (audit D7).
-}
module DataFrame.IO.CSV.Internal.Read (decodeCsvStrict) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import qualified Data.Map.Strict as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V

import Control.Monad (when, zipWithM)
import Control.Monad.ST (stToIO)
import Data.Char (ord)
import Data.Maybe (fromMaybe)
import Data.Type.Equality (TestEquality (testEquality))
import Data.Word (Word8)
import DataFrame.IO.CSV.Internal.Infer
import DataFrame.IO.CSV.Internal.Options
import DataFrame.IO.CSV.Internal.Scanner
import DataFrame.IO.CSV.Internal.Sink
import DataFrame.Internal.Column (Column, ensureOptional)
import DataFrame.Internal.ColumnBuilder
import DataFrame.Internal.DataFrame (DataFrame (..), forceDataFrame)
import DataFrame.Internal.Schema (SchemaType (..), schemaType)
import DataFrame.Operations.Typing (
    SafeReadMode (..),
    effectiveSafeRead,
    parseWithTypes,
 )
import Foreign.Ptr (castPtr)
import Type.Reflection (typeRep)

-- | Decode a strict CSV buffer into a fully forced DataFrame.
decodeCsvStrict :: ReadOptions -> BS.ByteString -> IO DataFrame
decodeCsvStrict opts bs = BSU.unsafeUseAsCStringLen bs $ \(cstr, _) -> do
    let !len = BS.length bs
        !sep = fromIntegral (ord (columnSeparator opts)) :: Word8

        -- Position of the next non-blank record at/after @pos@ (cassava
        -- drops single-empty-field records); @len@ when none remain.
        findRecord !pos
            | pos >= len = len
            | otherwise = withField bs len sep pos $ \cs ce _ term next ->
                if cs == ce && term /= termSep
                    then if term == termEof then len else findRecord next
                    else pos

        -- Decode the record at @pos@ as header fields; returns the
        -- position after the record.
        headerFields !pos = go pos id
          where
            go !p acc = withField bs len sep p $ \cs ce unesc term next ->
                let t =
                        TE.decodeUtf8Lenient
                            (if unesc then unescapeQuotes bs cs ce else sliceBS bs cs ce)
                 in if term == termSep
                        then go next (acc . (t :))
                        else (acc [t], next)

    let hdrPos = findRecord 0
    when (hdrPos >= len) (error "Empty CSV file")
    let (hdrFields, afterHdr) = headerFields hdrPos
        positional n = map (T.pack . show) [0 .. n - 1 :: Int]
        (names, dataPos0) = case headerSpec opts of
            UseFirstRow -> (map T.strip hdrFields, afterHdr)
            NoHeader -> (positional (length hdrFields), hdrPos)
            ProvideNames ns ->
                (ns ++ drop (length ns) (positional (length hdrFields)), hdrPos)
        !ncols = length names
        dataPos = findRecord dataPos0
    when (dataPos >= len) (error "Empty CSV file")

    let resolveMode = effectiveSafeRead (safeRead opts) (safeReadOverrides opts)
        -- If ANY column is EitherRead we keep every raw cell (including
        -- "N/A" etc.) verbatim; otherwise the missing-indicator list applies.
        anyEither = any (\n -> resolveMode n == EitherRead) names
        missing = if anyEither then [] else missingIndicators opts
        missMode
            | null missing = MissNone
            | missing == missingIndicators defaultReadOptions = MissCanonical
            | otherwise = MissCustom
        env = Env bs (castPtr cstr) missMode missing
        rowHint = len `div` max 1 (ncols * 8) + 16

    sinks <- mapM (newSink opts resolveMode rowHint) names
    let !sinksV = V.fromList sinks

    let pad !row !col
            | col >= ncols = pure ()
            | otherwise = nullSink (V.unsafeIndex sinksV col) row >> pad row (col + 1)
        dec b = if b > 0 then b - 1 else b
        rowLoop !pos !row !budget
            | budget == 0 || pos >= len = pure row
            | otherwise = withField bs len sep pos $ \cs ce unesc term next ->
                if cs == ce && term /= termSep
                    then if term == termEof then pure row else rowLoop next row budget
                    else do
                        feedSink env (V.unsafeIndex sinksV 0) row cs ce unesc
                        if term == termSep
                            then fieldLoop next row 1 budget
                            else pad row 1 >> rowLoop next (row + 1) (dec budget)
        fieldLoop !pos !row !col !budget =
            withField bs len sep pos $ \cs ce unesc term next -> do
                when (col < ncols) $
                    feedSink env (V.unsafeIndex sinksV col) row cs ce unesc
                if term == termSep
                    then fieldLoop next row (col + 1) budget
                    else pad row (col + 1) >> rowLoop next (row + 1) (dec budget)

    nrows <- rowLoop dataPos 0 (fromMaybe (-1) (numColumns opts))

    cols <- zipWithM (finalizeSink bs opts resolveMode nrows) names sinks
    let df =
            DataFrame
                (V.fromList cols)
                (M.fromList (zip names [0 ..]))
                (nrows, ncols)
                M.empty
    pure $!
        forceDataFrame
            (parseWithTypes resolveMode (schemaTypeMap (typeSpec opts)) df)

{- | Resolve a column's sink: EitherRead and to-be-inferred columns
collect raw slices; schema'd Int/Double parse straight into builders;
every other schema type builds Text ('parseWithTypes' converts after).
-}
newSink ::
    ReadOptions -> (T.Text -> SafeReadMode) -> Int -> T.Text -> IO Sink
newSink opts resolveMode rowHint name
    | resolveMode name == EitherRead = SinkBS <$> newSliceCol rowHint
    | Nothing <- entry
    , shouldInferFromSample (typeSpec opts) =
        SinkBS <$> newSliceCol rowHint
    | otherwise = case fromMaybe (schemaType @T.Text) entry of
        SType (_ :: P.Proxy a) -> case testEquality (typeRep @a) (typeRep @Int) of
            Just _ -> SinkInt <$> stToIO (newIntBuilder rowHint)
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just _ -> SinkDouble <$> stToIO (newDoubleBuilder rowHint)
                Nothing ->
                    SinkText <$> stToIO (newTextBuilder rowHint (rowHint * 8))
  where
    entry = M.lookup name (schemaTypeMap (typeSpec opts))

finalizeSink ::
    BS.ByteString ->
    ReadOptions ->
    (T.Text -> SafeReadMode) ->
    Int ->
    T.Text ->
    Sink ->
    IO Column
finalizeSink bs opts resolveMode nrows name sink = do
    col <- case sink of
        SinkInt b -> stToIO (freezeBuilder b)
        SinkDouble b -> stToIO (freezeBuilder b)
        SinkText b -> stToIO (freezeBuilder b)
        SinkBS sc -> do
            cells <- freezeCells bs sc nrows
            inferColumnFromBS (resolveMode name) opts cells
    pure $! case resolveMode name of
        MaybeRead -> ensureOptional col
        _ -> col
