{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TypeFamilies #-}

module DataFrame.IO.Parquet.Writer.PageWriter (
    PageWriter (..),
    PageState (..),
    newPageState,
    askPage,
    recordDef,
    bumpRows,
    pageRows,
    assemblePageBody,
    resetPage,
) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.IORef (IORef, modifyIORef', newIORef, readIORef, writeIORef)
import DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    flushDef,
    newDefLevels,
    pushDef,
 )
import DataFrame.IO.Utils.RandomAccess (
    HasBuffer (..),
    MemoryBuffer,
    ReaderIO (runReaderIO),
    Sink (..),
    bufferResidency,
    copyBuffer,
    mallocBuffer,
    onBuffer,
    putWord32LE,
    resetPosition,
 )

data PageState = PageState
    { psValues :: !MemoryBuffer
    , psScratch :: !MemoryBuffer
    , psDefs :: !DefLevels
    , psRows :: !(IORef Int)
    , psNullable :: !Bool
    }

newPageState :: Int -> Bool -> IO PageState
newPageState cap nullable = do
    values <- mallocBuffer cap
    scratch <- mallocBuffer cap
    defs <- newDefLevels
    rows <- newIORef 0
    pure
        PageState
            { psValues = values
            , psScratch = scratch
            , psDefs = defs
            , psRows = rows
            , psNullable = nullable
            }

newtype PageWriter a = PageWriter {runPageWriter :: PageState -> IO a}

instance Functor PageWriter where
    fmap f (PageWriter g) = PageWriter (fmap f . g)

instance Applicative PageWriter where
    pure x = PageWriter (const (pure x))
    PageWriter f <*> PageWriter g = PageWriter (\s -> f s <*> g s)

instance Monad PageWriter where
    PageWriter m >>= k = PageWriter (\s -> m s >>= \a -> runPageWriter (k a) s)

instance MonadIO PageWriter where
    liftIO io = PageWriter (const io)

instance HasBuffer PageWriter where
    type Buffer PageWriter = MemoryBuffer
    askBuffer = PageWriter (pure . psValues)
    residency = PageWriter (bufferResidency . psValues)
    writeBytes bytes = PageWriter (\ps -> runReaderIO (writeBytes bytes) (psValues ps))
    flushTo sink = PageWriter (\ps -> runReaderIO (flushTo sink) (psValues ps))

askPage :: PageWriter PageState
askPage = PageWriter pure

recordDef :: Bool -> PageWriter ()
recordDef present = PageWriter (\ps -> pushDef (psDefs ps) (if present then 1 else 0))

bumpRows :: PageWriter ()
bumpRows = PageWriter (\ps -> modifyIORef' (psRows ps) (+ 1))

pageRows :: PageWriter Int
pageRows = PageWriter (readIORef . psRows)

assemblePageBody :: Bool -> PageWriter MemoryBuffer
assemblePageBody nullable = do
    ps <- askPage
    if not nullable
        then pure (psValues ps)
        else do
            liftIO (flushDef (psDefs ps))
            liftIO (resetPosition (psScratch ps))
            defSize <- liftIO (bufferResidency ps.psDefs.dlBuf)
            onBuffer (psScratch ps) $ do
                putWord32LE (fromIntegral defSize)
                copyBuffer ps.psDefs.dlBuf
                copyBuffer (psValues ps)
            pure (psScratch ps)

resetPage :: PageWriter ()
resetPage = PageWriter $ \ps -> do
    resetPosition (psValues ps)
    resetPosition ps.psDefs.dlBuf
    resetPosition (psScratch ps)
    writeIORef (psRows ps) 0
