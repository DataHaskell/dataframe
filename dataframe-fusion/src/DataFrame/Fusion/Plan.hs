{-# LANGUAGE OverloadedStrings #-}

-- | Plan-handle lifecycle and error wrapping for the DataFusion bridge.
module DataFrame.Fusion.Plan (
    -- * Context
    Context,
    newContext,
    withContext,

    -- * Plan handles
    PlanHandle,
    wrapPlan,
    withPlan,

    -- * Calling Rust
    runPlanOp,
    DataFusionError (..),

    -- * Materialization
    collectArrow,
) where

import Control.Exception (Exception, throwIO)
import qualified Data.Text as T
import Foreign.C.String (peekCString)
import Foreign.ForeignPtr (ForeignPtr, newForeignPtr, withForeignPtr)
import Foreign.Marshal.Alloc (alloca)
import Foreign.Ptr (Ptr, nullPtr, wordPtrToPtr)
import Foreign.Storable (peek)

import qualified DataFrame.Fusion.FFI as F
import qualified DataFrame.IO.Arrow as Arrow
import DataFrame.Internal.DataFrame (DataFrame)

-- | Process-wide DataFusion session + tokio runtime.
newtype Context = Context (Ptr F.DfCtx)

-- | A DataFusion plan handle.
newtype PlanHandle = PlanHandle (ForeignPtr F.DfPlan)

newtype DataFusionError = DataFusionError T.Text
    deriving (Show)
instance Exception DataFusionError

{- | Allocate a session. The handle stays alive until garbage-collected; in
practice that is the program lifetime.
-}
newContext :: IO Context
newContext = do
    p <- F.df_ctx_new
    if p == nullPtr
        then do
            err <- readLastError
            throwIO (DataFusionError ("newContext: " <> err))
        else return (Context p)

-- | Use the underlying context pointer for the duration of an action.
withContext :: Context -> (Ptr F.DfCtx -> IO a) -> IO a
withContext (Context p) k = k p

-- | Wrap a freshly returned plan pointer with a finalizer.
wrapPlan :: Ptr F.DfPlan -> IO PlanHandle
wrapPlan p
    | p == nullPtr = do
        err <- readLastError
        throwIO (DataFusionError ("plan op returned null: " <> err))
    | otherwise = PlanHandle <$> newForeignPtr F.df_plan_freep p

-- | Use the plan pointer for the duration of the action.
withPlan :: PlanHandle -> (Ptr F.DfPlan -> IO a) -> IO a
withPlan (PlanHandle fp) = withForeignPtr fp

-- | Run a Rust function that returns a plan pointer; throw on null.
runPlanOp :: IO (Ptr F.DfPlan) -> IO PlanHandle
runPlanOp action = action >>= wrapPlan

readLastError :: IO T.Text
readLastError = do
    cs <- F.df_last_error
    if cs == nullPtr
        then return "<no error message>"
        else T.pack <$> peekCString cs

-- | Execute a plan and import the result as an untyped DataFrame.
collectArrow :: PlanHandle -> IO DataFrame
collectArrow plan = withPlan plan $ \p ->
    alloca $ \schemaOutPtr ->
        alloca $ \arrayOutPtr -> do
            rc <- F.df_plan_collect p schemaOutPtr arrayOutPtr
            if rc /= 0
                then do
                    err <- readLastError
                    throwIO (DataFusionError ("collect: " <> err))
                else do
                    schemaAddr <- peek schemaOutPtr
                    arrayAddr <- peek arrayOutPtr
                    Arrow.arrowToDataframe
                        (wordPtrToPtr (fromIntegral schemaAddr))
                        (wordPtrToPtr (fromIntegral arrayAddr))
