{-# LANGUAGE ForeignFunctionInterface #-}

{- | Foreign-function imports for the @dfusion-bridge@ Rust staticlib.
Pointers returned from Rust are owned by the caller and freed via
'df_ctx_free' / 'df_plan_free'. On error the @df_*@ functions return
null and set a thread-local message retrievable via 'df_last_error'.
-}
module DataFrame.Fusion.FFI (
    DfCtx,
    DfPlan,
    df_ctx_new,
    df_ctx_free,
    df_plan_free,
    df_plan_freep,
    df_last_error,
    df_scan_csv,
    df_plan_filter,
    df_plan_take,
    df_plan_select,
    df_plan_derive,
    df_plan_sort_by,
    df_plan_groupby_aggregate,
    df_plan_join,
    df_plan_collect,
) where

import Data.Word (Word64)
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (FunPtr, Ptr)

-- Opaque handle types. Layout is irrelevant on the Haskell side; we only
-- ever pass the pointers through.
data DfCtx
data DfPlan

foreign import ccall unsafe "df_ctx_new"
    df_ctx_new :: IO (Ptr DfCtx)

foreign import ccall unsafe "df_ctx_free"
    df_ctx_free :: Ptr DfCtx -> IO ()

foreign import ccall unsafe "df_plan_free"
    df_plan_free :: Ptr DfPlan -> IO ()

-- | A FunPtr to 'df_plan_free' suitable for use as a 'ForeignPtr' finalizer.
foreign import ccall unsafe "&df_plan_free"
    df_plan_freep :: FunPtr (Ptr DfPlan -> IO ())

foreign import ccall unsafe "df_last_error"
    df_last_error :: IO CString

foreign import ccall unsafe "df_scan_csv"
    df_scan_csv :: Ptr DfCtx -> CString -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_filter"
    df_plan_filter :: Ptr DfPlan -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_take"
    df_plan_take :: Ptr DfPlan -> Word64 -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_select"
    df_plan_select :: Ptr DfPlan -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_derive"
    df_plan_derive :: Ptr DfPlan -> CString -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_sort_by"
    df_plan_sort_by :: Ptr DfPlan -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_groupby_aggregate"
    df_plan_groupby_aggregate ::
        Ptr DfPlan -> CString -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_join"
    df_plan_join ::
        Ptr DfPlan -> Ptr DfPlan -> CString -> CString -> IO (Ptr DfPlan)

foreign import ccall unsafe "df_plan_collect"
    df_plan_collect :: Ptr DfPlan -> Ptr Word64 -> Ptr Word64 -> IO CInt
