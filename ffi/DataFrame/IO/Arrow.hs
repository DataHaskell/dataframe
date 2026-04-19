{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- | Convert a 'DataFrame' to Arrow C Data Interface structs for zero-copy
  transfer to Python (or any other Arrow consumer).
-}
module DataFrame.IO.Arrow (
    dataframeToArrow,
    columnToArrow,
    arrowToDataframe,
    -- exported only to force linking of release-callback symbols
    releaseSchemaImpl,
    releaseArrayImpl,
) where

import qualified Data.ByteString as BS
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.Internal.Column as DI

import Control.Monad (foldM_, forM, join, void, when, zipWithM_)
import Data.Bits (popCount, setBit, testBit)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe, isNothing)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Word (Word8)
import Foreign hiding (void)
import Foreign.C.String (CString, newCString, peekCString)
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Operations.Core (fromNamedColumns)

-- ---------------------------------------------------------------------------
-- Opaque phantom types for the Arrow structs
-- ---------------------------------------------------------------------------

data ArrowSchema
data ArrowArray

arrowSchemaSize :: Int
arrowSchemaSize = 72 -- 9 × 8 bytes

arrowArraySize :: Int
arrowArraySize = 80 -- 10 × 8 bytes

-- ArrowSchema field byte offsets
_schemaFormat
    , _schemaName
    , _schemaMetadata
    , _schemaFlags
    , _schemaNChildren
    , _schemaChildren
    , _schemaDictionary
    , _schemaRelease
    , _schemaPrivateData ::
        Int
_schemaFormat = 0
_schemaName = 8
_schemaMetadata = 16
_schemaFlags = 24
_schemaNChildren = 32
_schemaChildren = 40
_schemaDictionary = 48
_schemaRelease = 56
_schemaPrivateData = 64

-- ArrowArray field byte offsets
_arrayLength
    , _arrayNullCount
    , _arrayOffset
    , _arrayNBuffers
    , _arrayNChildren
    , _arrayBuffers
    , _arrayChildren
    , _arrayDictionary
    , _arrayRelease
    , _arrayPrivateData ::
        Int
_arrayLength = 0
_arrayNullCount = 8
_arrayOffset = 16
_arrayNBuffers = 24
_arrayNChildren = 32
_arrayBuffers = 40
_arrayChildren = 48
_arrayDictionary = 56
_arrayRelease = 64
_arrayPrivateData = 72

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

-- Write a Storable value at a byte offset from a base pointer.
at :: (Storable a) => Ptr b -> Int -> a -> IO ()
at p off = poke (castPtr (p `plusPtr` off))

-- Read a Storable value at a byte offset from a base pointer.
readAt :: (Storable a) => Ptr b -> Int -> IO a
readAt p off = peek (castPtr (p `plusPtr` off))

-- ---------------------------------------------------------------------------
-- Release callbacks (self-import trick for compile-time-constant FunPtr)
-- ---------------------------------------------------------------------------

foreign export ccall "df_release_schema"
    releaseSchemaImpl :: Ptr ArrowSchema -> IO ()

foreign import ccall "&df_release_schema"
    pReleaseSchema :: FunPtr (Ptr ArrowSchema -> IO ())

foreign export ccall "df_release_array"
    releaseArrayImpl :: Ptr ArrowArray -> IO ()

foreign import ccall "&df_release_array"
    pReleaseArray :: FunPtr (Ptr ArrowArray -> IO ())

-- Dynamic wrappers to call producer's release callbacks after copying.
foreign import ccall "dynamic"
    callRelSchema :: FunPtr (Ptr ArrowSchema -> IO ()) -> Ptr ArrowSchema -> IO ()

foreign import ccall "dynamic"
    callRelArray :: FunPtr (Ptr ArrowArray -> IO ()) -> Ptr ArrowArray -> IO ()

releaseSchemaImpl :: Ptr ArrowSchema -> IO ()
releaseSchemaImpl p = do
    rawPriv <- peek (castPtr (p `plusPtr` _schemaPrivateData) :: Ptr (Ptr ()))
    let sp = castPtrToStablePtr rawPriv :: StablePtr (IO ())
    join (deRefStablePtr sp)
    freeStablePtr sp
    -- Arrow spec: release callback must set release to NULL to signal completion.
    -- p here is Arrow C++'s internal copy of the struct (not our mallocBytes
    -- allocation); our original allocation is freed inside the cleanup closure.
    p `at` _schemaRelease $ (nullFunPtr :: FunPtr (Ptr ArrowSchema -> IO ()))

releaseArrayImpl :: Ptr ArrowArray -> IO ()
releaseArrayImpl p = do
    rawPriv <- peek (castPtr (p `plusPtr` _arrayPrivateData) :: Ptr (Ptr ()))
    let sp = castPtrToStablePtr rawPriv :: StablePtr (IO ())
    join (deRefStablePtr sp)
    freeStablePtr sp
    -- Same reasoning as releaseSchemaImpl.
    p `at` _arrayRelease $ (nullFunPtr :: FunPtr (Ptr ArrowArray -> IO ()))

makeLeafSchema :: String -> T.Text -> IO (Ptr ArrowSchema)
makeLeafSchema fmt colName = do
    p <- mallocBytes arrowSchemaSize
    fmtStr <- newCString fmt
    nameStr <- newCString (T.unpack colName)
    p `at` _schemaFormat $ fmtStr
    p `at` _schemaName $ nameStr
    p `at` _schemaMetadata $ (nullPtr :: Ptr ())
    p `at` _schemaFlags $ (0 :: Int64)
    p `at` _schemaNChildren $ (0 :: Int64)
    p `at` _schemaChildren $ (nullPtr :: Ptr ())
    p `at` _schemaDictionary $ (nullPtr :: Ptr ())
    p `at` _schemaRelease $ pReleaseSchema
    -- Capture p so our original mallocBytes allocation is freed when release runs.
    cleanup <- newStablePtr (free fmtStr >> free nameStr >> free p)
    p `at` _schemaPrivateData $ castStablePtrToPtr cleanup
    return p

makeLeafArray :: Int -> Int64 -> [Ptr ()] -> IO () -> IO (Ptr ArrowArray)
makeLeafArray nRows nullCnt bufPtrs extraCleanup = do
    p <- mallocBytes arrowArraySize
    let nb = length bufPtrs
    bufArr <- mallocArray nb :: IO (Ptr (Ptr ()))
    zipWithM_ (pokeElemOff bufArr) [0 ..] bufPtrs
    p `at` _arrayLength $ (fromIntegral nRows :: Int64)
    p `at` _arrayNullCount $ nullCnt
    p `at` _arrayOffset $ (0 :: Int64)
    p `at` _arrayNBuffers $ (fromIntegral nb :: Int64)
    p `at` _arrayNChildren $ (0 :: Int64)
    p `at` _arrayBuffers $ bufArr
    p `at` _arrayChildren $ (nullPtr :: Ptr ())
    p `at` _arrayDictionary $ (nullPtr :: Ptr ())
    p `at` _arrayRelease $ pReleaseArray
    -- Capture p so our original mallocBytes allocation is freed when release runs.
    cleanup <- newStablePtr (free bufArr >> extraCleanup >> free p)
    p `at` _arrayPrivateData $ castStablePtrToPtr cleanup
    return p

{- | Allocate an Arrow-format validity bitmap from a 'DI.Bitmap'.
Returns (ptr, nullCount). Caller must 'free' the pointer.
-}
bitmapToPtr :: Int -> DI.Bitmap -> IO (Ptr Word8, Int)
bitmapToPtr n bm = do
    let numBytes = max 1 ((n + 7) `div` 8)
        validCount = VU.foldl' (\acc b -> acc + popCount b) 0 bm
        nullCount = n - validCount
    bitmapPtr <- mallocBytes numBytes :: IO (Ptr Word8)
    VU.imapM_ (pokeElemOff bitmapPtr) bm
    when (VU.length bm < numBytes) $
        mapM_
            (\i -> pokeElemOff bitmapPtr i (0 :: Word8))
            [VU.length bm .. numBytes - 1]
    return (bitmapPtr, nullCount)

-- | Read an Arrow validity bitmap into a 'DI.Bitmap'.
readArrowBitmap :: Ptr Word8 -> Int -> IO DI.Bitmap
readArrowBitmap bitmapPtr n = VU.generateM ((n + 7) `div` 8) (peekElemOff bitmapPtr)

columnToArrow :: T.Text -> Column -> IO (Ptr ArrowSchema, Ptr ArrowArray)
columnToArrow colName (UnboxedColumn _ (vec :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = do
        let n = VU.length vec
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Int64)
        VU.imapM_ (\i v -> pokeElemOff dataPtr i (fromIntegral v)) vec
        sPtr <- makeLeafSchema "l" colName
        aPtr <- makeLeafArray n 0 [nullPtr, castPtr dataPtr] (free dataPtr)
        return (sPtr, aPtr)
columnToArrow colName (UnboxedColumn _ (vec :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = do
        let n = VU.length vec
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Double)
        VU.imapM_ (pokeElemOff dataPtr) vec
        sPtr <- makeLeafSchema "g" colName
        aPtr <- makeLeafArray n 0 [nullPtr, castPtr dataPtr] (free dataPtr)
        return (sPtr, aPtr)
columnToArrow colName (BoxedColumn _ (vec :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) = do
        let n = V.length vec
            bss = map TE.encodeUtf8 (V.toList vec)
            cumOff = scanl (+) 0 (map BS.length bss)
            total = last cumOff
        offPtr <- mallocArray (n + 1) :: IO (Ptr Int32)
        zipWithM_
            (\i o -> pokeElemOff offPtr i (fromIntegral o :: Int32))
            [0 ..]
            cumOff
        charsPtr <- mallocBytes (max 1 total) :: IO (Ptr Word8)
        foldM_
            ( \pos bs -> do
                BS.useAsCStringLen bs $ \(src, len) ->
                    copyBytes (charsPtr `plusPtr` pos) (castPtr src) len
                return (pos + BS.length bs)
            )
            0
            bss
        sPtr <- makeLeafSchema "u" colName
        aPtr <-
            makeLeafArray
                n
                0
                [nullPtr, castPtr offPtr, castPtr charsPtr]
                (free offPtr >> free charsPtr)
        return (sPtr, aPtr)
columnToArrow colName (BoxedColumn _ (vec :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = do
        let n = V.length vec
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Double)
        V.imapM_ (pokeElemOff dataPtr) vec
        sPtr <- makeLeafSchema "g" colName
        aPtr <- makeLeafArray n 0 [nullPtr, castPtr dataPtr] (free dataPtr)
        return (sPtr, aPtr)
columnToArrow colName (BoxedColumn _ (vec :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = do
        let n = V.length vec
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Int64)
        V.imapM_ (\i v -> pokeElemOff dataPtr i (fromIntegral v)) vec
        sPtr <- makeLeafSchema "l" colName
        aPtr <- makeLeafArray n 0 [nullPtr, castPtr dataPtr] (free dataPtr)
        return (sPtr, aPtr)
-- Nullable Int (UnboxedColumn with bitmap)
columnToArrow colName (UnboxedColumn (Just bm) (vec :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = do
        let n = VU.length vec
        (bitmapPtr, nullCount) <- bitmapToPtr n bm
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Int64)
        VU.imapM_ (\i v -> pokeElemOff dataPtr i (fromIntegral v :: Int64)) vec
        sPtr <- makeLeafSchema "l" colName
        aPtr <-
            makeLeafArray
                n
                (fromIntegral nullCount)
                [castPtr bitmapPtr, castPtr dataPtr]
                (free bitmapPtr >> free dataPtr)
        return (sPtr, aPtr)
-- Nullable Double (UnboxedColumn with bitmap)
columnToArrow colName (UnboxedColumn (Just bm) (vec :: VU.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = do
        let n = VU.length vec
        (bitmapPtr, nullCount) <- bitmapToPtr n bm
        dataPtr <- mallocArray (max 1 n) :: IO (Ptr Double)
        VU.imapM_ (\i v -> pokeElemOff dataPtr i (realToFrac v :: Double)) vec
        sPtr <- makeLeafSchema "g" colName
        aPtr <-
            makeLeafArray
                n
                (fromIntegral nullCount)
                [castPtr bitmapPtr, castPtr dataPtr]
                (free bitmapPtr >> free dataPtr)
        return (sPtr, aPtr)
-- Nullable Text (BoxedColumn with bitmap)
columnToArrow colName (BoxedColumn (Just bm) (vec :: V.Vector a))
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) = do
        let n = V.length vec
            -- For null positions, use empty BS (null placeholder in vec is never evaluated)
            bss =
                map
                    (\i -> if DI.bitmapTestBit bm i then TE.encodeUtf8 (vec V.! i) else BS.empty)
                    [0 .. n - 1]
            cumOff = scanl (+) 0 (map BS.length bss)
            total = last cumOff
        (bitmapPtr, nullCount) <- bitmapToPtr n bm
        offPtr <- mallocArray (n + 1) :: IO (Ptr Int32)
        zipWithM_
            (\i o -> pokeElemOff offPtr i (fromIntegral o :: Int32))
            [0 ..]
            cumOff
        charsPtr <- mallocBytes (max 1 total) :: IO (Ptr Word8)
        foldM_
            ( \pos bs -> do
                BS.useAsCStringLen bs $ \(src, len) ->
                    copyBytes (charsPtr `plusPtr` pos) (castPtr src) len
                return (pos + BS.length bs)
            )
            0
            bss
        sPtr <- makeLeafSchema "u" colName
        aPtr <-
            makeLeafArray
                n
                (fromIntegral nullCount)
                [castPtr bitmapPtr, castPtr offPtr, castPtr charsPtr]
                (free bitmapPtr >> free offPtr >> free charsPtr)
        return (sPtr, aPtr)
columnToArrow colName _ =
    error $
        "DataFrame.IO.Arrow.columnToArrow: unsupported column type for '"
            ++ T.unpack colName
            ++ "'"

dataframeToArrow :: DataFrame -> IO (Ptr ArrowSchema, Ptr ArrowArray)
dataframeToArrow df = do
    let idxToName = M.fromList [(v, k) | (k, v) <- M.toList (columnIndices df)]
        ncols = M.size (columnIndices df)
        colsInOrder =
            [ (idxToName M.! i, columns df V.! i)
            | i <- [0 .. ncols - 1]
            ]

    childPairs <- forM colsInOrder (uncurry columnToArrow)
    let childSPtrs = map fst childPairs
        childAPtrs = map snd childPairs

    let nRows = case colsInOrder of
            [] -> 0
            (_, col) : _ -> DI.columnLength col
    topSchema <- mallocBytes arrowSchemaSize
    fmtStr <- newCString "+s"
    nameStr <- newCString ""
    childSArr <- mallocArray ncols :: IO (Ptr (Ptr ArrowSchema))
    zipWithM_ (pokeElemOff childSArr) [0 ..] childSPtrs
    topSchema `at` _schemaFormat $ fmtStr
    topSchema `at` _schemaName $ nameStr
    topSchema `at` _schemaMetadata $ (nullPtr :: Ptr ())
    topSchema `at` _schemaFlags $ (0 :: Int64)
    topSchema `at` _schemaNChildren $ (fromIntegral ncols :: Int64)
    topSchema `at` _schemaChildren $ childSArr
    topSchema `at` _schemaDictionary $ (nullPtr :: Ptr ())
    topSchema `at` _schemaRelease $ pReleaseSchema
    -- Do NOT loop over children here: Arrow C++ zeroes children[i]->release
    -- during import, so reading it would yield a null function pointer.
    -- Children are released independently by Arrow C++; their own cleanup
    -- closures free their buffers and struct memory.
    cleanupS <- newStablePtr $ do
        free childSArr
        free fmtStr
        free nameStr
        free topSchema -- free our original mallocBytes allocation
    topSchema `at` _schemaPrivateData $ castStablePtrToPtr cleanupS

    -- ── Top-level struct array ──────────────────────────────────────────────
    topArray <- mallocBytes arrowArraySize
    childAArr <- mallocArray ncols :: IO (Ptr (Ptr ArrowArray))
    zipWithM_ (pokeElemOff childAArr) [0 ..] childAPtrs
    topBufArr <- mallocArray 1 :: IO (Ptr (Ptr ()))
    pokeElemOff topBufArr 0 nullPtr
    topArray `at` _arrayLength $ (fromIntegral nRows :: Int64)
    topArray `at` _arrayNullCount $ (0 :: Int64)
    topArray `at` _arrayOffset $ (0 :: Int64)
    topArray `at` _arrayNBuffers $ (1 :: Int64)
    topArray `at` _arrayNChildren $ (fromIntegral ncols :: Int64)
    topArray `at` _arrayBuffers $ topBufArr
    topArray `at` _arrayChildren $ childAArr
    topArray `at` _arrayDictionary $ (nullPtr :: Ptr ())
    topArray `at` _arrayRelease $ pReleaseArray
    -- Same reasoning as cleanupS: Arrow C++ manages children independently.
    cleanupA <- newStablePtr $ do
        free childAArr
        free topBufArr
        free topArray -- free our original mallocBytes allocation
    topArray `at` _arrayPrivateData $ castStablePtrToPtr cleanupA

    return (topSchema, topArray)

-- | Test whether bit i is set in a validity bitmap.
bitmapIsSet :: Ptr Word8 -> Int -> IO Bool
bitmapIsSet bitmapPtr i =
    testBit <$> peekElemOff bitmapPtr (i `div` 8) <*> pure (i `mod` 8)

{- | Import an Arrow RecordBatch from raw C Data Interface pointers.
  Copies all data into GC-managed Haskell vectors, then calls the
  producer's release callbacks.
-}
arrowToDataframe :: Ptr () -> Ptr () -> IO DataFrame
arrowToDataframe rawSchema rawArray = do
    let schemaPtr = castPtr rawSchema :: Ptr ArrowSchema
        arrayPtr = castPtr rawArray :: Ptr ArrowArray
    nCols <- readAt schemaPtr _schemaNChildren :: IO Int64
    childSArr <- readAt schemaPtr _schemaChildren :: IO (Ptr (Ptr ArrowSchema))
    childAArr <- readAt arrayPtr _arrayChildren :: IO (Ptr (Ptr ArrowArray))
    cols <- forM [0 .. fromIntegral nCols - 1] $ \i -> do
        cs <- peekElemOff childSArr i
        ca <- peekElemOff childAArr i
        readArrowColumn cs ca
    -- Call producer's release callbacks after all data has been copied.
    relA <- readAt arrayPtr _arrayRelease :: IO (FunPtr (Ptr ArrowArray -> IO ()))
    when (relA /= nullFunPtr) $ callRelArray relA arrayPtr
    relS <-
        readAt schemaPtr _schemaRelease :: IO (FunPtr (Ptr ArrowSchema -> IO ()))
    when (relS /= nullFunPtr) $ callRelSchema relS schemaPtr
    return $ fromNamedColumns cols

readArrowColumn :: Ptr ArrowSchema -> Ptr ArrowArray -> IO (T.Text, Column)
readArrowColumn schemaPtr arrayPtr = do
    fmtStr <- (readAt schemaPtr _schemaFormat :: IO CString) >>= peekCString
    nameStr <- (readAt schemaPtr _schemaName :: IO CString) >>= peekCString
    let name = T.pack nameStr
    len <- readAt arrayPtr _arrayLength :: IO Int64
    nullCnt <- readAt arrayPtr _arrayNullCount :: IO Int64
    bufArr <- readAt arrayPtr _arrayBuffers :: IO (Ptr (Ptr ()))
    let n = fromIntegral len
    col <- case fmtStr of
        "l" -> readInt64Col n nullCnt bufArr
        "i" -> readInt32Col n nullCnt bufArr
        "g" -> readFloat64Col n nullCnt bufArr
        "f" -> readFloat32Col n nullCnt bufArr
        "U" -> readLargeUtf8Col n nullCnt bufArr
        "u" -> readUtf8Col n nullCnt bufArr
        _ ->
            error $
                "DataFrame.IO.Arrow.readArrowColumn: unsupported format '"
                    ++ fmtStr
                    ++ "' for column '"
                    ++ nameStr
                    ++ "'"
    return (name, col)

readInt64Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readInt64Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    dataVoid <- peekElemOff bufArr 1
    let dataPtr = castPtr dataVoid :: Ptr Int64
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- VU.generateM n $ \i -> fmap fromIntegral (peekElemOff dataPtr i :: IO Int64)
            return $ UnboxedColumn (Just bm) (vec :: VU.Vector Int)
        else do
            vec <- VU.generateM n $ \i -> fmap fromIntegral (peekElemOff dataPtr i :: IO Int64)
            return $ UnboxedColumn Nothing (vec :: VU.Vector Int)

readInt32Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readInt32Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    dataVoid <- peekElemOff bufArr 1
    let dataPtr = castPtr dataVoid :: Ptr Int32
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- VU.generateM n $ \i -> fmap fromIntegral (peekElemOff dataPtr i :: IO Int32)
            return $ UnboxedColumn (Just bm) (vec :: VU.Vector Int)
        else do
            vec <- VU.generateM n $ \i -> fmap fromIntegral (peekElemOff dataPtr i :: IO Int32)
            return $ UnboxedColumn Nothing (vec :: VU.Vector Int)

readFloat64Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readFloat64Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    dataVoid <- peekElemOff bufArr 1
    let dataPtr = castPtr dataVoid :: Ptr Double
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- VU.generateM n (peekElemOff dataPtr)
            return $ UnboxedColumn (Just bm) (vec :: VU.Vector Double)
        else do
            vec <- VU.generateM n (peekElemOff dataPtr)
            return $ UnboxedColumn Nothing (vec :: VU.Vector Double)

readFloat32Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readFloat32Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    dataVoid <- peekElemOff bufArr 1
    let dataPtr = castPtr dataVoid :: Ptr Float
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- VU.generateM n $ \i -> fmap (realToFrac :: Float -> Double) (peekElemOff dataPtr i)
            return $ UnboxedColumn (Just bm) (vec :: VU.Vector Double)
        else do
            vec <- VU.generateM n $ \i -> fmap (realToFrac :: Float -> Double) (peekElemOff dataPtr i)
            return $ UnboxedColumn Nothing (vec :: VU.Vector Double)

-- | Read a large_string (format "U") column with int64 offsets.
readLargeUtf8Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readLargeUtf8Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    offsetVoid <- peekElemOff bufArr 1
    charVoid <- peekElemOff bufArr 2
    let offsetPtr = castPtr offsetVoid :: Ptr Int64
        charPtr = castPtr charVoid :: Ptr Word8
    let readText i = do
            start <- fromIntegral <$> peekElemOff offsetPtr i
            end <- fromIntegral <$> peekElemOff offsetPtr (i + 1)
            TE.decodeUtf8
                <$> BS.packCStringLen (castPtr (charPtr `plusPtr` start), end - start)
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- V.generateM n readText
            return $ BoxedColumn (Just bm) vec
        else do
            vec <- V.generateM n readText
            return $ BoxedColumn Nothing vec

-- | Read a utf8 (format "u") column with int32 offsets.
readUtf8Col :: Int -> Int64 -> Ptr (Ptr ()) -> IO Column
readUtf8Col n nullCnt bufArr = do
    bitmapVoid <- peekElemOff bufArr 0
    offsetVoid <- peekElemOff bufArr 1
    charVoid <- peekElemOff bufArr 2
    let offsetPtr = castPtr offsetVoid :: Ptr Int32
        charPtr = castPtr charVoid :: Ptr Word8
        readText i = do
            start <- fromIntegral <$> peekElemOff offsetPtr i
            end <- fromIntegral <$> peekElemOff offsetPtr (i + 1)
            TE.decodeUtf8
                <$> BS.packCStringLen (castPtr (charPtr `plusPtr` start), end - start)
    if nullCnt > 0
        then do
            let bitmapPtr = castPtr bitmapVoid :: Ptr Word8
            bm <- readArrowBitmap bitmapPtr n
            vec <- V.generateM n readText
            return $ BoxedColumn (Just bm) vec
        else do
            vec <- V.generateM n readText
            return $ BoxedColumn Nothing vec
