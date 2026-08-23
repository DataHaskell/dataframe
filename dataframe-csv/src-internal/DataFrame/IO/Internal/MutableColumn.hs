{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

{- |
Mutable-column ingest helpers used by CSV readers. Kept out of
@DataFrame.Internal.Column@ so that the core column type does not need to
depend on @DataFrame.Internal.Parsing@.
-}
module DataFrame.IO.Internal.MutableColumn (
    writeColumn,
    freezeColumn',
) where

import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Data.Maybe (fromMaybe)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.Column (
    Column (..),
    MutableColumn (..),
 )
import DataFrame.Internal.Column.Bitmap (buildBitmapFromNulls)
import DataFrame.Internal.Parsing (isNullish, readDouble, readInt)
import Type.Reflection (typeRep)

writeColumn :: Int -> T.Text -> MutableColumn -> IO (Either T.Text Bool)
writeColumn i value (MBoxedColumn (col :: VBM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl ->
            if isNullish value
                then VBM.unsafeWrite col i "" >> return (Left $! value)
                else VBM.unsafeWrite col i value >> return (Right True)
        Nothing -> return (Left value)
writeColumn i value (MUnboxedColumn (col :: VUM.IOVector a)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> case readInt value of
            Just v -> VUM.unsafeWrite col i v >> return (Right True)
            Nothing -> VUM.unsafeWrite col i 0 >> return (Left value)
        Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
            Nothing -> return (Left $! value)
            Just Refl -> case readDouble value of
                Just v -> VUM.unsafeWrite col i v >> return (Right True)
                Nothing -> VUM.unsafeWrite col i 0 >> return (Left $! value)
{-# INLINE writeColumn #-}

freezeColumn' :: [(Int, T.Text)] -> MutableColumn -> IO Column
freezeColumn' nulls (MBoxedColumn col)
    | null nulls = BoxedColumn Nothing <$> VB.unsafeFreeze col
    | all (isNullish . snd) nulls = do
        frozen <- VB.unsafeFreeze col
        let n = VB.length frozen
            bm = buildBitmapFromNulls n (map fst nulls)
        return $ BoxedColumn (Just bm) frozen
    | otherwise =
        BoxedColumn Nothing
            . VB.imap
                ( \i v ->
                    if i `elem` map fst nulls
                        then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                        else Right v
                )
            <$> VB.unsafeFreeze col
freezeColumn' nulls (MUnboxedColumn col)
    | null nulls = UnboxedColumn Nothing <$> VU.unsafeFreeze col
    | all (isNullish . snd) nulls = do
        c <- VU.unsafeFreeze col
        let n = VU.length c
            bm = buildBitmapFromNulls n (map fst nulls)
        return $ UnboxedColumn (Just bm) c
    | otherwise = do
        c <- VU.unsafeFreeze col
        return $
            BoxedColumn Nothing $
                VB.generate
                    (VU.length c)
                    ( \i ->
                        if i `elem` map fst nulls
                            then Left (fromMaybe (error "UNEXPECTED ERROR DURING FREEZE") (lookup i nulls))
                            else Right (c VU.! i)
                    )
{-# INLINE freezeColumn' #-}
