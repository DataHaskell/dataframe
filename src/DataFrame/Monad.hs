{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module DataFrame.Monad where

import DataFrame (DataFrame)
import qualified DataFrame as D
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (..))

import qualified Data.Text as T
import System.Random

-- A re-implementation of the state monad.
-- `mtl` might be too heavy a dependency just to get
-- a single monad instance.
newtype FrameM a = FrameM {runFrameM_ :: DataFrame -> (DataFrame, a)}

instance Functor FrameM where
    fmap :: (a -> b) -> FrameM a -> FrameM b
    fmap f (FrameM g) = FrameM $ \df ->
        let (df', x) = g df
         in (df', f x)

instance Applicative FrameM where
    pure x = FrameM (,x)
    (<*>) :: FrameM (a -> b) -> FrameM a -> FrameM b
    FrameM ff <*> FrameM fx = FrameM $ \df ->
        let (df1, f) = ff df
            (df2, x) = fx df1
         in (df2, f x)

instance Monad FrameM where
    (>>=) :: FrameM a -> (a -> FrameM b) -> FrameM b
    FrameM g >>= f = FrameM $ \df ->
        let (df1, x) = g df
            FrameM h = f x
         in h df1

modifyM :: (DataFrame -> DataFrame) -> FrameM ()
modifyM f = FrameM $ \df -> (f df, ())

inspectM :: (DataFrame -> b) -> FrameM b
inspectM f = FrameM $ \df -> (df, f df)

deriveM :: (Columnable a) => T.Text -> Expr a -> FrameM (Expr a)
deriveM name expr = FrameM $ \df ->
    let df' = D.derive name expr df
     in (df', Col name)

renameM :: (Columnable a) => Expr a -> T.Text -> FrameM (Expr a)
renameM (Col oldName) newName = FrameM $ \df ->
    let df' = D.rename oldName newName df
     in (df', Col newName)
renameM expr newName = deriveM newName expr

filterWhereM :: Expr Bool -> FrameM ()
filterWhereM p = modifyM (D.filterWhere p)

sampleM :: (RandomGen g) => g -> Double -> FrameM ()
sampleM pureGen p = modifyM (D.sample pureGen p)

takeM :: Int -> FrameM ()
takeM n = modifyM (D.take n)

filterJustM :: (Columnable a) => Expr (Maybe a) -> FrameM (Expr a)
filterJustM (Col name) = FrameM $ \df ->
    let df' = D.filterJust name df
     in (df', Col name)
filterJustM expr =
    error $ "Cannot filter on compound expression: " ++ show expr

imputeM :: (Columnable a) => Expr (Maybe a) -> a -> FrameM (Expr a)
imputeM expr@(Col name) value = FrameM $ \df ->
    let df' = D.impute expr value df
     in (df', Col name)
imputeM expr _ = error $ "Cannot impute on compound expression: " ++ show expr

runFrameM :: DataFrame -> FrameM a -> (a, DataFrame)
runFrameM df (FrameM action) =
    let (df', a) = action df
     in (a, df')

evalFrameM :: DataFrame -> FrameM a -> a
evalFrameM df m = fst (runFrameM df m)

execFrameM :: DataFrame -> FrameM a -> DataFrame
execFrameM df m = snd (runFrameM df m)
