{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module DataFrame.Monad where

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Internal.Nullable (BaseType)
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Subset as D
import DataFrame.Operations.Transformations (ImputeOp)
import qualified DataFrame.Operations.Transformations as D

import qualified Data.Text as T
import Data.Tuple (swap)
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

execWithExpr ::
    (Columnable a) => T.Text -> (DataFrame -> DataFrame) -> FrameM (Expr a)
execWithExpr name f = FrameM $ \df ->
    let df' = f df
     in (df', Col name)

deriveM :: (Columnable a) => T.Text -> Expr a -> FrameM (Expr a)
deriveM name expr = execWithExpr name (D.derive name expr)

insertM :: (Columnable a) => T.Text -> [a] -> FrameM (Expr a)
insertM name values = execWithExpr name (D.insert name values)

renameM :: (Columnable a) => Expr a -> T.Text -> FrameM (Expr a)
renameM (Col oldName) newName = execWithExpr newName (D.rename oldName newName)
renameM expr newName = deriveM newName expr

filterWhereM :: Expr Bool -> FrameM ()
filterWhereM p = modifyM (D.filterWhere p)

sampleM :: (RandomGen g) => g -> Double -> FrameM ()
sampleM pureGen p = modifyM (D.sample pureGen p)

takeM :: Int -> FrameM ()
takeM n = modifyM (D.take n)

dropM :: Int -> FrameM ()
dropM n = modifyM (D.drop n)

columnAsListM :: (Columnable a) => Expr a -> FrameM [a]
columnAsListM c = inspectM (D.columnAsList c)

filterJustM :: (Columnable a) => Expr (Maybe a) -> FrameM (Expr a)
filterJustM (Col name) = execWithExpr name (D.filterJust name)
filterJustM expr =
    error $ "Cannot filter on compound expression: " ++ show expr

imputeM ::
    (ImputeOp a, Columnable (BaseType a)) =>
    Expr a ->
    BaseType a ->
    FrameM (Expr (BaseType a))
imputeM expr@(Col name) value = execWithExpr name (D.impute expr value)
imputeM expr _ = error $ "Cannot impute on compound expression: " ++ show expr

runFrameM :: DataFrame -> FrameM a -> (a, DataFrame)
runFrameM df (FrameM action) = swap (action df)

evalFrameM :: DataFrame -> FrameM a -> a
evalFrameM df m = fst (runFrameM df m)

execFrameM :: DataFrame -> FrameM a -> DataFrame
execFrameM df m = snd (runFrameM df m)
