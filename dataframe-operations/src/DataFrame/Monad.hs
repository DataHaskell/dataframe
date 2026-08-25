{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}

module DataFrame.Monad (
    -- * The frame-state monad
    FrameM,
    runFrameM,
    evalFrameM,
    execFrameM,
    modifyM,
    inspectM,

    -- * Frame verbs
    deriveM,
    insertM,
    renameM,
    filterWhereM,
    sampleM,
    takeM,
    dropM,
    selectM,
    excludeM,
    sortByM,
    SortOrder (..),
    columnAsListM,
    filterJustM,
    imputeM,

    -- * Column-derivation pipelines
    Pipeline,
    letAs,
    letExpr,
    pipelineSteps,
    toFrameM,
    runPipeline,
) where

import Control.Monad (void)
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..), UExpr (..), prettyPrint)
import DataFrame.Internal.Expression.Operators.Nullable (BaseType)
import qualified DataFrame.Operations.Core as D
import DataFrame.Operations.Permutation (SortOrder)
import qualified DataFrame.Operations.Permutation as D
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

-- | Keep only the named columns. 'dropM' drops rows; this drops columns.
selectM :: [T.Text] -> FrameM ()
selectM names = modifyM (D.select names)

-- | Drop the named columns.
excludeM :: [T.Text] -> FrameM ()
excludeM names = modifyM (D.exclude names)

sortByM :: [SortOrder] -> FrameM ()
sortByM orders = modifyM (D.sortBy orders)

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

newtype Pipeline a = Pipeline {unPipeline :: Int -> (Int, [(T.Text, UExpr)], a)}

instance Functor Pipeline where
    fmap f (Pipeline g) = Pipeline $ \n -> let (n', w, a) = g n in (n', w, f a)

instance Applicative Pipeline where
    pure x = Pipeline (,[],x)
    Pipeline gf <*> Pipeline gx = Pipeline $ \n ->
        let (n1, w1, f) = gf n
            (n2, w2, x) = gx n1
         in (n2, w1 ++ w2, f x)

instance Monad Pipeline where
    Pipeline g >>= f = Pipeline $ \n ->
        let (n1, w1, a) = g n
            (n2, w2, b) = unPipeline (f a) n1
         in (n2, w1 ++ w2, b)

-- | Derive a column under an explicit name, returning a reference to it.
letAs :: (Columnable a) => T.Text -> Expr a -> Pipeline (Expr a)
letAs nm e = Pipeline (,[(nm, UExpr e)],Col nm)

-- | Derive a column under a fresh generated name.
letExpr :: (Columnable a) => Expr a -> Pipeline (Expr a)
letExpr e = Pipeline $ \n ->
    let nm = T.pack ('_' : 'v' : show n) in (n + 1, [(nm, UExpr e)], Col nm)

instance Show (Pipeline (Expr a)) where
    show p =
        let (_, steps, res) = unPipeline p 0
         in concatMap
                (\(nm, UExpr e) -> T.unpack nm ++ " = " ++ prettyPrint e ++ "\n")
                steps
                ++ "return "
                ++ prettyPrint res

-- | Number of column derivations the pipeline performs.
pipelineSteps :: Pipeline a -> Int
pipelineSteps p = let (_, w, _) = unPipeline p 0 in length w

-- | Interpret a pipeline into 'FrameM', deriving each logged column in order.
toFrameM :: Pipeline (Expr a) -> FrameM (Expr a)
toFrameM p =
    let (_, steps, res) = unPipeline p 0
     in mapM_ (\(nm, UExpr e) -> void (deriveM nm e)) steps >> pure res

{- | Run a pipeline over a frame: derive its columns and return the result
expression (a reference to the final column) and the resulting frame.
-}
runPipeline :: DataFrame -> Pipeline (Expr a) -> (Expr a, DataFrame)
runPipeline df = runFrameM df . toFrameM
