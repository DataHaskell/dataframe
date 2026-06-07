{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeAbstractions #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Access (
    -- * Typed column access
    columnAsVector,
    columnAsList,
) where

import Control.Exception (throw)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as V
import GHC.TypeLits (KnownSymbol, symbolVal)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (Col))
import qualified DataFrame.Operations.Core as D
import DataFrame.Typed.Schema (AssertPresent, SafeLookup)
import DataFrame.Typed.Types (TypedDataFrame (..))

{- | Retrieve a column as a boxed 'Vector', with the type determined by
the schema. The column must exist (enforced at compile time).
-}
columnAsVector ::
    forall name -> forall cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> V.Vector a
columnAsVector name @_ @a (TDF df) =
    either throw id $ D.columnAsVector (Col @a colName) df
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Retrieve a column as a list, with the type determined by the schema.
columnAsList ::
    forall name -> forall cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> [a]
columnAsList name @_ @a (TDF df) =
    D.columnAsList (Col @a colName) df
  where
    colName = T.pack (symbolVal (Proxy @name))
