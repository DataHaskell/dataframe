{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Access (
    -- * Typed column access
    columnAsVector,
    columnAsList,

    -- * Numeric vector extraction
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,

    -- * Matrix extraction
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,
) where

import Control.Exception (throw)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import GHC.TypeLits (KnownSymbol, symbolVal)

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr (Col))
import qualified DataFrame.Operations.Core as D
import DataFrame.Typed.Schema (
    AllColumnsReal,
    AssertPresent,
    AssertRealColumn,
    SafeLookup,
 )
import DataFrame.Typed.Types (TypedDataFrame (..))

{- | Retrieve a column as a boxed 'Vector', with the type determined by
the schema. The column must exist (enforced at compile time).
-}
columnAsVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> V.Vector a
columnAsVector (TDF df) =
    either throw id $ D.columnAsVector (Col @a colName) df
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Retrieve a column as a list, with the type determined by the schema.
columnAsList ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> [a]
columnAsList (TDF df) =
    D.columnAsList (Col @a colName) df
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Retrieve a column coerced to an unboxed 'Int' vector, named by type
application. The column must exist and be numeric — both are compile-time
checks via 'SafeLookup', so this is total (no 'Either', no runtime throw).
-}
columnAsIntVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertRealColumn "columnAsIntVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> VU.Vector Int
columnAsIntVector (TDF df) = either throw id (D.columnAsIntVector (Col @a colName) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Retrieve a column coerced to an unboxed 'Double' vector. See 'columnAsIntVector'.
columnAsDoubleVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertRealColumn "columnAsDoubleVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> VU.Vector Double
columnAsDoubleVector (TDF df) =
    either throw id (D.columnAsDoubleVector (Col @a colName) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Retrieve a column coerced to an unboxed 'Float' vector. See 'columnAsIntVector'.
columnAsFloatVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , AssertRealColumn "columnAsFloatVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> VU.Vector Float
columnAsFloatVector (TDF df) =
    either throw id (D.columnAsFloatVector (Col @a colName) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Retrieve a column as an unboxed vector of its own element type. The column
must exist and be unboxable — both compile-time checks, so this is total.
-}
columnAsUnboxedVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , Columnable a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> VU.Vector a
columnAsUnboxedVector (TDF df) =
    either throw id (D.columnAsUnboxedVector (Col @a colName) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

{- | Convert every column to 'Double' and transpose into a row-major matrix.
Total: 'AllColumnsReal' proves at compile time that every column is numeric and
unboxed, so the conversion cannot fail.
-}
toDoubleMatrix ::
    (AllColumnsReal "toDoubleMatrix" cols) =>
    TypedDataFrame cols -> V.Vector (VU.Vector Double)
toDoubleMatrix (TDF df) = either throw id (D.toDoubleMatrix df)

-- | Convert every column to 'Float' and transpose into a row-major matrix. See 'toDoubleMatrix'.
toFloatMatrix ::
    (AllColumnsReal "toFloatMatrix" cols) =>
    TypedDataFrame cols -> V.Vector (VU.Vector Float)
toFloatMatrix (TDF df) = either throw id (D.toFloatMatrix df)

-- | Convert every column to 'Int' and transpose into a row-major matrix. See 'toDoubleMatrix'.
toIntMatrix ::
    (AllColumnsReal "toIntMatrix" cols) =>
    TypedDataFrame cols -> V.Vector (VU.Vector Int)
toIntMatrix (TDF df) = either throw id (D.toIntMatrix df)
