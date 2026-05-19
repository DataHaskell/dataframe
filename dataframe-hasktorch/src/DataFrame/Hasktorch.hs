{-# LANGUAGE BangPatterns #-}

module DataFrame.Hasktorch (
    toTensor,
    toIntTensor,
) where

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified DataFrame.Operations.Core as D

import Control.Exception (throw)
import DataFrame.Internal.DataFrame (DataFrame)
import Torch

{- | Converts a dataframe to a floating-point tensor.

This function converts all columns in the dataframe to floats and creates
a tensor suitable for machine learning operations. The tensor dimensions
are determined by the dataframe's shape.

==== __Dimensional behavior__

* Multi-column dataframe: Creates a 2D tensor with shape @[rows, columns]@
* Single-column dataframe: Creates a 1D tensor with shape @[rows]@

==== __Conversion process__

1. Converts the dataframe to a float matrix using 'D.toFloatMatrix'
2. Flattens the matrix features into a 1D representation
3. Reshapes into the appropriate tensor dimensions

==== __Throws__

* 'DataFrameException' - if any column cannot be converted to float

==== __Examples__

>>> toTensor df  -- where df has shape (100, 5)
Tensor with shape [100, 5]

>>> toTensor df  -- where df has shape (100, 1)
Tensor with shape [100]

==== __See also__

* 'toIntTensor' - for integer tensor conversion
-}
toTensor :: DataFrame -> Tensor
toTensor df = case D.toFloatMatrix df of
    Left e -> throw e
    Right m ->
        let
            (r, c) = D.dimensions df
            dims' = if c == 1 then [r] else [r, c]
         in
            reshape dims' (asTensor (flattenFeatures m))

{- | Converts a dataframe to an integer tensor.

This function converts all columns in the dataframe to integers and creates
a tensor suitable for machine learning operations (e.g., classification labels,
discrete features). The tensor dimensions are determined by the dataframe's shape.

==== __Dimensional behavior__

* Multi-column dataframe: Creates a 2D tensor with shape @[rows, columns]@
* Single-column dataframe: Creates a 1D tensor with shape @[rows]@

==== __Conversion process__

1. Converts the dataframe to an int matrix using 'D.toIntMatrix'
2. Flattens the matrix features into a 1D representation
3. Reshapes into the appropriate tensor dimensions

==== __Throws__

* 'DataFrameException' - if any column cannot be converted to int

==== __Examples__

>>> toIntTensor labelsDf  -- where labelsDf has shape (100, 1)
Tensor with shape [100]

>>> toIntTensor featuresDf  -- where featuresDf has shape (100, 3)
Tensor with shape [100, 3]

==== __Note__

Floating-point values in the dataframe will be rounded to the nearest integer.
See 'D.toIntMatrix' for details on the conversion behavior.

==== __See also__

* 'toTensor' - for floating-point tensor conversion
-}
toIntTensor :: DataFrame -> Tensor
toIntTensor df = case D.toIntMatrix df of
    Left e -> throw e
    Right m ->
        let
            (r, c) = D.dimensions df
            dims' = if c == 1 then [r] else [r, c]
         in
            reshape dims' (asTensor (flattenFeatures m))

flattenFeatures :: (VU.Unbox a) => V.Vector (VU.Vector a) -> VU.Vector a
flattenFeatures rows =
    let
        total = V.foldl' (\s v -> s + VU.length v) 0 rows
     in
        VU.create $ do
            ret <- VUM.unsafeNew total
            let go !i !off
                    | i == V.length rows = pure ()
                    | otherwise = do
                        let v = rows V.! i
                            len = VU.length v
                        VU.unsafeCopy (VUM.unsafeSlice off len ret) v
                        go (i + 1) (off + len)
            go 0 0
            pure ret
