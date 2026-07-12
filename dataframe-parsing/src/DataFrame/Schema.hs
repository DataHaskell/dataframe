{- | The runtime schema surface for the @dataframe@ ecosystem: the 'Schema'
tag, its element types, and builders to describe a frame's columns by name.
Re-exported so callers never reach into @DataFrame.Internal.Schema@.
-}
module DataFrame.Schema (
    module DataFrame.Internal.Schema,
) where

import DataFrame.Internal.Schema
