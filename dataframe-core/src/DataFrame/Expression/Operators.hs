{- |
The public name for dataframe's expression operators.

This is a re-export shim: the implementation lives in
"DataFrame.Internal.Expression.Operators", which is internal and may be
reorganised without notice. Depend on this module instead.
-}
module DataFrame.Expression.Operators (
    module DataFrame.Internal.Expression.Operators,
) where

import DataFrame.Internal.Expression.Operators
