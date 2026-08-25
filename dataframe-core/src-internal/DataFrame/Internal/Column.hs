{- |
Umbrella re-export of the column implementation. The module is split by
concern:

* "DataFrame.Internal.Column.Types" — type-level machinery ('Rep', 'SBool', ...)
* "DataFrame.Internal.Column.Base" — the 'Column' GADT and core definitions
* "DataFrame.Internal.Column.Properties" — predicates and introspection
* "DataFrame.Internal.Column.Conversion" — vector\/list conversions
* "DataFrame.Internal.Column.Operations" — bulk transformations

Import this module to get the whole surface; import a submodule directly when
you only need one layer.
-}
module DataFrame.Internal.Column (
    module DataFrame.Internal.Column.Base,
    module DataFrame.Internal.Column.Conversion,
    module DataFrame.Internal.Column.Operations,
    module DataFrame.Internal.Column.Properties,
    module DataFrame.Internal.Column.Types,
) where

import DataFrame.Internal.Column.Base
import DataFrame.Internal.Column.Conversion
import DataFrame.Internal.Column.Operations
import DataFrame.Internal.Column.Properties
import DataFrame.Internal.Column.Types
