<!--
  Runnable scripths notebook. Run from the repo root so ./data/... resolves.
  Until the bumped packages are released, the sibling libraries have to be
  named explicitly or the solver takes them from Hackage:

      scripths $(for p in dataframe-core dataframe-parsing dataframe-operations \
                          dataframe-csv dataframe-json dataframe-parquet \
                          dataframe-th dataframe-csv-th dataframe-parquet-th \
                          dataframe-viz dataframe-learn dataframe-expr-serializer \
                          dataframe-lazy; do printf -- "--package %s " $p; done) \
          projected_csv_read.md -o out.md
-->

# Write the data

Starting with a small test file.

```haskell
-- cabal: build-depends: dataframe, text, directory
-- cabal: default-extensions: OverloadedStrings, TypeApplications, TemplateHaskell
-- cabal: default-extensions: DataKinds, TypeFamilies, TypeOperators, FlexibleInstances
-- cabal: default-extensions: FlexibleContexts, ScopedTypeVariables, UndecidableInstances
import qualified DataFrame as D
import qualified DataFrame.Lazy as L
import DataFrame.Expression.Operators
import Data.Text (Text)
import System.Directory (removeFile)

writeFile "./customers.csv" $ unlines
    [ "id,name,surname,dob"
    , "1,Ada,Lovelace,1815-12-10"
    , "2,Alan,Turing,1912-06-23"
    , "3,Grace,Hopper,1906-12-09"
    ]
```

## Driving the schema off the record

`deriveSchemaValues` builds the `Schema` value from the record;
`deriveSchemaFromType` adds the `HasSchema` instance `toRecords` needs.

```haskell
data Customer = Customer
    { customerId :: Int
    , customerName :: Text
    }
    deriving (Show, Eq)
```

```haskell
$(D.deriveSchemaValues ''Customer)
$(D.deriveSchemaFromType ''Customer)

customerSchema
```

We can now pass this to the typed CSV reader.

```haskell
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.IO.CSV as TCSV

typed <- TCSV.readCsv @(D.Schema Customer) "./customers_snake.csv"
D.columnNames (DT.thaw typed)
```
