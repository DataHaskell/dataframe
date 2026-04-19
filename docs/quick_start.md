# Quick Start

## Jupyter

### Online
* Try out `dataframe` on [binder](https://mybinder.org/v2/gh/mchav/ihaskell-dataframe/HEAD).

### Run locally from docker
* Clone the [ihaskell-dataframe repository](https://github.com/mchav/ihaskell-dataframe/).
* Ensure that docker is installed on your machine.
* Run `sudo make up` from the root directory.

### Examples
* There are pre-loaded examples in the Jupyter environment.

## Running Haskell locally

### Installation

* Install GHC (The Haskell compiler) and cabal
    * For MacOS/Linux/WSL2: `curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | BOOTSTRAP_HASKELL_NONINTERACTIVE=1 sh`
    * For windows: `$ErrorActionPreference = 'Stop';Set-ExecutionPolicy Bypass -Scope Process -Force;[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072;try { & ([ScriptBlock]::Create((Invoke-WebRequest https://www.haskell.org/ghcup/sh/bootstrap-haskell.ps1 -UseBasicParsing))) -InBash -InstallDir "C:\" } catch { Write-Error $_ }`

### Cabal scripts
You can run standalone scripts with minimal setup using cabal scripts.

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
-- Test.hs
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame.Operators

-- Creates the column references used below (namely total_rooms and households)
-- This gives us type-safe column access.
$(F.declareColumnsFromCsvFile "/home/yavinda/code/dataframe/data/housing.csv")

main :: IO ()
main = do
  df <- D.readCsv "./dataframe/data/housing.csv"
  print (df |> D.derive "rooms_per_household" (total_rooms / households))
```

Save the file as `Test.hs` and run with:

`cabal run Test.hs`

We provide a small, monadic DSL for scripts where you want relatively more type safety.

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame.Monad

import Data.Text (Text)
import DataFrame.Functions ((.&&.), (.>=.))

$(F.declareColumnsFromCsvFile "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    print $ execFrameM df $ do
        -- 1) Type safe reference to `median_house_value` and `median_income`
        -- 2) creates a type safe reference to the newly created column.
        is_expensive <- deriveM "is_expensive" (median_house_value .>=. 500000)
        luxury <- deriveM "luxury" (is_expensive .&&. median_income .>=. 8)
        filterWhereM luxury
```

### As a commandline tool
* Run `cabal install dataframe`.
* Start the dataframe REPL by running `dataframe` which should be in your PATH.

#### Example usage

##### GHCi/Jupyter notebooks
Looking through the structure of the columns.

```haskell    
dataframe> df <- D.readCsv "./data/housing.csv"
dataframe> D.describeColumns df
------------------------------------------------------------------------
    Column Name     | ## Non-null Values | ## Null Values |     Type    
--------------------|--------------------|----------------|-------------
        Text        |         Int        |      Int       |     Text    
--------------------|--------------------|----------------|-------------
 total_bedrooms     | 20433              | 207            | Maybe Double
 ocean_proximity    | 20640              | 0              | Text        
 median_house_value | 20640              | 0              | Double      
 median_income      | 20640              | 0              | Double      
 households         | 20640              | 0              | Double      
 population         | 20640              | 0              | Double      
 total_rooms        | 20640              | 0              | Double      
 housing_median_age | 20640              | 0              | Double      
 latitude           | 20640              | 0              | Double      
 longitude          | 20640              | 0              | Double
```

Automatically generate column names.

```haskell
dataframe> :declareColumns df
```

We can use the generated columns in expressions.

```haskell
dataframe> import DataFrame.Operators
dataframe> df |> D.groupBy ["ocean_proximity"] |> D.aggregate [(F.mean median_house_value) `as` "avg_house_value" ]
-------------------------------------
 ocean_proximity |  avg_house_value  
-----------------|-------------------
      Text       |       Double      
-----------------|-------------------
 <1H OCEAN       | 240084.28546409807
 INLAND          | 124805.39200122119
 ISLAND          | 380440.0          
 NEAR BAY        | 259212.31179039303
 NEAR OCEAN      | 249433.97742663656
```

Create a new column based on other columns.

```haskell
dataframe> df |> D.derive "rooms_per_household" (total_rooms / households)
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 longitude | latitude | housing_median_age | total_rooms | total_bedrooms | population | households |   median_income    | median_house_value | ocean_proximity | rooms_per_household
-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
  Double   |  Double  |       Double       |   Double    |  Maybe Double  |   Double   |   Double   |       Double       |       Double       |      Text       |       Double       
-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
 -122.23   | 37.88    | 41.0               | 880.0       | Just 129.0     | 322.0      | 126.0      | 8.3252             | 452600.0           | NEAR BAY        | 6.984126984126984  
 -122.22   | 37.86    | 21.0               | 7099.0      | Just 1106.0    | 2401.0     | 1138.0     | 8.3014             | 358500.0           | NEAR BAY        | 6.238137082601054  
 -122.24   | 37.85    | 52.0               | 1467.0      | Just 190.0     | 496.0      | 177.0      | 7.2574             | 352100.0           | NEAR BAY        | 8.288135593220339  
 -122.25   | 37.85    | 52.0               | 1274.0      | Just 235.0     | 558.0      | 219.0      | 5.6431000000000004 | 341300.0           | NEAR BAY        | 5.8173515981735155 
 -122.25   | 37.85    | 52.0               | 1627.0      | Just 280.0     | 565.0      | 259.0      | 3.8462             | 342200.0           | NEAR BAY        | 6.281853281853282  
 -122.25   | 37.85    | 52.0               | 919.0       | Just 213.0     | 413.0      | 193.0      | 4.0368             | 269700.0           | NEAR BAY        | 4.761658031088083  
 -122.25   | 37.84    | 52.0               | 2535.0      | Just 489.0     | 1094.0     | 514.0      | 3.6591             | 299200.0           | NEAR BAY        | 4.9319066147859925 
 -122.25   | 37.84    | 52.0               | 3104.0      | Just 687.0     | 1157.0     | 647.0      | 3.12               | 241400.0           | NEAR BAY        | 4.797527047913447  
 -122.26   | 37.84    | 42.0               | 2555.0      | Just 665.0     | 1206.0     | 595.0      | 2.0804             | 226700.0           | NEAR BAY        | 4.294117647058823  
 -122.25   | 37.84    | 52.0               | 3549.0      | Just 707.0     | 1551.0     | 714.0      | 3.6912000000000003 | 261100.0           | NEAR BAY        | 4.970588235294118
```

If two columns don't type check we catch this with a type error instead of a runtime error.

```haskell

dataframe> df |> D.derive "nonsense_feature" (latitude + ocean_proximity) |> D.take 10

<interactive>:14:47: error: [GHC-83865]
    • Couldn't match type ‘Text’ with ‘Double’
        Expected: Expr Double
        Actual: Expr Text
    • In the second argument of ‘(+)’, namely ‘ocean_proximity’
        In the second argument of ‘derive’, namely
        ‘(latitude + ocean_proximity)’
        In the second argument of ‘(|>)’, namely
        ‘derive "nonsense_feature" (latitude + ocean_proximity)’
```

Key features in example:

* Intuitive, SQL-like API to get from data to insights.
* Create type-safe references to columns in a dataframe using :declareColumns
* Type-safe column transformations for faster and safer exploration.
* Fluid, chaining API that makes code easy to reason about.
