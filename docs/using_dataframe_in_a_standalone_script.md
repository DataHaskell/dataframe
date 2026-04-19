# Using dataframe in a standalone script

What you’ll learn
* How to write a small, compiled Haskell program that reads a CSV, engineers features, imputes missing values, and filters rows.
* How Template Haskell column declarations eliminate stringly‑typed references.
* How using FrameM lets you update a dataframe without threading the df variable by hand.
* (Advanced) How the `Typed` module helps you write safe data pipeline.

## Peeking at our file
We'll be looking at the California Housing dataset for this tutorial. Download the [housing.csv](https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/data/housing.csv) file into whatever folder you're working from.

Create a file called `Housing.hs` in your working folder and copy/type the following code:

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"
  print df
```

It's not very useful yet but it gets the boilerplate out of the way. Test that everything works by running `cabal run Housing.hs`. You should see the first few rows of the dataframe printed out.

## Generating our dataframe expressions
Now that we can load our dataframe we want to run some interesting computations on it. We want to create some combinations of columns that are predictive of `median_house_value`. Let's see how dense each place is by computing the number of rooms per household (more rooms per household means lower density).

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
-- We need this so because most dataframe functions take
-- `Text` not `String`.
{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"
  print (D.derive "rooms_per_household" (F.col @Double "total_rooms" / F.col @Double "households") df)
```

You'll notice the dataframe now has a new column called `rooms_per_household` at the end. This is great! But what if we had made a mistake? What if we had instead typed "totalrooms"? Or what if we had assumed both columns were ints and used integer division? These would have both resulted in runtime errors. This gets worse as we rewrite more transformations using the same columns (we're more likely to have a stray typo or get the type wrong).

Can we protect ourselves better?

We can mitigate this by lifting them to top level scope to be separate variables that we reuse.

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
-- We need this so because most dataframe functions take
-- `Text` not `String`.
{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

total_rooms = F.col @Double "total_rooms"
households = F.col @Double "households"

main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"
  print (D.derive "rooms_per_household" (total_rooms / households) df)
```

This does make things a little clearer but we could still make all the same mistakes. Can we do better? We absolutely can. We can automatically generate the references to our columns.

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
-- We need this so because most dataframe functions take
-- `Text` not `String`.
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

$(F.declareColumnsFromCsvFile "./housing.csv")

main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"
  print (D.derive "rooms_per_household" (total_rooms / households) df)
```

`declareColumnsFromCsvFile` runs at compile time and generates top‑level, typed bindings for each column in the CSV header in snake case. This is a big improvement. Misspelling column names or misspecifying the type are now both compile time errors!

Because Template Haskell reads the file at compile time, make sure `./housing.csv` exists and is in the same folder as the script. Paths are resolved relative to your current working directory when you run `cabal run Housing.hs`.

## The FrameM monad
We're still not entirely in the clear. Suppose we wanted to:

* define a new feature called `is_expensive` which has a value of true when the house price is greater than or equal to `500_000` and is false otherwise.
* define a new feature for the average number of rooms in each house called `rooms_per_household`. 
* we also wanted to impute the mean value of total bedrooms in places where it's `Nothing`/`Null`.
* filter all rows where `is_expensive` is true, `rooms_per_household` is greater than or equal to 7, and `total_bedrooms` is greater than or equal to 200.

We have a version of `derive` that returns the column reference and the updated dataframe. The function is called `deriveWithExpr`.

So we can write:

```haskell
#!/usr/bin/env cabal
{- cabal:
  build-depends: base >= 4, dataframe
-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame.Operators

$(F.declareColumnsFromCsvFile "./housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./housing.csv"
    let (isExpensive, dfWithExpensive) = D.deriveWithExpr "is_expensive" (median_house_value .>=. 500000) df
        (roomsPerHoushold, dfWithRoomsPerHousehold) = D.deriveWithExpr "rooms_per_household" (total_rooms / households) dfWithExpensive
        meanBedrooms = D.meanMaybe total_bedrooms dfWithRoomsPerHousehold
        dfWithTotalBedroomsImputed = D.impute total_bedrooms meanBedrooms dfWithRoomsPerHousehold

    print $ dfWithTotalBedroomsImputed |> D.filterWhere (isExpensive .&&. roomsPerHousehold .>=. 7 .&&. (F.col @Double "total_bedrooms") .>=. 200)
```

Our code is now inundated with different dataframe variables and a bunch of pipeline management. Should we just revert back to using the old `derive` and keep track of names and types ourselves? Not at all. We can use a structure called a `FrameM` to remove this boilerplate while still keeping the guarantees we unlocked before.

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame.Operators

import DataFrame.Monad

$(F.declareColumnsFromCsvFile "./housing.csv")

main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"
  let df' = execFrameM df $ do
              isExpensive   <- deriveM "is_expensive" (median_house_value .>=. 500000)
              roomsPerHousehold <- deriveM "rooms_per_household" (total_rooms / households)
              meanBedrooms   <- inspectM (D.meanMaybe total_bedrooms)
              totalBedrooms  <- imputeM total_bedrooms meanBedrooms
              filterWhereM (isExpensive .&&. roomsPerHousehold .>=. 7 .&&.totalBedrooms .>=. 200)
  print df'
```

In the earlier version (without FrameM), every operation has to take an input dataframe and return a new one. We do this because dataframes are immutable but it forces us to keep naming each intermediate result (dfWithRoomsPerHousehold, dfWithTotalBedroomsImputed, ...). We end up doing bookkeeping that has nothing to do with our data work. We're manually threading the dataframe through a pipeline, and the pipeline is getting visually buried under variable names.

FrameM is a way to say: "For the next few lines, assume we're working on one evolving dataframe, and keep passing it along for me." You can think of it as a do-block where the dataframe is the implicit context. Inside the block, each step sees "the current dataframe" and can update it, but you don’t have to keep writing the plumbing yourself.

That’s what the M suffix is hinting at. When you see functions like deriveM, imputeM, or filterWhereM, the M is just a conventional signal that this version runs in a monad. And "monad", here, can be understood very simply: it’s a pattern for sequencing operations where each line may carry along some hidden context. In IO (like in the main function for example), the hidden context is "the outside world" (the console screen, system clocks, environment variables etc). In FrameM, the hidden context is "the current dataframe".

A nice way to read the FrameM part without doing "monads as a theory lesson" is to treat it like a mini language for dataframe steps. You write your steps in a natural order, and the monad takes care of passing the context to the next step.

FrameM currently focuses on single-table, column-at-a-time transformations (derive/impute/filter/rename). Operations that change row grouping or produce new tables (groupBy/aggregate/join) aren’t in the monadic API yet.

How do you get stuff out of this series of computations? There are three functions for doing so: `execFrameM`, `runFrameM`, and `evalFrameM`. These are just different ways to "end the story" depending on what you want to keep. If you only care about the final table, use `execFrameM`. If you want both the final table and some values you computed along the way (like an imputed expression you want to reuse), use `runFrameM`. If you only care about the extracted results and not the updated table, use `evalFrameM`. The important beginner-friendly idea is: you’re writing a sequence of dataframe steps, and you get to choose whether you want the updated dataframe, the computed results, or both at the end.

FrameM is actually an implementation of a [state monad](https://wiki.haskell.org/State_Monad) and its function names are meant to mirror the same naming scheme.

So let's extract some expressions and use them outside of FrameM:

```haskell
main :: IO ()
main = do
  df <- D.readCsv "./housing.csv"

  let ((is_expensive, totalBedrooms), df') =
        runFrameM df $ do
          is_expensive  <- deriveM "is_expensive" (median_house_value .>=. 500000)
          meanBedrooms  <- inspectM (D.meanMaybe total_bedrooms)
          totalBedrooms <- imputeM total_bedrooms meanBedrooms
          -- Return the Exprs we want to reuse outside the monad:
          pure (is_expensive, totalBedrooms)

  print $
    df'
      |> D.filterWhere (isExpensive .&&. totalBedrooms .>=. 200)
```

Now we can reuse the derived Exprs elsewhere - for example, in plotting, or further filtering with the regular dataframe API.

We can go ahead and run the complete file with the same command as before.

## Type safety, schema evolution, and the “foot‑gun”

What’s type‑safe here?
* Expressions are typed: `median_house_value .>=. 500000` won’t compile if `median_house_value` isn’t numeric.
`ocean_proximity .&&. median_house_value .>=. 500000` won’t compile because `.&&.` expects a `Bool` on both sides.
* Operators are typed: `(.>=.)` expects both sides to be numeric Exprs of the same type; `(.&&.)` expects non-nullable boolean Exprs (use `(.&&)` when either side may be `Maybe Bool`).

Where is it not type‑safe?
* DataFrame schema isn’t in the type: Expressions are name‑based (e.g "median_house_value"), not tied to a specific DataFrame at the type level. This is deliberate: it enables schema evolution (you can add/remove/rename columns without changing every type signature).
* Foot‑gun: You can accidentally apply an Expr built with one frame to a different dataframe that’s missing that column (or has it with a different meaning/type). That error won’t be caught at compile time.

Practical mitigations
* Keep related steps inside one FrameM block and finish with execFrameM when possible.
* If you must reuse Exprs outside, pass the updated df' and the Exprs together (use runFrameM instead of the other two evalFrameM) to remind yourself they're from the same logical schema version.
* Normalize and rename columns immediately after ingest so names are stable throughout the pipeline.
* Consider module‑scoped helpers that build the “contract” expressions in one place (e.g a function that returns a record of Exprs) so evolution is centralized.

## Is there more?

What if you wanted yet more type safety? What if we wanted to make sure that a column does exist in a dataframe at compile time? What if we wanted to track the schema across complex operations like groupings and joins? Dataframe provides one more level of abstraction for those that want to write data pipelines with virtually no runtime errors. We provide an module called `DataFrame.Typed` that makes this possible.

## The `DataFrame.Typed` module

We mentioned a specific hazard in the previous section where you might accidentally apply an expression built for one dataframe to a completely different one. This happens because the compiler only knows that you have a dataframe, not what columns are inside it. The DataFrame.Typed module offers a powerful solution by moving your schema directly into the Haskell type system. If a column does not exist or if the types do not match, your code simply will not compile.

### Generating a Type Level Schema

To start, we need to define our exact data structure. In the full code example, you will see a Template Haskell function called deriveSchemaFromCsvFile. This function does much more than create variable bindings. It reads your CSV file at compile time and constructs a complete Haskell type named Housing. This new type acts as a strict blueprint representing the exact structure and data types of your underlying file.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import qualified DataFrame as D
import qualified DataFrame.Typed as DT

$(DT.deriveSchemaFromCsvFile "Housing" "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    print df
```

You can also define the schema by hand as follows:

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import qualified DataFrame as D
import qualified DataFrame.Typed as DT

type Housing = [ DT.Column "latitude" Double
               , DT.Column "longitude" Double
               -- other fields are skipped for brevity.
               ]

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    print df
```

This is equivalent to what the `TemplateHaskell` is doing under the hood.

### Freezing the Dataframe

When we read data from a file at runtime using D.readCsv, the initial dataframe is dynamic. We need a way to bridge the gap between this dynamic data and our strict Housing blueprint.  We borrow a convention from the `vector` package and call this process "freezing" (and its opposite "thawing"). Freezing a dataframe checks that it conforms to the applied Schema. In our case, it verifies that the dataframe matches the Housing schema we generated above. If everything aligns, it gives us a statically typed dataframe. If there is a mismatch, it throws an error immediately, preventing bad data from moving forward. That way, even the stuff we don't catch at compile it is caught near the beginning of the pipeline (instead of an hour into the pipeline run).

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

import qualified DataFrame as D
import qualified DataFrame.Typed as DT

$(DT.deriveSchemaFromCsvFile "Housing" "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    let tdf = either (error . show) id (DT.freezeWithError @Housing df)
    print tdf
```

### Statically Typed Transformations

Once your dataframe is safely frozen, you can perform transformations with complete confidence. You will notice the use of the `@` symbol in functions like `DT.derive` and `DT.col`. This syntax is enabled by the `TypeApplications` extension in Haskell. Instead of passing standard string values, we are passing type level strings directly to the compiler. When you write `DT.col @"total_rooms"`, the compiler checks the schema of your dataframe immediately. It verifies that the column exists and knows exactly what type of data it holds. Every time you derive a new column, the compiler automatically updates the schema to include it for the very next step.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplication #-}

import qualified DataFrame as D
import qualified DataFrame.Typed as DT

import DataFrame.Operators ((|>))

$(DT.deriveSchemaFromCsvFile "Housing" "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    let tdf = either (error . show) id (DT.freezeWithError @Housing df)
    -- We could generate this with `F.declareColumnsFromCsvFile` as before
    let total_bedrooms = F.col @(Maybe Double) "total_bedrooms"
    print $ tdf
          |> DT.derive @"rooms_per_household" (DT.col @"total_rooms" / DT.col @"households")
          |> DT.impute @"total_bedrooms" (D.meanMaybe total_bedrooms df)
          |> DT.derive @"bedrooms_per_household" (DT.col @"total_bedrooms" / DT.col @"households")
          |> DT.derive @"population_per_household" (DT.col @"population" / DT.col @"households")
```

### Complex Operations

This strict tracking is incredibly useful during complex operations like grouping and aggregating. When the code calls `DT.groupBy @'["ocean_proximity"]` (show below), the compiler guarantees that the column exists before any grouping logic even runs. The aggregation step then ensures that the final output schema correctly reflects only the grouped keys and the newly calculated total column. You cannot accidentally reference a dropped column after an aggregation because the type system will catch the mistake immediately.

```haskell
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Typed as DT

import DataFrame.Operators ((|>))

$(DT.deriveSchemaFromCsvFile "Housing" "./data/housing.csv")

main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    -- We could generate this with `F.declareColumnsFromCsvFile` as before
    let total_bedrooms = F.col @(Maybe Double) "total_bedrooms"
    let tdf = either (error . show) id (DT.freezeWithError @Housing df)
    print $ tdf
          |> DT.derive @"rooms_per_household" (DT.col @"total_rooms" / DT.col @"households")
          |> DT.impute @"total_bedrooms" (D.meanMaybe total_bedrooms df)
          |> DT.derive @"bedrooms_per_household" (DT.col @"total_bedrooms" / DT.col @"households")
          |> DT.derive @"population_per_household" (DT.col @"population" / DT.col @"households")

    print $ tdf
          |> DT.groupBy @'["ocean_proximity"]
          |> DT.aggregate
                (DT.agg @"total" (DT.count (DT.col @"median_house_value")) DT.aggNil)
```

## The Trade Off
Adopting the typed module requires learning some advanced language extensions and writing a bit more explicit code upfront. The major benefit is that it provides total peace of mind for production data pipelines by virtually eliminating runtime schema errors.

DataFrame provides you with a gauntlet of tools to do everything from quick scripting without any ceremony to pipelines with type-checked schema evolution.
