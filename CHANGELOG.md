# Revision history for dataframe

## 1.1.1.0
### New features
* Add `DataFrame.Typed.Lazy` module — a type-safe lazy query pipeline combining compile-time schema tracking with deferred execution.
* Add `fromCsv` function for parsing a CSV string directly into a DataFrame.
* Add `DataKinds` extension and `DataFrame.Typed` import to the GHCi file for easier interactive typed dataframe workflows.

### Performance
* Specialize and inline aggregation functions (`sum`, `mean`, `variance`, `median`, `stddev`, etc.) to avoid expensive numeric conversions at runtime.
* Remove `Ord` constraint from `Columnable'` and move it to call sites, reducing unnecessary constraint propagation.
* Replace exponential type-level `If` nesting in typed schema families with linear helper type families, fixing slow compilation times.

### Bug fixes
* Fix Functions module compilation under GHC 9.10 (#194).
* Document and test `safeColumns` option in `ParquetReadOptions` (#190).

## 1.1.0.0
### Breaking changes
* Remove `OptionalColumn` constructor; fold nullability into `BoxedColumn`/`UnboxedColumn` via bit-packed bitmap.
* Remove `NFData` instance from `Columnable` constraint.

### New features
* Add `toCsv` and `toSeparated` for converting a DataFrame to CSV/delimited text without writing to a file.
* `safeRead` now defaults reading columns to `Maybe a`.
* Split SIMD CSV reader into a separate `dataframe-fastcsv` package.

### Bug fixes
* Fix joins for missing key columns (#187).
* Fix single column not found error when using typed dataframe.
* Fix Synthesis to use `SafeLookup` constraint.
* Fix `writeSeparated` ignoring separator parameter (was hardcoded to comma).

### Internal
* Slice groups now use custom backpermute instead of converting unboxed vectors.
* Reuse comparison operators in Subset.
* Refactor `getRowAsText` for readability using pattern guards.

## 1.0.0.1
* toMarkdownTable is now toMarkdown (mostly used internally)
* Provide toMarkdown' that outputs string
* Add associativity to nullable operators
* Better null dataframe handling/error messages for core operations.
* Fix some function display names.
* Examples now build with CI

## 1.0.0.0
* Fix mappend to respect schema of empty columns.
* Add cast operators that force column schema
* Add null aware operators so some operations are easier.
* Add arrow shim with python example.
* Add numeric promotion for numeric operations.
* Add column provenance tracking
* Add stratified sampling
* Read files from hugging face

## 0.7.0.0
* This release adds A LOT of AI code to the repo (which we'll now pause in favour of refactoring, testing, and completeness for 1.0)
* The lazy reader now has a custom binary format that it spills to. This almost halved the time it takes to run the 1 billion row challenge. The lazy evaluation now also supports Parquet.
* You can now explicitly set the schema of a subset of columns (named) as opposed to setting a list.
* Join order is changed from pipelining style to function call style.
* Bug in division being marked as commutative.
* Columnable now requires nfdata.
* Parquet reads are now faster across the board.
* Better test coverage.

## 0.6.0.0
* New typed API see https://dataframe.readthedocs.io/en/latest/using_dataframe_in_a_standalone_script.html
* Faster joins
* Fine grained parquet reads using `readParquetFilesWithOpts`.

## 0.5.0.0
* SortOrder now takes an expression rather than a string.
* selectBy now respects the original column order.
* Some changes to the internal representation of the expression GADT.
* Added fixity to binary operations for column comparisons.
* Fixed ZSTD decompression for Parquet files.
* Added logical type handling for timestamps and decimals.
* aggregation now handles null dataframes gracefully.
* Drop random dep to 1.2 and use random-shuffle for shuffling instead.
* Join now merges columns to a `These`.
* Added writeTsv function.
* Move expression operators to `Operators` namespace.

## 0.4.1.0
* Improve signal handling of dataframe repl.
* `writeCsv` not correctly writes `Maybe` values (thanks to @mcoady).
* Create a boilerplate package in cache that will be used to start repl.
* Tree implementation is now a TAO tree instead of greedy cart trees.
* Add `sampleM` and `takeM` functions.

## 0.4.0.10
* License in cabal was wrong.
* Remove ollama-haskell dependencies.
* readParquetFiles for reading globs.
* Fixed printing of `neq` function. 

## 0.4.0.9
* Update license to MIT.

## 0.4.0.8
* LLM guided decision tree
* Parquet fixes: floats read properly and bit width now properly interpreted.
* renameM added to monadic functions.
* No need to explicitly import text for declareColumns

## 0.4.0.7
* Pretty printer for expression
* Fix issue with how `pow` was getting displayed.
* `exposeColumns` has been renamed to `declareColumns`

## 0.4.0.6
* Even faster groupby: uses radix sort rather than mergesort.
* Created `rowValue` function - `df |> D.toRowList |> map (D.rowValue some_column)`.
* SIMD reads for TSV files (thanks to @jhingon).
* Fix string representation of recodeWithDefault.
* Fix gain function for decision tree.
* Add disallowed pairs for decision tree analyst.
* Decision tree percentiles are now a tree-level configuration.

## 0.4.0.5
* Faster groupby: does less allocations by keeping everything in a mutable vector.
* declareColumnsFromCsvFile now infers types from a sample rather than reading the whole dataframe.
* Decision trees API is now more configurable.
* Add annotation to show what expressions were used to derive a column.

## 0.4.0.4
* More robust synthesis based decision tree
* Improved performance on sum and mean.
* recodeWithCondition - to change a value given a condition
* medianMaybe, genericPercentile, percentile - self explanatory
* all the maybe functions as dataframe functions
* Fix concatColumnsEither when types are the same.
* Decision tree implementation is more robust now.

## 0.4.0.3
* Improved performance for folds and reductions.
* Improve standalone mean and correlation functions.
* Remove buggy boxedness check in aggregations.
* CSV files shouldn't have spaces in headers.
* Small decision tree implementation (experimental).

## 0.4.0.1
* Fuse literals in binary expressions and conditionals: we can now express computations like: `df |> D.groupBy [F.name ocean_proximity] |> D.aggregate ["rand" .= F.sum (F.ifThenElse (ocean_proximity .== "ISLAND") 1 0)]`.
* Unary aggregations do not mistakenly boxed unboxed instances.

## 0.4.0.0
* `readSeparated` no longer takes the separator as an argument. This is not placed into readOptions.
* Some improvements to the synthesis demo
* Add a `declareColumnsParquetFile` function. 
* Column conversion functions now take expressions instead of strings.
* Add more monadic functions to make previously tricky transformations easier to write:
    ```haskell
    {-# LANGUAGE OverloadedStrings #-}
    {-# LANGUAGE TemplateHaskell #-}

    module Main where

    import qualified DataFrame as D
    import qualified DataFrame.Functions as F

    import DataFrame.Monad

    import Data.Text (Text)
    import DataFrame.Functions ((.&&), (.>=))

    $(F.declareColumnsFromCsvFile "./data/housing.csv")

    main :: IO ()
    main = do
        df <- D.readCsv "./data/housing.csv"
        print $ execFrameM df $ do
            is_expensive <- deriveM "is_expensive" (median_house_value .>= 500000)
            meanBedrooms <- inspectM (D.meanMaybe total_bedrooms)
            totalBedrooms <- imputeM total_bedrooms meanBedrooms
            filterWhereM (totalBedrooms .>= 200 .&& is_expensive)
    ```

## 0.3.5.0
* Add a `deriveWithExpr` that returns an expression that you can use in a subsequent expressions.
* Add `declareColumnsFromCsvFile` which can create the expressions up front for use in scripts.
    ```haskell
    import qualified DataFrame as D
    import qualified DataFrame.Functions as F

    import Data.Text (Text)
    import DataFrame.Functions ((.==), (.>=))

    $(F.declareColumnsFromCsvFile "./data/housing.csv")

    main :: IO ()
    main = do
        df <- D.readCsv "./data/housing.csv"
        let (df', test) = D.deriveWithExpr "test" (median_house_value .>= 500000) df
        print (D.filterWhere test df')
    ```
* Fix bounds on random.
* Parquet Column chunks weren't reading properly because we didn't correctly calculate the list size.
* Sum function had a bug where the first number was summed twice.
* Add monadic interface for building dataframe expressions that makes schema evolution nice.
    ```haskell
    {-# LANGUAGE OverloadedStrings #-}
    {-# LANGUAGE TemplateHaskell #-}

    module Main where

    import qualified DataFrame as D
    import qualified DataFrame.Functions as F

    import DataFrame.Monad

    import Data.Text (Text)
    import DataFrame.Functions ((.&&), (.>=))

    $(F.declareColumnsFromCsvFile "./data/housing.csv")

    main :: IO ()
    main = do
        df <- D.readCsv "./data/housing.csv"
        print $ runFrameM df $ do
            is_expensive <- deriveM "is_expensive" (median_house_value .>= 500000)
            filterWhereM is_expensive
            luxury <- deriveM "luxury" (is_expensive .&& median_income .>= 8)
            filterWhereM luxury
    ```
* Change order of exponentiation to putting the exponent second. It was initially first cause of some internal efficiency detail but that's silly.
* Fix bug where we didn't concat columns from row groups.

## 0.3.4.1
* Faster sum operation (now does a reduction instead of collecting the vector and aggregating)
* Update the fixity of comparison operations. Before `(x + y) .<= 10`. Now: `x + y ,<= 10`.
* Revert sort for groupby back to mergesort.

## 0.3.4.0
* Fix right join - previously erased some values in the key.
* Change sort API so we can sort on different rows.
* Add meanMaybe and stddevMaybe that work on `Maybe` values.
* More efficient numeric groupby - use radix sort for indices and pre-sort when collecting.

## 0.3.3.9
* Fix compilation issue for ghc 9.12.*

## 0.3.3.8
* More efficient inner joins using hashmaps.
* Initial JSON lines implementation
* More robust logic when specifying CSV types.
* Strip spaces from titles and rows in CSV reading.
* Auto parsing bools in CSV.
* Add `imputeWith`, `bind`, `nRows`, `nColumns`, `recodeWitDefault` function that takes 
* Better support for proper markdown
* Fix bug with full outer join.
* Unify `insertVector` and `insertList` functions into insert.

## 0.3.3.7
* Many functions how rely on expressions (not strings).
* full, left, and right join now implemented.
* fastCsv now strips quotations from text.
* Add "NA" as a nullish pattern.
* Add bin parameter to terminal plotting.
* Implement filterAllNothing for null handling.
* Remove behaviour where we parse mixed types as `Either`
* Add `whenPresent`, `whenBothPresent` and `recode` functions.
* Web charts now show on first load.
* Add deriveMay function for multiple column derivations.

## 0.3.3.6
* Fix bug where doubles were parsing as ints
* Fix bugs where optionals were left in boxed column (instead of optionals)
* Change syntax for conditional operations so it doesn't clash with regular operations.

## 0.3.3.5
* Fix parsing logic for doubles. Entire parsing logic is still a work in progress.
* Speed up index selection by using backPermute.
* Add `mode` function to `Functions`.
* Rewrite some expressions to evaluation more efficient.
* Show correct number of rows in message after truncating for display.
* Add experimental fast CSV parsers (thanks @jhingon)
* Add support to read dataframes from SQL databases.

## 0.3.3.4
* Add linting CI step + fix existing lint errors.
* Show now only prints 10 row. To print more you should use the new `display` function that takes the number of rows as a parameter in its configuration.
* Add `toDouble`, `div`, and, `mod` functions.
* Define an `IsString` instance for columns so you can use string literals without `F.lit`.
* Include variance expression.
* Improved filter performance.
* Make beam search loss function configurable for synthesizing features.

## 0.3.3.3
* Split `toMatrix` into more specific `to<Type>Matrix` functions.

## 0.3.3.2
* Update documentation on both readthedocs and hackage.

## 0.3.3.1
* Fix bug in `randomSplit` causing two splits to overlap.

## 0.3.3.0
* Better error messaging for expression failures.
* Fix bug where exponentials were not being properly during CSV parsing.
* `toMatrix` now returns either an exception or the a vector of vector doubles.
* Add `sample`, `kFolds`, and `randomSplit` to sample dataframes.

## 0.3.2.0
* Fix dataframe semigroup instance. Appending two rows of the same name but different types now gives a row of `Either a b` (work by @jhrcek).
* Fix left expansion of semigroup instance (work by @jhrcek). 
* Added `hasElemType` function that can be used with `selectBy` to filter columns by type. E.g. `selectBy [byProperty (hasElemType @Int)] df.`
* Added basic support for program synthesis for feature generation (`synthesizeFeatureExpr`) and symbolic regression (`fitRegression`).
* Web plotting doesn't embed entire script anymore.
* Added `relu`, `min`, and `max` functions for expressions.
* Add `fromRows` function to build a dataframe from rows. Also add `toAny` function that converts a value to a dynamic-like Columnable value.
* `isNumeric` function now recognises `Integer` types.
* Added `readCsvWithOpts` function that allows read specification.
* Expose option to specify data formats when parsing CSV.
* Added setup script for Hasktorch example.


## 0.3.1.2
* Update granite version, again, for stackage.

## 0.3.1.1
* Aggregation now works on expressions rather than just column references.
* Export writeCsv
* Loosen bounds for dependencies to keep library on stackage.
* Add `filterNothing` function that returns all empty rows of a column.
* Add `IfThenElse` function for conditional expressions.
* Add `synthesizeFeatureExpr` function that does a search for a predictive variable in a `Double` dataframe.

## 0.3.1.0
* Add new `selectBy` function which subsumes all the other select functions. Specifically we can:
    * `selectBy [byName "x"] df`: normal select.
    * `selectBy [byProperty isNumeric] df`: all columns with a given property.
    * `selectBy [byNameProperty (T.isPrefixOf "weight")] df`: select by column name predicate.
    * `selectBy [byIndexRange (0, 5)] df`: picks the first size columns.
    * `selectBy [byNameRange ("a", "c")] df`: select names within a range.
* Cut down dependencies to reduce binary/installation size.
* Add module for web plots that uses chartjs.
* Web plots can open in the browser.

## 0.3.0.4
* Fix bug with parquet reader.

## 0.3.0.3
* Improved parquet reader. The reader now supports most parquet files downloaded from internet sources
  * Supports all primitive parquet types plain and uncompressed.
  * Can decode both v1 and v2 data pages.
  * Supports Snappy and ZSTD compression.
  * Supports RLE/bitpacking encoding for primitive types
  * Backward compatible with INT96 type.
  * From the parquet-testing repo we can successfully read the following:
    * alltypes_dictionary.parquet
    * alltypes_plain.parquet
    * alltypes_plain.snappy.parquet
    * alltypes_tiny_pages_plain.parquet
    * binary_truncated_min_max.parquet
    * datapage_v1-corrupt-checksum.parquet
    * datapage_v1-snappy-compressed-checksum.parquet
    * datapage_v1-uncompressed-checksum.parquet
* Improve CSV parsing: Parse bytestring and convert to text only at the end. Remove some redundancies in parsing with suggestions from @Jhingon.
* Faster correlation computation.
* Update version of granite that ships with dataframe and add new scatterBy plot.

## 0.3.0.2
* Re-enable Parquet.
* Change columnInfo to describeColumns
* We can now convert columns to lists.
* Fast reductions and groupings. GroupBys are now a dataframe construct not a column construct (thanks to @stites).
* Filter is now faster because we do mutation on the index vector.
* Frequencies table nnow correctly display percentages (thanks @kayvank)
* Show table implementations have been unified (thanks @metapho-re)
* We now compute statistics on null columns
* Drastic improvement in plotting since we now use granite.

## 0.3.0.1
* Temporarily remove Parquet support. I think it'll be worth creating a spin off of snappy that doesn't rely on C bindings. Also I'll probably spin Parquet off into a separate library.

## 0.3.0.0
* Now supports inner joins
```haskell
ghci> df |> D.innerJoin ["key_1", "key_2"] other
```
* Aggregations are now expressions allowing for more expressive aggregation logic. Previously: `D.aggregate [("quantity", D.Mean), ("price", D.Sum)] df` now ``D.aggregate [(F.sum (F.col @Double "label") / (F.count (F.col @Double "label")) `F.as` "positive_rate")]``
* In GHCI, you can now create type-safe bindings for each column and use those in expressions.

```haskell
ghci> :exposeColumns df
ghci> D.aggregate  [(F.sum label / F.count label) `F.as` "positive_rate"]
```
* Added pandas and polars benchmarks.
* Performance improvements to `groupBy`.
* Various bug fixes.

## 0.2.0.2
* Experimental Apache Parquet support.
* Rename conversion columns (changed from toColumn and toColumn' to fromVector and fromList).
* Rename constructor for dataframe to fromNamedColumns
* Create an error context for error messages so we can change the exceptions as they are thrown.
* Provide safe versions of building block functions that allow us to build good traces.
* Add readthedocs support.

## 0.2.0.1
* Fix bug with new comparison expressions. gt and geq were actually implemented as lt and leq.
* Changes to make library work with ghc 9.10.1 and 9.12.2

## 0.2.0.0
### Replace `Function` adt with a column expression syntax.

Previously, we tried to stay as close to Haskell as possible. We used the explicit
ordering of the column names in the first part of the tuple to determine the function
arguments and the a regular Haskell function that we evaluated piece-wise on each row.

```haskell
let multiply (a :: Int) (b :: Double) = fromIntegral a * b
let withTotalPrice = D.deriveFrom (["quantity", "item_price"], D.func multiply) "total_price" df
```

Now, we have a column expression syntax that mirrors Pyspark and Polars.

```haskell
let withTotalPrice = D.derive "total_price" (D.lift fromIntegral (D.col @Int "quantity") * (D.col @Double"item_price")) df
```

### Adds a coverage report to the repository (thanks to @oforero)
We don't have good test coverage right now. This will help us determine where to invest.
@oforero provided a script to make an HPC HTML report for coverage.

### Convenience functions for comparisons
Instead of lifting all bool operations we provide `eq`, `leq` etc.

## 0.1.0.3
* Use older version of correlation for ihaskell itegration

## 0.1.0.2
* Change namespace from `Data.DataFrame` to `DataFrame`
* Add `toVector` function for converting columns to vectors.
* Add `impute` function for replacing `Nothing` values in optional columns.
* Add `filterAllJust` to filter out all rows with missing data.
* Add `distinct` function that returns a dataframe with distict rows.

## 0.1.0.1
* Fixed parse failure on nested, escaped quotation.
* Fixed column info when field name isn't found.

## 0.1.0.0
* Initial release
