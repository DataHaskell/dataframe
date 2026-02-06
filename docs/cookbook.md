# Cookbook

The following exercies are adapted from Hackerrank's SQL challenges. They outline how to do basic SQL-like operations using dataframe.

## Working with DataFrames in Haskell

This tutorial introduces you to data manipulation using Haskell's DataFrame library. We'll work through filtering, selecting, sorting, and combining data using a functional programming approach that's both powerful and expressive.

Make sure you install `dataframe` and run the custom REPL which provides all the necessary imports and extensions.

## Getting Started with DataFrames

Before we begin, let's load our data. We'll be working primarily with city and station data stored in CSV files. To load a CSV file and expose its columns for easy access:

```haskell
dataframe> df <- D.readCsv "./data/city.csv"
dataframe> :declareColumns df
```

The `:declareColumns` command makes column names available as variables in your scope, allowing you to reference them directly (e.g., `id`, `name`, `population`).

## Filtering Data

One of the most fundamental operations in data analysis is filtering - selecting rows that meet certain criteria. In Haskell's DataFrame library, we use the `filterWhere` function combined with comparison operators.

### Basic Comparisons

The `filterWhere` function takes a boolean expression and returns only the rows where that expression evaluates to true. For example, to find rows where a column equals a specific value, we use the `.==` operator:

```haskell
df |> D.filterWhere (columnName .== value)
```

The pipe operator `|>` allows us to chain operations in a readable left-to-right style, similar to Unix pipes.

**Exercise 1: Basic filtering**

For this question we will use the data in `./data/city.csv`.

Query all columns for a city with the ID 1661.

### Solution
```haskell
dataframe> df |> D.filterWhere (id .== 1661)
-----------------------------------------------------
  id  |  name  | country_code | district | population
------|--------|--------------|----------|-----------
 Int  |  Text  |     Text     |   Text   |    Int    
------|--------|--------------|----------|-----------
 1661 | Sayama | JPN          | Saitama  | 162472
```

**Exercise 2: Basic filtering (cont)**

For this question we will use the data in `./data/city.csv`.

Query all columns of every Japanese city. The `country_code` for Japan is "JPN".

### Solution
```haskell
dataframe> df |> D.filterWhere (country_code .== "JPN")
--------------------------------------------------------
  id  |   name   | country_code | district  | population
------|----------|--------------|-----------|-----------
 Int  |   Text   |     Text     |   Text    |    Int    
------|----------|--------------|-----------|-----------
 1613 | Neyagawa | JPN          | Osaka     | 257315    
 1630 | Ageo     | JPN          | Saitama   | 209442    
 1661 | Sayama   | JPN          | Saitama   | 162472    
 1681 | Omuta    | JPN          | Fukuoka   | 142889    
 1739 | Tokuyama | JPN          | Yamaguchi | 107078
```

### Combining Conditions

Often you'll need to filter on multiple conditions simultaneously. You can combine boolean expressions using logical operators:
- `.&&` for AND (both conditions must be true)
- `.||` for OR (either condition can be true)
- `.>`, `.>=`, `.<`, `.<=` for comparisons

For example, to find cities with large populations in a specific country:
```haskell
df |> D.filterWhere ((population .> 100000) .&& (country_code .== "USA"))
```

**Exercise 3: Basic filtering (cont)**

For this question we will use the data in `./data/city.csv`.

Query all columns for all American cities in city dataframe with:
* populations larger than 100000, and
* the CountryCode for America is "USA".

### Solution
```haskell
dataframe> D.readCsv "./data/country.csv"
dataframe> :declareColumns df
dataframe> df |> D.filterWhere ((population .> 100000) .&& (country_code .== "USA"))
--------------------------------------------------------------
  id  |     name      | country_code |  district  | population
------|---------------|--------------|------------|-----------
 Int  |     Text      |     Text     |    Text    |    Int    
------|---------------|--------------|------------|-----------
 3878 | Scottsdale    | USA          | Arizona    | 202705    
 3965 | Corona        | USA          | California | 124966    
 3973 | Concord       | USA          | California | 121780    
 3977 | Cedar Rapids  | USA          | Iowa       | 120758    
 3982 | Coral Springs | USA          | Florida    | 117549
```

## Limiting Results

When working with large datasets, you often want to preview just a few rows rather than displaying thousands of results. The `take` function limits the output to a specified number of rows from the beginning of the dataframe.

```haskell
df |> D.take n  -- Shows first n rows
```

This is particularly useful for quickly inspecting data or when you only need a sample of results.

**Exercise 4: Constraining output**

For this question we will use the data in `./data/city.csv`.

Show the first 5 rows of the dataframe.

### Solution
```haskell
dataframe> df |> D.take 5
----------------------------------------------------------------
 id  |       name       | country_code |     district      | population
-----|------------------|--------------|-------------------|-----------
 Int |       Text       |     Text     |       Text        |    Int    
-----|------------------|--------------|-------------------|-----------
 6   | Rotterdam        | NLD          | Zuid-Holland      | 593321    
 19  | Zaanstad         | NLD          | Noord-Holland     | 135621    
 214 | Porto Alegre     | BRA          | Rio Grande do Sul | 1314032   
 397 | Lauro de Freitas | BRA          | Bahia             | 109236    
 547 | Dobric           | BGR          | Varna             | 100399
```

## Selecting Specific Columns

While filtering chooses which rows to include, selecting chooses which columns to display. The `select` function takes a list of column specifications. You can reference columns using `F.name columnName`:

```haskell
df |> D.select [F.name column1, F.name column2]
```

This is useful when you want to focus on specific attributes and reduce visual clutter in your output.

**Exercise 5: Basic selection**

For this question we will use the data in `./data/city.csv`.

Get the first 5 names of the city names.

### Solution
```haskell
dataframe> df |> D.select [F.name name] |> D.take 5
-----------------
       name      
-----------------
       Text      
-----------------
 Rotterdam       
 Zaanstad        
 Porto Alegre    
 Lauro de Freitas
 Dobric
```

### Combining Selection and Filtering

The real power of these operations comes from chaining them together. You can filter rows and then select specific columns (or vice versa) to get exactly the data you need:

```haskell
df |> D.filterWhere (condition) |> D.select [columns] |> D.take n
```

The order of operations matters - filtering first reduces the data before selection, which can be more efficient.

**Exercise 6: Selection and filtering**

For this question we will use the data in `./data/city.csv`.

Query the names of all the Japanese cities and show only the first 5 results.

### Solution
```haskell
dataframe> df |> D.filterWhere (country_code .== "JPN") |> D.select [F.name name] |> D.take 5
---------
   name  
---------
   Text  
---------
 Neyagawa
 Ageo    
 Sayama  
 Omuta   
 Tokuyama
```

**Exercise 7: Basic select (cont)**

For this question we will use the data in `./data/station.csv`.

Show the first five city and state rows.

### Solution
```haskell
dataframe> df |> D.select [F.name city, F.name state] |> D.take 5
---------------------
     city     | state
--------------|------
     Text     | Text 
--------------|------
 Kissee Mills | MO   
 Loma Mar     | CA   
 Sandy Hook   | CT   
 Tipton       | IN   
 Arlington    | CO 
```

## Removing Duplicates

When analyzing categorical data, you often want to see unique values rather than repeated entries. The `distinct` function removes duplicate rows from your result set:

```haskell
df |> D.select [F.name column] |> D.distinct
```

This is particularly useful when exploring what values exist in a column or when preparing data for aggregation.

**Exercise 8: Distinct**

For this question we will use the data in `./data/station.csv`.

Query a list of city names for cities that have an even ID number. Show the results in any order, but exclude duplicates from the answer.

### Solution
```haskell
dataframe> df |> D.filterWhere (F.lift even id) |> D.select [F.name city] |> D.distinct 
----------------------
         city         
----------------------
         Text         
----------------------
 Rockton              
 Forest Lakes         
 Yellow Pine          
 Mosca                
 Rocheport            
 Millville            
...
 Lee                  
 Elm Grove            
 Orange City          
 Baker                
 Clutier
```

## Sorting and Combining Results

Sometimes you need to sort data and then combine results from multiple queries. The `sortBy` function orders rows by specified columns. Much like SQL, you can specify multiple columns to
order by. The results are ordered by the first column, with ties broken by the next column 
respectively. 

You can also can use the `<>` operator to concatenate dataframes vertically (similar to SQL's UNION).

```haskell
-- Sort by ascending age
df |> D.sortBy [D.Asc age]
-- 1. Sort by descending age
-- 2. Within those who have the same age, sort by alphabetical order of name.
df |> D.sortBy [D.Asc age, D.Desc name]  
```

You can also derive new columns using `derive` to compute values based on existing columns:

```haskell
df |> D.derive "newColumn" (F.lift function existingColumn)
```

**Exercise 9: Merging**

For this question we will use the data in `./data/station.csv`.

Query the two cities in STATION with the shortest and longest city names, as well as their respective lengths (i.e.: number of characters in the name).

### Solution

We'll include the SQL for comparison:

```SQL
(SELECT CITY, LENGTH(CITY) FROM STATION ORDER BY LENGTH(CITY) DESC LIMIT 1)
UNION
(SELECT CITY, LENGTH(CITY) FROM STATION ORDER BY LENGTH(CITY) ASC LIMIT 1);
```

```haskell
dataframe> letterSort s = df |> D.derive "length" (F.lift T.length city) |> D.select [F.name city, "length"] |> D.sortBy [s "length"] |> D.take 1
dataframe> (letterSort D.Desc) <> (letterSort D.Asc)
-------------------------------
         city          | length
-----------------------|-------
         Text          |  Int
-----------------------|-------
 Marine On Saint Croix | 21
 Roy                   | 3
```

## Using Custom Functions

One of the strengths of working with dataframes in Haskell is the ability to use any Haskell function in your queries. The `F.lift` function allows you to apply regular Haskell functions to DataFrame columns. This means you can use string functions, mathematical operations, or even your own custom logic:

```haskell
df |> D.filterWhere (F.lift customFunction columnName)
```

This enables sophisticated filtering that goes beyond simple comparisons. For example, you can check string prefixes, perform calculations, or apply complex business logic.

**Exercise 10: Duplicates and user defined functions**

For this question we will use the data in `./data/station.csv`.

Query the list of city names starting with vowels (i.e., a, e, i, o, or u). Your result cannot contain duplicates.

### Solution

```haskell
dataframe> df |> D.select [F.name city] |> D.filterWhere (F.lift (\c -> any (`T.isPrefixOf` (T.toLower c)) ["a", "e", "i", "o", "u"]) city) |> D.take 5
----------
   city
----------
   Text
----------
 Arlington
 Albany
 Upperco
 Aguanga
 Odin
```

## Summary

You've now learned the fundamental operations for working with dataframes in Haskell:
- **Filtering** with `filterWhere` to select rows based on conditions
- **Selecting** with `select` to choose specific columns
- **Limiting** with `take` to control output size
- **Removing duplicates** with `distinct`
- **Sorting** with `sortBy` and combining results with `<>`
- **Applying custom functions** with `F.lift` for sophisticated data manipulation

These building blocks can be composed together to answer complex data analysis questions in a clear, functional style.