# Design document - Dataframes in Haskell

Author: [Michael Chavinda](mailto:mschavinda@gmail.com)  
Created: 29 Nov 2024  
Implementation: [https://github.com/mchav/dataframe](https://github.com/mchav/dataframe)

## Overview

The goal of this document is to detail the design of a dataframe library for exploratory data analysis (EDA) in Haskell. In addition to fulfilling the usual functional requirements of a dataframe library, the library must also have many modern features learned from years of development in the space.

## What is a dataframe?

A dataframe is an amalgamation of concepts from relational databases, spreadsheets and linear algebra. There has been an interesting debate on whether or not they are a good/principled database abstraction[^1]. But theory aside, their utility has been undeniable. Enough so that formalizing their definition and algebra is an active research area.

The most authoritative paper on the subject[^2] defines a dataframe as a tuple (A<sub>mn</sub>, R<sub>m</sub>, C<sub>n</sub>, D<sub>n</sub>) where:

* A<sub>mn</sub> is a two-dimensional array of strings,  
* C<sub>n</sub> is a vector of column labels,  
* R<sub>m</sub> is a vector of row labels  
* D<sub>n</sub> is a vector of n domains/types of each of the columns

While this definition describes a dataframe it’s not very prescriptive. That is, it doesn’t give us a very strong sense of what operations are permitted in a dataframe and what a dataframe algebra could look like. So for the majority of their history dataframes had no formal algebra and amorphously took on the shape compelled by the different problem spaces they were applied to. This is all to say, dataframes are still difficult to model rigorously with a lot left to library implementers and their target audience.

Every analytical dataframe library (R, pandas, Spark, Polars) realises this abstract object with additional behaviours (mutability rules, distributed storage, lazy evaluation, etc.), but the structural invariants above remain constant.

## What are dataframes used for?

The primary use case for dataframes is exploratory data analysis. They have typically been used to load unstructured data from CSV files and run statistical computations on the data. In recent history, they have seeped into other parts of the data science world becoming a sort of standard for representing data in ML workflows, a model for distributed data computation, and even integrating with/competing with other database systems for online analytical processing (OLAP). What each implementation becomes is also left to the library implementers and their target audience.

## Why create a library in Haskell? 

Dataframe systems, as mentioned before, are central to a data ecosystem. Any modern language that works with data (no matter how big or small the data is) must have a dataframe library. Our hope is that this library can form the bedrock of Haskell’s data ecosystem.

We believe the unique strength of Haskell in exploratory data analysis will be the ability to marry approaches in program synthesis and data analytics to create tools for automated data cleaning. Purely functional/declarative (domain specific) languages make for a good search space when synthesizing programs.

Additionally this can showcase some of Haskell’s strengths such as an expressive syntax and easy parallelism. Similarly this can be an experiment in High performance computing in Haskell e.g SIMD support.

## Who is our target audience?

Initially, our target audience will be data scientists/analysts that are already proficient in SQL, R, or Python. Since these languages, and their corresponding libraries, are simple, simplicity will be a core value of this library. We would like this library to be a fully featured exploratory data analysis tool and eventually form the basis of a self-service data wrangling tool[^3].

## Why use this over other libraries?

The ultimate value proposition of this library will be:

* A simple, fast and efficient library to conduct data analysis.  
* A sharp focus on data wrangling and cleaning that other tools may compromise in the name of speed and scale.  
* A relatively easy, type-aware syntax. 

## Design philosophy

* Audience, audience, audience. In other words: domain, domain domain. As we’ve discussed above, the looseness of the space gives us a lot of leeway in design. On the other hand, the ubiquity of dataframes guides what APIs and functionality users have come to expect of such systems. Our functional and nonfunctional requirements are informed by this reality. Which leads us to our first overarching design principle: design for familiarity.

Dataframe architectures were developed in conversation with data scientists. This is still the case. It’s impossible to join this conversation if we don’t design with familiarity and simplicity in mind. A tool should not get in the way (introduce conceptual complexity) unless doing so has orders of magnitude more benefit than letting the user make a mistake.

Our second design principle isn’t so much a principle that will determine the evolution of the library. Rather it is one that will inform this initial iteration. This library will be designed primarily for datasets that fit in memory \- or at the very least for a non-distributed setting.

Why? Recall that dataframes have become a data science jackknife encompassing everything from data exploration, pre-training data-cleaning, query engines, and ETL pipelines. These are all related domains but compel different design decisions.

For example, exploratory data analysis doesn’t require a schema. We usually figure out the shape of the data as we go. On the other hand, efficient querying and storage requires the “rigidity” of a traditional relational database. Some dataframe libraries do both of these e.g Polars has an eager mode for working with data that fits in memory and a lazy mode (which requires a schema) for larger-than-memory datasets that effectively works like a query engine with predicate pushdown optimization etc[^4].

EDA (which is often accompanied by data wrangling/cleaning) is an extremely important step in data science, often taking up more than half an analysts’ time.[^5]

Our implementation will be limited to in-memory datasets and focus on being REALLY good at EDA as opposed to optimizing for long-lived queries and computations. However, we will optimize for those use cases as they relate to EDA.

## Prior work

There have been prior efforts to create something of this nature in the Haskell ecosystem:

* [Analyze](https://hackage.haskell.org/package/analyze) \-  a seemingly discontinued row-oriented library for EDA in Haskell.  
* [Frames](https://hackage.haskell.org/package/Frames) \- a type-safe library for working with data from CSV files.

The most obvious drawback of the “analyze” library is that it is row-oriented. This makes columnar operations (which are ubiquitous in analytics) slower and less intuitive.

Frames is a promising attempt but has a syntax that looks more like an advanced Haskell tool than a data science tool. While it pursues a very useful direction it isn’t simple. It contains more Haskell domain knowledge than data science domain knowledge. We’ll explore this design choice in a subsequent section.

There is space in the ecosystem for something both columnar and user/domain oriented.

## Functional requirements

The library should allow users to:

### Input/Output

Import/export data from common data sources e.g text-based tabular formats (e.g CSV and Excel), text based formats (e.g JSON, Toml and XML), relational database formats (e.g SQLite), and more modern column-oriented data formats (e.g arrow and parquet).

Also support reading/writing compressed versions of these where applicable.

### User operations on data

Perform data manipulations including:

* Adding/removing/modifying rows and columns  
* Applying functions to rows or columns  
* Filtering rows by a predicate  
* Sorting by one or more columns  
* Grouping a column and applying aggregations  
* Combining or merging dataframes by appending rows or joining  
* Melt/Explode  
* Nest/Unnest/Flatten  
* Select specific rows and columns  
* Windowing functions

### Automated data processing

Perform various kinds of data cleaning and preprocessing

* Handling missing/malformed data by parsing to either Optional or Either types   
* Sensible defaults for type conversions (automatic or manually)

### Data exploration

Support univariate and multivariate non-graphical analysis

* Descriptive statistics (mean, median, variance etc)  
* Value Counts  
* Correlations  
* Tables summarizing data

Support univariate and multivariate graphical analysis

* Frequency histograms  
* Stem and leaf plots  
* Box plots  
* Quantile-normal plots

Environments:

* Works completely in a terminal either with a native shell or Turtle Haskell shell  
* Supports IHaskell  
* Allow the tool to be flexible enough to be plugged into different contexts e.g web apps (this is more a matter of supporting various outputs and parsing ad hoc commands)

I list these to provide a working specification of all the operations I’d like to support and evaluate an initial implementation against.

## Non-functional requirements

* Interoperability with other tools in the data science ecosystem e.g Notebooks, data interchange formats (Arrow).  
* Interoperability with other Haskell tools such as Frames and Javelin  
* Intuitive syntax that mirrors other dataframe libraries.  
* Good error messaging.  
* Technical documentation for contributors.  
* User guides and tutorials specifically tailored for data scientists/analysts.  
* Performance comparable to other dataframe libraries.  
* Support for parallelism.  
* Support for streaming

## Possible solutions to the problem

### Create a new library from scratch

Pros

* More control over what features to support and the overall design  
* Lessons learnt during the project would be transferable to other purely Haskell efforts hence this is good for the ecosystem.  
* No need to worry about breaking changes or drift from a main library.  
* Easier to integrate with the rest of the Haskell ecosystem.

Cons

* High development effort  
* Performance might lag behind other solutions

### Create a wrapper/DSL around Polars

Pros

* Leverages a proven engine this means we inherit a sound API, performant code, and an adequately tested set of core primitives.  
* Faster development  
* Reduced maintenance since all the heavy lifting happens upstream.

Cons

* Interfacing overhead from FFI could mean performance overhead and difficult debugging.  
* Less customizability since we are ultimately constrained by the design of Polars.

### Expand functionality and design of Frames

Pros

* Avoids reinventing the wheel (Frames already supports streaming, for example).  
* Allows us to focus on extending a solution rather than building a new one.

Cons

* Potential for bloat if the interfaces are conceptually very different.

Since our goals are innovating in the space and creating a deeply integrated Haskell solution, we will create a library from scratch, accepting the high initial development cost.

## High level Design

### Core data type

Recall that the core data structure in a dataframe is a 2-dimensional array (“table”) with homogeneous values in each column but heterogeneous rows.  Our first implementation decision centers on this question: is it better to model a dataframe as a list of columns or a list of rows?

The data science community seems to have converged on the former (i.e lists of columns). Interchange and computational formats (such as Parquet and Arrow) assume columnar data. This isn’t without good reason though. Columnar structures provide better data locality for column-based operations and column operations tend to be more compute efficient[^6].

We follow this convention and define our core data structure as a heterogeneous list of vectors.

```haskell
data DataFrame = DataFrame  
    {  
        columns :: HeterogeneousCollection [Vector T1, Vector T2…]  
    }
```

This isn’t a rigorous (or implementable) definition. Instead it gives us a north star for our implementation.

### Implementing heterogeneous collections

There are two main ways of defining heterogeneous collections in Haskell.[^7]

1) Creating an “Object” type (either with Data.Typeable or Data.Dynamic) and doing runtime instance/type checks on the objects. In this world, a heterogeneous collection is a regular Haskell list containing instances of whatever this “Object” type is.  
2) Using type-level programming to define a true heterogenous list.

To my knowledge, these are the only ways to implement heterogeneous collections that also support schema evolution (i.e adding columns or changing the types of columns dynamically). While the second approach is truer to Haskell’s overall philosophy of type safety it doesn’t make for an intuitive APIs unless hidden behind a lot of other Haskell machinery e.g TemplateHaskell.

To keep the implementation as close to vanilla Haskell as possible, we’ll implement the “Object” approach. This also ensures that the only learning curves are Haskell itself (parts of it) and the domain.

### Defining Columns

Our Object-like primitive in this case will be a column type defined as a GADT.

```haskell
data Column where  
  ValueColumn :: (Typeable a, Ord a, Show a) => Vector a -> Column
```

Our choice of vector here is too coarse. We want to store the data in the most ergonomic or memory efficient way.

```haskell
-- enable constraint kinds for constraint synonyms  
type Columnable a = Typeable a, Ord a, Show a, Read a

data Column where  
  BoxedColumn :: Columnable a => B.Vector a -> Column  
  UnboxedColumn :: (Columnable a, Unboxable a) => U.Vector a -> Column  
  OptionalColumn :: Columnable a => B.Vector (Maybe a) -> Column
```

DataFrames are row-ordered[^8] so their elements are instances of Ord since we can sort a dataframe by any of its columns.

A minimal dataframe definition is:

```haskell
data DataFrame = DataFrame  
    {  
        columns :: Vector Column,  
        columnNames :: Map String Int  
    }
```

### Schema induction

Exploratory data analysis requires type flexibility. Unlike a relational database where we have an explicitly versioned schema-on-read and a schema-on-write, in most cases of EDA we have to induce the schema from some unstructured format. In this world, inferred types aren’t ground truth but are hypotheses themselves that need to be tested. The ground truth is discovered incrementally by testing, validating and partitioning.

While our column representation permits any showable, typeable, ordered type, reading data from a schema-on-read data source like CSV requires us to define a constrained schema induction function with sensible defaults.

### Induction model

**Goal**: For each input field, choose a target type and a decoder with a confidence score, then materialize a column.

Default candidate order (highest priority first):

* Int  
* Double  
* Date (configurable formats, e.g. YYYY-MM-DD, RFC3339)  
* Text

**Null-ish tokens**: "", "NA", "N/A", "NULL", "null", and domain-configurable additions.

Algorithm (per column):

1. Sample & profile the first k rows (configurable; default 4k–16k) to estimate:  
   * null rate,  
   * per-candidate parse success rate,  
   * min/max, cardinality, and basic stats.  
2. Choose a winner using a simple type lattice and thresholds:  
   * Prefer Int if success rate ≥ τ and no overflow; otherwise consider Double.  
   * Accept Date if success rate ≥ τ and strict format match (avoid false positives like 2021-13-40).  
   * Fall back to Text.  
   * Default τ = 0.98 (tunable).  
3. Materialize the column using the chosen decoder and null policy:  
   * If nulls exist: use OptionalColumn a \~ Column (Maybe a).  
   * If parse failures exist but are rare: use Either Text a (so errors are visible).  
   * If failures are common: drop to a wider type (e.g., Double → Text) and record a warning.  
4. Emit an induction report (per column): chosen type, confidence, null rate, example failures, detected formats.

This incremental approach makes “ground truth” something we converge to: users refine types, we re-decode cheaply, and the report documents why the system chose what it chose.

### An expression DSL for row operations

This approach is similar to [Polars](https://docs.pola.rs/user-guide/expressions/basic-operations/#basic-arithmetic). An expression DSL allows us to refer to columns by name. We needn’t have the whole row as input like before. But this approach does move us much further towards dynamism. But since this approach feels both natural and ubiquitous it is preferable over the row map approach.

Implementing the expression DSL means defining an expression datatype that looks roughly like the following:

```haskell
data Expr a where  
  Col :: Text -> Expr a              -- Reference to a column  
  Lit :: a -> Expr a                 -- A constant value  
  UnaryOp :: (b -> a) -> Expr b -> Expr a  
  BinaryOp :: (c -> b -> a) -> Expr c -> Expr b -> Expr a
```

With some convenience function we can define our expression for Compound interest as:

```haskell
ciExpr :: Expr Double  
ciExpr =  
  let p = Col "principal"  
      r = Col "rate"  
      n = Col "numCompounds"  
      t = Col "years"  
  in  p * (Lit 1 + r / n) ** (n * t) - p
```

To interpret the expression we go through the Expr a syntax tree resolving each column reference and function application against the dataframe.

#### Local vs global type safety

This design gives us local type-safety. That is, expressions must always type check. It doesn't guarantee us global type safety however. You could refer to a column that doesn't exist or specify the wrong type for an expression. We can solve this by exporting typed references in template Haskell. E.g.

```haskell
$(exportColumns "data.csv")  
-- generates bindings like:  
principal :: Expr Double; principal = Col "principal"
```

Would put the typed column expressions for the dataframe in context using normal template haskell machinery.

We could also do automatic code generation with a CLI tool or I/O function that writes these to a module in the project. In short, there are many ways to solve ergonomics problem. 

## Nulls, Errors, and Diagnostics

* **Null handling**  
  * Empty tokens → Maybe a columns.  
  * Rare parse failures → Either ParseError a (surfaced in reports and UI).  
  * Users can normalize later: coalesce, fillMissing, dropMissing.

* **Exceptions**  
  * Use exceptions sparingly; prefer structured error values in pure code and raise only at unsafe boundaries (I/O, schema mismatch on write).  
  * Enrich messages with column name, offending value sample, and remediation hints.

* **Induction report**  
  * Always attach per-column stats: chosen type, confidence, null rate, failure examples, and detected formats. Useful for audits and reproducibility.

## API Design

The API favors **consistency, small primitives, and composition**. Names mirror common dataframe libraries; argument order leans functional: **function → args → dataframe**. A pipeline operator (e.g., `|>`) keeps left-to-right flow readable.

**Core principles**

* **Composable:** `df |> filter (col "b" .==. 1) |> derive "a2" (col "a" + 2) |> sortBy Asc ["e"] |> take 10`  
* **Column/row symmetry:** columns are first-class; row logic expressed via the DSL.  
* **Declarative & explicit:** minimal magic; predictable behavior despite dynamic inputs.

**Selected operations**

* Columns  
  * `apply :: (a -> b) -> Text -> DataFrame -> DataFrame`  
  * `derive :: Text -> Expr a -> DataFrame -> DataFrame`  
  * `select :: [Text] -> DataFrame -> DataFrame`  
  * `rename :: Text -> Text -> DataFrame -> DataFrame`  
* Rows (DSL-based)  
  * `filter :: Expr Bool -> DataFrame -> DataFrame`  
  * `sortBy :: [SortOrder] -> DataFrame -> DataFrame`  
  * `groupBy :: [UExpr] -> DataFrame -> DataFrame`  
* Whole-frame  
  * `transpose :: DataFrame -> DataFrame`  
  * `join :: JoinType -> [Text] -> DataFrame -> DataFrame -> DataFrame`  
  * `pivot :: PivotSpec -> DataFrame -> DataFrame`

*Notes:*

* `Order = Asc | Desc`  
* `UExpr` is an untyped expression and is defined as `data UExpr = Expr a`.  
* Joins assume explicit key columns; types must be compatible (coercions explicit).

#### **Integration with Tools and Workflows**

To ensure seamless adoption, the library will integrate with the broader Haskell ecosystem and external data science tools:

* **File Formats**: Native support for widely-used formats like Parquet, Arrow, and CSV.  
* **Interactivity**: Integration with Jupyter notebooks (IHaskell) and other REPL (e.g GHCI) environments for a smooth exploratory workflow.  
* **Export Compatibility**: Easy conversion of DataFrames to formats usable by tools in the data science ecosystem.

By leveraging existing Haskell libraries for parsing, compression, and visualization, the project minimizes duplication of effort while ensuring compatibility with established workflows.

[^1]:  Is a Dataframe just a table? https://drops.dagstuhl.de/entities/document/10.4230/OASIcs.PLATEAU.2019.6

[^2]:  Towards Scalable Dataframe systems https://arxiv.org/pdf/2001.00888

[^3]:  Data Wrangling: What It Is & Why It’s Important \- https://online.hbs.edu/blog/post/data-wrangling

[^4]:  Optimizations https://docs.pola.rs/user-guide/lazy/optimizations/

[^5]:  Self-Service Data Preparation: Research to Practice \- http://sites.computer.org/debull/A18june/p23.pdf

[^6]:  https://www.sciencedirect.com/topics/computer-science/columnar-database

[^7]:  https://wiki.haskell.org/Heterogenous\_collections

[^8]:  Page 2 of https://arxiv.org/pdf/2001.00888