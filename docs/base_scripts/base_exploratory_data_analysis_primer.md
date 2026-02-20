# A primer on Exploratory Data Analysis

Exploratory data analysis (EDA), in brief, is what you do when you first get a dataset. EDA should help us answer questions about the data and help us formulate new ones. It is the step before any modelling or inference where we look at the data so we can:

* check for completeness/correctness of data.
* understand the relationships between the explanatory variables.
* understand the relationship between the explanatory and outcome variables.
* preliminarily determine what models would be appropriate for our data.

It's important for EDA tools to be feature-rich and intuitive so we can answer many different kinds of questions about the data without the tool getting in the way.


There are four types of explanatory data analysis:

* univariate non-graphical analysis
* multivariate non-graphical analysis
* univariate graphical analysis
* multivariate graphical analysis

We will look at each type of EDA and describe how we can use dataframe for each type. We'll be using the [California Housing Dataset](https://www.kaggle.com/datasets/camnugent/california-housing-prices) to demonstrate the concepts as we explain them.

## Univariate non-graphical analysis

Univariate non-graphical analysis should give us a sense of the distribution of our dataset's variables. In the real world our variables are measurable characteristics. How they are distributed (the "sample distribution") and this may often help us estimate the overall distribution ("population distribution") of the variable. For example, if our variable was finishing times for a race, our analysis should be able to answer questions like what was the slowest time, what time did people tend to run, who was the fastest, were all times recorded etc.

For categorical data the best univariate non-graphical analysis is a tabulation of the frequency of each category.

```haskell
-- cabal: build-depends: dataframe, text
-- cabal: default-extensions: TemplateHaskell, TypeApplications, OverloadedStrings
import qualified DataFrame as D
import qualified Data.Text.IO as TIO

df <- D.readCsv "../dataframe/data/housing.csv" 

TIO.putStrLn $ D.toMarkdownTable $ D.frequencies "ocean_proximity" df
```

We can also plot similar tables for non-categorical data with a small value set e.g shoe sizes.

For quantitative data our goal is to understand the population distribution through our sample distribution. For a given quantitative variable we typically care about its:

* presence (how much data is missing from each charateristic/variable)
* center (what a "typical" value looks like for some definition of typical),
* spread (how far values are from the "typical" value),
* modality (what are the most popular ranges of values),
* shape (is the data normally distributed? does it skew left or right?),
* and outliers (how common are outliers)

We can calculate sample statistics from the data such as the sample mean, sample variance etc. Although it's most often useful to use graphs to visualize the data's distribution, univariate non-graphical EDA describes aspects of the data's histogram.

### Missing data
Arguably the first thing to do when presented with a datset is check for null values.

```haskell
TIO.putStrLn $ D.toMarkdownTable $ D.describeColumns df
```

It seems we have most of the data except some missing total bedrooms. Dealing with nulls is a separate topic that requires intimate knowledge of the data. So for this initial pass we'll leave out the total_bedrooms variable.

### Central tendency
The central tendency of a distribution describes a "typical" value of that distribution. The most common statistical measures of central tendency are arithmetic mean and median. For symmetric distributions the mean and the median are the same. But for a skewed distribution the mean is pulled towards the "heavier" side wherease the median is more robust to these changes.

For a given column calculating the mean and median is fairly straightfoward and shown below.

```haskell
import qualified DataFrame.Functions as F

D.mean (F.col @Double "housing_median_age") df

D.median (F.col @Double "housing_median_age") df
```

Note: You need to pass the expression for the column into these functions not the column name so the program knows that you are actually calling `mean` or `median` on a column containing numbers.

### Spread
Spread is a measure of how far away from the center we are still likely to find data values. There are three main measures of spread: variance, mean absolute deviation, standard deviation, and interquartile range.

### Mean absolute deviation
We start by looking at mean absolute deviation since it's the simplest measure of spread. The mean absolute deviation measures how far from the average values are on average. We calcuate it by taking the absolute value of the difference between each observation and the mean of that variable, then finally taking the average of those.

In the housing dataset it'll tell how "typical" our typical home price is.

```haskell
import DataFrame ((|>))

$(F.declareColumns df)

TIO.putStrLn $ D.toMarkdownTable $ 
   df |> D.derive "deviation" (abs (median_house_value - (F.mean median_house_value)))
      |> D.select ["median_house_value", "deviation"]
      |> D.take 10
```

The first part (`:declareColumns df`) creates typed references to our columns that we can use in expressions. This command gets the types from a snapshot of the schema.

The main logic, read left to right, we begin by calling `derive` which creates a new column computed from a given expression. The order of arguments is `derive <target column> <expression>  <dataframe>`. We then select only the two columns we want and take the first 10 rows.

This gives us a list of the deviations.

From the small sample it does seem like there are some wild deviations. The first one is greater than the mean! How typical is this? Well to answer that we take the average of all these values.

```haskell
df |> D.derive "deviation" (abs (median_house_value - (F.mean median_house_value)))
   |> D.select ["median_house_value", "deviation"]
   |> D.mean (F.col @Double "deviation")
```

Getting the mean of the deviations was as simple as tacking `D.mean "deviation"` to the end of our existing pipeline. Composability is a big strength of Haskell code.

So the $200'000 deviation we saw in the sample isn't very typical but it raises a question about outliers.
What if we give more weight to the further deviations?


### Standard deviation
That's what standard deviation aims to do. Standard deviation considers the spread of outliers. Instead of calculating the absolute difference of each observation from the mean we calculate the square of the difference. This has the effect of exaggerating further outliers.

```haskell
withDeviation = df |> D.derive "deviation" (abs (median_house_value - (F.mean median_house_value)))
                   |> D.select ["median_house_value", "deviation"]
                   |> D.take 10

$(F.declareColumns withDeviation)

import Data.Maybe

sumOfSqureDifferences = withDeviation |> D.derive "deviation^2" (F.pow deviation 2)
                                      |> D.sum (F.col @Double "deviation^2")

n = fromIntegral (fst (D.dimensions df) - 1)

sqrt (sumOfSqureDifferences / n)
```
The standard deviation being larger than the mean absolute deviation means we do have some outliers. However, since the difference is fairly small we can conclude that there aren't very many outliers in our dataset.

We can calculate the standard deviation in one line as follows:

```haskell
D.standardDeviation (F.col @Double "median_house_value") df
```

## Interquartile range (IQR)
A quantile is a value of the distribution such that n% of values in the distribution are smaller than that value. A quartile is a division of the data into four quantiles. So the 1st quantile is a value such that 25% of values are smaller than it. The median is the second quartile. And the third quartile is a value such that 75% of values are smaller than that value. The IQR is the difference between the 3rd and 1st quartiles. It measures how close to middle the middle 50% of values are.

The IQR is a more robust measure of spread than the variance or standard deviation. Any number of values in the top or bottom quarters of the data can be moved any distance from the median without affecting the IQR at all. More practically, a few extreme outliers have little or no effect on the IQR

For our dataset:

```haskell
D.interQuartileRange (F.col @Double "median_house_value") df
```

This is larger than the standard deviation but not by much. This means that outliers don't have a significant influence on the distribution and most values are close to typical.

### Variance
Variance is the square of the standard deviation. It is much more sensitive to outliers. Variance does not have the same units as our original variable (it is in units squared). Therefore, it's much more difficult to interpret.

In our example it's a very large number:

``` haskell
D.variance (F.col @Double "median_house_value") df
```

The variance is more useful when comparing different datasets. If the variance of house prices in Minnesota was lower than California this would mean there were much fewer really cheap and really expensive house in Minnesota.

## Shape
Skewness measures how left or right shifted a distribution is from a normal distribution. A positive skewness means the distribution is left shifted, a negative skew means the distribution is right shifted.

The formula for skewness is the mean cubic deviation divided by the cube of the standard deviation. It captures the relationship between the mean deviation (asymmetry of the data) and the standard deviation (spread of the data).

The intuition behind why a positive skew is left shifted follows from the formula. The numerator is more sensitive to outliers. So the futher left a distribution is the more the right-tail values will be exaggerated by the cube causing the skewness to be positive.

A skewness score between -0.5 and 0.5 means the data has little skew. A score between -0.5 and -1 or 0.5 and 1 means the data has moderate skew. A skewness greater than 1 or less than -1 means the data is heavily skewed.

```haskell
D.skewness (F.col @Double "median_house_value") df
```
So the median house value is moderately skewed to the left. That is, there are more houses that are cheaper than the mean values and a tail of expensive outliers. Having lived in California, I can confirm that this data reflects reality.


## Summarising the data

We can get all these statistics with a single command:

```haskell
TIO.putStrLn $ D.toMarkdownTable $ D.summarize df
```

As a recap we'll go over what this tells us about the data:
* median_house_value: house prices tend to be close to the median but there are some pretty expensive houses.
* median_income: incomes are also generally fairly typical (small standard deviation with median close to mean) but there are some really rich people (high skewness).
* households: household sizes are very similar across the sample and they tend to be smaller.
* population: California is generally very sparsely populated (low skewness) with some REALLY densely populated areas (high max/ low IQR).
* total_rooms: a lot of the blocks have few rooms (Again sparse population) but there are some very dense areas (high max).
* housing_median_age: there are as many new houses as there are old (skewness close to 0) and not many extremes (low max, standard deviation lower than IQR)
* latitude: the south has slightly more people than the north (moderate skew)
* longitude: most houses are in the west coast (moderate right skew)

## Univariate graphical EDA

Pictures oftentimes give a more informative account of what our data looks like. We can densely embed a lot of information in a picture: colour, shape, size, hue etc. All things the human mind has spent centuries getting better at. The informativeness of graphical tools comes at the cost of precision. Thus, non-graphical and graphical methods complement each other to create a holistic view of data. 

In this section, we'll next look at some techniques for visualizing univariate data.

### Histograms
Histograms are bar plots where each bar represents the frequency (count) or propotion (count / total) of cases for a
range of value. Going back to our california housing dataset, we can plot a histogram of house prices:

```haskell
D.plotHistogram "median_house_value" df
```

From the histogram above we can already tell things like whether or not there are outliers, the central tendency of the data, and the spread.
