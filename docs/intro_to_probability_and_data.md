# Introduction to probability and data

This is a port of the [exercises from Coursera](https://www.coursera.org/learn/probability-intro).

<div id="instructions">
Complete all **Exercises**, and submit answers to **Questions** on the Coursera 
platform.
</div>

After opening the Haskell interpreter you should see `dataframe>` on your terminal. It will appear in
all subsequent expressions to indicate we are in the interpreter. This is not meant to be typed in manually.

You can also compare the to the [R version of this assignment](https://rstudio-pubs-static.s3.amazonaws.com/344813_acea062c212a430ab9cf6b83cf26b170.html). 


## Dataset 1: Dr. Arbuthnot's Baptism Records

To get you started, run the following command to load the data. We will store 
Arbuthnot's data in a kind of spreadsheet or table called a *data frame*.

```haskell
dataframe> :script dataframe.ghci
========================================
              📦Dataframe
========================================

✨  Modules were automatically imported.

💡  Use prefix 'D' for core functionality.
        ● E.g. D.readCsv "/path/to/file"
💡  Use prefix 'F' for expression functions.
        ● E.g. F.sum (F.col @Int "value")

✅ Ready.
dataframe> df <- D.readCsv "./data/arbuthnot.csv"
```

The Arbuthnot data set refers to Dr. John Arbuthnot, an 18<sup>th</sup> century 
physician, writer, and mathematician. He was interested in the ratio of newborn
boys to newborn girls, so he gathered the baptism records for children born in
London for every year from 1629 to 1710. We can take a look at the data by 
typing its name into the console.

```haskell
dataframe>  df
```

You can see the dimensions of this data frame by typing:

```haskell
dataframe> D.dimensions arbuthnot

```

This command should output `(82, 3)`, indicating that there are 82 rows and 3 
columns. You can see the names of these columns (or variables) by typing:

```haskell
dataframe> D.columnNames df
```

You should see that the data frame contains the columns `year`,  `boys`, and 
`girls`.

1. How many variables are included in this data set? 
<ol>
<li> 2 </li>
<li> 3 </li>
<li> 4 </li>
<li> 82 </li>
<li> 1710 </li>
</ol>

We can get a quick run-down of the data's numeric variables by using `D.summarize`.

```haskell
dataframe> D.summarize df
----------------------------------------
 Statistic |  year   |  boys   |  girls
-----------|---------|---------|--------
   Text    | Double  | Double  | Double
-----------|---------|---------|--------
 Count     | 82.0    | 82.0    | 82.0
 Mean      | 1669.5  | 5907.1  | 5534.65
 Minimum   | 1629.0  | 2890.0  | 2722.0
 25%       | 1649.25 | 4759.25 | 4457.0
 Median    | 1669.5  | 6073.0  | 5718.0
 75%       | 1689.75 | 7576.5  | 7150.25
 Max       | 1710.0  | 8426.0  | 7779.0
 StdDev    | 23.82   | 1652.75 | 1592.14
 IQR       | 40.5    | 2817.25 | 2693.25
 Skewness  | 0.0     | -0.22   | -0.22
```

<div id="exercise">
**Exercise**: What years are included in this dataset?
</div>

### Some Exploration

Let's start to examine the data a little more closely. We can access the data in
a single column of a data frame separately using a command like

```haskell
dataframe> D.select ["boys"] df
```

This command will only show the number of boys baptized each year. The `select` function
basically says "go to the data frame at the end, and find the all the variables that comes after me".

2. What command would you use to extract just the counts of girls born? 
<ol>
<li> `D.select ["boys"] df` </li>
<li> `D.select ["girl"] df` </li>
<li> `girls` </li>
<li> `D.select ["girls"]` </li>
<li> `["boys"] df` </li>
</ol>

We can create a simple plot 
of the number of girls baptized per year with the command

```haskell
dataframe> D.plotScatter "year" "girls" df
8031.9│
      │                                                ⠁⠄ ⡐⠂⠈ ⠢
      │                                        ⡀  ⠠⠂⠌ ⢀ ⠈    ⠂ ⠂⡀
      │                                       ⠑ ⠊⠂⠁      ⠄
      │                                      ⠄        ⡀
      │                                     ⠐        ⠁
      │                               ⢀   ⠄ ⠂
      │                             ⢀ ⠁⡀ ⡀⠈⠁
      │                          ⠂ ⢀ ⠂  ⠒                   ⠂
      │          ⠄                 ⠄
5250.5│          ⠈
      │     ⠐⠌ ⠈⠠ ⠈            ⠠⠐⠐
      │  ⠈⢀⠐  ⠐⡀   ⠁
      │                           ⠐
      │    ⠁       ⠐⠠          ⠁
      │
      │              ⠌     ⡠⢀
      │               ⠐   ⠐  ⡀⠈
      │                ⣀⠠⠊   ⢀
2469.1│
      └────────────────────────────────────────────────────────────
       1624.9                        1669.5                       1714.1

⣿ year vs girls
```

Or if you prefer to see it in an interactive browser chart:

```haskell
dataframe> P.plotScatter "year" "girls" df >>= P.showInDefaultBrowser
Saving plot to: ~/plot-chart_ACzzzidiLidnydNLE32ZmgMH114vwdH87VQwxANWcezbIZ.html
```

1. Which of the following best describes the number of girls baptised over the years included in this dataset? 
<ol>
<li> There appears to be no trend in the number of girls baptised from 1629 to 1710. </li>
<li> There is initially an increase in the number of girls baptised, which peaks around 1640. After 1640 there is a decrease in the number of girls baptised, but the number begins to increase again in 1660. Overall the trend is an increase in the number of girls baptised. </li>
<li> There is initially an increase in the number of girls baptised. This number peaks around 1640 and then after 1640 the number of girls baptised decreases. </li>
<li> The number of girls baptised has decreased over time. </li>
<li> There is an initial increase in the number of girls baptised but this number appears to level around 1680 and not change after that time point. </li>
</ol>

### Haskell as a big calculator

Now, suppose we want to plot the total number of baptisms. To compute this, we 
could use the fact that Haskell is really just a big calculator. We can type in 
mathematical expressions like

```haskell
dataframe> 5218 + 4683
```

to see the total number of baptisms in 1629. We could repeat this once for each 
year, but there is a faster way. If we add the vector for baptisms for boys to 
that of girls, Haskell will compute all sums simultaneously.

```haskell
dataframe> bs = D.columnAsList @Int "boys" df
dataframe> gs = D.columnAsList @Int "girls" df
dataframe> zipWith (+) bs gs
```

What you will see are 82 numbers each one representing the sum we are after. Take a
look at a few of them and verify that they are right.

### Adding a new variable to the data frame

We'll be using this new vector to generate some plots, so we'll want to save it 
as a permanent column in our data frame.

```haskell
dataframe> withTotal = df |> D.derive "total" (F.col @Int "boys" + F.col @Int "girls")
dataframe> D.take 10 withTotal
```


What in the world is going on here? The `|>` operator is called the **piping** 
operator. Basically, it takes the output of the current line and pipes it into 
the following line of code.

<div id="boxedtext">
**A note on piping: ** Note that we can read these three lines of code as the following: 

*"Take the `arbuthnot` dataset and **pipe** it into the `derive` function. 
Using this derive a new variable called `total` that is the sum of the variables
called `boys` and `girls`. Then assign this new resulting dataset to the object
called `withTotal` (we can't replace the old variable because Haskell dataframes are immutable)."*

This is essentially equivalent to going through each row and adding up the boys 
and girls counts for that year and recording that value in a new column called
total.

The `F.col @Int "boys" + F.col @Int "girls"` part is how we right expressions for our dataframe. Read left to right, this expression says take the `Int` called boys and add it to the `Int` column called girls. This saves us the work of having to work with vectors directly. But having to remember the name and type of each column is tedious and error prone. We can ask Haskell to expose correct references to these columns by using `:declareColumns`.

```haskell
dataframe> :declareColumns df
"year :: Expr Int"
"boys :: Expr Int"
"girls :: Expr Int"
```

We have created as many expressions for us as there are columns in the dataset. Now we can rewrite out `withTotal` data frame.

```haskell
dataframe> withTotal = D.derive "total" (boys + girls) df
```

</div>

We can make a plot of the total number of baptisms per year with the following command.

```haskell
dataframe> D.plotScatter "year" "total" withTotal
```

We can use expressions to compute the proportion of boys each year. To do this we have to learn about three functions:
* (/) - divides two `Fractional` numbers.
* F.lift - performs a user defined operation on an expression
* fromIntegral - converts an `Int` to the more general `Num` type.

Trying to divide `boys` and `total` will result in the following type error:

```haskell
dataframe> withTotal |> D.derive "percentage_boys" (boys / total) |> D.take 10
<interactive>:45:47: error: [GHC-39999]
    • No instance for ‘Fractional Int’ arising from a use of ‘/’
    • In the second argument of ‘derive’, namely ‘(boys / total)’
      In the second argument of ‘(|>)’, namely
        ‘derive "percentage_boys" (boys / total)’
      In the first argument of ‘(|>)’, namely
        ‘withTotal |> derive "percentage_boys" (boys / total)’
```

This means that the function (/) doesn't work on integers. So we'll need to do some conversion. Our conversion function is `fromIntegral`.

Trying to use it on our column references will fail:

```haskell
dataframe> withTotal |> D.derive "percentage_boys" ((fromIntegral boys) / (fromIntegral total)) |> D.take 10
<interactive>:46:43: error: [GHC-39999]
    • No instance for ‘Integral (Expr Int)’
        arising from a use of ‘fromIntegral’
    • In the first argument of ‘(/)’, namely ‘(fromIntegral boys)’
      In the second argument of ‘derive’, namely
        ‘((fromIntegral boys) / (fromIntegral total))’
      In the second argument of ‘(|>)’, namely
        ‘derive
           "percentage_boys" ((fromIntegral boys) / (fromIntegral total))’
```

The compiler tells us that `boys` isn't an `Int`- it's an `Expr Int`. It's an integer hidden inside an expression. We have to take the integer out of this expression, convert it, then re-wrap it in the expression again so we can continue to do other things to the expression. The `lift` function does just that. It says, take a function and make it reach into the `Expr` container to change the object inside.

```haskell
dataframe> withTotal |> D.derive "percentage_boys" (F.toDouble boys / (F.toDouble total)) |> D.take 10
-------------------------------------------------
 year | boys | girls | total |  percentage_boys
------|------|-------|-------|-------------------
 Int  | Int  |  Int  |  Int  |       Double
------|------|-------|-------|-------------------
 1629 | 5218 | 4683  | 9901  | 0.527017472982527
 1630 | 4858 | 4457  | 9315  | 0.5215244229736984
 1631 | 4422 | 4102  | 8524  | 0.5187705302674801
 1632 | 4994 | 4590  | 9584  | 0.5210767946577629
 1633 | 5158 | 4839  | 9997  | 0.5159547864359307
 1634 | 5035 | 4820  | 9855  | 0.510908168442415
 1635 | 5106 | 4928  | 10034 | 0.5088698425353797
 1636 | 4917 | 4605  | 9522  | 0.5163831127914303
 1637 | 4703 | 4457  | 9160  | 0.5134279475982533
 1638 | 5359 | 4952  | 10311 | 0.519736204053923
```

While this may seem tedious at first this will, in future, help us write better data pipelines since the compiler can help us not get things wrong. The trade off is a few key strokes but that's a small price for safety.

<div id="exercise">
**Exercise**: Now, generate a plot of the proportion of boys born over time. What 
do you see? 
</div>


Finally, in addition to simple mathematical operators like subtraction and 
division, you can ask R to make comparisons like greater than, `.>.`, less than,
`.<.`, and equality, `.==.` (use `.>`, `.<`, `.==` when either operand may be nullable). For example, we can ask if boys outnumber girls in each
year with the expression

```haskell
dataframe> withTotal |> D.derive "more_boys" (boys .>. girls)
----------------------------------------
 year | boys | girls | total | more_boys
------|------|-------|-------|----------
 Int  | Int  |  Int  |  Int  |   Bool
------|------|-------|-------|----------
 1629 | 5218 | 4683  | 9901  | True
 1630 | 4858 | 4457  | 9315  | True
 1631 | 4422 | 4102  | 8524  | True
 1632 | 4994 | 4590  | 9584  | True
 1633 | 5158 | 4839  | 9997  | True
 1634 | 5035 | 4820  | 9855  | True
 1635 | 5106 | 4928  | 10034 | True
 1636 | 4917 | 4605  | 9522  | True
 1637 | 4703 | 4457  | 9160  | True
 1638 | 5359 | 4952  | 10311 | True
```

This command add a new variable to the data frame containing the values
of either `True` if that year had more boys than girls, or `False` if that year 
did not (the answer may surprise you). This variable contains different kind of 
data than we have considered so far. All other columns in the data 
frame have values are numerical (the year, the number of boys and girls). Here, 
we've asked Haskell to create *logical* data, data where the values are either `True` 
or `False`. In general, data analysis will involve many different kinds of data 
types, and one reason for using Haskell is that it allows our data processing to be guided by the types of the data.


## Dataset 2: Present birth records

In the previous few pages, you recreated some of the displays and preliminary 
analysis of Arbuthnot's baptism data. Next you will do a similar analysis, 
but for present day birth records in the United States. Load up the 
present day data with the following command.

```haskell
dataframe> :script dataframe.ghci
========================================
              📦Dataframe
========================================

✨  Modules were automatically imported.

💡  Use prefix 'D' for core functionality.
        ● E.g. D.readCsv "/path/to/file"
💡  Use prefix 'F' for expression functions.
        ● E.g. F.sum (F.col @Int "value")

✅ Ready.
dataframe> df <- D.readCsv "./data/present.csv"
```

4. How many variables are included in this data set?
<ol>
<li> 2 </li>
<li> 3 </li>
<li> 4 </li>
<li> 74 </li>
<li> 2013 </li>
</ol>

<div id="exercise">
**Exercise**: What years are included in this dataset?
</div>

5. Calculate the total number of births for each year and store these values in a new 
variable called `total` in the `present` dataset. Then, calculate the proportion of 
boys born each year and store these values in a new variable called `prop_boys` in 
the same dataset. Plot these values over time and based on the plot determine if the 
following statement is true or false: The proportion of boys born in the US has 
decreased over time. 
<ol>
<li> True </li>
<li> False </li>
</ol>


6. Create a new variable called `more_boys` which contains the value of either `True` 
if that year had more boys than girls, or `False` if that year did not. Based on this 
variable which of the following statements is true? 
<ol>
<li> Every year there are more girls born than boys. </li>
<li> Every year there are more boys born than girls. </li>
<li> Half of the years there are more boys born, and the other half more girls born. </li>
</ol>

7. Calculate the boy-to-girl ratio each year, and store these values in a new variable called `prop_boy_girl` in the `present` dataset. Plot these values over time. Which of the following best describes the trend? 
<ol>
<li> There appears to be no trend in the boy-to-girl ratio from 1940 to 2013. </li>
<li> There is initially an increase in boy-to-girl ratio, which peaks around 1960. After 1960 there is a decrease in the boy-to-girl ratio, but the number begins to increase in the mid 1970s. </li>
<li> There is initially a decrease in the boy-to-girl ratio, and then an increase between 1960 and 1970, followed by a decrease. </li>
<li> The boy-to-girl ratio has increased over time. </li>
<li> There is an initial decrease in the boy-to-girl ratio born but this number appears to level around 1960 and remain constant since then. </li>
</ol>


8. In what year did we see the most total number of births in the U.S.? *Hint:* Sort 
your dataset in descending order based on the `total` column. You can do this with the new function: `D.sortBy [D.Desc <column name to sort by>]` (for 
descending order).
<ol>
<li> 1940 </li>
<li> 1957 </li>
<li> 1961 </li>
<li> 1991 </li>
<li> 2007 </li>
</ol>
