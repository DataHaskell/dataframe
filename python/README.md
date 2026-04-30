# hyrax

Python bindings for the Haskell [`dataframe`](https://github.com/DataHaskell/dataframe)
library. Tabular data ops, statistics, and decision-tree training, ferried
across the Arrow C Data Interface.

## Quickstart

```python
import hyrax as hx

# Read a CSV — columns with nulls come back as nullable Arrow types.
df = hx.read_csv("data/titanic.csv")

# Filter / derive / aggregate. Column types are inferred from the source
# schema, so `hx.col("Age") >= 18.0` Just Works.
adults = (df
    .filter(hx.col("Age") > 0.0)
    .derive("price_per_class", hx.col("Fare") / hx.col("Pclass"))
    .groupBy(["Sex"])
    .aggregate({
        "n":          hx.count(hx.col("Sex")),
        "median_age": hx.median(hx.col("Age")),
        "mean_fare":  hx.mean(hx.col("Fare")),
    }))

# pyarrow.RecordBatch
print(adults.to_pandas())
```

## Decision tree (sklearn-style)

```python
features = df.filter(hx.col("Age") > 0.0).select(
    ["Survived", "Pclass", "Sex", "Age", "Fare"]
)

clf = hx.DecisionTreeClassifier(max_depth=3, min_samples_split=20).fit(
    features, target="Survived", target_type="int"
)
preds = clf.predict_array(features).to_pylist()
```

## Expressions

```python
hx.col("age")                  # type inferred from source schema
hx.col("price", type=float)    # explicit; Python type or string both work
hx.lit(0.5)                    # Python literal
hx.col("a") > hx.col("b") + 1  # comparisons return Bool
(hx.col("x") > 0) & (hx.col("y") < 0)  # & | ~ for boolean composition
hx.when(cond).then(a).otherwise(b)
expr.cast(float)               # toDouble cast
```
