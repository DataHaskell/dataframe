"""
Example: Polars -> hyrax -> Polars round-trip via Arrow C Data Interface,
plus a sklearn-style DecisionTreeClassifier on titanic.

Run from repo root:
    python3 -m venv .venv
    source ./.venv/bin/activate
    pip install polars pyarrow
    cabal build dataframe-arrow
    python3 python/example.py
"""
import polars as pl
import hyrax as hx

raw = pl.read_csv("data/titanic.csv").drop_nulls(subset=["Age"])
print("Polars input shape:", raw.shape)

df = hx.from_arrow(raw.to_arrow())
agg = df.groupBy(["Sex"]).aggregate({
    "n":          hx.count(hx.col("Sex")),
    "median_age": hx.median(hx.col("Age")),
    "mean_fare":  hx.mean(hx.col("Fare")),
})
print("\nGroupBy result:")
print(pl.from_arrow(agg))

adults = (df
    .filter(hx.col("Age") >= 18.0)
    .derive("price_per_class", hx.col("Fare") / hx.col("Pclass"))
    .select(["Sex", "Pclass", "Age", "Fare", "price_per_class"]))
print("\nAdults (head 3):")
print(pl.from_arrow(adults.limit(3).collect()))

print(pl.from_arrow(hx.read_csv("data/titanic.csv").collect()))  # schema round-trip test

features = df.select(["Survived", "Pclass", "Sex", "Age", "Fare"])
clf = hx.DecisionTreeClassifier(max_depth=3, min_samples_split=20).fit(
    features, target="Survived", target_type="int"
)
preds = clf.predict_array(features).to_pylist()
truth = raw["Survived"].to_list()
acc = sum(1 for t, p in zip(truth, preds) if t == p) / len(truth)
print(f"\nDecision tree accuracy on train set: {acc:.3f}")
