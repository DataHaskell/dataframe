#!/usr/bin/env python3
"""Generate clean Kaggle-style datasets and scikit-learn reference values.

Writes numeric CSVs under data/ml/ plus data/ml/golden.json with the reference
outputs the Haskell parity tests assert against. Closed-form models (OLS, ridge,
PCA) are checked for tight coefficient parity; iterative models are checked by an
accuracy/inertia floor. Re-run after a sklearn upgrade; commit the outputs raw.
"""

import json
import os

import numpy as np
from sklearn.datasets import load_diabetes, load_iris, make_blobs
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge
from sklearn.metrics import accuracy_score
from sklearn.svm import LinearSVC

OUT = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
os.makedirs(OUT, exist_ok=True)


def write_csv(name, header, rows):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(repr(float(x)) for x in r) + "\n")


golden = {}

# ---- Regression: diabetes (clean numeric) ----
dia = load_diabetes()
Xr, yr = dia.data, dia.target
rfeat = [f"f{i}" for i in range(Xr.shape[1])]
write_csv(
    "regression.csv",
    rfeat + ["target"],
    np.column_stack([Xr, yr]),
)
ols = LinearRegression().fit(Xr, yr)
golden["ols"] = {"coef": ols.coef_.tolist(), "intercept": float(ols.intercept_)}
ridge = Ridge(alpha=1.0, solver="cholesky").fit(Xr, yr)
golden["ridge"] = {
    "alpha": 1.0,
    "coef": ridge.coef_.tolist(),
    "intercept": float(ridge.intercept_),
}

# ---- Iris: PCA + multiclass logistic ----
iris = load_iris()
Xi, yi = iris.data, iris.target
ifeat = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
write_csv("iris.csv", ifeat + ["species"], np.column_stack([Xi, yi]))

pca = PCA(n_components=2).fit(Xi)
golden["pca"] = {
    "evr": pca.explained_variance_ratio_.tolist(),
    "components_abs": np.abs(pca.components_).tolist(),
}

logm = LogisticRegression(max_iter=1000, C=1.0).fit(Xi, yi)
golden["logistic_iris"] = {"accuracy": float(accuracy_score(yi, logm.predict(Xi)))}

# ---- Iris binary (setosa vs rest): logistic + linear SVC + GBM ----
yb = (yi != 0).astype(int)
write_csv("iris_binary.csv", ifeat + ["label"], np.column_stack([Xi, yb]))
logb = LogisticRegression(max_iter=1000).fit(Xi, yb)
golden["logistic_binary"] = {"accuracy": float(accuracy_score(yb, logb.predict(Xi)))}
svc = LinearSVC(C=1.0, max_iter=5000).fit(Xi, yb)
golden["linear_svc"] = {"accuracy": float(accuracy_score(yb, svc.predict(Xi)))}
gbm = GradientBoostingClassifier(
    n_estimators=100, max_depth=3, learning_rate=0.1, random_state=0
).fit(Xi, yb)
golden["gbm"] = {"accuracy": float(accuracy_score(yb, gbm.predict(Xi)))}

# ---- Blobs: k-means inertia ----
Xb, yb2 = make_blobs(
    n_samples=150, centers=3, n_features=2, cluster_std=0.8, random_state=7
)
write_csv("blobs.csv", ["x", "y", "cluster"], np.column_stack([Xb, yb2]))
km = KMeans(n_clusters=3, n_init=10, random_state=0).fit(Xb)
golden["kmeans"] = {"inertia": float(km.inertia_)}

with open(os.path.join(OUT, "golden.json"), "w") as f:
    json.dump(golden, f, indent=2, sort_keys=True)

print("wrote", OUT)
print(json.dumps({k: list(v.keys()) for k, v in golden.items()}, indent=2))
