import os
import sys

import pytest

# Make the in-tree `python/` package importable when running from the repo root.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, os.path.join(_REPO_ROOT, "python"))


@pytest.fixture(scope="session")
def _titanic_clean():
    """Read titanic, drop the rows with missing Age, project to columns
    that are then fully non-null, and materialize. We round-trip through
    Arrow once so downstream consumers see non-nullable columns — this
    matters for stat aggs and the decision-tree synthesizer, which don't
    yet support the ``Maybe`` wrapper on every code path."""
    import hyrax as hx

    return (
        hx.read_csv("data/titanic.csv")
        .filter(hx.col("Age") > 0.0)
        .select([
            "PassengerId", "Survived", "Pclass", "Sex", "Age",
            "SibSp", "Parch", "Fare",
        ])
        .collect()
    )


@pytest.fixture
def titanic(_titanic_clean):
    """Fresh hyrax EagerFrame backed by the cleaned titanic record batch."""
    import hyrax as hx

    return hx.from_arrow(_titanic_clean)
