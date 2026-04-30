"""Smoke tests for DecisionTreeClassifier (sklearn-style fit / predict)."""

import json

import hyrax as hx


def _features(titanic):
    return titanic.select(["Survived", "Pclass", "Sex", "Age", "Fare"])


def test_fit_predict_basic(titanic):
    """A small tree on a few features should reproduce the training labels
    above chance (we look for >70% accuracy on train; titanic + tiny tree
    can usually get ~80%)."""
    df = _features(titanic)
    clf = hx.DecisionTreeClassifier(max_depth=3, min_samples_split=20).fit(
        df, target="Survived", target_type="int"
    )
    assert clf.tree_json
    tree_obj = json.loads(clf.tree_json)
    assert tree_obj["out_type"] == "int"

    rb_truth = df.select(["Survived"]).collect()
    truth = rb_truth.column(0).to_pylist()
    pred = clf.predict_array(df).to_pylist()
    assert len(truth) == len(pred)
    correct = sum(1 for t, p in zip(truth, pred) if t == p)
    accuracy = correct / len(truth)
    assert accuracy > 0.70, f"Accuracy too low: {accuracy:.3f}"


def test_predict_works_after_fit_on_same_frame(titanic):
    """Critical: after .fit() consumes a FromArrow-backed frame's buffers,
    .predict() on the same EagerFrame must still work — relies on the
    re-export-on-execute path in _execute."""
    df = _features(titanic)
    clf = hx.DecisionTreeClassifier(max_depth=2).fit(df, target="Survived", target_type="int")
    out = clf.predict_array(df)
    assert out is not None and len(out) > 0


def test_auto_target_type_inference(titanic):
    """Passing target_type='auto' should infer from the schema."""
    df = _features(titanic)
    clf = hx.DecisionTreeClassifier(max_depth=2, min_samples_split=50).fit(
        df, target="Survived", target_type="auto"
    )
    assert clf._target_type in ("int", "int64")


def test_tree_predicts_consistently_across_runs(titanic):
    """Same hyperparameters + data → same tree (DT here is deterministic)."""
    df = _features(titanic)
    clf1 = hx.DecisionTreeClassifier(max_depth=2, min_samples_split=50).fit(
        df, target="Survived", target_type="int"
    )
    clf2 = hx.DecisionTreeClassifier(max_depth=2, min_samples_split=50).fit(
        df, target="Survived", target_type="int"
    )
    assert json.loads(clf1.tree_json) == json.loads(clf2.tree_json)


def test_predict_returns_eager_frame(titanic):
    """predict() returns an EagerFrame that can be further composed."""
    df = _features(titanic)
    clf = hx.DecisionTreeClassifier(max_depth=2).fit(df, target="Survived", target_type="int")
    augmented = clf.predict(df)
    assert isinstance(augmented, hx.EagerFrame)
    rb = augmented.select(["Survived", "prediction"]).collect()
    assert "prediction" in rb.schema.names
    assert "Survived" in rb.schema.names


def test_chained_filter_then_predict(titanic):
    """Predict on a filtered sub-frame; the model fitted on the full data
    should also classify the subset reasonably."""
    df = _features(titanic)
    clf = hx.DecisionTreeClassifier(max_depth=3).fit(df, target="Survived", target_type="int")
    female_only = df.filter(hx.col("Sex", type="text") == "female")
    pred = clf.predict_array(female_only).to_pylist()
    assert len(pred) > 0
    # On titanic, women survived at much higher rates — the prediction
    # should be majority-1.
    surv_rate = sum(pred) / len(pred)
    assert surv_rate > 0.5, f"Expected female survival prediction > 0.5, got {surv_rate:.3f}"
