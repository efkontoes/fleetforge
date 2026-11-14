"""The synthetic label, leakage, drift statistics and the training run."""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from fleetforge.ml import dataset as ds


def episode(xs, episode_id="e1", robot="r1", split="train"):
    n = len(xs)
    return pd.DataFrame(
        {
            "episode_id": episode_id,
            "robot_id": robot,
            "split": split,
            "split_robot": split,
            "sequence_number": range(n),
            "x_m": xs,
            "y_m": [25.0] * n,
            "vx_mps": [np.nan] + list(np.diff(xs)),
            "vy_mps": [np.nan] + [0.0] * (n - 1),
            "battery_pct": 80.0,
        }
    )


def test_the_label_looks_only_at_the_next_horizon():
    xs = [5.0 - 0.1 * i for i in range(60)]  # reaches x = 0 at step 50 and stays
    xs = [max(0.0, x) for x in xs]
    out = ds.add_label(ds.add_features(episode(xs))).set_index("sequence_number")
    assert out.loc[19, ds.LABEL] == 0  # contact at 50 is 31 steps away
    assert out.loc[20, ds.LABEL] == 1  # 30 steps away
    assert 50 not in out.index  # already at the wall
    assert out.index.max() <= 60 - ds.HORIZON - 1  # no full horizon after that


def test_features_never_use_the_future():
    xs = [10.0 + 0.1 * i for i in range(80)]
    base = ds.add_features(episode(xs))
    changed = list(xs)
    changed[60:] = [49.9] * 20
    other = ds.add_features(episode(changed))
    cols = ds.FEATURES
    pd.testing.assert_frame_equal(base.loc[:59, cols], other.loc[:59, cols])


def test_psi_and_tests_see_a_shift():
    pytest.importorskip("scipy")
    from fleetforge.ml import drift

    rng = np.random.default_rng(0)
    a = pd.DataFrame({"f": rng.normal(0, 1, 5000)})
    same = drift.compare(a, pd.DataFrame({"f": rng.normal(0, 1, 5000)}), ["f"])["f"]
    moved = drift.compare(a, pd.DataFrame({"f": rng.normal(1, 1, 5000)}), ["f"])["f"]
    assert same["verdict"] == "ok" and same["psi"] < 0.05
    assert moved["verdict"] == "act" and moved["wasserstein"] == pytest.approx(1.0, abs=0.1)


def test_training_tracks_and_registers_a_model(tmp_path):
    pytest.importorskip("sklearn")
    pytest.importorskip("mlflow")
    from fleetforge.ml import train

    rng = np.random.default_rng(1)
    export = tmp_path / "r1"
    export.mkdir()
    frames = []
    for i in range(60):
        xs = np.clip(np.cumsum(rng.uniform(-0.3, 0.3, 200)) + rng.uniform(1, 49), 0, 50)
        split = ["train", "validation", "test"][i % 3]
        frames.append(episode(list(xs), f"e{i}", f"r{i % 7}", split))
    pd.concat(frames).to_parquet(export / "train.parquet")
    (export / "manifest.json").write_text(json.dumps({"release_id": "r1", "content_sha256": "x"}))
    results = train.train(export)
    summary = train.track(export, results, f"file:{tmp_path / 'mlruns'}", "split")
    assert str(summary["registered"]["version"]) == "1"
    gbdt = summary["runs"]["gbdt_all_features"]
    assert 0 < gbdt["positive_rate"] < 0.5 and gbdt["pr_auc"] > gbdt["positive_rate"]
