"""Train a baseline and a stronger model on an exported release and track both in MLflow.

python -m fleetforge.ml.train --export /exports/r1 --tracking-uri file:/mlruns
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_fscore_support, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from fleetforge.ml import dataset as ds

log = logging.getLogger("fleetforge.ml.train")
SEED = 7
MODEL_NAME = "fleetforge-wall-contact"
# Trusted only because this run wrote the model itself; never for downloaded files.
TRUSTED_TYPES = ["sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"]


def models() -> dict[str, Any]:
    # Scaling sits inside each pipeline, so its statistics come from the training rows only.
    return {
        "baseline_logreg_distance": (
            ["dist_wall"],
            make_pipeline(
                StandardScaler(), LogisticRegression(class_weight="balanced", max_iter=1000)
            ),
        ),
        "gbdt_all_features": (ds.FEATURES, None),  # capacity chosen on validation
    }


def gbdt(depth: int | None, leaf: int) -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(max_depth=depth, min_samples_leaf=leaf,
                                          l2_regularization=1.0, max_iter=200, learning_rate=0.1,
                                          class_weight="balanced", random_state=SEED)  # fmt: skip


GBDT_CANDIDATES = {"deep": (None, 20), "shallow": (3, 50)}


def best_threshold(y: np.ndarray, p: np.ndarray) -> float:
    """Threshold that maximises F1 on the validation split."""
    best, best_f1 = 0.5, -1.0
    for t in np.unique(np.quantile(p, np.linspace(0.5, 0.999, 200))):
        _, _, f1, _ = precision_recall_fscore_support(y, p >= t, average="binary", zero_division=0)
        if f1 > best_f1:
            best, best_f1 = float(t), f1
    return best


def evaluate(y: np.ndarray, p: np.ndarray, threshold: float) -> dict[str, float]:
    prec, rec, f1, _ = precision_recall_fscore_support(
        y, p >= threshold, average="binary", zero_division=0
    )
    return {
        "precision": prec,
        "recall": rec,
        "f1": f1,
        "pr_auc": average_precision_score(y, p),
        "roc_auc": roc_auc_score(y, p),
        "positive_rate": float(y.mean()),
        "rows": len(y),
    }


def train(export_dir: Path, split_column: str = "split") -> dict[str, Any]:
    splits = ds.load(export_dir, split_column)
    train_df, val_df, test_df = splits["train"], splits["validation"], splits["test"]
    results = {}
    for name, (features, model) in models().items():
        chosen = None
        if model is None:  # pick the capacity by validation PR-AUC, never by test
            fitted = {}
            for label, (depth, leaf) in GBDT_CANDIDATES.items():
                m = gbdt(depth, leaf).fit(train_df[features], train_df[ds.LABEL])
                p = m.predict_proba(val_df[features])[:, 1]
                fitted[label] = (average_precision_score(val_df[ds.LABEL], p), m)
            chosen = max(fitted, key=lambda k: fitted[k][0])
            model = fitted[chosen][1]
        else:
            model.fit(train_df[features], train_df[ds.LABEL])
        threshold = best_threshold(
            val_df[ds.LABEL].to_numpy(), model.predict_proba(val_df[features])[:, 1]
        )
        scores = {}
        for split, frame in (("validation", val_df), ("test", test_df)):
            p = model.predict_proba(frame[features])[:, 1]
            scores[split] = evaluate(frame[ds.LABEL].to_numpy(), p, threshold)
        results[name] = {
            "model": model,
            "features": features,
            "threshold": threshold,
            "scores": scores,
            "capacity": chosen,
        }
    return results


def fingerprint(export_dir: Path) -> str:
    """Hash of the export manifest: the exact data a run trained on."""
    return hashlib.sha256((export_dir / "manifest.json").read_bytes()).hexdigest()


def track(
    export_dir: Path, results: dict[str, Any], tracking_uri: str, split_column: str
) -> dict[str, Any]:
    os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")  # no usage data leaves the machine
    os.environ.setdefault("DO_NOT_TRACK", "true")
    import mlflow
    import mlflow.sklearn

    manifest = json.loads((export_dir / "manifest.json").read_text())
    if tracking_uri.startswith("file:"):
        # MLflow 3.17 keeps the file store only on request; skinny has no SQL store.
        os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(MODEL_NAME)
    summary: dict[str, Any] = {
        "release_id": manifest["release_id"],
        "content_sha256": manifest["content_sha256"],
        "runs": {},
    }
    for name, r in results.items():
        with mlflow.start_run(run_name=f"{name}-{split_column}") as run:
            mlflow.set_tags(
                {
                    "release_id": manifest["release_id"],
                    "synthetic_label": "true",
                    "split_column": split_column,
                }
            )
            mlflow.log_params(
                {
                    "model": name,
                    "features": ",".join(r["features"]),
                    "horizon_steps": ds.HORIZON,
                    "threshold": r["threshold"],
                    "capacity": r["capacity"] or "fixed",
                    "data_fingerprint": fingerprint(export_dir),
                    "content_sha256": manifest["content_sha256"],
                }
            )
            for split, scores in r["scores"].items():
                mlflow.log_metrics({f"{split}_{k}": float(v) for k, v in scores.items()})
            logged = mlflow.sklearn.log_model(r["model"], name="model",
                                              skops_trusted_types=TRUSTED_TYPES)  # fmt: skip
            mlflow.log_artifact(str(export_dir / "manifest.json"), artifact_path="data")
            summary["runs"][name] = {
                "run_id": run.info.run_id,
                "model_uri": logged.model_uri,
                **r["scores"]["test"],
                "threshold": r["threshold"],
                "capacity": r["capacity"],
            }
    best = max(results, key=lambda n: results[n]["scores"]["validation"]["pr_auc"])  # never test
    version = mlflow.register_model(summary["runs"][best]["model_uri"], MODEL_NAME)
    summary["registered"] = {"name": MODEL_NAME, "version": version.version, "model": best}
    return summary


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser(prog="fleet-ml-train")
    ap.add_argument("--export", type=Path, required=True)
    ap.add_argument("--tracking-uri", default="file:/mlruns")
    ap.add_argument("--split-column", default="split", choices=["split", "split_robot"])
    args = ap.parse_args(argv)
    results = train(args.export, args.split_column)
    print(
        json.dumps(track(args.export, results, args.tracking_uri, args.split_column), default=str)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
