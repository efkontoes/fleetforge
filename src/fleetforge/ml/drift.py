"""Feature drift between a reference dataset (training) and a newer one.

Three views, each with a caveat:
* PSI on reference-quantile bins: easy to threshold (0.1 watch, 0.25 act), but it
  depends on the binning and ignores order within a bin.
* Kolmogorov-Smirnov: a test, so with tens of thousands of rows tiny shifts become
  "significant"; read the statistic, not only the p-value.
* Wasserstein distance: in the feature's own units, so it is comparable over time for
  one feature but not across features.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from scipy import stats

PSI_WATCH, PSI_ACT = 0.1, 0.25


def psi(reference: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
    edges = np.unique(np.quantile(reference, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0  # constant feature: no bins to compare
    edges[0], edges[-1] = -np.inf, np.inf
    ref = np.histogram(reference, edges)[0] / len(reference)
    cur = np.histogram(current, edges)[0] / len(current)
    ref, cur = np.clip(ref, 1e-6, None), np.clip(cur, 1e-6, None)
    return float(np.sum((cur - ref) * np.log(cur / ref)))


def compare(reference, current, features: list[str]) -> dict[str, dict[str, float | str]]:
    out = {}
    for f in features:
        a, b = reference[f].dropna().to_numpy(), current[f].dropna().to_numpy()
        ks = stats.ks_2samp(a, b)
        value = psi(a, b)
        out[f] = {
            "psi": value,
            "ks_statistic": float(ks.statistic),
            "ks_pvalue": float(ks.pvalue),
            "wasserstein": float(stats.wasserstein_distance(a, b)),
            "verdict": "act" if value >= PSI_ACT else "watch" if value >= PSI_WATCH else "ok",
        }
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse

    from fleetforge.ml import dataset as ds

    ap = argparse.ArgumentParser(prog="fleet-ml-drift")
    ap.add_argument("--reference", type=Path, required=True, help="export dir of the training data")
    ap.add_argument("--current", type=Path, required=True, help="export dir of newer data")
    args = ap.parse_args(argv)
    ref = ds.load(args.reference)["train"]
    cur = ds.load(args.current)
    current = cur["test"] if "test" in cur else next(iter(cur.values()))
    print(json.dumps(compare(ref, current, ds.FEATURES)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
