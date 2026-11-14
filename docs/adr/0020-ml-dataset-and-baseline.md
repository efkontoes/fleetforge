# ADR 0020: A reproducible dataset export, a synthetic label, and an honest baseline

Status: accepted. Gate C exports a Gate A release twice with identical checksums, trains
and registers twice with identical metrics, and checks drift between two releases.

## Context
Milestone 5 asks for a dataset someone could train on, a baseline task, experiment
tracking and drift checks. The fleet is simulated and has no real labels. A label that
pretends to be real would be worse than none.

## Decision
* **Export from the release, not from Silver.** `jobs/export.py` writes the release's
  tagged rows in `observation_id` order:
  * one Parquet file per split;
  * `manifest.json` (content hash, build, snapshot, seed, schema);
  * `SHA256SUMS`;
  * a data card that states that the data is synthetic.

  It refuses to finish unless the exported rows hash to the release's content hash.
* **A synthetic label, stated as synthetic.** The label asks: does the robot touch the
  warehouse boundary within the next 30 steps of its episode? The simulator clamps
  positions at the walls, so the event really happens in the data, but it tells us
  nothing about real robots.
  * Features use only the current and earlier steps.
  * The label uses only later steps.
  * Rows already at a wall, and rows without a full 30-step horizon, are dropped.
* **Two models, chosen without looking at test.**
  * A logistic regression on the distance to the nearest wall is the baseline.
  * A gradient-boosted model on all features picks its capacity by validation PR-AUC
    from two candidates.
  * Scaling is fitted inside the pipeline, so its statistics come from train only.
  * The decision threshold maximises F1 on validation.
  * Test reports precision, recall, F1, PR-AUC and ROC-AUC, on the episode split and on
    the stricter robot split.
* **MLflow, lightly.** `mlflow-skinny` with a file store (opted in explicitly), models
  saved with skops, telemetry disabled. One reviewed type is trusted, only for models
  the same run wrote. The model with the better validation PR-AUC is registered.
* **Drift with caveats.** PSI on reference quantiles, the Kolmogorov-Smirnov statistic
  and the Wasserstein distance per feature; each has a known blind spot, written in
  `ml/drift.py`.

## Results (Gate C, release gate-r2, 27,938 rows)
| Split | Model | PR-AUC | F1 | Positive rate |
|---|---|---:|---:|---:|
| episode | baseline, distance only | 0.629 | 0.554 | 4.1% |
| episode | gradient boosting (shallow, by validation) | 0.276 | 0.284 | 4.1% |
| robot | baseline, distance only | 0.844 | 0.756 | 13.7% |
| robot | gradient boosting (deep, by validation) | 0.731 | 0.650 | 13.7% |

The baseline wins, and that is the expected answer. The simulator moves each robot by an
independent random step, so velocity history carries no information about the next
steps, and distance to the wall is close to a sufficient statistic. The extra features
only give the boosted model room to overfit about 2,000 labelled test rows. Drift from
release gate-r1 to gate-r2 is real for position and battery (PSI 0.21–0.30), because
later batches observe robots later in time. Velocity features are stable (PSI < 0.01).

## Consequences
* The export, not Silver, is the training interface. Retraining on the same release is
  bit-reproducible in the same image.
* An LLM assistant (optional in the mandate) was not built. Nothing else depends on it.
