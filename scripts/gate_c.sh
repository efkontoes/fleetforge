#!/bin/sh
# Gate C: a Gate A release -> Parquet export (twice, byte-identical) -> training and
# registration in MLflow (twice, identical metrics) -> drift between two releases.
set -eu
E=build/lakehouse-evidence
RUN="docker compose --env-file .env.lakehouse -f infrastructure/lakehouse/compose.yaml \
  --profile jobs run --rm \
  -e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
  -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse spark"
ML="docker run --rm -v $PWD/$E:/data fleetforge-ml:dev"
rm -rf "$E/exports" "$E/exports-again" "$E/mlruns" && mkdir -p "$E/exports" "$E/exports-again" "$E/mlruns"
chmod 777 "$E/exports" "$E/exports-again" "$E/mlruns"

$RUN -m fleetforge.lakehouse.jobs.export --release-id gate-r2 --to /evidence/exports
$RUN -m fleetforge.lakehouse.jobs.export --release-id gate-r2 --to /evidence/exports-again
$RUN -m fleetforge.lakehouse.jobs.export --release-id gate-r1 --to /evidence/exports
cmp "$E/exports/gate-r2/SHA256SUMS" "$E/exports-again/gate-r2/SHA256SUMS"
echo "export reproducible: $(sha256sum "$E/exports/gate-r2/SHA256SUMS" | cut -c1-16)"

$ML fleetforge.ml.train --export /data/exports/gate-r2 --tracking-uri file:/data/mlruns > "$E/gate-c-train-1.json"
$ML fleetforge.ml.train --export /data/exports/gate-r2 --tracking-uri file:/data/mlruns > "$E/gate-c-train-2.json"
$ML fleetforge.ml.train --export /data/exports/gate-r2 --tracking-uri file:/data/mlruns \
  --split-column split_robot > "$E/gate-c-train-robot-split.json"
$ML fleetforge.ml.drift --reference /data/exports/gate-r1 --current /data/exports/gate-r2 > "$E/gate-c-drift.json"
python3 - "$E" <<'PY'
import json, sys
e = sys.argv[1]
one, two = (json.load(open(f"{e}/gate-c-train-{n}.json")) for n in (1, 2))
strip = lambda s: {m: {k: v for k, v in r.items() if k not in ("run_id", "model_uri")} for m, r in s["runs"].items()}
assert strip(one) == strip(two), "training is not reproducible"
assert str(two["registered"]["version"]) == "2", two["registered"]
print("training reproducible; registered", two["registered"])
PY
