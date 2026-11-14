#!/bin/sh
# Lakehouse backup and a real restore test into a separate, disposable stack.
#   backup: catalog dumps + every object of the bucket -> build/dr/
#   restore-check: fresh fleetforge-dr stack <- backup, then rows, snapshots, release
#   hashes and reconciliation must match what was recorded at backup time.
# The original stack is stopped (never deleted) while the restore runs on its ports.
set -eu
E=build/lakehouse-evidence
B=build/dr
TOPIC=$(python3 -c "import json; print(json.load(open('$E/gate-a-vars.json'))['telemetry_topic'])" 2>/dev/null || echo telemetry.raw.v1)
TEST="-e FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL=jdbc:postgresql://catalog-db:5432/iceberg_catalog_test \
      -e FLEETFORGE_LAKEHOUSE_WAREHOUSE=s3://lakehouse/test-warehouse -e FLEETFORGE_LAKEHOUSE_RECORD_RUNS=false \
      -e FLEETFORGE_LAKEHOUSE_TELEMETRY_TOPIC=$TOPIC"
compose() { docker compose --env-file .env.lakehouse -f infrastructure/lakehouse/compose.yaml "$@"; }
now() { date +%s; }

backup() {
  rm -rf "$B" && mkdir -p "$B/objects" && chmod -R 777 "$B"
  compose --profile jobs run --rm $TEST spark /workspace/scripts/dr_check.py record
  for db in iceberg_catalog iceberg_catalog_test; do
    compose exec -T catalog-db pg_dump -U iceberg -Fc "$db" > "$B/$db.dump"
  done
  compose --profile jobs run --rm $TEST -v "$PWD/$B:/backup" spark /workspace/scripts/dr_check.py copy-out
  cp "$E/dr-expected.json" "$B/"
  du -sh "$B"
}

restore_check() {
  start=$(now)
  compose stop
  DR="docker compose -p fleetforge-dr --env-file .env.lakehouse -f infrastructure/lakehouse/compose.yaml"
  $DR up -d --wait garage catalog-db
  $DR up garage-init
  for db in iceberg_catalog iceberg_catalog_test; do
    $DR exec -T catalog-db pg_restore -U iceberg -d "$db" --no-owner < "$B/$db.dump"
  done
  $DR --profile jobs run --rm $TEST -v "$PWD/$B:/backup" spark /workspace/scripts/dr_check.py copy-in
  restored=$(now)
  status=0
  $DR --profile jobs run --rm $TEST -v "$PWD/$B:/backup" spark /workspace/scripts/dr_check.py verify \
    > "$B/verify.log" 2>&1 || status=$?
  tail -1 "$B/verify.log" > "$E/dr-verify.json"
  echo "{\"restore_seconds\": $((restored - start)), \"verified_seconds\": $(( $(now) - start ))}" > "$E/dr-timing.json"
  cat "$E/dr-verify.json" "$E/dr-timing.json"
  $DR --profile jobs down -v   # disposable restore stack only
  compose start garage catalog-db
  return $status
}

case "${1:-}" in
  backup) backup ;;
  restore-check) restore_check ;;
  *) echo "usage: $0 backup|restore-check" >&2; exit 2 ;;
esac
