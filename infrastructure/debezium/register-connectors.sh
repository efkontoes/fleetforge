#!/bin/bash
# Idempotently create or update the Debezium connectors (PUT /connectors/<name>/config).
# Re-running after a config change updates the connector in place; the connector
# resumes from its stored offset (in _connect_offsets), not from the beginning.
set -euo pipefail
URL="${CONNECT_URL:-http://connect:8083}"
cd "$(dirname "$0")"
for name in outbox reference; do
  code=$(curl -s -o /tmp/resp -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
         --data @"${name}-connector.json" "$URL/connectors/fleetforge-${name}/config")
  echo "fleetforge-${name}: HTTP $code"
  if [ "$code" -ge 300 ]; then cat /tmp/resp; exit 1; fi
done
sleep 3
for name in outbox reference; do
  curl -s "$URL/connectors/fleetforge-${name}/status"; echo
done
