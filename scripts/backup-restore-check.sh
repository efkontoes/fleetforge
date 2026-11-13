#!/bin/sh
# Logical backup of the operational database and a restore drill.
#   1. pg_dump -Fc of `fleetforge` into ./backups/ (git-ignored)
#   2. restore into a scratch database `fleetforge_restore_check`
#   3. compare row counts of every fleet/downstream table, then drop the scratch DB
# This proves the dump is restorable. It is NOT point-in-time recovery: anything
# committed after the dump is lost, and Kafka offsets/ledger rows restored from an
# older dump make consumers re-read (safe, because processing is idempotent).
set -eu
cd "$(dirname "$0")/.."
mkdir -p backups
file="backups/fleetforge-$(date -u +%Y%m%dT%H%M%SZ).dump"
psql_() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-fleetforge}" -At "$@"; }

docker compose exec -T postgres pg_dump -U "${POSTGRES_USER:-fleetforge}" -Fc fleetforge > "$file"
echo "dump: $file ($(du -h "$file" | cut -f1))"

psql_ -d postgres -c "DROP DATABASE IF EXISTS fleetforge_restore_check"
psql_ -d postgres -c "CREATE DATABASE fleetforge_restore_check"
# --no-owner/--no-acl: roles are cluster-wide and already exist; publications are
# restored but inert (no slot points at the scratch database).
docker compose exec -T postgres pg_restore -U "${POSTGRES_USER:-fleetforge}" --no-owner --no-acl \
  -d fleetforge_restore_check < "$file"

q="SELECT string_agg(format('%s.%s=%s', schemaname, relname, n), ' ' ORDER BY schemaname, relname)
   FROM (SELECT schemaname, relname,
                (xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I.%I',
                  schemaname, relname), false, true, '')))[1]::text::bigint AS n
         FROM pg_stat_user_tables WHERE schemaname IN ('fleet', 'downstream')) t"
src=$(psql_ -d fleetforge -c "$q")
dst=$(psql_ -d fleetforge_restore_check -c "$q")
psql_ -d postgres -c "DROP DATABASE fleetforge_restore_check"
if [ "$src" = "$dst" ]; then
  echo "restore check OK: $(echo "$src" | wc -w) tables, identical row counts"
else
  echo "restore check FAILED"; echo "source:   $src"; echo "restored: $dst"; exit 1
fi
