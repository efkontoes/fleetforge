#!/bin/bash
# Idempotent topic creation. Topics are declared, never auto-created
# (auto.create.topics.enable=false on the broker), so a typo cannot silently
# create a new topic with default settings.
#
# Replication factor 1 / min.insync.replicas 1: correct for ONE local broker only.
# Production: RF=3, min.insync.replicas=2, acks=all  -> survives one broker loss.
set -euo pipefail
B="${BOOTSTRAP:-kafka:9092}"
T=/opt/kafka/bin/kafka-topics.sh

create() {  # name partitions extra-configs...
  local name=$1 parts=$2; shift 2
  local args=()
  for c in "$@"; do args+=(--config "$c"); done
  $T --bootstrap-server "$B" --create --if-not-exists --topic "$name" \
     --partitions "$parts" --replication-factor 1 \
     --config min.insync.replicas=1 --config max.message.bytes=1048576 "${args[@]}"
}

# Telemetry: keyed by robot_id. 6 partitions = up to 6 projector instances.
# 7 days retention: the raw log is the replay source until the M3 bronze layer exists.
create telemetry.raw.v1 6 retention.ms=604800000 cleanup.policy=delete compression.type=producer
# Business events from the outbox (Debezium). Keyed by aggregate id.
create fleet.mission-events.v1 3 retention.ms=2592000000 cleanup.policy=delete
create fleet.robot-events.v1 3 retention.ms=2592000000 cleanup.policy=delete
# Table-level CDC of reference data: compacted, latest row image per key survives.
create fleetforge.fleet.firmware_versions 1 cleanup.policy=compact

$T --bootstrap-server "$B" --list
