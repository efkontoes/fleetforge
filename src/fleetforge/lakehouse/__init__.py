"""Milestone 3 lakehouse: Kafka -> Bronze -> Silver -> Gold on Apache Iceberg.

Pure modules (no Spark, no network; unit-tested without a JVM):

    tables     exact table specifications, rendered to Spark DDL and documentation
    bronze     Kafka record -> Bronze row, offset checkpoints, ingestion planning
    reference  firmware-contract CDC and robot outbox events -> reference data
    silver     Bronze row -> canonical event or rejection, plus the set-level
               reference implementation (dedup, conflicts, corrections, ordering)
    fixtures   a deterministic, Kafka-compatible scenario for tests

Spark jobs live in ``fleetforge.lakehouse.jobs`` and import pyspark lazily, so
nothing here needs Java. The jobs must produce exactly what the pure modules
produce; ``tests/lakehouse`` checks that differentially once the Spark stack runs.
"""
