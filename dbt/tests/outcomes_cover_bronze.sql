-- N_bronze = accepted + duplicate + rejected + unresolved: every Bronze telemetry record has one outcome.
{% set build = silver_build() %}
with bronze as (
    select count(*) as n
    from {{ at_snapshot("bronze", "kafka_records", build.bronze_snapshot_id) }}
    where source_topic = '{{ var("telemetry_topic") }}'
),
outcomes as (
    select count(*) as n, count(distinct (kafka_topic_id, kafka_partition, kafka_offset)) as keys
    from {{ at_snapshot("silver", "telemetry_record_outcomes", build.outcomes_snapshot_id) }}
)
select b.n as bronze_records, o.n as outcomes, o.keys
from bronze as b cross join outcomes as o
where b.n <> o.n or o.n <> o.keys
