{#- Silver outcomes per ingestion day, topic, outcome and reason, for one Silver build. -#}
{{ config(
    materialized="table",
    on_table_exists="replace",
    properties={"partitioning": "ARRAY['month(ingest_date)']"},
) }}

{%- set build = silver_build() %}

with outcomes as (
    select *
    from {{ at_snapshot("silver", "telemetry_record_outcomes", build.outcomes_snapshot_id) }}
),

topics as (
    select distinct kafka_topic_id, source_topic
    from {{ at_snapshot("bronze", "kafka_records", build.bronze_snapshot_id) }}
)

select
    date(o.bronze_ingested_at) as ingest_date,
    t.source_topic,
    o.outcome,
    coalesce(o.reason_code, 'NONE') as reason_code,
    o.retry_eligible,
    count(*) as records,
    count(distinct o.event_id) as distinct_events,
    count(distinct o.robot_id) as distinct_robots,
    cast('{{ build.build_id }}' as varchar) as silver_build_id
from outcomes as o
inner join topics as t on o.kafka_topic_id = t.kafka_topic_id
group by 1, 2, 3, 4, 5
