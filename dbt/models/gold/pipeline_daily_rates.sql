{#- Daily rates over classified records; UNRESOLVED is counted but not in the denominator. -#}
{{ config(
    materialized="table",
    on_table_exists="replace",
    properties={"partitioning": "ARRAY['month(ingest_date)']"},
) }}

{%- set build = silver_build() %}

with bronze as (
    select date(ingested_at) as ingest_date, source_topic, count(*) as bronze_records
    from {{ at_snapshot("bronze", "kafka_records", build.bronze_snapshot_id) }}
    where source_topic = '{{ var("telemetry_topic") }}'
    group by 1, 2
),

outcomes as (
    select
        date(bronze_ingested_at) as ingest_date,
        count_if(outcome = 'ACCEPTED') as accepted,
        count_if(outcome = 'DUPLICATE') as duplicates,
        count_if(outcome = 'REJECTED') as rejected,
        count_if(outcome = 'UNRESOLVED') as unresolved
    from {{ at_snapshot("silver", "telemetry_record_outcomes", build.outcomes_snapshot_id) }}
    group by 1
),

counted as (
    select
        b.ingest_date,
        b.source_topic,
        b.bronze_records,
        coalesce(o.accepted, 0) as accepted,
        coalesce(o.duplicates, 0) as duplicates,
        coalesce(o.rejected, 0) as rejected,
        coalesce(o.unresolved, 0) as unresolved
    from bronze as b
    left join outcomes as o on b.ingest_date = o.ingest_date
)

select
    ingest_date,
    source_topic,
    bronze_records,
    accepted + duplicates + rejected as classified,
    accepted,
    duplicates,
    rejected,
    unresolved,
    cast(rejected as double) / nullif(accepted + duplicates + rejected, 0) as invalid_rate,
    cast(duplicates as double) / nullif(accepted + duplicates + rejected, 0) as duplicate_rate,
    cast('{{ build.build_id }}' as varchar) as silver_build_id
from counted
