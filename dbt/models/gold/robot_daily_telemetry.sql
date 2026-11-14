{#- One row per robot and observed day. Days that are final stay frozen unless reopened. -#}
{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["observed_date", "robot_id"],
    on_schema_change="fail",
    properties={"partitioning": "ARRAY['month(observed_date)']"},
    post_hook="delete from {{ this }} where silver_build_id <> '{{ silver_build().build_id }}'
               and (not is_final or {{ reopened('observed_date') }})",
) }}

{%- set build = silver_build() %}

with events as (
    select *
    from {{ at_snapshot("silver", "telemetry_events", build.events_snapshot_id) }}
    where is_current
),

outcomes as (
    select *
    from {{ at_snapshot("silver", "telemetry_record_outcomes", build.outcomes_snapshot_id) }}
),

steps as (
    select
        *,
        date(observed_at) as observed_date,
        lag(sequence_number) over boot as prev_sequence,
        lag(observed_at) over boot as prev_observed_at,
        lag(x_m) over boot as prev_x,
        lag(y_m) over boot as prev_y
    from events
    window boot as (partition by robot_id, boot_number order by sequence_number)
),

-- A record whose payload could not be read is attributed to the robot of its Kafka key.
open_retries as (
    select coalesce(o.robot_id, nullif(from_utf8(b.record_key), '')) as robot_id
    from outcomes as o
    left join {{ at_snapshot("bronze", "kafka_records", build.bronze_snapshot_id) }} as b
        on o.robot_id is null
        and b.kafka_topic_id = o.kafka_topic_id
        and b.kafka_partition = o.kafka_partition
        and b.kafka_offset = o.kafka_offset
    where o.outcome in ('REJECTED', 'UNRESOLVED') and o.retry_eligible
),

finality as (
    select
        date(max(bronze_ingested_at) - interval '{{ var("correction_window_days") }}' day) as final_until,
        (select count(*) from open_retries where robot_id is null) > 0 as robotless_open
    from outcomes
),

daily as (
    select
        observed_date,
        robot_id,
        max(robot_model) as robot_model,
        count(*) as events,
        count(distinct boot_id) as boots,
        count(distinct format('%d/%d', boot_number, sequence_number)) as observed_positions,
        coalesce(sum(sequence_number - prev_sequence - 1), 0) as missing_sequences,
        count_if(arrived_out_of_order) as out_of_order_events,
        sum(duplicate_deliveries) as duplicate_deliveries,
        count_if(correction_of is not null) as corrected_events,
        min(battery_pct) as battery_pct_min,
        avg(battery_pct) as battery_pct_avg,
        max(battery_pct) as battery_pct_max,
        coalesce(sum(
            case when sequence_number = prev_sequence + 1 and observed_at >= prev_observed_at
                then sqrt((x_m - prev_x) * (x_m - prev_x) + (y_m - prev_y) * (y_m - prev_y))
            end
        ), 0e0) as distance_m,
        max(motor_temp_c) as motor_temp_c_max,
        count_if(lidar_ok = false) as lidar_fault_events,
        min(observed_at) as first_observed_at,
        max(observed_at) as last_observed_at
    from steps
    group by observed_date, robot_id
),

fresh as (
    select
        d.observed_date,
        d.robot_id,
        d.robot_model,
        d.events,
        d.boots,
        d.observed_positions,
        d.observed_positions + d.missing_sequences as expected_events,
        d.missing_sequences,
        d.out_of_order_events,
        d.duplicate_deliveries,
        d.corrected_events,
        d.battery_pct_min,
        d.battery_pct_avg,
        d.battery_pct_max,
        d.distance_m,
        d.motor_temp_c_max,
        d.lidar_fault_events,
        d.first_observed_at,
        d.last_observed_at,
        coalesce(
            d.observed_date <= f.final_until
            and not f.robotless_open
            and d.robot_id not in (select robot_id from open_retries where robot_id is not null),
            false
        ) as is_final,
        cast('{{ build.build_id }}' as varchar) as silver_build_id
    from daily as d
    cross join finality as f
)

select *
from fresh
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as t
    where t.is_final
        and not ({{ reopened("t.observed_date") }})
        and t.observed_date = fresh.observed_date
        and t.robot_id = fresh.robot_id
)
{% endif %}
