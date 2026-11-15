-- Rows of the current build count exactly the current Silver events of their robot and day.
{% set build = silver_build() %}
with silver as (
    select date(observed_at) as observed_date, robot_id, count(*) as events
    from {{ at_snapshot("silver", "telemetry_events", build.events_snapshot_id) }}
    where is_current
    group by 1, 2
),
gold as (
    select observed_date, robot_id, events, is_final, silver_build_id
    from {{ ref("robot_daily_telemetry") }}
)
select coalesce(s.observed_date, g.observed_date) as observed_date,
       coalesce(s.robot_id, g.robot_id) as robot_id, s.events as silver_events,
       g.events as gold_events
from silver as s
full outer join gold as g on s.observed_date = g.observed_date and s.robot_id = g.robot_id
where (g.robot_id is null)
    or (g.silver_build_id = '{{ build.build_id }}' and (s.robot_id is null or s.events <> g.events))
    or (g.silver_build_id <> '{{ build.build_id }}' and not g.is_final)
